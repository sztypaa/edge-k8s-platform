// Package agent defines agent that runs in each Kubernetes cluster
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"k8s-mqtt-agent/internal/k8s"
	"k8s-mqtt-agent/internal/model"
	"k8s-mqtt-agent/pkg/protocol"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"k8s.io/client-go/kubernetes"
)

// Agent is responsible for monitoring resource usage on cluster, executing function pods 
// and communication with MQTT broker
type Agent struct {
	clusterID      string						// ID of Kubernetes cluster
	namespace      string						// Kubernetes namespace
	metricsTimeout time.Duration				// specifies timeout for fetching resource metrics
	clientset      *kubernetes.Clientset		// set of clients for Kubernetes group
	reservations   *model.ReservationTracker	// used for keeping track of resource reservation by tasks accepted by Agent
	execMu         sync.Mutex					// execution mutex
}

// New creates agent based on [Config] and [kubernetes.Clientset]. 
// If cfg.MetricsTimeout is below 0 then [DefaultMetricsTimeout] is used.
// If cfg.K8sNamespace is empty string then [DefaultK8sNamespace] is used.
func New(cfg Config, clientset *kubernetes.Clientset) *Agent {
	timeout := cfg.MetricsTimeout
	if timeout <= 0 {
		timeout = DefaultMetricsTimeout
	}
	ns := cfg.K8sNamespace
	if ns == "" {
		ns = DefaultK8sNamespace
	}
	return &Agent{
		clusterID:      cfg.ClusterID,
		namespace:      ns,
		metricsTimeout: timeout,
		clientset:      clientset,
		reservations:   model.NewReservationTracker(),
	}
}

// Run starts [Agent] and connects it to MQTT broker.
func (a *Agent) Run(broker string) {
	mqttClient := mqtt.NewClient(a.newMQTTClientOptions(broker))
	connectWithRetry(mqttClient)
	select {}
}

// execTopic returns MQTT topic used to dispatch task to cluster specified by [Agent.clusterID]
func (a *Agent) execTopic() string {
	return protocol.TopicExecute(a.clusterID)
}

// handleCFP describes callback for [protocol.TopicCFP]. 
// If cluster has sufficient resources [model.BidPayload] gets published to [protocol.TopicBids]
func (a *Agent) handleCFP(client mqtt.Client, msg mqtt.Message) {
	var cfp model.TaskCFP
	if err := json.Unmarshal(msg.Payload(), &cfp); err != nil {
		log.Printf("[ERROR] Invalid CFP payload: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), a.metricsTimeout)
	defer cancel()

	res, err := k8s.GetClusterResources(ctx, a.clientset, a.reservations, cfp.Image)
	if err != nil {
		log.Printf("[ERROR] Failed to read cluster metrics: %v", err)
		return
	}

	if !res.CanFit(cfp) {
		log.Printf("[BID SKIPPED] Task %s: insufficient resources (Avail CPU: %dm, Mem: %dMiB)",
			cfp.TaskID, res.AvailCPUMilli, res.AvailMemMiB)
		return
	}

	bid := model.BidPayload{
		TaskID:        cfp.TaskID,
		ClusterID:     a.clusterID,
		AvailCPUMilli: res.AvailCPUMilli,
		AvailMemMiB:   res.AvailMemMiB,
		ImageCached:   res.ImageCached,
		Score:         res.CalculateScore(),
	}

	publishJSON(client, protocol.TopicBids, bid)
	log.Printf("[BID SENT] Task: %s | AvailCPU: %dm | AvailMem: %dMiB | Cached: %v | Score: %d",
		cfp.TaskID, res.AvailCPUMilli, res.AvailMemMiB, res.ImageCached, bid.Score)
}

// handleExecute defines callback for topic specified in [Agent.execTopic]
// If [Agent.tryAcceptAndReserve] finishes with success [model.AckPayload] is published to [protocol.TopicAck]
// and goroutine for [Agent.runTask] is started
func (a *Agent) handleExecute(client mqtt.Client, msg mqtt.Message) {
	var task model.TaskCFP
	if err := json.Unmarshal(msg.Payload(), &task); err != nil {
		log.Printf("[ERROR] Invalid execute payload: %v", err)
		return
	}

	accepted, reason := a.tryAcceptAndReserve(task)
	if !accepted {
		publishJSON(client, protocol.TopicAck, model.AckPayload{
			TaskID:    task.TaskID,
			ClusterID: a.clusterID,
			Status:    protocol.AckStatusResigned,
			Reason:    reason,
		})
		log.Printf("[TASK RESIGNED] Task %s rejected by %s: %s", task.TaskID, a.clusterID, reason)
		return
	}

	publishJSON(client, protocol.TopicAck, model.AckPayload{
		TaskID:    task.TaskID,
		ClusterID: a.clusterID,
		Status:    protocol.AckStatusAccepted,
	})
	log.Printf("[EXECUTING] Task %s accepted (ACK) by %s (reserved %dm CPU, %dMiB Mem). Spawning pod...",
		task.TaskID, a.clusterID, task.MinCPUMilli, task.MinMemMiB)

	go a.runTask(client, task)
}

// tryAcceptAndReserve attempts to reserve task based on current resources in cluster.
// Returns success flag and reason for failure
func (a *Agent) tryAcceptAndReserve(task model.TaskCFP) (bool, string) {
	a.execMu.Lock()
	defer a.execMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), a.metricsTimeout)
	defer cancel()

	res, err := k8s.GetClusterResources(ctx, a.clientset, a.reservations, task.Image)
	if err != nil {
		log.Printf("[ERROR] Failed to verify cluster resources before executing %s: %v", task.TaskID, err)
		return false, fmt.Sprintf("metrics read error: %v", err)
	}

	if !res.CanFit(task) {
		return false, fmt.Sprintf("insufficient resources (avail CPU: %dm, Mem: %dMiB, required CPU: %dm, Mem: %dMiB)",
			res.AvailCPUMilli, res.AvailMemMiB, task.MinCPUMilli, task.MinMemMiB)
	}

	a.reservations.Reserve(task)
	return true, ""
}

// runTask executes function pod and publishes [model.TaskResult] to [protocol.TopicResults]
func (a *Agent) runTask(client mqtt.Client, task model.TaskCFP) {
	defer a.reservations.Release(task.TaskID)

	ctx, cancel := context.WithTimeout(context.Background(), protocol.DefaultExecutionTimeout)
	defer cancel()

	status, duration := k8s.ExecuteFunctionPod(ctx, a.clientset, a.namespace, task)
	res := model.TaskResult{
		TaskID:     task.TaskID,
		ClusterID:  a.clusterID,
		Status:     status,
		DurationMs: duration,
	}

	publishJSON(client, protocol.TopicResults, res)
	log.Printf("[COMPLETED] Task %s finished with status %s in %dms", task.TaskID, status, duration)
}
