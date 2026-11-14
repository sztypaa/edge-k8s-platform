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

type Agent struct {
	clusterID      string
	namespace      string
	metricsTimeout time.Duration
	clientset      *kubernetes.Clientset
	reservations   *model.ReservationTracker
	execMu         sync.Mutex
}

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

func (a *Agent) Run(broker string) {
	mqttClient := mqtt.NewClient(a.newMQTTClientOptions(broker))
	connectWithRetry(mqttClient)
	select {}
}

func (a *Agent) execTopic() string {
	return protocol.TopicExecute(a.clusterID)
}

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
