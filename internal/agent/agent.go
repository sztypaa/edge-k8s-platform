package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"k8s-mqtt-agent/internal/config"
	"k8s-mqtt-agent/internal/k8s"
	"k8s-mqtt-agent/internal/model"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"k8s.io/client-go/kubernetes"
)

type Agent struct {
	clusterID    string
	clientset    *kubernetes.Clientset
	reservations *model.ReservationTracker
}

func New(clusterID string, clientset *kubernetes.Clientset) *Agent {
	return &Agent{
		clusterID:    clusterID,
		clientset:    clientset,
		reservations: model.NewReservationTracker(),
	}
}

func (a *Agent) Run(broker string) {
	mqttClient := mqtt.NewClient(a.newMQTTClientOptions(broker))
	connectWithRetry(mqttClient)
	select {}
}

func (a *Agent) execTopic() string {
	return fmt.Sprintf(config.TopicExecuteFmt, a.clusterID)
}

func (a *Agent) handleCFP(client mqtt.Client, msg mqtt.Message) {
	var cfp model.TaskCFP
	if err := json.Unmarshal(msg.Payload(), &cfp); err != nil {
		log.Printf("[ERROR] Invalid CFP payload: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), config.MetricsTimeout)
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

	publishJSON(client, config.TopicBids, bid)
	log.Printf("[BID SENT] Task: %s | AvailCPU: %dm | AvailMem: %dMiB | Cached: %v | Score: %d",
		cfp.TaskID, res.AvailCPUMilli, res.AvailMemMiB, res.ImageCached, bid.Score)
}

func (a *Agent) handleExecute(client mqtt.Client, msg mqtt.Message) {
	var task model.TaskCFP
	if err := json.Unmarshal(msg.Payload(), &task); err != nil {
		log.Printf("[ERROR] Invalid execute payload: %v", err)
		return
	}

	a.reservations.Reserve(task)
	log.Printf("[EXECUTING] Task %s won by %s (reserved %dm CPU, %dMiB Mem). Spawning pod...",
		task.TaskID, a.clusterID, task.MinCPUMilli, task.MinMemMiB)

	go a.runTask(client, task)
}

func (a *Agent) runTask(client mqtt.Client, task model.TaskCFP) {
	defer a.reservations.Release(task.TaskID)

	ctx, cancel := context.WithTimeout(context.Background(), config.ExecutionTimeout)
	defer cancel()

	status, duration := k8s.ExecuteFunctionPod(ctx, a.clientset, task)
	res := model.TaskResult{
		TaskID:     task.TaskID,
		ClusterID:  a.clusterID,
		Status:     status,
		DurationMs: duration,
	}

	publishJSON(client, config.TopicResults, res)
	log.Printf("[COMPLETED] Task %s finished with status %s in %dms", task.TaskID, status, duration)
}
