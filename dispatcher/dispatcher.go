package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type TaskCFP struct {
	TaskID      string   `json:"task_id"`
	Image       string   `json:"image"`
	Command     []string `json:"command"`
	MinCPUMilli int64    `json:"min_cpu_milli"`
	MinMemMiB   int64    `json:"min_mem_mib"`
}

type BidPayload struct {
	TaskID        string `json:"task_id"`
	ClusterID     string `json:"cluster_id"`
	AvailCPUMilli int64  `json:"avail_cpu_milli"`
	AvailMemMiB   int64  `json:"avail_mem_mib"`
	ImageCached   bool   `json:"image_cached"`
	Score         int64  `json:"score"`
}

type TaskResult struct {
	TaskID      string `json:"task_id"`
	ClusterID   string `json:"cluster_id"`
	Status      string `json:"status"`
	DurationMs  int64  `json:"duration_ms"`
}

func main() {
	opts := mqtt.NewClientOptions().AddBroker("tcp://127.0.0.1:1883")
	opts.SetUsername("clusterless_admin")
	opts.SetPassword("zaq1@WSX")
	opts.SetClientID("auction-dispatcher")

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("[FATAL] MQTT connect error: %v", token.Error())
	}

	var mu sync.Mutex
	var bids []BidPayload
	resultChan := make(chan TaskResult, 1)

	taskID := fmt.Sprintf("job-%d", time.Now().Unix())

	client.Subscribe("clusterless/auction/bids", 1, func(c mqtt.Client, msg mqtt.Message) {
		var bid BidPayload
		if err := json.Unmarshal(msg.Payload(), &bid); err == nil && bid.TaskID == taskID {
			mu.Lock()
			bids = append(bids, bid)
			mu.Unlock()
			log.Printf("[BID RECEIVED] Cluster: %s | AvailCPU: %dm | AvailMem: %dMiB | Cached: %v | Score: %d",
				bid.ClusterID, bid.AvailCPUMilli, bid.AvailMemMiB, bid.ImageCached, bid.Score)
		}
	})

	client.Subscribe("clusterless/results", 1, func(c mqtt.Client, msg mqtt.Message) {
		var res TaskResult
		if err := json.Unmarshal(msg.Payload(), &res); err == nil && res.TaskID == taskID {
			resultChan <- res
		}
	})

	cfp := TaskCFP{
		TaskID:      taskID,
		Image:       "busybox:latest",
		Command:     []string{"sh", "-c", "echo 'Running serverless task...'; sleep 2; echo 'Task done'"},
		MinCPUMilli: 500,
		MinMemMiB:   256,
	}

	cfpBytes, _ := json.Marshal(cfp)
	log.Printf("[CFP PUBLISHED] Starting auction for TaskID: %s", taskID)
	client.Publish("clusterless/auction/cfp", 1, false, cfpBytes).Wait()

	time.Sleep(1 * time.Second)

	mu.Lock()
	if len(bids) == 0 {
		mu.Unlock()
		log.Fatalf("[AUCTION FAILED] No clusters submitted a valid bid for task %s", taskID)
	}

	bestBid := bids[0]
	for _, b := range bids[1:] {
		if b.Score > bestBid.Score {
			bestBid = b
		}
	}
	mu.Unlock()

	log.Printf("[WINNER SELECTED] Cluster %s won the auction with Score %d. Dispatching function...", bestBid.ClusterID, bestBid.Score)

	execTopic := fmt.Sprintf("clusterless/execute/%s", bestBid.ClusterID)
	client.Publish(execTopic, 1, false, cfpBytes).Wait()

	log.Println("[WAITING] Awaiting function completion...")
	select {
	case res := <-resultChan:
		log.Printf("[SUCCESS] Task %s completed on %s | Status: %s | Total Time: %dms",
			res.TaskID, res.ClusterID, res.Status, res.DurationMs)
	case <-time.After(60 * time.Second):
		log.Fatalf("[TIMEOUT] Task %s did not finish in time", taskID)
	}
}
