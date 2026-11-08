package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type TaskCFP struct {
	TaskID      string   `json:"task_id"`
	OriginClusterID string   `json:"origin_cluster_id,omitempty"`
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

type AuctionSession struct {
	OriginClusterID string
	Bids            []BidPayload
	ResultChan      chan TaskResult
}

type Dispatcher struct {
	mqttClient mqtt.Client
	mu         sync.Mutex
	sessions   map[string]*AuctionSession
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

func NewDispatcher(client mqtt.Client) *Dispatcher {
	return &Dispatcher{
		mqttClient: client,
		sessions:   make(map[string]*AuctionSession),
	}
}

func (d *Dispatcher) registerSession(taskID, originCluster string) *AuctionSession {
	d.mu.Lock()
	defer d.mu.Unlock()
	session := &AuctionSession{
		OriginClusterID: originCluster,
		Bids:            make([]BidPayload, 0),
		ResultChan:      make(chan TaskResult, 1),
	}
	d.sessions[taskID] = session
	return session
}

func (d *Dispatcher) removeSession(taskID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.sessions, taskID)
}

func (d *Dispatcher) handleBid(client mqtt.Client, msg mqtt.Message) {
	var bid BidPayload
	if err := json.Unmarshal(msg.Payload(), &bid); err != nil {
		log.Printf("[ERROR] Invalid bid payload: %v", err)
		return
	}

	d.mu.Lock()
	session, exists := d.sessions[bid.TaskID]
	if exists {
		if session.OriginClusterID != "" && bid.ClusterID == session.OriginClusterID {
			d.mu.Unlock()
			log.Printf("[BID IGNORED] Cluster %s is the origin of task %s", bid.ClusterID, bid.TaskID)
			return
		}
		session.Bids = append(session.Bids, bid)
	}
	d.mu.Unlock()

	if exists {
		log.Printf("[BID RECEIVED] Task: %s | Cluster: %s | AvailCPU: %dm | AvailMem: %dMiB | Cached: %v | Score: %d",
			bid.TaskID, bid.ClusterID, bid.AvailCPUMilli, bid.AvailMemMiB, bid.ImageCached, bid.Score)
	}
}

func (d *Dispatcher) handleResult(client mqtt.Client, msg mqtt.Message) {
	var res TaskResult
	if err := json.Unmarshal(msg.Payload(), &res); err != nil {
		log.Printf("[ERROR] Invalid result payload: %v", err)
		return
	}

	d.mu.Lock()
	session, exists := d.sessions[res.TaskID]
	d.mu.Unlock()

	if exists {
		select {
		case session.ResultChan <- res:
		default:
		}
	}
}

func (d *Dispatcher) handleMQTTRequest(client mqtt.Client, msg mqtt.Message) {
	var req TaskCFP
	if err := json.Unmarshal(msg.Payload(), &req); err != nil {
		log.Printf("[ERROR] Invalid task request payload: %v", err)
		return
	}

	go func(t TaskCFP) {
		_, _ = d.runAuction(t)
	}(req)
}

func (d *Dispatcher) runAuction(req TaskCFP) (*TaskResult, error) {
	if req.TaskID == "" {
		req.TaskID = fmt.Sprintf("job-%d", time.Now().UnixNano()/1e6)
	}
	if req.Image == "" {
		req.Image = "busybox:latest"
	}
	if len(req.Command) == 0 {
		req.Command = []string{"sh", "-c", "echo 'Running serverless task...'; sleep 2; echo 'Task done'"}
	}
	if req.MinCPUMilli == 0 {
		req.MinCPUMilli = 200
	}
	if req.MinMemMiB == 0 {
		req.MinMemMiB = 128
	}

	session := d.registerSession(req.TaskID, req.OriginClusterID)
	defer d.removeSession(req.TaskID)

	cfpBytes, _ := json.Marshal(req)
	log.Printf("[CFP PUBLISHED] Task: %s | Origin: %s | Image: %s | MinCPU: %dm | MinMem: %dMiB",
		req.TaskID, req.OriginClusterID, req.Image, req.MinCPUMilli, req.MinMemMiB)
	d.mqttClient.Publish("clusterless/auction/cfp", 1, false, cfpBytes).Wait()

	time.Sleep(1 * time.Second)

	d.mu.Lock()
	bidsCopy := make([]BidPayload, len(session.Bids))
	copy(bidsCopy, session.Bids)
	d.mu.Unlock()

	if len(bidsCopy) == 0 {
		log.Printf("[AUCTION FAILED] No clusters submitted a valid bid for task %s", req.TaskID)
		return nil, fmt.Errorf("no valid bids received for task %s", req.TaskID)
	}

	bestBid := bidsCopy[0]
	for _, b := range bidsCopy[1:] {
		if b.Score > bestBid.Score {
			bestBid = b
		}
	}

	log.Printf("[WINNER SELECTED] Task: %s | Winner: %s (Score: %d). Dispatching...",
		req.TaskID, bestBid.ClusterID, bestBid.Score)

	execTopic := fmt.Sprintf("clusterless/execute/%s", bestBid.ClusterID)
	d.mqttClient.Publish(execTopic, 1, false, cfpBytes).Wait()

	select {
	case res := <-session.ResultChan:
		log.Printf("[TASK COMPLETED] Task: %s | Cluster: %s | Status: %s | Duration: %dms",
			res.TaskID, res.ClusterID, res.Status, res.DurationMs)
		return &res, nil
	case <-time.After(2 * time.Minute):
		log.Printf("[TASK TIMEOUT] Task %s did not finish in time on cluster %s", req.TaskID, bestBid.ClusterID)
		return nil, fmt.Errorf("execution timeout for task %s", req.TaskID)
	}
}

func (d *Dispatcher) handleHTTPSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req TaskCFP
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	res, err := d.runAuction(req)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(res)
}

func main() {
	mqttBroker := getEnv("MQTT_BROKER", "tcp://127.0.0.1:1883")
	mqttUser := getEnv("MQTT_USER", "clusterless_admin")
	mqttPass := getEnv("MQTT_PASS", "zaq1@WSX")
	httpAddr := getEnv("HTTP_ADDR", ":8080")

	var dispatcher *Dispatcher

	opts := mqtt.NewClientOptions().AddBroker(mqttBroker)
	opts.SetUsername(mqttUser)
	opts.SetPassword(mqttPass)
	opts.SetClientID("clusterless-central-dispatcher")
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)
	opts.SetKeepAlive(60 * time.Second)

	opts.SetOnConnectHandler(func(c mqtt.Client) {
		log.Println("[INFO] Dispatcher connected to MQTT broker, subscribing topics...")
		c.Subscribe("clusterless/auction/bids", 1, dispatcher.handleBid)
		c.Subscribe("clusterless/results", 1, dispatcher.handleResult)
		c.Subscribe("clusterless/requests/submit", 1, dispatcher.handleMQTTRequest)
	})

	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		log.Printf("[WARN] Connection to MQTT broker lost: %v. Reconnecting...", err)
	})

	client := mqtt.NewClient(opts)
	dispatcher = NewDispatcher(client)
	for {
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			log.Fatalf("[WARN] Initial MQTT connect failed: %v, retrying in 5s...", token.Error())
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}
	
	http.HandleFunc("/submit", dispatcher.handleHTTPSubmit)
	log.Printf("[INFO] Dispatcher daemon listening for HTTP requests on %s and MQTT on clusterless/requests/submit", httpAddr)
	if err := http.ListenAndServe(httpAddr, nil); err != nil {
		log.Fatalf("[FATAL] HTTP server error: %v", err)
	}
}
