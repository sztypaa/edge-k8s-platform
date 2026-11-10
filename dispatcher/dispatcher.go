package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"dispatcher/strategy"
	"k8s-mqtt-agent/pkg/protocol"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	defaultAckChanCapacity    = 16
	defaultResultChanCapacity = 1
	defaultDispatcherClientID = "clusterless-central-dispatcher"
	endpointSubmit            = "/submit"

	defaultTaskImage       = "busybox:latest"
	defaultTaskMinCPUMilli = int64(200)
	defaultTaskMinMemMiB   = int64(128)
)

var defaultTaskCommand = []string{"sh", "-c", "echo 'Running serverless task...'; sleep 2; echo 'Task done'"}

type AuctionSession struct {
	taskID          string
	originClusterID string
	mqttClient      mqtt.Client

	mu         sync.Mutex
	bids       []protocol.BidPayload
	ackChan    chan protocol.AckPayload
	resultChan chan protocol.TaskResult
}

func newAuctionSession(taskID, originClusterID string, client mqtt.Client) *AuctionSession {
	return &AuctionSession{
		taskID:          taskID,
		originClusterID: originClusterID,
		mqttClient:      client,
		bids:            make([]protocol.BidPayload, 0),
		ackChan:         make(chan protocol.AckPayload, defaultAckChanCapacity),
		resultChan:      make(chan protocol.TaskResult, defaultResultChanCapacity),
	}
}

func (s *AuctionSession) TaskID() string {
	return s.taskID
}

func (s *AuctionSession) OriginClusterID() string {
	return s.originClusterID
}

func (s *AuctionSession) PublishCFP(ctx context.Context, task protocol.TaskCFP) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal CFP error: %w", err)
	}

	log.Printf("[CFP PUBLISHED] Task: %s | Origin: %s | Image: %s | MinCPU: %dm | MinMem: %dMiB",
		task.TaskID, task.OriginClusterID, task.Image, task.MinCPUMilli, task.MinMemMiB)

	token := s.mqttClient.Publish(protocol.TopicCFP, protocol.MQTTQoS, false, data)
	token.Wait()
	return token.Error()
}

func (s *AuctionSession) CollectBids(ctx context.Context, duration time.Duration) ([]protocol.BidPayload, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(duration):
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	bidsCopy := make([]protocol.BidPayload, len(s.bids))
	copy(bidsCopy, s.bids)
	return bidsCopy, nil
}

func (s *AuctionSession) Dispatch(ctx context.Context, clusterID string, task protocol.TaskCFP) error {
	for {
		select {
		case <-s.ackChan:
		default:
			goto drained
		}
	}
drained:

	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task error: %w", err)
	}

	topic := protocol.TopicExecute(clusterID)
	token := s.mqttClient.Publish(topic, protocol.MQTTQoS, false, data)
	token.Wait()
	return token.Error()
}

func (s *AuctionSession) AwaitAck(ctx context.Context, clusterID string, timeout time.Duration) (protocol.AckPayload, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return protocol.AckPayload{}, ctx.Err()
		case <-timer.C:
			return protocol.AckPayload{}, fmt.Errorf("timeout waiting for ACK from cluster %s after %v", clusterID, timeout)
		case ack := <-s.ackChan:
			if ack.ClusterID == clusterID {
				return ack, nil
			}
			log.Printf("[ACK IGNORED] Received ACK from cluster %s while expecting %s", ack.ClusterID, clusterID)
		}
	}
}

func (s *AuctionSession) AwaitResult(ctx context.Context, timeout time.Duration) (*protocol.TaskResult, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("execution timeout waiting for result of task %s after %v", s.taskID, timeout)
	case res := <-s.resultChan:
		return &res, nil
	}
}

func (s *AuctionSession) AddBid(bid protocol.BidPayload) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.originClusterID != "" && bid.ClusterID == s.originClusterID {
		log.Printf("[BID IGNORED] Cluster %s is the origin of task %s", bid.ClusterID, bid.TaskID)
		return
	}
	s.bids = append(s.bids, bid)
	log.Printf("[BID RECEIVED] Task: %s | Cluster: %s | AvailCPU: %dm | AvailMem: %dMiB | Cached: %v | Score: %d",
		bid.TaskID, bid.ClusterID, bid.AvailCPUMilli, bid.AvailMemMiB, bid.ImageCached, bid.Score)
}

func (s *AuctionSession) NotifyAck(ack protocol.AckPayload) {
	select {
	case s.ackChan <- ack:
	default:
		log.Printf("[WARN] Ack channel full for task %s, dropping ack from %s", ack.TaskID, ack.ClusterID)
	}
}

func (s *AuctionSession) NotifyResult(res protocol.TaskResult) {
	select {
	case s.resultChan <- res:
	default:
		log.Printf("[WARN] Result channel full for task %s, dropping result from %s", res.TaskID, res.ClusterID)
	}
}

type Dispatcher struct {
	mqttClient mqtt.Client
	strategy   strategy.AuctioneerStrategy
	mu         sync.Mutex
	sessions   map[string]*AuctionSession
}

func NewDispatcher(client mqtt.Client, strat strategy.AuctioneerStrategy) *Dispatcher {
	if strat == nil {
		strat = strategy.ResolveStrategy(protocol.DefaultStrategy)
	}
	return &Dispatcher{
		mqttClient: client,
		strategy:   strat,
		sessions:   make(map[string]*AuctionSession),
	}
}

func (d *Dispatcher) Strategy() strategy.AuctioneerStrategy {
	return d.strategy
}

func (d *Dispatcher) NewAuction(ctx context.Context, task protocol.TaskCFP) (strategy.AuctionSession, func(), error) {
	session := d.registerSession(task.TaskID, task.OriginClusterID)
	cleanup := func() {
		d.removeSession(task.TaskID)
	}
	return session, cleanup, nil
}

func (d *Dispatcher) registerSession(taskID, originCluster string) *AuctionSession {
	d.mu.Lock()
	defer d.mu.Unlock()
	session := newAuctionSession(taskID, originCluster, d.mqttClient)
	d.sessions[taskID] = session
	return session
}

func (d *Dispatcher) removeSession(taskID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.sessions, taskID)
}

func (d *Dispatcher) handleBid(client mqtt.Client, msg mqtt.Message) {
	var bid protocol.BidPayload
	if err := json.Unmarshal(msg.Payload(), &bid); err != nil {
		log.Printf("[ERROR] Invalid bid payload: %v", err)
		return
	}

	d.mu.Lock()
	session, exists := d.sessions[bid.TaskID]
	d.mu.Unlock()

	if exists {
		session.AddBid(bid)
	}
}

func (d *Dispatcher) handleAck(client mqtt.Client, msg mqtt.Message) {
	var ack protocol.AckPayload
	if err := json.Unmarshal(msg.Payload(), &ack); err != nil {
		log.Printf("[ERROR] Invalid ack payload: %v", err)
		return
	}

	d.mu.Lock()
	session, exists := d.sessions[ack.TaskID]
	d.mu.Unlock()

	if exists {
		log.Printf("[ACK RECEIVED] Task: %s | Cluster: %s | Status: %s | Reason: %s",
			ack.TaskID, ack.ClusterID, ack.Status, ack.Reason)
		session.NotifyAck(ack)
	}
}

func (d *Dispatcher) handleResult(client mqtt.Client, msg mqtt.Message) {
	var res protocol.TaskResult
	if err := json.Unmarshal(msg.Payload(), &res); err != nil {
		log.Printf("[ERROR] Invalid result payload: %v", err)
		return
	}

	d.mu.Lock()
	session, exists := d.sessions[res.TaskID]
	d.mu.Unlock()

	if exists {
		log.Printf("[TASK COMPLETED] Task: %s | Cluster: %s | Status: %s | Duration: %dms",
			res.TaskID, res.ClusterID, res.Status, res.DurationMs)
		session.NotifyResult(res)
	}
}

func (d *Dispatcher) handleMQTTRequest(client mqtt.Client, msg mqtt.Message) {
	var req protocol.TaskCFP
	if err := json.Unmarshal(msg.Payload(), &req); err != nil {
		log.Printf("[ERROR] Invalid task request payload: %v", err)
		return
	}

	go func(t protocol.TaskCFP) {
		_, _ = d.runAuction(context.Background(), t)
	}(req)
}

func (d *Dispatcher) runAuction(ctx context.Context, req protocol.TaskCFP) (*protocol.TaskResult, error) {
	if req.TaskID == "" {
		req.TaskID = fmt.Sprintf("job-%d", time.Now().UnixNano()/1e6)
	}
	if req.Image == "" {
		req.Image = defaultTaskImage
	}
	if len(req.Command) == 0 {
		req.Command = defaultTaskCommand
	}
	if req.MinCPUMilli == 0 {
		req.MinCPUMilli = defaultTaskMinCPUMilli
	}
	if req.MinMemMiB == 0 {
		req.MinMemMiB = defaultTaskMinMemMiB
	}

	return d.strategy.RunAuction(ctx, d, req)
}

func (d *Dispatcher) handleHTTPSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req protocol.TaskCFP
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	res, err := d.runAuction(r.Context(), req)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(res)
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

func main() {
	strategyFlag := flag.String("strategy", getEnv("DISPATCHER_STRATEGY", protocol.DefaultStrategy), "Auctioneer strategy: 'fallback' (ranking fallback) or 'reauction'")
	brokerFlag := flag.String("broker", getEnv("MQTT_BROKER", protocol.DefaultMQTTBroker), "MQTT broker address")
	httpAddrFlag := flag.String("http", getEnv("HTTP_ADDR", protocol.DefaultHTTPAddr), "HTTP server listen address")
	flag.Parse()

	strat := strategy.ResolveStrategy(*strategyFlag)
	log.Printf("[INIT] Dispatcher strategy configured: %s (fixed for application lifetime)", strat.Name())

	mqttUser := getEnv("MQTT_USER", protocol.DefaultMQTTUser)
	mqttPass := getEnv("MQTT_PASS", protocol.DefaultMQTTPass)

	var dispatcher *Dispatcher

	opts := mqtt.NewClientOptions().AddBroker(*brokerFlag)
	opts.SetUsername(mqttUser)
	opts.SetPassword(mqttPass)
	opts.SetClientID(defaultDispatcherClientID)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(protocol.MQTTRetryInterval)
	opts.SetKeepAlive(protocol.MQTTKeepAlive)

	opts.SetOnConnectHandler(func(c mqtt.Client) {
		log.Println("[INFO] Dispatcher connected to MQTT broker, subscribing topics...")
		c.Subscribe(protocol.TopicBids, protocol.MQTTQoS, dispatcher.handleBid)
		c.Subscribe(protocol.TopicAck, protocol.MQTTQoS, dispatcher.handleAck)
		c.Subscribe(protocol.TopicResults, protocol.MQTTQoS, dispatcher.handleResult)
		c.Subscribe(protocol.TopicSubmit, protocol.MQTTQoS, dispatcher.handleMQTTRequest)
	})

	opts.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		log.Printf("[WARN] Connection to MQTT broker lost: %v. Reconnecting...", err)
	})

	client := mqtt.NewClient(opts)
	dispatcher = NewDispatcher(client, strat)
	for {
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			log.Printf("[WARN] Initial MQTT connect failed: %v, retrying in %v...", token.Error(), protocol.MQTTRetryInterval)
			time.Sleep(protocol.MQTTRetryInterval)
			continue
		}
		break
	}

	http.HandleFunc(endpointSubmit, dispatcher.handleHTTPSubmit)
	log.Printf("[INFO] Dispatcher daemon listening for HTTP requests on %s and MQTT on %s", *httpAddrFlag, protocol.TopicSubmit)
	if err := http.ListenAndServe(*httpAddrFlag, nil); err != nil {
		log.Fatalf("[FATAL] HTTP server error: %v", err)
	}
}
