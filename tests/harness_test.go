package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"k8s-mqtt-agent/internal/model"
	"k8s-mqtt-agent/pkg/protocol"
)

type TaskCFP = protocol.TaskCFP
type BidPayload = protocol.BidPayload
type AckPayload = protocol.AckPayload
type TaskResult = protocol.TaskResult

type AuctionTrace struct {
	Request       TaskCFP
	HTTPStatus    int
	Result        *TaskResult
	ErrorResponse string
	Bids          []BidPayload
	Acks          []AckPayload
	RoundTripTime time.Duration
}

type ScoreFormula func(bid BidPayload) int64

func CurrentDefaultScoreFormula(bid BidPayload) int64 {
	score := bid.AvailCPUMilli
	if bid.ImageCached {
		score += model.CachedImageBonus
	}
	return score
}

type Expectation func(t *testing.T, trace *AuctionTrace)

type AuctionTestCase struct {
	Name         string
	Request      TaskCFP
	Timeout      time.Duration
	Expectations []Expectation
}

type TestHarness struct {
	DispatcherURL string
	MQTTBroker    string
	MQTTUser      string
	MQTTPass      string

	httpClient *http.Client
	mqttClient mqtt.Client

	mu      sync.Mutex
	bids    map[string][]BidPayload
	acks    map[string][]AckPayload
	results map[string]chan TaskResult
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return fallback
}

func NewTestHarness(t *testing.T) *TestHarness {
	t.Helper()

	h := &TestHarness{
		DispatcherURL: getEnv("DISPATCHER_URL", "http://127.0.0.1"+protocol.DefaultHTTPAddr),
		MQTTBroker:    getEnv("MQTT_BROKER", protocol.DefaultMQTTBroker),
		MQTTUser:      getEnv("MQTT_USER", protocol.DefaultMQTTUser),
		MQTTPass:      getEnv("MQTT_PASS", protocol.DefaultMQTTPass),
		httpClient: &http.Client{
			Timeout: 3 * time.Minute,
		},
		bids:    make(map[string][]BidPayload),
		acks:    make(map[string][]AckPayload),
		results: make(map[string]chan TaskResult),
	}

	opts := mqtt.NewClientOptions().
		AddBroker(h.MQTTBroker).
		SetUsername(h.MQTTUser).
		SetPassword(h.MQTTPass).
		SetClientID(fmt.Sprintf("test-suite-observer-%d", time.Now().UnixNano()%100000)).
		SetAutoReconnect(true).
		SetConnectTimeout(5 * time.Second)

	opts.SetOnConnectHandler(func(c mqtt.Client) {
		c.Subscribe(protocol.TopicBids, protocol.MQTTQoS, h.onBidReceived).Wait()
		c.Subscribe(protocol.TopicAck, protocol.MQTTQoS, h.onAckReceived).Wait()
		c.Subscribe(protocol.TopicResults, protocol.MQTTQoS, h.onResultReceived).Wait()
	})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		t.Logf("[WARN] Failed to connect MQTT observer (%s): %v. Bid-level assertions will be skipped.",
			h.MQTTBroker, token.Error())
	} else {
		h.mqttClient = client
		t.Cleanup(func() {
			client.Disconnect(250)
		})
	}

	return h
}

func (h *TestHarness) onBidReceived(_ mqtt.Client, msg mqtt.Message) {
	var bid BidPayload
	if err := json.Unmarshal(msg.Payload(), &bid); err != nil {
		return
	}
	h.mu.Lock()
	h.bids[bid.TaskID] = append(h.bids[bid.TaskID], bid)
	h.mu.Unlock()
}

func (h *TestHarness) onAckReceived(_ mqtt.Client, msg mqtt.Message) {
	var ack AckPayload
	if err := json.Unmarshal(msg.Payload(), &ack); err != nil {
		return
	}
	h.mu.Lock()
	h.acks[ack.TaskID] = append(h.acks[ack.TaskID], ack)
	h.mu.Unlock()
}

func (h *TestHarness) onResultReceived(_ mqtt.Client, msg mqtt.Message) {
	var res TaskResult
	if err := json.Unmarshal(msg.Payload(), &res); err != nil {
		return
	}
	h.mu.Lock()
	ch, ok := h.results[res.TaskID]
	h.mu.Unlock()
	if ok {
		select {
		case ch <- res:
		default:
		}
	}
}

func (h *TestHarness) ensureTaskID(req *TaskCFP) {
	if req.TaskID == "" {
		req.TaskID = fmt.Sprintf("test-%d", time.Now().UnixNano()/1e6)
	}
}

func (h *TestHarness) SubmitHTTP(ctx context.Context, req TaskCFP) (*AuctionTrace, error) {
	h.ensureTaskID(&req)

	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, h.DispatcherURL+"/submit", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create HTTP request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := h.httpClient.Do(httpReq)
	rtt := time.Since(start)
	if err != nil {
		return nil, fmt.Errorf("call dispatcher %s/submit: %w", h.DispatcherURL, err)
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)

	trace := &AuctionTrace{
		Request:       req,
		HTTPStatus:    resp.StatusCode,
		RoundTripTime: rtt,
	}

	if resp.StatusCode == http.StatusOK {
		var res TaskResult
		if err := json.Unmarshal(respBytes, &res); err != nil {
			return nil, fmt.Errorf("decode TaskResult: %w (body: %s)", err, string(respBytes))
		}
		trace.Result = &res
	} else {
		trace.ErrorResponse = string(respBytes)
	}

	h.mu.Lock()
	if bids, ok := h.bids[req.TaskID]; ok {
		trace.Bids = make([]BidPayload, len(bids))
		copy(trace.Bids, bids)
	}
	if acks, ok := h.acks[req.TaskID]; ok {
		trace.Acks = make([]AckPayload, len(acks))
		copy(trace.Acks, acks)
	}
	h.mu.Unlock()

	return trace, nil
}

func (h *TestHarness) SubmitMQTT(ctx context.Context, req TaskCFP) (*AuctionTrace, error) {
	if h.mqttClient == nil || !h.mqttClient.IsConnected() {
		return nil, fmt.Errorf("MQTT client is not connected (%s)", h.MQTTBroker)
	}

	h.ensureTaskID(&req)
	resChan := make(chan TaskResult, 1)

	h.mu.Lock()
	h.results[req.TaskID] = resChan
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		delete(h.results, req.TaskID)
		h.mu.Unlock()
	}()

	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	start := time.Now()
	token := h.mqttClient.Publish(protocol.TopicSubmit, 1, false, payload)
	token.Wait()
	if token.Error() != nil {
		return nil, fmt.Errorf("mqtt publish failed: %w", token.Error())
	}

	select {
	case res := <-resChan:
		rtt := time.Since(start)
		trace := &AuctionTrace{
			Request:       req,
			HTTPStatus:    http.StatusOK,
			Result:        &res,
			RoundTripTime: rtt,
		}
		h.mu.Lock()
		if bids, ok := h.bids[req.TaskID]; ok {
			trace.Bids = make([]BidPayload, len(bids))
			copy(trace.Bids, bids)
		}
		if acks, ok := h.acks[req.TaskID]; ok {
			trace.Acks = make([]AckPayload, len(acks))
			copy(trace.Acks, acks)
		}
		h.mu.Unlock()
		return trace, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("timed out waiting for MQTT result for task %s: %w", req.TaskID, ctx.Err())
	}
}

func (h *TestHarness) SubmitParallel(ctx context.Context, requests []TaskCFP, staggerDelay time.Duration) ([]*AuctionTrace, []error) {
	traces := make([]*AuctionTrace, len(requests))
	errs := make([]error, len(requests))
	var wg sync.WaitGroup

	for i, req := range requests {
		if i > 0 && staggerDelay > 0 {
			time.Sleep(staggerDelay)
		}
		wg.Add(1)
		go func(idx int, r TaskCFP) {
			defer wg.Done()
			traces[idx], errs[idx] = h.SubmitHTTP(ctx, r)
		}(i, req)
	}

	wg.Wait()
	return traces, errs
}

func (h *TestHarness) RunCase(t *testing.T, tc AuctionTestCase) *AuctionTrace {
	t.Helper()

	timeout := tc.Timeout
	if timeout == 0 {
		timeout = 90 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	trace, err := h.SubmitHTTP(ctx, tc.Request)
	if err != nil {
		t.Fatalf("[%s] Dispatcher communication error: %v", tc.Name, err)
	}

	t.Logf("[%s] TaskID=%s | HTTP=%d | RTT=%v | Bids=%d",
		tc.Name, trace.Request.TaskID, trace.HTTPStatus, trace.RoundTripTime.Round(time.Millisecond), len(trace.Bids))

	for _, b := range trace.Bids {
		t.Logf("   -> Bid from %s: AvailCPU=%dm, AvailMem=%dMiB, Cached=%v, Score=%d",
			b.ClusterID, b.AvailCPUMilli, b.AvailMemMiB, b.ImageCached, b.Score)
	}
	for _, a := range trace.Acks {
		t.Logf("   -> Ack from %s: Status=%s, Reason=%s", a.ClusterID, a.Status, a.Reason)
	}
	if trace.Result != nil {
		t.Logf("   => Winner: %s | Status=%s | Duration=%dms",
			trace.Result.ClusterID, trace.Result.Status, trace.Result.DurationMs)
	} else if trace.ErrorResponse != "" {
		t.Logf("   => Error response: %s", trace.ErrorResponse)
	}

	for _, exp := range tc.Expectations {
		exp(t, trace)
	}

	return trace
}

func ExpectAuctionSuccess() Expectation {
	return func(t *testing.T, trace *AuctionTrace) {
		t.Helper()
		if trace.HTTPStatus != http.StatusOK {
			t.Fatalf("Expected HTTP 200 OK, got %d (%s)", trace.HTTPStatus, trace.ErrorResponse)
		}
		if trace.Result == nil {
			t.Fatalf("Expected non-nil TaskResult")
		}
		if trace.Result.ClusterID == "" {
			t.Errorf("TaskResult is missing winner ClusterID")
		}
		if trace.Result.TaskID != trace.Request.TaskID {
			t.Errorf("TaskID mismatch in result: expected %s, got %s", trace.Request.TaskID, trace.Result.TaskID)
		}
	}
}

func ExpectAuctionRejected() Expectation {
	return func(t *testing.T, trace *AuctionTrace) {
		t.Helper()
		if trace.HTTPStatus != http.StatusServiceUnavailable {
			t.Fatalf("Expected auction rejection (HTTP 503 Service Unavailable), got HTTP %d (Result: %+v)",
				trace.HTTPStatus, trace.Result)
		}
	}
}

func ExpectPodStatus(expectedStatus string) Expectation {
	return func(t *testing.T, trace *AuctionTrace) {
		t.Helper()
		if trace.Result == nil {
			t.Fatalf("Missing TaskResult, cannot verify pod status (expected %q)", expectedStatus)
		}
		if trace.Result.Status != expectedStatus {
			t.Errorf("Unexpected task execution status: expected %q, got %q", expectedStatus, trace.Result.Status)
		}
	}
}

func ExpectWinnerCluster(expectedClusterID string) Expectation {
	return func(t *testing.T, trace *AuctionTrace) {
		t.Helper()
		if trace.Result == nil {
			t.Fatalf("Missing TaskResult, expected winner %q", expectedClusterID)
		}
		if trace.Result.ClusterID != expectedClusterID {
			t.Errorf("Expected cluster %q to win, but %q won", expectedClusterID, trace.Result.ClusterID)
		}
	}
}

func ExpectExcludedClusterNotChosen(excludedClusterID string) Expectation {
	return func(t *testing.T, trace *AuctionTrace) {
		t.Helper()
		if trace.Result != nil && trace.Result.ClusterID == excludedClusterID {
			t.Errorf("Excluded origin cluster (%s) must not win the auction, but was selected", excludedClusterID)
		}
	}
}

func ExpectBidsMeetMinimumResources() Expectation {
	return func(t *testing.T, trace *AuctionTrace) {
		t.Helper()
		for _, bid := range trace.Bids {
			if bid.AvailCPUMilli < trace.Request.MinCPUMilli {
				t.Errorf("Cluster %s submitted a bid with insufficient CPU: available %dm < required %dm",
					bid.ClusterID, bid.AvailCPUMilli, trace.Request.MinCPUMilli)
			}
			if bid.AvailMemMiB < trace.Request.MinMemMiB {
				t.Errorf("Cluster %s submitted a bid with insufficient memory: available %dMiB < required %dMiB",
					bid.ClusterID, bid.AvailMemMiB, trace.Request.MinMemMiB)
			}
		}
	}
}

func ExpectBidScoresMatchFormula(formula ScoreFormula) Expectation {
	return func(t *testing.T, trace *AuctionTrace) {
		t.Helper()
		for _, bid := range trace.Bids {
			expected := formula(bid)
			if bid.Score != expected {
				t.Errorf("Invalid score for cluster %s: got %d, expected %d (AvailCPU=%dm, AvailMem=%dMiB, Cached=%v)",
					bid.ClusterID, bid.Score, expected, bid.AvailCPUMilli, bid.AvailMemMiB, bid.ImageCached)
			}
		}
	}
}

func ExpectHighestBidderWins() Expectation {
	return func(t *testing.T, trace *AuctionTrace) {
		t.Helper()
		if trace.Result == nil || len(trace.Bids) == 0 {
			return
		}

		var validBids []BidPayload
		for _, b := range trace.Bids {
			if trace.Request.OriginClusterID != "" && b.ClusterID == trace.Request.OriginClusterID {
				continue
			}
			validBids = append(validBids, b)
		}

		if len(validBids) == 0 {
			t.Errorf("Cluster %s won the task even though no valid bids were observed", trace.Result.ClusterID)
			return
		}

		maxScore := validBids[0].Score
		var winnerScore int64 = -1
		for _, b := range validBids {
			if b.Score > maxScore {
				maxScore = b.Score
			}
			if b.ClusterID == trace.Result.ClusterID {
				winnerScore = b.Score
			}
		}

		if winnerScore == -1 {
			t.Errorf("Winning cluster %s was not found in the collected MQTT bids", trace.Result.ClusterID)
			return
		}

		if winnerScore < maxScore {
			t.Errorf("Cluster %s won with Score=%d, although a higher bid with Score=%d was available",
				trace.Result.ClusterID, winnerScore, maxScore)
		}
	}
}

func ExpectClusterAccepted(clusterID string) Expectation {
	return func(t *testing.T, trace *AuctionTrace) {
		t.Helper()
		if len(trace.Acks) == 0 {
			t.Logf("Note: no MQTT ACKs captured by observer for task %s", trace.Request.TaskID)
			return
		}
		for _, a := range trace.Acks {
			if a.ClusterID == clusterID && a.Status == protocol.AckStatusAccepted {
				return
			}
		}
		t.Errorf("Expected cluster %s to submit ACK, but got acks: %+v", clusterID, trace.Acks)
	}
}

func ExpectClusterResigned(clusterID string) Expectation {
	return func(t *testing.T, trace *AuctionTrace) {
		t.Helper()
		if len(trace.Acks) == 0 {
			t.Logf("Note: no MQTT ACKs captured by observer for task %s", trace.Request.TaskID)
			return
		}
		for _, a := range trace.Acks {
			if a.ClusterID == clusterID && a.Status == protocol.AckStatusResigned {
				return
			}
		}
		t.Errorf("Expected cluster %s to submit RES (resignation), but got acks: %+v", clusterID, trace.Acks)
	}
}
