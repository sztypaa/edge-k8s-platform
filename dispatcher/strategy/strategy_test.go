package strategy

import (
	"context"
	"testing"
	"time"

	"k8s-mqtt-agent/pkg/protocol"
)

type mockAuctionSession struct {
	taskID          string
	originClusterID string

	cfpCalls     int
	dispatchedTo []string

	bids         []protocol.BidPayload
	ackResponses map[string]protocol.AckPayload
	ackErrors    map[string]error
	result       *protocol.TaskResult
	resultErr    error
}

func (m *mockAuctionSession) TaskID() string {
	return m.taskID
}

func (m *mockAuctionSession) PublishCFP(ctx context.Context, task protocol.TaskCFP) error {
	m.cfpCalls++
	return nil
}

func (m *mockAuctionSession) CollectBids(ctx context.Context, duration time.Duration) ([]protocol.BidPayload, error) {
	return m.bids, nil
}

func (m *mockAuctionSession) Dispatch(ctx context.Context, clusterID string, task protocol.TaskCFP) error {
	m.dispatchedTo = append(m.dispatchedTo, clusterID)
	return nil
}

func (m *mockAuctionSession) AwaitAck(ctx context.Context, clusterID string, timeout time.Duration) (protocol.AckPayload, error) {
	if err, ok := m.ackErrors[clusterID]; ok {
		return protocol.AckPayload{}, err
	}
	if ack, ok := m.ackResponses[clusterID]; ok {
		return ack, nil
	}
	return protocol.AckPayload{
		TaskID:    m.taskID,
		ClusterID: clusterID,
		Status:    protocol.AckStatusAccepted,
	}, nil
}

func (m *mockAuctionSession) AwaitResult(ctx context.Context, timeout time.Duration) (*protocol.TaskResult, error) {
	if m.resultErr != nil {
		return nil, m.resultErr
	}
	return m.result, nil
}

type mockAuctionManager struct {
	sessionsCreated []*mockAuctionSession
	cleanupsCalled  int
	sessionsQueue   []*mockAuctionSession
}

func (mgr *mockAuctionManager) NewAuction(ctx context.Context, task protocol.TaskCFP) (AuctionSession, func(), error) {
	var s *mockAuctionSession
	if len(mgr.sessionsQueue) > 0 {
		s = mgr.sessionsQueue[0]
		mgr.sessionsQueue = mgr.sessionsQueue[1:]
	} else {
		s = &mockAuctionSession{taskID: task.TaskID}
	}
	s.taskID = task.TaskID
	mgr.sessionsCreated = append(mgr.sessionsCreated, s)

	cleanup := func() {
		mgr.cleanupsCalled++
	}
	return s, cleanup, nil
}

func TestRankingFallbackStrategy_Resignation(t *testing.T) {
	task := protocol.TaskCFP{TaskID: "task-test-fallback"}
	sess := &mockAuctionSession{
		taskID: task.TaskID,
		bids: []protocol.BidPayload{
			{TaskID: task.TaskID, ClusterID: "cluster-1", Score: 1000},
			{TaskID: task.TaskID, ClusterID: "cluster-2", Score: 800},
			{TaskID: task.TaskID, ClusterID: "cluster-3", Score: 500},
		},
		ackResponses: map[string]protocol.AckPayload{
			"cluster-1": {TaskID: task.TaskID, ClusterID: "cluster-1", Status: protocol.AckStatusResigned, Reason: "busy"},
			"cluster-2": {TaskID: task.TaskID, ClusterID: "cluster-2", Status: protocol.AckStatusAccepted},
		},
		result: &protocol.TaskResult{
			TaskID:     task.TaskID,
			ClusterID:  "cluster-2",
			Status:     "Succeeded",
			DurationMs: 120,
		},
	}

	mgr := &mockAuctionManager{
		sessionsQueue: []*mockAuctionSession{sess},
	}

	strat := &RankingFallbackStrategy{
		BiddingDuration: 10 * time.Millisecond,
		AckTimeout:      100 * time.Millisecond,
		ExecTimeout:     100 * time.Millisecond,
	}

	res, err := strat.RunAuction(context.Background(), mgr, task)
	if err != nil {
		t.Fatalf("Expected auction success with fallback, got error: %v", err)
	}

	if res.ClusterID != "cluster-2" {
		t.Errorf("Expected cluster-2 to win after cluster-1 resigned, got: %s", res.ClusterID)
	}

	// RankingFallback must create exactly ONE auction session
	if len(mgr.sessionsCreated) != 1 {
		t.Errorf("RankingFallbackStrategy should create only 1 auction session, created %d", len(mgr.sessionsCreated))
	}

	if len(sess.dispatchedTo) != 2 || sess.dispatchedTo[0] != "cluster-1" || sess.dispatchedTo[1] != "cluster-2" {
		t.Errorf("Expected dispatch sequence [cluster-1, cluster-2], got: %v", sess.dispatchedTo)
	}
}

func TestReauctionStrategy_CreatesNewAuction(t *testing.T) {
	task := protocol.TaskCFP{TaskID: "task-test-reauction"}

	// Round 1: auction #1 (cluster-1 wins and resigns)
	sess1 := &mockAuctionSession{
		bids: []protocol.BidPayload{
			{ClusterID: "cluster-1", Score: 1000},
			{ClusterID: "cluster-2", Score: 800},
		},
		ackResponses: map[string]protocol.AckPayload{
			"cluster-1": {Status: protocol.AckStatusResigned, Reason: "overloaded"},
		},
	}

	// Round 2: auction #2 (brand new auction session, cluster-2 wins)
	sess2 := &mockAuctionSession{
		bids: []protocol.BidPayload{
			{ClusterID: "cluster-2", Score: 800},
		},
		ackResponses: map[string]protocol.AckPayload{
			"cluster-2": {Status: protocol.AckStatusAccepted},
		},
		result: &protocol.TaskResult{
			ClusterID:  "cluster-2",
			Status:     "Succeeded",
			DurationMs: 150,
		},
	}

	mgr := &mockAuctionManager{
		sessionsQueue: []*mockAuctionSession{sess1, sess2},
	}

	strat := &ReauctionStrategy{
		BiddingDuration: 10 * time.Millisecond,
		AckTimeout:      100 * time.Millisecond,
		ExecTimeout:     100 * time.Millisecond,
		MaxAttempts:     3,
	}

	res, err := strat.RunAuction(context.Background(), mgr, task)
	if err != nil {
		t.Fatalf("Expected re-auction to succeed on round 2, got: %v", err)
	}

	if res.ClusterID != "cluster-2" {
		t.Errorf("Expected cluster-2 to win in round 2, got: %s", res.ClusterID)
	}

	// Key assertion: Re-auction MUST create a new auction session!
	if len(mgr.sessionsCreated) != 2 {
		t.Fatalf("Expected exactly 2 separate auction sessions to be created, got: %d", len(mgr.sessionsCreated))
	}

	// Verify auction IDs: the second round must have a distinct auction ID
	if mgr.sessionsCreated[0].taskID == mgr.sessionsCreated[1].taskID {
		t.Errorf("Expected distinct auction IDs for consecutive rounds, but both had: %s", mgr.sessionsCreated[0].taskID)
	}

	// Both sessions must have cleanup invoked
	if mgr.cleanupsCalled != 2 {
		t.Errorf("Expected 2 cleanups (old session destroyed, new session destroyed upon completion), got: %d", mgr.cleanupsCalled)
	}

	// Client-facing result must retain the original TaskID
	if res.TaskID != task.TaskID {
		t.Errorf("Result TaskID mismatch: expected %s, got %s", task.TaskID, res.TaskID)
	}
}

func TestResolveStrategy(t *testing.T) {
	s1 := ResolveStrategy("fallback", nil)
	if s1.Name() != protocol.StrategyRankingFallback {
		t.Errorf("Expected fallback, got %s", s1.Name())
	}

	s2 := ResolveStrategy("reauction", nil)
	if s2.Name() != protocol.StrategyReauction {
		t.Errorf("Expected reauction, got %s", s2.Name())
	}

	s3 := ResolveStrategy("unknown", nil)
	if s3.Name() != protocol.StrategyRankingFallback {
		t.Errorf("Expected default to fallback, got %s", s3.Name())
	}
}
