package strategy

import (
	"context"
	"testing"
	"time"

	"k8s-mqtt-agent/pkg/protocol"
)

func TestRoundRobinStrategy_Rotates(t *testing.T) {
	strat := NewRoundRobinStrategy([]string{"rpi-1", "rpi-2"})
	mgr := &mockAuctionManager{}

	var got []string
	for i := 0; i < 4; i++ {
		task := protocol.TaskCFP{TaskID: "task-rr"}
		mgr.sessionsQueue = []*mockAuctionSession{{
			result: &protocol.TaskResult{TaskID: task.TaskID, Status: "Succeeded"},
		}}
		if _, err := strat.RunAuction(context.Background(), mgr, task); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		got = append(got, mgr.sessionsCreated[i].dispatchedTo[0])
	}

	want := []string{"rpi-1", "rpi-2", "rpi-1", "rpi-2"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Expected order %v, got %v", want, got)
		}
	}
}

func TestRoundRobinStrategy_NextClusterAfterResignation(t *testing.T) {
	task := protocol.TaskCFP{TaskID: "task-rr-res"}
	sess := &mockAuctionSession{
		ackResponses: map[string]protocol.AckPayload{
			"rpi-1": {TaskID: task.TaskID, ClusterID: "rpi-1", Status: protocol.AckStatusResigned, Reason: "busy"},
		},
		result: &protocol.TaskResult{TaskID: task.TaskID, ClusterID: "rpi-2", Status: "Succeeded"},
	}
	mgr := &mockAuctionManager{sessionsQueue: []*mockAuctionSession{sess}}

	strat := NewRoundRobinStrategy([]string{"rpi-1", "rpi-2"})
	strat.AckTimeout = 100 * time.Millisecond

	res, err := strat.RunAuction(context.Background(), mgr, task)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if res.ClusterID != "rpi-2" {
		t.Errorf("Expected rpi-2 after rpi-1 resigned, got %s", res.ClusterID)
	}
	if sess.cfpCalls != 0 {
		t.Errorf("Baseline strategy should not publish CFP, published %d", sess.cfpCalls)
	}
}

func TestBaselineStrategy_NoClusters(t *testing.T) {
	strat := NewRandomStrategy(nil)
	if _, err := strat.RunAuction(context.Background(), &mockAuctionManager{}, protocol.TaskCFP{TaskID: "x"}); err == nil {
		t.Error("Expected error when no clusters are configured")
	}
}
