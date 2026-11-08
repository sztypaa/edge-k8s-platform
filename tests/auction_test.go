package tests

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestAuctionScenarios(t *testing.T) {
	h := NewTestHarness(t)

	cases := []AuctionTestCase{
		{
			Name:    "Default task parameters fallback",
			Request: TaskCFP{},
			Expectations: []Expectation{
				ExpectAuctionSuccess(),
				ExpectPodStatus("Succeeded"),
				ExpectBidScoresMatchFormula(CurrentDefaultScoreFormula),
				ExpectHighestBidderWins(),
			},
		},
		{
			Name: "Task with custom CPU/RAM requirements and custom command",
			Request: TaskCFP{
				Image:       "busybox:latest",
				Command:     []string{"sh", "-c", "echo 'Starting test'; uname -a; sleep 1; echo 'Done'"},
				MinCPUMilli: 150,
				MinMemMiB:   64,
			},
			Expectations: []Expectation{
				ExpectAuctionSuccess(),
				ExpectPodStatus("Succeeded"),
				ExpectBidsMeetMinimumResources(),
				ExpectBidScoresMatchFormula(CurrentDefaultScoreFormula),
				ExpectHighestBidderWins(),
			},
		},
		{
			Name: "Task failing inside container with non-zero exit code",
			Request: TaskCFP{
				Image:       "busybox:latest",
				Command:     []string{"sh", "-c", "echo 'Simulating process failure'; exit 1"},
				MinCPUMilli: 100,
				MinMemMiB:   64,
			},
			Expectations: []Expectation{
				ExpectAuctionSuccess(),
				ExpectPodStatus("Failed"),
				ExpectBidsMeetMinimumResources(),
				ExpectHighestBidderWins(),
			},
		},
		{
			Name: "Auction rejected when requested CPU exceeds cluster capacity",
			Request: TaskCFP{
				Image:       "busybox:latest",
				Command:     []string{"sh", "-c", "echo 'Should not run'"},
				MinCPUMilli: 999_000,
				MinMemMiB:   64,
			},
			Expectations: []Expectation{
				ExpectAuctionRejected(),
				func(t *testing.T, trace *AuctionTrace) {
					if len(trace.Bids) > 0 {
						t.Errorf("Expected 0 bids for 999 CPU cores, got %d bids: %+v", len(trace.Bids), trace.Bids)
					}
				},
			},
		},
		{
			Name: "Auction rejected when requested memory exceeds cluster capacity",
			Request: TaskCFP{
				Image:       "busybox:latest",
				Command:     []string{"sh", "-c", "echo 'Should not run'"},
				MinCPUMilli: 100,
				MinMemMiB:   1_048_576,
			},
			Expectations: []Expectation{
				ExpectAuctionRejected(),
				func(t *testing.T, trace *AuctionTrace) {
					if len(trace.Bids) > 0 {
						t.Errorf("Expected 0 bids for 1 TiB RAM, got %d bids: %+v", len(trace.Bids), trace.Bids)
					}
				},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			h.RunCase(t, tc)
		})
	}
}

func TestOriginClusterExclusion(t *testing.T) {
	h := NewTestHarness(t)

	probeTrace := h.RunCase(t, AuctionTestCase{
		Name: "Probe active clusters",
		Request: TaskCFP{
			Image:       "busybox:latest",
			Command:     []string{"sh", "-c", "true"},
			MinCPUMilli: 50,
			MinMemMiB:   32,
		},
		Expectations: []Expectation{
			ExpectAuctionSuccess(),
			ExpectPodStatus("Succeeded"),
		},
	})

	originCluster := probeTrace.Result.ClusterID
	t.Logf("Detected primary cluster to exclude (OriginClusterID): %s (bids in probe: %d)",
		originCluster, len(probeTrace.Bids))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	offloadTrace, err := h.SubmitHTTP(ctx, TaskCFP{
		OriginClusterID: originCluster,
		Image:           "busybox:latest",
		Command:         []string{"sh", "-c", "echo 'Offloaded task'; sleep 1"},
		MinCPUMilli:     50,
		MinMemMiB:       32,
	})
	if err != nil {
		t.Fatalf("HTTP request failed: %v", err)
	}

	ExpectExcludedClusterNotChosen(originCluster)(t, offloadTrace)

	otherClusters := 0
	for _, b := range offloadTrace.Bids {
		if b.ClusterID != originCluster {
			otherClusters++
		}
	}

	if otherClusters > 0 {
		ExpectAuctionSuccess()(t, offloadTrace)
		ExpectPodStatus("Succeeded")(t, offloadTrace)
		ExpectHighestBidderWins()(t, offloadTrace)
		t.Logf("Offloading succeeded: task from %s was executed by cluster %s", originCluster, offloadTrace.Result.ClusterID)
	} else if offloadTrace.HTTPStatus == http.StatusServiceUnavailable {
		t.Logf("Only 1 cluster (%s) responded; its bid was ignored and the auction was rejected with HTTP 503 as expected.", originCluster)
	}
}

func TestImageCacheBonus(t *testing.T) {
	h := NewTestHarness(t)

	firstTrace := h.RunCase(t, AuctionTestCase{
		Name: "Warm up image cache (busybox:latest)",
		Request: TaskCFP{
			Image:       "busybox:latest",
			Command:     []string{"sh", "-c", "echo 'Warmup cache'"},
			MinCPUMilli: 50,
			MinMemMiB:   32,
		},
		Expectations: []Expectation{
			ExpectAuctionSuccess(),
			ExpectPodStatus("Succeeded"),
		},
	})

	warmedCluster := firstTrace.Result.ClusterID

	h.RunCase(t, AuctionTestCase{
		Name: "Verify ImageCached=true and score bonus",
		Request: TaskCFP{
			Image:       "busybox:latest",
			Command:     []string{"sh", "-c", "echo 'Cache hit test'"},
			MinCPUMilli: 50,
			MinMemMiB:   32,
		},
		Expectations: []Expectation{
			ExpectAuctionSuccess(),
			ExpectPodStatus("Succeeded"),
			ExpectBidScoresMatchFormula(CurrentDefaultScoreFormula),
			ExpectHighestBidderWins(),
			func(t *testing.T, trace *AuctionTrace) {
				if len(trace.Bids) == 0 {
					t.Skip("MQTT observer unavailable, skipping ImageCached field verification")
				}
				found := false
				for _, b := range trace.Bids {
					if b.ClusterID == warmedCluster {
						found = true
						if !b.ImageCached {
							t.Errorf("Cluster %s previously ran busybox:latest, but reported ImageCached=false in the subsequent auction", warmedCluster)
						}
					}
				}
				if !found {
					t.Logf("Note: cluster %s did not submit a bid in the second run", warmedCluster)
				}
			},
		},
	})
}

func TestConcurrentLoadAndReservation(t *testing.T) {
	h := NewTestHarness(t)

	const reservedCPU int64 = 350
	const reservedMem int64 = 128

	reqs := []TaskCFP{
		{
			TaskID:      fmt.Sprintf("test-conc-1-%d", time.Now().UnixNano()/1e6),
			Image:       "busybox:latest",
			Command:     []string{"sh", "-c", "sleep 4"},
			MinCPUMilli: reservedCPU,
			MinMemMiB:   reservedMem,
		},
		{
			TaskID:      fmt.Sprintf("test-conc-2-%d", time.Now().UnixNano()/1e6),
			Image:       "busybox:latest",
			Command:     []string{"sh", "-c", "sleep 4"},
			MinCPUMilli: reservedCPU,
			MinMemMiB:   reservedMem,
		},
		{
			TaskID:      fmt.Sprintf("test-conc-3-%d", time.Now().UnixNano()/1e6),
			Image:       "busybox:latest",
			Command:     []string{"sh", "-c", "sleep 2"},
			MinCPUMilli: reservedCPU,
			MinMemMiB:   reservedMem,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	traces, errs := h.SubmitParallel(ctx, reqs, 1500*time.Millisecond)

	clusterWins := make(map[string]int)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Parallel task #%d failed with communication error: %v", i+1, err)
		}
		tr := traces[i]
		ExpectAuctionSuccess()(t, tr)
		ExpectPodStatus("Succeeded")(t, tr)
		ExpectBidsMeetMinimumResources()(t, tr)
		ExpectBidScoresMatchFormula(CurrentDefaultScoreFormula)(t, tr)
		ExpectHighestBidderWins()(t, tr)

		clusterWins[tr.Result.ClusterID]++
		t.Logf("[Task #%d] Winner=%s | Duration=%dms | Bids=%d",
			i+1, tr.Result.ClusterID, tr.Result.DurationMs, len(tr.Bids))
	}

	t.Logf("Task distribution across clusters: %+v", clusterWins)

	if len(traces[0].Bids) > 0 && len(traces[1].Bids) > 0 {
		firstWinner := traces[0].Result.ClusterID
		var bid1, bid2 *BidPayload
		for i := range traces[0].Bids {
			if traces[0].Bids[i].ClusterID == firstWinner {
				bid1 = &traces[0].Bids[i]
			}
		}
		for i := range traces[1].Bids {
			if traces[1].Bids[i].ClusterID == firstWinner {
				bid2 = &traces[1].Bids[i]
			}
		}

		if bid1 != nil && bid2 != nil {
			t.Logf("Cluster %s resources: before reservation (task #1) AvailCPU=%dm, AvailMem=%dMiB -> during execution (task #2) AvailCPU=%dm, AvailMem=%dMiB",
				firstWinner, bid1.AvailCPUMilli, bid1.AvailMemMiB, bid2.AvailCPUMilli, bid2.AvailMemMiB)
			if bid2.AvailMemMiB >= bid1.AvailMemMiB && bid2.AvailCPUMilli >= bid1.AvailCPUMilli {
				t.Errorf("Expected available resources on cluster %s to decrease during task #1 execution (ReservationTracker), but they did not (Bid1: %+v, Bid2: %+v)",
					firstWinner, *bid1, *bid2)
			}
		}
	}
}

func TestMQTTSubmission(t *testing.T) {
	h := NewTestHarness(t)
	if h.mqttClient == nil || !h.mqttClient.IsConnected() {
		t.Skip("Direct MQTT connection unavailable, skipping MQTT task submission test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	trace, err := h.SubmitMQTT(ctx, TaskCFP{
		Image:       "busybox:latest",
		Command:     []string{"sh", "-c", "echo 'Task submitted via MQTT'; sleep 1"},
		MinCPUMilli: 100,
		MinMemMiB:   64,
	})
	if err != nil {
		t.Fatalf("Failed to execute task submitted via MQTT: %v", err)
	}

	ExpectAuctionSuccess()(t, trace)
	ExpectPodStatus("Succeeded")(t, trace)
	ExpectBidsMeetMinimumResources()(t, trace)
	ExpectBidScoresMatchFormula(CurrentDefaultScoreFormula)(t, trace)
	ExpectHighestBidderWins()(t, trace)

	t.Logf("MQTT task succeeded on cluster %s in %dms (RTT=%v)",
		trace.Result.ClusterID, trace.Result.DurationMs, trace.RoundTripTime.Round(time.Millisecond))
}
