package strategy

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"k8s-mqtt-agent/pkg/protocol"
)

var (
	ErrNoValidBids       = errors.New("no valid bids received")
	ErrAllClustersFailed = errors.New("all candidate clusters resigned or timed out")
	ErrMaxAttempts       = errors.New("max re-auction attempts exceeded")
)

// AuctionSession defines the transport and orchestration primitives for a single active auction session.
type AuctionSession interface {
	TaskID() string
	PublishCFP(ctx context.Context, task protocol.TaskCFP) error
	CollectBids(ctx context.Context, duration time.Duration) ([]protocol.BidPayload, error)
	Dispatch(ctx context.Context, clusterID string, task protocol.TaskCFP) error
	AwaitAck(ctx context.Context, clusterID string, timeout time.Duration) (protocol.AckPayload, error)
	AwaitResult(ctx context.Context, timeout time.Duration) (*protocol.TaskResult, error)
}

// AuctionManager allows auctioneer strategies to create new, independent auction sessions.
type AuctionManager interface {
	NewAuction(ctx context.Context, task protocol.TaskCFP) (AuctionSession, func(), error)
}

// AuctioneerStrategy represents a pluggable decision-making algorithm for assigning tasks
// and reacting to cluster acceptance (ACK) or resignation (RES).
type AuctioneerStrategy interface {
	Name() string
	RunAuction(ctx context.Context, mgr AuctionManager, task protocol.TaskCFP) (*protocol.TaskResult, error)
}

// FilterAndSortBids removes bids from OriginClusterID and sorts the remaining bids descending by Score.
func FilterAndSortBids(bids []protocol.BidPayload, originClusterID string) []protocol.BidPayload {
	var valid []protocol.BidPayload
	for _, b := range bids {
		if originClusterID != "" && b.ClusterID == originClusterID {
			continue
		}
		valid = append(valid, b)
	}

	sort.SliceStable(valid, func(i, j int) bool {
		return valid[i].Score > valid[j].Score
	})
	return valid
}

// RankingFallbackStrategy creates a single auction and ranks all participating clusters by score.
// If the best cluster resigns (RES) or fails to acknowledge, it immediately falls back to the next
// best candidate from the collected ranking without creating a new auction.
type RankingFallbackStrategy struct {
	BiddingDuration time.Duration
	AckTimeout      time.Duration
	ExecTimeout     time.Duration
}

func NewRankingFallbackStrategy() *RankingFallbackStrategy {
	return &RankingFallbackStrategy{
		BiddingDuration: protocol.DefaultBiddingDuration,
		AckTimeout:      protocol.DefaultAckTimeout,
		ExecTimeout:     protocol.DefaultExecutionTimeout,
	}
}

func (s *RankingFallbackStrategy) Name() string {
	return protocol.StrategyRankingFallback
}

func (s *RankingFallbackStrategy) RunAuction(ctx context.Context, mgr AuctionManager, task protocol.TaskCFP) (*protocol.TaskResult, error) {
	log.Printf("[STRATEGY: %s] Starting auction for task %s", s.Name(), task.TaskID)

	session, cleanup, err := mgr.NewAuction(ctx, task)
	if err != nil {
		return nil, fmt.Errorf("failed to create auction session: %w", err)
	}
	defer cleanup()

	if err := session.PublishCFP(ctx, task); err != nil {
		return nil, fmt.Errorf("failed to publish CFP: %w", err)
	}

	rawBids, err := session.CollectBids(ctx, s.BiddingDuration)
	if err != nil {
		return nil, fmt.Errorf("failed to collect bids: %w", err)
	}

	candidates := FilterAndSortBids(rawBids, task.OriginClusterID)
	if len(candidates) == 0 {
		log.Printf("[AUCTION FAILED] Task %s: no valid bids received", task.TaskID)
		return nil, fmt.Errorf("%w for task %s", ErrNoValidBids, task.TaskID)
	}

	log.Printf("[STRATEGY: %s] Task %s: collected %d candidate bids (ranking generated)", s.Name(), task.TaskID, len(candidates))

	for rank, candidate := range candidates {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		log.Printf("[DISPATCHING] Task: %s | Rank: #%d/%d | Candidate: %s | Score: %d",
			task.TaskID, rank+1, len(candidates), candidate.ClusterID, candidate.Score)

		if err := session.Dispatch(ctx, candidate.ClusterID, task); err != nil {
			log.Printf("[DISPATCH ERROR] Task %s -> %s: %v, trying next candidate...", task.TaskID, candidate.ClusterID, err)
			continue
		}

		ack, err := session.AwaitAck(ctx, candidate.ClusterID, s.AckTimeout)
		if err != nil {
			log.Printf("[ACK TIMEOUT] Task %s | Cluster %s did not send ACK within %v: %v. Falling back to next candidate...",
				task.TaskID, candidate.ClusterID, s.AckTimeout, err)
			continue
		}

		if ack.Status == protocol.AckStatusResigned {
			log.Printf("[RESIGNATION RECEIVED] Task %s | Cluster %s resigned (reason: %q). Falling back to next candidate...",
				task.TaskID, candidate.ClusterID, ack.Reason)
			continue
		}

		if ack.Status != protocol.AckStatusAccepted {
			log.Printf("[UNKNOWN ACK STATUS] Task %s | Cluster %s sent unexpected status %q. Trying next candidate...",
				task.TaskID, candidate.ClusterID, ack.Status)
			continue
		}

		log.Printf("[TASK ACCEPTED] Task %s confirmed by %s (ACK received). Awaiting execution result...",
			task.TaskID, candidate.ClusterID)

		res, err := session.AwaitResult(ctx, s.ExecTimeout)
		if err != nil {
			log.Printf("[EXECUTION TIMEOUT/ERROR] Task %s on cluster %s: %v", task.TaskID, candidate.ClusterID, err)
			return nil, err
		}
		return res, nil
	}

	return nil, fmt.Errorf("%w: all %d candidates resigned/failed for task %s",
		ErrAllClustersFailed, len(candidates), task.TaskID)
}

// ReauctionStrategy selects the highest scoring bidder. If that cluster resigns (RES) or fails
// to acknowledge, the dispatcher creates a brand new auction session and issues a fresh CFP.
type ReauctionStrategy struct {
	BiddingDuration time.Duration
	AckTimeout      time.Duration
	ExecTimeout     time.Duration
	MaxAttempts     int
}

func NewReauctionStrategy() *ReauctionStrategy {
	return &ReauctionStrategy{
		BiddingDuration: protocol.DefaultBiddingDuration,
		AckTimeout:      protocol.DefaultAckTimeout,
		ExecTimeout:     protocol.DefaultExecutionTimeout,
		MaxAttempts:     protocol.DefaultMaxReauctions,
	}
}

func (s *ReauctionStrategy) Name() string {
	return protocol.StrategyReauction
}

func (s *ReauctionStrategy) RunAuction(ctx context.Context, mgr AuctionManager, task protocol.TaskCFP) (*protocol.TaskResult, error) {
	maxAttempts := s.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = protocol.DefaultMaxReauctions
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Re-auction creates a brand new auction session. Subsequent rounds receive a unique auction ID.
		attemptTask := task
		if attempt > 1 {
			attemptTask.TaskID = fmt.Sprintf("%s-r%d", task.TaskID, attempt)
		}

		log.Printf("[STRATEGY: %s] Task %s: creating new auction (round %d/%d, auction ID: %s)",
			s.Name(), task.TaskID, attempt, maxAttempts, attemptTask.TaskID)

		res, resigned, err := s.executeSingleAuction(ctx, mgr, attemptTask)
		if err != nil && !resigned {
			return nil, err
		}

		if !resigned && res != nil {
			// Restore the original task ID in the result
			res.TaskID = task.TaskID
			return res, nil
		}

		log.Printf("[RE-AUCTION TRIGGERED] Auction %s round %d failed due to resignation/timeout. Next round will create a new auction...",
			task.TaskID, attempt)
	}

	return nil, fmt.Errorf("%w: task %s was not accepted after %d auction rounds",
		ErrMaxAttempts, task.TaskID, maxAttempts)
}

func (s *ReauctionStrategy) executeSingleAuction(ctx context.Context, mgr AuctionManager, task protocol.TaskCFP) (*protocol.TaskResult, bool, error) {
	session, cleanup, err := mgr.NewAuction(ctx, task)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create auction session: %w", err)
	}
	defer cleanup()

	if err := session.PublishCFP(ctx, task); err != nil {
		return nil, false, fmt.Errorf("failed to publish CFP: %w", err)
	}

	rawBids, err := session.CollectBids(ctx, s.BiddingDuration)
	if err != nil {
		return nil, false, fmt.Errorf("failed to collect bids: %w", err)
	}

	candidates := FilterAndSortBids(rawBids, task.OriginClusterID)
	if len(candidates) == 0 {
		log.Printf("[AUCTION ROUND FAILED] Auction %s: no valid bids received", task.TaskID)
		return nil, true, fmt.Errorf("%w for auction %s", ErrNoValidBids, task.TaskID)
	}

	bestBid := candidates[0]
	log.Printf("[ROUND WINNER SELECTED] Auction: %s | Winner: %s | Score: %d",
		task.TaskID, bestBid.ClusterID, bestBid.Score)

	if err := session.Dispatch(ctx, bestBid.ClusterID, task); err != nil {
		log.Printf("[DISPATCH ERROR] Auction %s -> %s: %v", task.TaskID, bestBid.ClusterID, err)
		return nil, true, err
	}

	ack, err := session.AwaitAck(ctx, bestBid.ClusterID, s.AckTimeout)
	if err != nil {
		log.Printf("[ACK TIMEOUT] Auction %s | Cluster %s did not send ACK within %v: %v",
			task.TaskID, bestBid.ClusterID, s.AckTimeout, err)
		return nil, true, err
	}

	if ack.Status == protocol.AckStatusResigned {
		log.Printf("[RESIGNATION RECEIVED] Auction %s | Cluster %s resigned (reason: %q)",
			task.TaskID, bestBid.ClusterID, ack.Reason)
		return nil, true, nil
	}

	if ack.Status != protocol.AckStatusAccepted {
		log.Printf("[UNKNOWN ACK STATUS] Auction %s | Cluster %s sent unexpected status %q",
			task.TaskID, bestBid.ClusterID, ack.Status)
		return nil, true, fmt.Errorf("unexpected ack status: %s", ack.Status)
	}

	log.Printf("[TASK ACCEPTED] Auction %s confirmed by %s (ACK received). Awaiting execution result...",
		task.TaskID, bestBid.ClusterID)

	res, err := session.AwaitResult(ctx, s.ExecTimeout)
	if err != nil {
		log.Printf("[EXECUTION TIMEOUT/ERROR] Auction %s on cluster %s: %v", task.TaskID, bestBid.ClusterID, err)
		return nil, false, err
	}

	return res, false, nil
}

// ResolveStrategy returns an appropriate strategy instance based on the strategy name.
// Supported names (case-insensitive):
// - "fallback", "ranking": RankingFallbackStrategy
// - "reauction", "retry": ReauctionStrategy
// - "roundrobin", "random": BaselineStrategy over the given clusters (benchmarks only)
func ResolveStrategy(name string, clusters []string) AuctioneerStrategy {
	normalized := strings.ToLower(strings.TrimSpace(name))
	switch normalized {
	case "reauction", "retry":
		return NewReauctionStrategy()
	case protocol.StrategyRoundRobin:
		return NewRoundRobinStrategy(clusters)
	case protocol.StrategyRandom:
		return NewRandomStrategy(clusters)
	case "fallback", "ranking":
		fallthrough
	default:
		return NewRankingFallbackStrategy()
	}
}
