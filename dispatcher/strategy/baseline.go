package strategy

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"k8s-mqtt-agent/pkg/protocol"
)

var ErrNoClusters = errors.New("no clusters configured (CLUSTERS)")

// BaselineStrategy assigns tasks without an auction, using a fixed list of clusters.
// It is used in benchmarks as a reference for the auction (round-robin or random order).
// On resignation (RES) or ACK timeout the next cluster from the list is tried.
type BaselineStrategy struct {
	name        string
	clusters    []string
	AckTimeout  time.Duration
	ExecTimeout time.Duration

	mu   sync.Mutex
	next int
	rng  *rand.Rand
}

func NewRoundRobinStrategy(clusters []string) *BaselineStrategy {
	return &BaselineStrategy{
		name:        protocol.StrategyRoundRobin,
		clusters:    clusters,
		AckTimeout:  protocol.DefaultAckTimeout,
		ExecTimeout: protocol.DefaultExecutionTimeout,
	}
}

func NewRandomStrategy(clusters []string) *BaselineStrategy {
	return &BaselineStrategy{
		name:        protocol.StrategyRandom,
		clusters:    clusters,
		AckTimeout:  protocol.DefaultAckTimeout,
		ExecTimeout: protocol.DefaultExecutionTimeout,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *BaselineStrategy) Name() string {
	return s.name
}

func (s *BaselineStrategy) order() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := len(s.clusters)
	order := make([]string, 0, n)
	if s.rng != nil {
		for _, i := range s.rng.Perm(n) {
			order = append(order, s.clusters[i])
		}
		return order
	}

	for i := 0; i < n; i++ {
		order = append(order, s.clusters[(s.next+i)%n])
	}
	s.next = (s.next + 1) % n
	return order
}

func (s *BaselineStrategy) RunAuction(ctx context.Context, mgr AuctionManager, task protocol.TaskCFP) (*protocol.TaskResult, error) {
	if len(s.clusters) == 0 {
		return nil, ErrNoClusters
	}

	session, cleanup, err := mgr.NewAuction(ctx, task)
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %w", err)
	}
	defer cleanup()

	for _, clusterID := range s.order() {
		if clusterID == task.OriginClusterID {
			continue
		}

		log.Printf("[DISPATCHING] Task: %s | Strategy: %s | Cluster: %s", task.TaskID, s.name, clusterID)
		if err := session.Dispatch(ctx, clusterID, task); err != nil {
			log.Printf("[DISPATCH ERROR] Task %s -> %s: %v, trying next cluster...", task.TaskID, clusterID, err)
			continue
		}

		ack, err := session.AwaitAck(ctx, clusterID, s.AckTimeout)
		if err != nil {
			log.Printf("[ACK TIMEOUT] Task %s | Cluster %s: %v, trying next cluster...", task.TaskID, clusterID, err)
			continue
		}
		if ack.Status != protocol.AckStatusAccepted {
			log.Printf("[RESIGNATION RECEIVED] Task %s | Cluster %s resigned (reason: %q), trying next cluster...",
				task.TaskID, clusterID, ack.Reason)
			continue
		}

		return session.AwaitResult(ctx, s.ExecTimeout)
	}

	return nil, fmt.Errorf("%w: task %s", ErrAllClustersFailed, task.TaskID)
}
