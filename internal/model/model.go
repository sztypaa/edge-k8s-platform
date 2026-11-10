package model

import (
	"sync"

	"k8s-mqtt-agent/pkg/protocol"
)

const (
	CachedImageBonus = 500
)

// Re-export protocol DTOs as type aliases for backward compatibility within agent packages.
type TaskCFP = protocol.TaskCFP
type BidPayload = protocol.BidPayload
type AckPayload = protocol.AckPayload
type TaskResult = protocol.TaskResult

type ClusterResources struct {
	AvailCPUMilli int64
	AvailMemMiB   int64
	ImageCached   bool
}

func (r ClusterResources) CanFit(task protocol.TaskCFP) bool {
	return r.AvailCPUMilli >= task.MinCPUMilli && r.AvailMemMiB >= task.MinMemMiB
}

func (r ClusterResources) CalculateScore() int64 {
	score := r.AvailCPUMilli
	if r.ImageCached {
		score += CachedImageBonus
	}
	return score
}

type ReservationTracker struct {
	mu    sync.Mutex
	tasks map[string]protocol.TaskCFP
}

func NewReservationTracker() *ReservationTracker {
	return &ReservationTracker{
		tasks: make(map[string]protocol.TaskCFP),
	}
}

func (r *ReservationTracker) Reserve(task protocol.TaskCFP) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tasks[task.TaskID] = task
}

func (r *ReservationTracker) Release(taskID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tasks, taskID)
}

func (r *ReservationTracker) ReservedTotals() (int64, int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var cpu, mem int64
	for _, t := range r.tasks {
		cpu += t.MinCPUMilli
		mem += t.MinMemMiB
	}
	return cpu, mem
}
