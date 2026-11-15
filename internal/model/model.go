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

// ClusterResources is contains data used for calculation of score by [ClusterResources.CalculateScore]
type ClusterResources struct {
	AvailCPUMilli int64	// available CPU millis
	AvailMemMiB   int64	// available memory in MiB
	ImageCached   bool	// is image cached on cluster
}

// CanFit checks if task specified by [protocol.TaskCFP] can fit in cluster
func (r ClusterResources) CanFit(task protocol.TaskCFP) bool {
	return r.AvailCPUMilli >= task.MinCPUMilli && r.AvailMemMiB >= task.MinMemMiB
}

// CalculateScore calculates score used in auction
func (r ClusterResources) CalculateScore() int64 {
	score := r.AvailCPUMilli
	if r.ImageCached {
		score += CachedImageBonus
	}
	return score
}

// ReservationTracker is used to keep track how much resources each task requires
type ReservationTracker struct {
	mu    sync.Mutex					// mutex
	tasks map[string]protocol.TaskCFP	// map of [protocol.TaskCFP]
}

// NewReservationTracker creates new [ReservationTracker]
func NewReservationTracker() *ReservationTracker {
	return &ReservationTracker{
		tasks: make(map[string]protocol.TaskCFP),
	}
}

// Reserve adds task to [ReservationTracker]
func (r *ReservationTracker) Reserve(task protocol.TaskCFP) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tasks[task.TaskID] = task
}

// Release removes task specified by taskID from [ReservationTracker]
func (r *ReservationTracker) Release(taskID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tasks, taskID)
}

// ReserveTotals returns total CPU millis and memory reserved by [ReservationTracker]
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
