package model

import (
	"sync"

	"k8s-mqtt-agent/pkg/protocol"
)

const (
	ScoreWeightCPU   = 0.4
	ScoreWeightMem   = 0.2
	ScoreWeightImage = 0.4
	ScoreScale       = 1000
)

// Re-export protocol DTOs as type aliases for backward compatibility within agent packages.
type TaskCFP = protocol.TaskCFP
type BidPayload = protocol.BidPayload
type AckPayload = protocol.AckPayload
type TaskResult = protocol.TaskResult

// ClusterResources is contains data used for calculation of score by [ClusterResources.CalculateScore]
type ClusterResources struct {
	AvailCPUMilli int64
	AvailMemMiB   int64
	TotalCPUMilli int64
	TotalMemMiB   int64
	ImageCached   bool
}

// CanFit checks if task specified by [protocol.TaskCFP] can fit in cluster
func (r ClusterResources) CanFit(task protocol.TaskCFP) bool {
	return r.AvailCPUMilli >= task.MinCPUMilli && r.AvailMemMiB >= task.MinMemMiB
}

// CalculateScore calculates score used in auction for given task
func (r ClusterResources) CalculateScore(task protocol.TaskCFP) int64 {
	return Score(r.AvailCPUMilli, r.AvailMemMiB, r.TotalCPUMilli, r.TotalMemMiB, r.ImageCached, task)
}

// Score calculates score used in auction for given task
// Score = 1000 * (0.4*cpu + 0.2*mem + 0.4*cached), where cpu and mem are the
// share of the cluster that stays free after the task is placed.
func Score(availCPU, availMem, totalCPU, totalMem int64, imageCached bool, task protocol.TaskCFP) int64 {
	if totalCPU <= 0 || totalMem <= 0 {
		return 0
	}
	cpuRatio := ratio(availCPU-task.MinCPUMilli, totalCPU)
	memRatio := ratio(availMem-task.MinMemMiB, totalMem)

	score := ScoreWeightCPU*cpuRatio + ScoreWeightMem*memRatio
	if imageCached {
		score += ScoreWeightImage
	}
	return int64(score * ScoreScale)
}

// ratio calculates ratio between left and right, capped in [0; 1] range
func ratio(left, total int64) float64 {
	return min(max(float64(left)/float64(total), 0), 1)
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
