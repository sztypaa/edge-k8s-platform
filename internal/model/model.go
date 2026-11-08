package model

import (
	"sync"

	"k8s-mqtt-agent/internal/config"
)

type TaskCFP struct {
	TaskID      string   `json:"task_id"`
	Image       string   `json:"image"`
	Command     []string `json:"command"`
	MinCPUMilli int64    `json:"min_cpu_milli"`
	MinMemMiB   int64    `json:"min_mem_mib"`
}

type BidPayload struct {
	TaskID        string `json:"task_id"`
	ClusterID     string `json:"cluster_id"`
	AvailCPUMilli int64  `json:"avail_cpu_milli"`
	AvailMemMiB   int64  `json:"avail_mem_mib"`
	ImageCached   bool   `json:"image_cached"`
	Score         int64  `json:"score"`
}

type TaskResult struct {
	TaskID     string `json:"task_id"`
	ClusterID  string `json:"cluster_id"`
	Status     string `json:"status"`
	DurationMs int64  `json:"duration_ms"`
}

type ClusterResources struct {
	AvailCPUMilli int64
	AvailMemMiB   int64
	ImageCached   bool
}

func (r ClusterResources) CanFit(task TaskCFP) bool {
	return r.AvailCPUMilli >= task.MinCPUMilli && r.AvailMemMiB >= task.MinMemMiB
}

func (r ClusterResources) CalculateScore() int64 {
	score := r.AvailCPUMilli
	if r.ImageCached {
		score += config.CachedImageBonus
	}
	return score
}

type ReservationTracker struct {
	mu    sync.Mutex
	tasks map[string]TaskCFP
}

func NewReservationTracker() *ReservationTracker {
	return &ReservationTracker{
		tasks: make(map[string]TaskCFP),
	}
}

func (r *ReservationTracker) Reserve(task TaskCFP) {
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
