package protocol

import (
	"fmt"
	"time"
)

const (
	// Default platform network settings
	DefaultMQTTBroker = "tcp://localhost:1883"
	DefaultHTTPAddr   = ":8080"

	// Default credentials for MQTT broker authentication
	DefaultMQTTUser = "clusterless_admin"
	DefaultMQTTPass = "zaq1@WSX"

	// MQTT protocol configuration parameters
	MQTTQoS           = 1
	MQTTRetryInterval = 5 * time.Second
	MQTTKeepAlive     = 60 * time.Second

	// Platform MQTT topics
	TopicCFP        = "clusterless/auction/cfp"
	TopicBids       = "clusterless/auction/bids"
	TopicAck        = "clusterless/auction/ack"
	TopicResults    = "clusterless/results"
	TopicExecuteFmt = "clusterless/execute/%s"
	TopicSubmit     = "clusterless/requests/submit"

	// Acknowledgment statuses (two-phase dispatch)
	AckStatusAccepted = "ACK"
	AckStatusResigned = "RES"

	// Task execution outcome statuses (TaskResult.Status)
	TaskStatusSucceeded      = "Succeeded"
	TaskStatusFailed         = "Failed"
	TaskStatusTimeout        = "Timeout"
	TaskStatusFailedToCreate = "FailedToCreate"

	// Auctioneer strategy identifiers
	StrategyRankingFallback = "fallback"
	StrategyReauction       = "reauction"
	StrategyRoundRobin      = "roundrobin"
	StrategyRandom          = "random"
	DefaultStrategy         = StrategyRankingFallback

	// Default auction timing and retry parameters
	DefaultBiddingDuration  = 1 * time.Second
	DefaultAckTimeout       = 3 * time.Second
	DefaultExecutionTimeout = 2 * time.Minute
	DefaultMaxReauctions    = 3

	// Default task requirements used by the dispatcher when the request leaves them empty
	DefaultTaskMinCPUMilli = int64(200)
	DefaultTaskMinMemMiB   = int64(128)
)

// TopicExecute returns the MQTT topic used to dispatch a task to a specific cluster.
func TopicExecute(clusterID string) string {
	return fmt.Sprintf(TopicExecuteFmt, clusterID)
}

// TaskCFP represents a Call For Proposal published by the dispatcher.
type TaskCFP struct {
	TaskID          string   `json:"task_id"`
	OriginClusterID string   `json:"origin_cluster_id,omitempty"`
	Image           string   `json:"image"`
	Command         []string `json:"command"`
	MinCPUMilli     int64    `json:"min_cpu_milli"`
	MinMemMiB       int64    `json:"min_mem_mib"`
}

// BidPayload represents an auction offer submitted by a cluster.
type BidPayload struct {
	TaskID        string `json:"task_id"`
	ClusterID     string `json:"cluster_id"`
	AvailCPUMilli int64  `json:"avail_cpu_milli"`
	AvailMemMiB   int64  `json:"avail_mem_mib"`
	TotalCPUMilli int64  `json:"total_cpu_milli"`
	TotalMemMiB   int64  `json:"total_mem_mib"`
	ImageCached   bool   `json:"image_cached"`
	Score         int64  `json:"score"`
}

// AckPayload represents a confirmation (ACK) or resignation (RES) from the winning cluster.
type AckPayload struct {
	TaskID    string `json:"task_id"`
	ClusterID string `json:"cluster_id"`
	Status    string `json:"status"` // "ACK" | "RES"
	Reason    string `json:"reason,omitempty"`
}

// TaskResult represents the final execution outcome published by the executing cluster.
type TaskResult struct {
	TaskID      string `json:"task_id"`
	ClusterID   string `json:"cluster_id"`
	Status      string `json:"status"`
	DurationMs  int64  `json:"duration_ms"`
	ImageCached bool   `json:"image_cached"`
}
