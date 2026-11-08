package config

import (
	"os"
	"time"
)

const (
	DefaultClusterID  = "rpi-1"
	DefaultMQTTBroker = "tcp://localhost:1883"
	DefaultNamespace  = "default"

	TopicCFP        = "clusterless/auction/cfp"
	TopicBids       = "clusterless/auction/bids"
	TopicResults    = "clusterless/results"
	TopicExecuteFmt = "clusterless/execute/%s"

	CachedImageBonus = 500

	MetricsTimeout    = 3 * time.Second
	ExecutionTimeout  = 2 * time.Minute
	PodPollInterval   = 500 * time.Millisecond
	MQTTRetryInterval = 5 * time.Second
	MQTTKeepAlive     = 60 * time.Second
	MQTTQoS           = 1
)

type Config struct {
	ClusterID  string
	MQTTBroker string
	Kubeconfig string
}

func Load() Config {
	return Config{
		ClusterID:  getEnv("CLUSTER_ID", DefaultClusterID),
		MQTTBroker: getEnv("MQTT_BROKER", DefaultMQTTBroker),
		Kubeconfig: getEnv("KUBECONFIG", os.Getenv("HOME")+"/.kube/config"),
	}
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}
