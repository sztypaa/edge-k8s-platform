package agent

import (
	"os"
	"time"

	"k8s-mqtt-agent/pkg/protocol"
)

const (
	DefaultClusterID      = "rpi-1"
	DefaultK8sNamespace   = "default"
	DefaultMetricsTimeout = 3 * time.Second
)

type Config struct {
	ClusterID      string
	K8sNamespace   string
	MQTTBroker     string
	Kubeconfig     string
	MetricsTimeout time.Duration
}

func LoadConfig() Config {
	return Config{
		ClusterID:      getEnv("CLUSTER_ID", DefaultClusterID),
		K8sNamespace:   getEnv("K8S_NAMESPACE", DefaultK8sNamespace),
		MQTTBroker:     getEnv("MQTT_BROKER", protocol.DefaultMQTTBroker),
		Kubeconfig:     getEnv("KUBECONFIG", os.Getenv("HOME")+"/.kube/config"),
		MetricsTimeout: DefaultMetricsTimeout,
	}
}

// Load provides backward compatibility for config loading.
func Load() Config {
	return LoadConfig()
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}
