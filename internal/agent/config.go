package agent

import (
	"os"
	"time"

	"k8s-mqtt-agent/pkg/protocol"
)

const (
	DefaultClusterID      = "rpi-1"			// default ID of Kubernetes cluster
	DefaultK8sNamespace   = "default"		// default Kubernetes namespace
	DefaultMetricsTimeout = 3 * time.Second	// default timeout for fetching resource metrics
)

// Config groups configuration data.
type Config struct {
	ClusterID      string			// ID of Kubernetes cluster
	K8sNamespace   string			// Kubernetes namespace
	MQTTBroker     string			// address of MQTT broker
	Kubeconfig     string			// path to Kubernetes configuration
	MetricsTimeout time.Duration	// specifies timeout for fetching resource metrics
}


// LoadConfig returns [Config] object initialized with default values.
func LoadConfig() Config {
	return Config{
		ClusterID:      getEnv("CLUSTER_ID", DefaultClusterID),
		K8sNamespace:   getEnv("K8S_NAMESPACE", DefaultK8sNamespace),
		MQTTBroker:     getEnv("MQTT_BROKER", protocol.DefaultMQTTBroker),
		Kubeconfig:     getEnv("KUBECONFIG", os.Getenv("HOME")+"/.kube/config"),
		MetricsTimeout: DefaultMetricsTimeout,
	}
}

// Load provides backward compatibility for loading [Config].
func Load() Config {
	return LoadConfig()
}


// getEnv returns value of environmental variable specified by key. If lookup fails fallback is returned. 
func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}
