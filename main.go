package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type ClusterEventPayload struct {
	ClusterID string    `json:"cluster_id"`
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"type"`
	Reason    string    `json:"reason"`
	Component string    `json:"component"`
	Message   string    `json:"message"`
	Object    string    `json:"object"`
	Namespace string    `json:"namespace"`
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

func main() {
	clusterID := getEnv("CLUSTER_ID", "rpi-dryrun-1")
	mqttBroker := getEnv("MQTT_BROKER", "tcp://localhost:1883")
	mqttTopic := fmt.Sprintf("k8s/%s/control-plane/events", clusterID)

	log.Printf("[INFO] Start agenta Control Plane dla: %s", clusterID)
	log.Printf("[INFO] Broker MQTT: %s | Topic: %s", mqttBroker, mqttTopic)

	opts := mqtt.NewClientOptions().AddBroker(mqttBroker)
	opts.SetClientID(fmt.Sprintf("cp-agent-%s", clusterID))
	opts.SetAutoReconnect(true)
	opts.SetKeepAlive(60 * time.Second)

	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("[FATAL] MQTT connect error: %v", token.Error())
	}
	log.Println("[INFO] Połączono z brokerem MQTT!")

	config, err := rest.InClusterConfig()
	if err != nil {
		kubeconfig := getEnv("KUBECONFIG", os.Getenv("HOME")+"/.kube/config")
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			log.Fatalf("[FATAL] K8s config error: %v", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("[FATAL] K8s client error: %v", err)
	}

	for {
		watcher, err := clientset.CoreV1().Events("").Watch(context.Background(), metav1.ListOptions{})
		if err != nil {
			log.Printf("[ERROR] Watcher error: %v, ponawiam za 5s...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		for event := range watcher.ResultChan() {
			if event.Type != watch.Added && event.Type != watch.Modified {
				continue
			}

			k8sEvent, ok := event.Object.(*corev1.Event)
			if !ok {
				continue
			}

			payload := ClusterEventPayload{
				ClusterID: clusterID,
				Timestamp: time.Now().UTC(),
				Type:      k8sEvent.Type,
				Reason:    k8sEvent.Reason,
				Component: k8sEvent.Source.Component,
				Message:   k8sEvent.Message,
				Object:    fmt.Sprintf("%s/%s", k8sEvent.InvolvedObject.Kind, k8sEvent.InvolvedObject.Name),
				Namespace: k8sEvent.Namespace,
			}

			data, err := json.Marshal(payload)
			if err != nil {
				continue
			}

			token := mqttClient.Publish(mqttTopic, 1, false, data)
			token.Wait()

			log.Printf("[SENT MQTT] [%s] %s: %s", payload.Type, payload.Object, payload.Message)
		}
		log.Println("[WARN] Watcher przerwany, wznawianie...")
	}
}
