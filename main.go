package main

import (
	"log"

	"k8s-mqtt-agent/internal/agent"
	"k8s-mqtt-agent/internal/k8s"
)

func main() {
	cfg := agent.LoadConfig()

	clientset, err := k8s.NewClient(cfg.Kubeconfig)
	if err != nil {
		log.Fatalf("[FATAL] %v", err)
	}

	cpAgent := agent.New(cfg, clientset)
	cpAgent.Run(cfg.MQTTBroker)
}