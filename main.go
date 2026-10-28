package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
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
	TaskID      string `json:"task_id"`
	ClusterID   string `json:"cluster_id"`
	Status      string `json:"status"`
	DurationMs  int64  `json:"duration_ms"`
}

type NodeMetricsList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Usage corev1.ResourceList `json:"usage"`
	} `json:"items"`
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

func getClusterResources(ctx context.Context, clientset *kubernetes.Clientset, targetImage string) (int64, int64, bool, error) {
	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, 0, false, err
	}

	rawMetrics, err := clientset.Discovery().RESTClient().Get().AbsPath("/apis/metrics.k8s.io/v1beta1/nodes").DoRaw(ctx)
	if err != nil {
		return 0, 0, false, err
	}

	var metrics NodeMetricsList
	if err := json.Unmarshal(rawMetrics, &metrics); err != nil {
		return 0, 0, false, err
	}

	usageMap := make(map[string]corev1.ResourceList)
	for _, item := range metrics.Items {
		usageMap[item.Metadata.Name] = item.Usage
	}

	var totalAvailCPU int64
	var totalAvailMem int64
	imageCached := false

	for _, node := range nodes.Items {
		allocCPU := node.Status.Allocatable.Cpu().MilliValue()
		allocMem := node.Status.Allocatable.Memory().Value()

		var usedCPU int64
		var usedMem int64
		if usage, ok := usageMap[node.Name]; ok {
			usedCPU = usage.Cpu().MilliValue()
			usedMem = usage.Memory().Value()
		}

		totalAvailCPU += (allocCPU - usedCPU)
		totalAvailMem += (allocMem - usedMem) / (1024 * 1024)

		for _, img := range node.Status.Images {
			for _, name := range img.Names {
				if strings.Contains(name, targetImage) {
					imageCached = true
				}
			}
		}
	}

	return totalAvailCPU, totalAvailMem, imageCached, nil
}

func executeFunctionPod(ctx context.Context, clientset *kubernetes.Clientset, task TaskCFP) (string, int64) {
	startTime := time.Now()
	podName := fmt.Sprintf("fn-%s", strings.ToLower(task.TaskID))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:            "worker",
					Image:           task.Image,
					ImagePullPolicy: corev1.PullIfNotPresent,
					Command:         task.Command,
				},
			},
		},
	}

	_, err := clientset.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		log.Printf("[ERROR] Failed to create pod %s: %v", podName, err)
		return "FailedToCreate", 0
	}

	defer func() {
		_ = clientset.CoreV1().Pods("default").Delete(context.Background(), podName, metav1.DeleteOptions{})
	}()

	for {
		select {
		case <-ctx.Done():
			return "Timeout", time.Since(startTime).Milliseconds()
		case <-time.After(500 * time.Millisecond):
			p, err := clientset.CoreV1().Pods("default").Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				continue
			}
			if p.Status.Phase == corev1.PodSucceeded {
				return "Succeeded", time.Since(startTime).Milliseconds()
			}
			if p.Status.Phase == corev1.PodFailed {
				return "Failed", time.Since(startTime).Milliseconds()
			}
		}
	}
}

func main() {
	clusterID := getEnv("CLUSTER_ID", "rpi-1")
	mqttBroker := getEnv("MQTT_BROKER", "tcp://localhost:1883")

	opts := mqtt.NewClientOptions()
	brokerURL, err := url.Parse(mqttBroker)
	if err == nil && brokerURL.User != nil {
		opts.AddBroker(fmt.Sprintf("%s://%s", brokerURL.Scheme, brokerURL.Host))
		opts.SetUsername(brokerURL.User.Username())
		password, _ := brokerURL.User.Password()
		opts.SetPassword(password)
	} else {
		opts.AddBroker(mqttBroker)
	}

	opts.SetClientID(fmt.Sprintf("cp-agent-%s", clusterID))
	opts.SetAutoReconnect(true)
	opts.SetKeepAlive(60 * time.Second)

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

	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("[FATAL] MQTT connect error: %v", token.Error())
	}
	log.Printf("[INFO] Agent %s connected to MQTT broker", clusterID)

	cfpTopic := "clusterless/auction/cfp"
	execTopic := fmt.Sprintf("clusterless/execute/%s", clusterID)

	mqttClient.Subscribe(cfpTopic, 1, func(client mqtt.Client, msg mqtt.Message) {
		var cfp TaskCFP
		if err := json.Unmarshal(msg.Payload(), &cfp); err != nil {
			log.Printf("[ERROR] Invalid CFP payload: %v", err)
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		availCPU, availMem, cached, err := getClusterResources(ctx, clientset, cfp.Image)
		if err != nil {
			log.Printf("[ERROR] Failed to read cluster metrics: %v", err)
			return
		}

		if availCPU < cfp.MinCPUMilli || availMem < cfp.MinMemMiB {
			log.Printf("[BID SKIPPED] Task %s: insufficient resources (Avail CPU: %dm, Mem: %dMiB)", cfp.TaskID, availCPU, availMem)
			return
		}

		score := availCPU
		if cached {
			score += 500
		}

		bid := BidPayload{
			TaskID:        cfp.TaskID,
			ClusterID:     clusterID,
			AvailCPUMilli: availCPU,
			AvailMemMiB:   availMem,
			ImageCached:   cached,
			Score:         score,
		}

		data, _ := json.Marshal(bid)
		client.Publish("clusterless/auction/bids", 1, false, data)
		log.Printf("[BID SENT] Task: %s | AvailCPU: %dm | AvailMem: %dMiB | Cached: %v | Score: %d", cfp.TaskID, availCPU, availMem, cached, score)
	})

	mqttClient.Subscribe(execTopic, 1, func(client mqtt.Client, msg mqtt.Message) {
		var task TaskCFP
		if err := json.Unmarshal(msg.Payload(), &task); err != nil {
			log.Printf("[ERROR] Invalid execute payload: %v", err)
			return
		}

		log.Printf("[EXECUTING] Task %s won by %s. Spawning pod...", task.TaskID, clusterID)

		go func(t TaskCFP) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			status, duration := executeFunctionPod(ctx, clientset, t)
			res := TaskResult{
				TaskID:     t.TaskID,
				ClusterID:  clusterID,
				Status:     status,
				DurationMs: duration,
			}

			data, _ := json.Marshal(res)
			client.Publish("clusterless/results", 1, false, data)
			log.Printf("[COMPLETED] Task %s finished with status %s in %dms", t.TaskID, status, duration)
		}(task)
	})

	select {}
}