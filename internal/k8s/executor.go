package k8s

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"k8s-mqtt-agent/internal/config"
	"k8s-mqtt-agent/internal/model"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func ExecuteFunctionPod(ctx context.Context, clientset *kubernetes.Clientset, task model.TaskCFP) (string, int64) {
	startTime := time.Now()
	podName := fmt.Sprintf("fn-%s", strings.ToLower(task.TaskID))
	podsClient := clientset.CoreV1().Pods(config.DefaultNamespace)

	pod := buildFunctionPod(podName, task)
	if _, err := podsClient.Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		log.Printf("[ERROR] Failed to create pod %s: %v", podName, err)
		return "FailedToCreate", 0
	}

	defer func() {
		_ = podsClient.Delete(context.Background(), podName, metav1.DeleteOptions{})
	}()

	return waitForPodCompletion(ctx, clientset, podName, startTime)
}

func buildFunctionPod(podName string, task model.TaskCFP) *corev1.Pod {
	requests := corev1.ResourceList{}
	if task.MinCPUMilli > 0 {
		requests[corev1.ResourceCPU] = *resource.NewMilliQuantity(task.MinCPUMilli, resource.DecimalSI)
	}
	if task.MinMemMiB > 0 {
		requests[corev1.ResourceMemory] = *resource.NewQuantity(task.MinMemMiB*bytesPerMiB, resource.BinarySI)
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: config.DefaultNamespace,
			Labels: map[string]string{
				"app":     "clusterless-worker",
				"task-id": task.TaskID,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:            "worker",
					Image:           task.Image,
					ImagePullPolicy: corev1.PullIfNotPresent,
					Command:         task.Command,
					Resources:       corev1.ResourceRequirements{Requests: requests},
				},
			},
		},
	}
}

func waitForPodCompletion(ctx context.Context, clientset *kubernetes.Clientset, podName string, startTime time.Time) (string, int64) {
	podsClient := clientset.CoreV1().Pods(config.DefaultNamespace)
	for {
		select {
		case <-ctx.Done():
			return "Timeout", time.Since(startTime).Milliseconds()
		case <-time.After(config.PodPollInterval):
			p, err := podsClient.Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				continue
			}
			switch p.Status.Phase {
			case corev1.PodSucceeded:
				return "Succeeded", time.Since(startTime).Milliseconds()
			case corev1.PodFailed:
				return "Failed", time.Since(startTime).Milliseconds()
			}
		}
	}
}
