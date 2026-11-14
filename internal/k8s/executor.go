package k8s

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"k8s-mqtt-agent/internal/model"
	"k8s-mqtt-agent/pkg/protocol"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	podPollInterval     = 500 * time.Millisecond
	workerAppLabelKey   = "app"
	workerAppLabelValue = "clusterless-worker"
	workerTaskLabelKey  = "task-id"
	workerContainerName = "worker"
)

func ExecuteFunctionPod(ctx context.Context, clientset *kubernetes.Clientset, namespace string, task model.TaskCFP) (string, int64) {
	startTime := time.Now()
	podName := fmt.Sprintf("fn-%s", strings.ToLower(task.TaskID))
	podsClient := clientset.CoreV1().Pods(namespace)

	pod := buildFunctionPod(podName, namespace, task)
	if _, err := podsClient.Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		log.Printf("[ERROR] Failed to create pod %s: %v", podName, err)
		return protocol.TaskStatusFailedToCreate, 0
	}

	defer func() {
		_ = podsClient.Delete(context.Background(), podName, metav1.DeleteOptions{})
	}()

	return waitForPodCompletion(ctx, clientset, namespace, podName, startTime)
}

func buildFunctionPod(podName, namespace string, task model.TaskCFP) *corev1.Pod {
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
			Namespace: namespace,
			Labels: map[string]string{
				workerAppLabelKey:  workerAppLabelValue,
				workerTaskLabelKey: task.TaskID,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:            workerContainerName,
					Image:           task.Image,
					ImagePullPolicy: corev1.PullIfNotPresent,
					Command:         task.Command,
					Resources:       corev1.ResourceRequirements{Requests: requests},
				},
			},
		},
	}
}

func waitForPodCompletion(ctx context.Context, clientset *kubernetes.Clientset, namespace, podName string, startTime time.Time) (string, int64) {
	podsClient := clientset.CoreV1().Pods(namespace)
	for {
		select {
		case <-ctx.Done():
			return protocol.TaskStatusTimeout, time.Since(startTime).Milliseconds()
		case <-time.After(podPollInterval):
			p, err := podsClient.Get(ctx, podName, metav1.GetOptions{})
			if err != nil {
				continue
			}
			switch p.Status.Phase {
			case corev1.PodSucceeded:
				return protocol.TaskStatusSucceeded, time.Since(startTime).Milliseconds()
			case corev1.PodFailed:
				return protocol.TaskStatusFailed, time.Since(startTime).Milliseconds()
			}
		}
	}
}
