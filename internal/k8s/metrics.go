package k8s

import (
	"context"
	"encoding/json"
	"strings"

	"k8s-mqtt-agent/internal/model"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	metricsPath = "/apis/metrics.k8s.io/v1beta1/nodes"
	bytesPerMiB = 1024 * 1024
)

type NodeMetricsList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Usage corev1.ResourceList `json:"usage"`
	} `json:"items"`
}

func GetClusterResources(ctx context.Context, clientset *kubernetes.Clientset, reservations *model.ReservationTracker, targetImage string) (model.ClusterResources, error) {
	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return model.ClusterResources{}, err
	}

	usageMap, err := fetchNodeUsageMap(ctx, clientset)
	if err != nil {
		return model.ClusterResources{}, err
	}

	var totalAvailCPU, totalAvailMem int64
	imageCached := false

	for _, node := range nodes.Items {
		allocCPU := node.Status.Allocatable.Cpu().MilliValue()
		allocMem := node.Status.Allocatable.Memory().Value()

		var usedCPU, usedMem int64
		if usage, ok := usageMap[node.Name]; ok {
			usedCPU = usage.Cpu().MilliValue()
			usedMem = usage.Memory().Value()
		}

		totalAvailCPU += allocCPU - usedCPU
		totalAvailMem += (allocMem - usedMem) / bytesPerMiB

		if isImageCachedOnNode(node, targetImage) {
			imageCached = true
		}
	}

	reservedCPU, reservedMem := reservations.ReservedTotals()
	return model.ClusterResources{
		AvailCPUMilli: max(int64(0), totalAvailCPU-reservedCPU),
		AvailMemMiB:   max(int64(0), totalAvailMem-reservedMem),
		ImageCached:   imageCached,
	}, nil
}

func fetchNodeUsageMap(ctx context.Context, clientset *kubernetes.Clientset) (map[string]corev1.ResourceList, error) {
	rawMetrics, err := clientset.Discovery().RESTClient().Get().AbsPath(metricsPath).DoRaw(ctx)
	if err != nil {
		return nil, err
	}

	var metrics NodeMetricsList
	if err := json.Unmarshal(rawMetrics, &metrics); err != nil {
		return nil, err
	}

	usageMap := make(map[string]corev1.ResourceList, len(metrics.Items))
	for _, item := range metrics.Items {
		usageMap[item.Metadata.Name] = item.Usage
	}
	return usageMap, nil
}

func isImageCachedOnNode(node corev1.Node, targetImage string) bool {
	for _, img := range node.Status.Images {
		for _, name := range img.Names {
			if strings.Contains(name, targetImage) {
				return true
			}
		}
	}
	return false
}
