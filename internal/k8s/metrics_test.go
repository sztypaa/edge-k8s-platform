package k8s

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestIsImageCachedOnNode(t *testing.T) {
	node := corev1.Node{Status: corev1.NodeStatus{Images: []corev1.ContainerImage{
		{Names: []string{"docker.io/sztypaa/rpi-task@sha256:abc", "docker.io/sztypaa/rpi-task:size-500"}},
		{Names: []string{"docker.io/library/nginx:latest"}},
		{Names: []string{"ghcr.io/org/app:1.0"}},
	}}}

	tests := []struct {
		image string
		want  bool
	}{
		{"sztypaa/rpi-task:size-500", true},
		{"docker.io/sztypaa/rpi-task:size-500", true},
		{"sztypaa/rpi-task:size-50", false},
		{"sztypaa/rpi-task", false},
		{"nginx", true},
		{"nginx:latest", true},
		{"library/nginx", true},
		{"ghcr.io/org/app:1.0", true},
		{"ghcr.io/org/app:1", false},
	}
	for _, tt := range tests {
		if got := isImageCachedOnNode(node, tt.image); got != tt.want {
			t.Errorf("isImageCachedOnNode(%q) = %v, want %v", tt.image, got, tt.want)
		}
	}
}
