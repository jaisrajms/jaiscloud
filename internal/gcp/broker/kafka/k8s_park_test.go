package kafka

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// TestParkClusterDeletesWorkloadKeepsNamespace proves a park terminates the
// broker Pod and Service and drops the endpoint, but keeps the per-cluster
// namespace (and therefore every record/identity a resume needs).
func TestParkClusterDeletesWorkloadKeepsNamespace(t *testing.T) {
	client := fake.NewSimpleClientset()
	b := newK8sBroker(client, "jaiscloud", "redpanda:test", discardLogger())
	ctx := context.Background()
	key := ClusterKey{Project: "proj", Location: "us-central1", Cluster: "c1"}
	ns := ClusterNamespace(key.Project, key.Location, key.Cluster)
	name := brokerResourceName(key)

	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if _, err := client.CoreV1().Pods(ns).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create broker pod: %v", err)
	}
	if _, err := client.CoreV1().Services(ns).Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create broker service: %v", err)
	}

	b.mu.Lock()
	b.namespaces[key] = ns
	b.endpoints[key] = "broker:9092"
	b.mu.Unlock()

	if err := b.ParkCluster(ctx, key.Project, key.Location, key.Cluster); err != nil {
		t.Fatalf("ParkCluster: %v", err)
	}

	if _, err := client.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("broker pod still present (err=%v)", err)
	}
	if _, err := client.CoreV1().Services(ns).Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("broker service still present (err=%v)", err)
	}
	if _, err := client.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{}); err != nil {
		t.Fatalf("per-cluster namespace deleted by park: %v", err)
	}
	if ep := b.Endpoint(key.Project, key.Location, key.Cluster); ep != "" {
		t.Fatalf("endpoint still advertised after park: %q", ep)
	}
}
