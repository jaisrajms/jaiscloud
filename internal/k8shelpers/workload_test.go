package k8shelpers

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func testPod(name string, labels map[string]string) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers:    []corev1.Container{{Name: "app", Image: "nginx:latest"}},
		},
	}
}

func TestEnsureWorkloadLifecycle(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()
	name := "wl-1"
	labels := map[string]string{"app": name, "jaiscloud.io/owned": "test"}

	addr, err := EnsureWorkload(ctx, client, WorkloadSpec{
		Namespace: "jaiscloud",
		Pod:       testPod(name, labels),
		Ports:     []ServicePort{{Name: "http", Port: 8080}},
		Probe:     func(string) bool { return true },
	})
	if err != nil {
		t.Fatalf("EnsureWorkload: %v", err)
	}
	if want := "wl-1.jaiscloud.svc.cluster.local:8080"; addr != want {
		t.Fatalf("endpoint = %q, want %q", addr, want)
	}

	if _, err := client.CoreV1().Namespaces().Get(ctx, "jaiscloud", metav1.GetOptions{}); err != nil {
		t.Errorf("namespace not created: %v", err)
	}
	pod, err := client.CoreV1().Pods("jaiscloud").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("pod not created: %v", err)
	}
	if pod.Spec.Containers[0].Image != "nginx:latest" {
		t.Errorf("pod image = %q", pod.Spec.Containers[0].Image)
	}
	svc, err := client.CoreV1().Services("jaiscloud").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("service not created: %v", err)
	}
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("service type = %q, want ClusterIP", svc.Spec.Type)
	}
	if svc.Spec.Selector["app"] != name {
		t.Errorf("service selector = %v", svc.Spec.Selector)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 8080 {
		t.Errorf("service ports = %v", svc.Spec.Ports)
	}

	// Idempotent: a second ensure reuses the resources and returns the same address.
	again, err := EnsureWorkload(ctx, client, WorkloadSpec{
		Namespace: "jaiscloud",
		Pod:       testPod(name, labels),
		Ports:     []ServicePort{{Name: "http", Port: 8080}},
		Probe:     func(string) bool { return true },
	})
	if err != nil || again != addr {
		t.Fatalf("second EnsureWorkload = %q, %v; want %q, nil", again, err, addr)
	}

	if err := DeleteWorkload(ctx, client, "jaiscloud", name); err != nil {
		t.Fatalf("DeleteWorkload: %v", err)
	}
	if _, err := client.CoreV1().Pods("jaiscloud").Get(ctx, name, metav1.GetOptions{}); err == nil {
		t.Error("pod survived DeleteWorkload")
	}
	if _, err := client.CoreV1().Services("jaiscloud").Get(ctx, name, metav1.GetOptions{}); err == nil {
		t.Error("service survived DeleteWorkload")
	}
	// Deleting an absent workload is a no-op.
	if err := DeleteWorkload(ctx, client, "jaiscloud", "absent"); err != nil {
		t.Errorf("DeleteWorkload(absent): %v", err)
	}
}

func TestEnsureWorkloadNotReadyReaps(t *testing.T) {
	client := fake.NewSimpleClientset()
	name := "wl-never-ready"
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := EnsureWorkload(ctx, client, WorkloadSpec{
		Namespace: "jaiscloud",
		Pod:       testPod(name, map[string]string{"app": name}),
		Ports:     []ServicePort{{Name: "http", Port: 8080}},
		Probe:     func(string) bool { return false },
	})
	if err == nil {
		t.Fatal("EnsureWorkload succeeded with a never-ready workload; want error")
	}
	background := context.Background()
	if _, err := client.CoreV1().Pods("jaiscloud").Get(background, name, metav1.GetOptions{}); err == nil {
		t.Error("pod leaked after a failed readiness wait")
	}
	if _, err := client.CoreV1().Services("jaiscloud").Get(background, name, metav1.GetOptions{}); err == nil {
		t.Error("service leaked after a failed readiness wait")
	}
}

func TestEnsureWorkloadValidation(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()
	if _, err := EnsureWorkload(ctx, client, WorkloadSpec{}); err == nil {
		t.Error("empty spec: want error")
	}
	if _, err := EnsureWorkload(ctx, client, WorkloadSpec{Namespace: "ns", Pod: testPod("p", nil)}); err == nil {
		t.Error("no ports: want error")
	}
}

func TestSweepWorkloadsDeletesOnlyLabeled(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "wl-a", Namespace: "jaiscloud", Labels: map[string]string{"group": "mine"}}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "jaiscloud"}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "wl-a", Namespace: "jaiscloud", Labels: map[string]string{"group": "mine"}}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "jaiscloud"}},
	)
	n, err := SweepWorkloads(context.Background(), client, "jaiscloud", "group=mine")
	if err != nil {
		t.Fatalf("SweepWorkloads: %v", err)
	}
	if n != 2 {
		t.Errorf("deleted = %d, want 2", n)
	}
	ctx := context.Background()
	if _, err := client.CoreV1().Pods("jaiscloud").Get(ctx, "wl-a", metav1.GetOptions{}); err == nil {
		t.Error("labeled pod survived sweep")
	}
	if _, err := client.CoreV1().Services("jaiscloud").Get(ctx, "wl-a", metav1.GetOptions{}); err == nil {
		t.Error("labeled service survived sweep")
	}
	if _, err := client.CoreV1().Pods("jaiscloud").Get(ctx, "other", metav1.GetOptions{}); err != nil {
		t.Error("unlabeled pod was swept")
	}
	if _, err := client.CoreV1().Services("jaiscloud").Get(ctx, "other", metav1.GetOptions{}); err != nil {
		t.Error("unlabeled service was swept")
	}
}

func TestEnsureNamespaceForbiddenIsNotFatal(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, "jaiscloud", errors.New("forbidden"))
	})
	if err := EnsureNamespace(context.Background(), client, "jaiscloud", nil); err != nil {
		t.Fatalf("EnsureNamespace with a forbidden Get: %v", err)
	}
}
