package k8shelpers

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func makePod(name, ns string, labels map[string]string, ownerName string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "c", Image: "img"}},
		},
	}
	if ownerName != "" {
		pod.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "batch/v1",
			Kind:       "Job",
			Name:       ownerName,
			UID:        types.UID("owner-uid"),
		}}
	}
	return pod
}

func TestStartOwnershipPatcher_ReconcileSweep_PatchesUnowned(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create 3 pods without owner refs.
	for i := 0; i < 3; i++ {
		pod := makePod(
			"executor-"+string(rune('a'+i)),
			"default",
			map[string]string{"spark-role": "executor"},
			"",
		)
		_, _ = client.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})
	}

	cfg := PatcherConfig{
		Namespace:     "default",
		LabelSelector: "spark-role=executor",
		ResolveOwner: func(pod *corev1.Pod) (*OwnerRefHint, error) {
			return &OwnerRefHint{
				APIVersion: "batch/v1",
				Kind:       "Job",
				Name:       "spark-driver-job",
				UID:        types.UID("driver-uid"),
			}, nil
		},
	}

	// reconcileSweep is synchronous.
	if err := reconcileSweep(ctx, client, cfg, cfg.Namespace); err != nil {
		t.Fatalf("reconcileSweep: %v", err)
	}

	// All 3 pods should now have ownerReferences.
	pods, _ := client.CoreV1().Pods("default").List(ctx, metav1.ListOptions{
		LabelSelector: "spark-role=executor",
	})
	for _, pod := range pods.Items {
		if len(pod.OwnerReferences) == 0 {
			t.Errorf("pod %s missing ownerReferences after reconcile sweep", pod.Name)
		}
	}
}

func TestStartOwnershipPatcher_ResolveOwnerNilSkipsPod(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pod := makePod("executor-skip", "default", map[string]string{"spark-role": "executor"}, "")
	_, _ = client.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})

	cfg := PatcherConfig{
		Namespace:     "default",
		LabelSelector: "spark-role=executor",
		ResolveOwner: func(pod *corev1.Pod) (*OwnerRefHint, error) {
			return nil, nil // skip
		},
	}

	if err := reconcileSweep(ctx, client, cfg, cfg.Namespace); err != nil {
		t.Fatalf("reconcileSweep: %v", err)
	}

	updated, _ := client.CoreV1().Pods("default").Get(ctx, "executor-skip", metav1.GetOptions{})
	if len(updated.OwnerReferences) != 0 {
		t.Error("expected pod to remain unowned when ResolveOwner returns nil")
	}
}

func TestStartOwnershipPatcher_AlreadyOwnedPodSkipped(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Pod already has owner.
	pod := makePod("owned-executor", "default", map[string]string{"spark-role": "executor"}, "original-owner")
	_, _ = client.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})

	patchCalled := false
	cfg := PatcherConfig{
		Namespace:     "default",
		LabelSelector: "spark-role=executor",
		ResolveOwner: func(pod *corev1.Pod) (*OwnerRefHint, error) {
			patchCalled = true
			return &OwnerRefHint{APIVersion: "batch/v1", Kind: "Job", Name: "new-owner", UID: "new-uid"}, nil
		},
	}

	if err := reconcileSweep(ctx, client, cfg, cfg.Namespace); err != nil {
		t.Fatalf("reconcileSweep: %v", err)
	}

	if patchCalled {
		t.Error("expected ResolveOwner NOT to be called for already-owned pod")
	}
}

func TestCleanupOrphans_TerminalJobInvokesCallback(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()

	// Create a terminal failed job.
	trueVal := true
	job := makeManagedJob("terminal-job", "default", true)
	_, _ = client.BatchV1().Jobs("default").Create(ctx, job, metav1.CreateOptions{})
	_ = trueVal

	var gotState string
	cfg := CleanupConfig{
		Namespace: "default",
		OnTerminalJob: func(name, state, reason string) {
			gotState = state
		},
	}

	if err := CleanupOrphans(ctx, client, cfg); err != nil {
		t.Fatalf("CleanupOrphans: %v", err)
	}
	if gotState == "" {
		t.Error("expected OnTerminalJob to be called")
	}
}

func TestCleanupOrphans_OrphanPod_DeletedWhenTrue(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()

	pod := makePod("orphan-driver", "default", map[string]string{"spark-role": "driver"}, "")
	_, _ = client.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})

	cfg := CleanupConfig{
		Namespace:       "default",
		OrphanSelectors: []string{"spark-role=driver"},
		OnUnownedPod: func(pod *corev1.Pod) bool {
			return true // delete
		},
	}

	if err := CleanupOrphans(ctx, client, cfg); err != nil {
		t.Fatalf("CleanupOrphans: %v", err)
	}

	_, err := client.CoreV1().Pods("default").Get(ctx, "orphan-driver", metav1.GetOptions{})
	if err == nil {
		t.Error("expected orphan pod to be deleted")
	}
}

func TestCleanupOrphans_MultipleNamespaces(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx := context.Background()
	for _, ns := range []string{"ns-a", "ns-b"} {
		_, _ = client.BatchV1().Jobs(ns).Create(ctx, makeManagedJob("job-"+ns, ns, false), metav1.CreateOptions{})
	}

	var swept []string
	cfg := CleanupConfig{
		Namespace:  "ns-a",
		Namespaces: []string{"ns-b"},
		OnTerminalJob: func(name, state, reason string) {
			swept = append(swept, name)
		},
	}
	if err := CleanupOrphans(ctx, client, cfg); err != nil {
		t.Fatalf("CleanupOrphans: %v", err)
	}
	if len(swept) != 2 {
		t.Fatalf("OnTerminalJob called %d times (%v); want 2", len(swept), swept)
	}
	for _, ns := range []string{"ns-a", "ns-b"} {
		if _, err := client.BatchV1().Jobs(ns).Get(ctx, "job-"+ns, metav1.GetOptions{}); err == nil {
			t.Errorf("terminal job in %s survived the multi-namespace sweep", ns)
		}
	}
}

func TestUniqueNamespaces(t *testing.T) {
	got := uniqueNamespaces("primary", []string{"", "primary", "extra", "extra"})
	if len(got) != 2 || got[0] != "primary" || got[1] != "extra" {
		t.Fatalf("uniqueNamespaces = %v; want [primary extra]", got)
	}
	if got := uniqueNamespaces("", nil); len(got) != 0 {
		t.Fatalf("uniqueNamespaces with no namespaces = %v; want empty", got)
	}
}

// makeManagedJob creates a terminal Job with managed-by label.
func makeManagedJob(name, ns string, failed bool) *batchv1.Job {
	status := "True"
	condType := batchv1.JobComplete
	if failed {
		condType = batchv1.JobFailed
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    map[string]string{"app.kubernetes.io/managed-by": "jaiscloud"},
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{
				Type:   condType,
				Status: corev1.ConditionStatus(status),
			}},
		},
	}
}
