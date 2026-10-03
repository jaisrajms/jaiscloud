package dataproc

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"jaiscloud/internal/clock"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/store"
)

// TestRunJob_SubmitClientModeWithFakeK8s exercises the k8s path with a fake
// clientset: the spark-submit k8s Job is created (submission path), and the
// owner-patch/cleanup wiring is exercised without a live cluster.
func TestRunJob_SubmitClientModeWithFakeK8s(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	p := NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithK8s(k8s, "jaiscloud", nil),
		WithSparkImage("spark:test"),
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	j := dpstore.Job{
		ProjectID: "proj",
		Region:    "us-central1",
		JobID:     "j1",
		Type:      "pysparkJob",
		TypeJob:   []byte(`{"mainPythonFileUri":"gs://b/main.py"}`),
		Status:    dpstore.JobStatus{State: "RUNNING", StateStartTime: clock.Now().UTC()},
	}
	if err := p.store.CreateJob(context.Background(), "proj", "us-central1", j); err != nil {
		t.Fatalf("create job: %v", err)
	}

	// Run in a goroutine; the fake clientset means SubmitClientMode creates a
	// k8s Job but WaitTerminal blocks (no controller). Cancel after a short
	// window to exercise the cancel-context map.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.runJob(ctx, "proj", "us-central1", j, "")
	}()

	// Wait until the spark-submit Job appears, then cancel the core context.
	require.Eventually(t, func() bool {
		jobs, err := k8s.BatchV1().Jobs("jaiscloud").List(context.Background(), metav1.ListOptions{})
		return err == nil && len(jobs.Items) > 0
	}, 5*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runJob did not exit after cancel")
	}
}

// TestSubmitJobAsOperation_K8sDoneFalseThenCompletes verifies the CB-2 contract:
// in k8s mode a SubmitJobAsOperation returns a not-done operation (job still
// RUNNING), and finishJob flips it to done=true with the terminal job response.
func TestSubmitJobAsOperation_K8sDoneFalseThenCompletes(t *testing.T) {
	k8s := fake.NewSimpleClientset()
	p := NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithK8s(k8s, "jaiscloud", nil),
		WithSparkImage("spark:test"),
	)
	// Unblock the runJob goroutine spawned by submitJob.
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	ctx := context.Background()
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{}); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	op, err := p.SubmitJobAsOperation(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference": map[string]any{"jobId": "j1"},
		"placement": map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{
			"mainPythonFileUri": "gs://b/main.py",
		},
	}))
	if err != nil {
		t.Fatalf("SubmitJobAsOperation: %v", err)
	}
	if op.Done {
		t.Fatalf("expected done=false for a still-RUNNING k8s job, got %v", op.Done)
	}

	// Simulate terminal completion: finishJob writes DONE and flips the op.
	j, err := p.store.GetJob(ctx, "proj", "us-central1", "j1")
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	p.finishJob("proj", "us-central1", j, "DONE", "", nil)

	// Operation id == job id, so GetOperation finds it via the job id.
	got, err := p.GetOperation(ctx, "proj", "us-central1", "j1")
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if !got.Done {
		t.Fatalf("expected done=true after finishJob, got %v", got.Done)
	}
	var respMap map[string]any
	if err := json.Unmarshal([]byte(got.Response), &respMap); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if state := respMap["status"].(map[string]any)["state"]; state != "DONE" {
		t.Fatalf("expected response status DONE, got %v", state)
	}
}
