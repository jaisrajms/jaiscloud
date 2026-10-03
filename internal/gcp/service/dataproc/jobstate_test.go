package dataproc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"jaiscloud/internal/clock"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/store"
)

// newJobStateProvider returns a service over a memory store with a job state
// delay, so each hop is observable under a frozen clock.
func newJobStateProvider(t *testing.T, delay time.Duration, opts ...Option) *Service {
	t.Helper()
	opts = append([]Option{WithJobStateDelay(delay)}, opts...)
	return NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore(), opts...)
}

// submitTestJob creates the shared cluster and submits a minimal pyspark job.
func submitTestJob(t *testing.T, p *Service, jobID string) dpstore.Job {
	t.Helper()
	ctx := context.Background()
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	j, err := p.SubmitJob(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": jobID},
		"placement":  map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
	}))
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	return j
}

func getJob(t *testing.T, p *Service, jobID string) dpstore.Job {
	t.Helper()
	j, err := p.GetJob(context.Background(), "proj", "us-central1", jobID)
	require.NoError(t, err)
	return j
}

func jobStates(j dpstore.Job) []string {
	out := make([]string, 0, len(j.StatusHistory))
	for _, s := range j.StatusHistory {
		out = append(out, s.State)
	}
	return out
}

// TestJobStateMachine_MockProgression verifies a mock-mode job walks
// PENDING -> SETUP_DONE -> RUNNING -> DONE one hop per elapsed delay, with
// RUNNING carrying the QUEUED substate and history recorded in order.
func TestJobStateMachine_MockProgression(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	p := newJobStateProvider(t, 30*time.Second)

	j := submitTestJob(t, p, "j1")
	if j.Status.State != "PENDING" {
		t.Fatalf("submitted job state = %q, want PENDING", j.Status.State)
	}

	// Before the delay elapses, reads keep it PENDING.
	if got := getJob(t, p, "j1"); got.Status.State != "PENDING" {
		t.Fatalf("pre-delay state = %q, want PENDING", got.Status.State)
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(31 * time.Second)})
	if got := getJob(t, p, "j1"); got.Status.State != "SETUP_DONE" {
		t.Fatalf("state = %q, want SETUP_DONE", got.Status.State)
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(62 * time.Second)})
	got := getJob(t, p, "j1")
	if got.Status.State != "RUNNING" || got.Status.Substate != "QUEUED" {
		t.Fatalf("state = %q/%q, want RUNNING/QUEUED", got.Status.State, got.Status.Substate)
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(93 * time.Second)})
	final := getJob(t, p, "j1")
	if final.Status.State != "DONE" {
		t.Fatalf("state = %q, want DONE", final.Status.State)
	}
	if final.Status.Substate != "" {
		t.Fatalf("terminal substate = %q, want empty", final.Status.Substate)
	}
	if final.DriverOutputResourceURI == "" {
		t.Fatal("DONE job has no driverOutputResourceUri")
	}
	require.Equal(t, []string{"PENDING", "SETUP_DONE", "RUNNING"}, jobStates(final))
}

// TestJobStateMachine_CancelProgression verifies CancelJob moves a non-terminal
// job to CANCEL_PENDING and reads settle CANCEL_STARTED -> CANCELLED.
func TestJobStateMachine_CancelProgression(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	p := newJobStateProvider(t, 30*time.Second)

	submitTestJob(t, p, "j1")

	cancelled, err := p.CancelJob(context.Background(), "proj", "us-central1", "j1")
	require.NoError(t, err)
	require.Equal(t, "CANCEL_PENDING", cancelled.Status.State)

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(31 * time.Second)})
	if got := getJob(t, p, "j1"); got.Status.State != "CANCEL_STARTED" {
		t.Fatalf("state = %q, want CANCEL_STARTED", got.Status.State)
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(62 * time.Second)})
	if got := getJob(t, p, "j1"); got.Status.State != "CANCELLED" {
		t.Fatalf("state = %q, want CANCELLED", got.Status.State)
	}

	// A second cancel of the now-terminal job is a no-op.
	again, err := p.CancelJob(context.Background(), "proj", "us-central1", "j1")
	require.NoError(t, err)
	require.Equal(t, "CANCELLED", again.Status.State)
}

// TestJobStateMachine_AttemptFailure verifies the attempt-failure hook turns
// the RUNNING -> DONE hop into ATTEMPT_FAILURE, which then settles to ERROR.
func TestJobStateMachine_AttemptFailure(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	p := newJobStateProvider(t, 0, WithJobAttemptFailureHook(func(_, _, _ string) bool { return true }))

	submitTestJob(t, p, "j1")

	// With a zero delay each read settles one hop.
	for _, want := range []string{"SETUP_DONE", "RUNNING", "ATTEMPT_FAILURE", "ERROR"} {
		if got := getJob(t, p, "j1"); got.Status.State != want {
			t.Fatalf("state = %q, want %q", got.Status.State, want)
		}
	}
}

// TestSubmitJobAsOperation_MockNotDoneThenTerminal verifies a mock-mode submit
// operation is returned in flight, its polled JobMetadata tracks the job, and
// it closes with the terminal Job as its response.
func TestSubmitJobAsOperation_MockNotDoneThenTerminal(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	p := newJobStateProvider(t, 30*time.Second)
	ctx := context.Background()

	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	op, err := p.SubmitJobAsOperation(ctx, "proj", "us-central1", JobInputFromMap(map[string]any{
		"reference":  map[string]any{"jobId": "j1"},
		"placement":  map[string]any{"clusterName": "c1"},
		"pysparkJob": map[string]any{"mainPythonFileUri": "gs://b/main.py"},
	}))
	require.NoError(t, err)
	require.False(t, op.Done, "mock submit operation must be in flight")

	meta := mustJSONMap([]byte(op.Metadata))
	require.Equal(t, "PENDING", meta["status"].(map[string]any)["state"])

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(31 * time.Second)})
	polled, err := p.GetOperation(ctx, "proj", "us-central1", op.ID)
	require.NoError(t, err)
	require.False(t, polled.Done)
	meta = mustJSONMap([]byte(polled.Metadata))
	require.Equal(t, "SETUP_DONE", meta["status"].(map[string]any)["state"])

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(62 * time.Second)})
	polled, err = p.GetOperation(ctx, "proj", "us-central1", op.ID)
	require.NoError(t, err)
	require.False(t, polled.Done)
	meta = mustJSONMap([]byte(polled.Metadata))
	require.Equal(t, "RUNNING", meta["status"].(map[string]any)["state"])

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(93 * time.Second)})
	polled, err = p.GetOperation(ctx, "proj", "us-central1", op.ID)
	require.NoError(t, err)
	require.True(t, polled.Done)
	resp := mustJSONMap([]byte(polled.Response))
	require.Equal(t, "DONE", resp["status"].(map[string]any)["state"])
}
