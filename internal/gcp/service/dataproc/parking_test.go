package dataproc

import (
	"context"
	"testing"
	"time"

	dpstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/store"
)

// TestParkCancelsNonTerminalJobs verifies Park cancels every live job, leaves
// terminal jobs alone, keeps clusters untouched, and is idempotent.
func TestParkCancelsNonTerminalJobs(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	// A long delay means the frozen clock never settles the seeded jobs.
	p := NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithJobStateDelay(time.Hour))
	ctx := context.Background()

	for _, j := range []struct {
		project, region, id, state string
		long                       bool
	}{
		{"proj", "us-central1", "live", "RUNNING", true},
		{"proj", "europe-west1", "done", "DONE", false},
		{"other", "us-central1", "foreign", "RUNNING", true},
	} {
		if err := p.store.CreateJob(ctx, j.project, j.region, dpstore.Job{
			JobID:       j.id,
			Status:      dpstore.JobStatus{State: j.state},
			LongRunning: j.long,
		}); err != nil {
			t.Fatalf("seed job %s: %v", j.id, err)
		}
	}
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", gceInput()); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	res, err := p.Park(ctx, "proj")
	if err != nil {
		t.Fatalf("Park: %v", err)
	}
	if got := res["jobsCancelled"]; got != 1 {
		t.Fatalf("jobsCancelled = %v, want 1", got)
	}

	live, err := p.store.GetJob(ctx, "proj", "us-central1", "live")
	if err != nil {
		t.Fatalf("GetJob live: %v", err)
	}
	if live.Status.State != jobStateCancelPending {
		t.Fatalf("live job state = %q, want CANCEL_PENDING", live.Status.State)
	}
	done, err := p.store.GetJob(ctx, "proj", "europe-west1", "done")
	if err != nil {
		t.Fatalf("GetJob done: %v", err)
	}
	if done.Status.State != "DONE" {
		t.Fatalf("terminal job state = %q, want DONE", done.Status.State)
	}
	foreign, err := p.store.GetJob(ctx, "other", "us-central1", "foreign")
	if err != nil {
		t.Fatalf("GetJob foreign: %v", err)
	}
	if foreign.Status.State != "RUNNING" {
		t.Fatalf("job outside the project was touched: %q", foreign.Status.State)
	}

	// Park keeps the cluster record (clusters are control-plane only).
	if _, err := p.GetCluster(ctx, "proj", "us-central1", "c1"); err != nil {
		t.Fatalf("cluster record lost by Park: %v", err)
	}

	// Idempotent: a second park cancels nothing new.
	res, err = p.Park(ctx, "proj")
	if err != nil {
		t.Fatalf("second Park: %v", err)
	}
	if got := res["jobsCancelled"]; got != 0 {
		t.Fatalf("second jobsCancelled = %v, want 0", got)
	}
}

func TestResumeIsNoOp(t *testing.T) {
	p := NewService(dpstore.NewMemoryStore(), store.NewMemoryResourceStore())
	if got := p.Name(); got != "dataproc" {
		t.Fatalf("Name() = %q, want dataproc", got)
	}
	if err := p.Resume(context.Background(), "proj"); err != nil {
		t.Fatalf("Resume: %v", err)
	}
}
