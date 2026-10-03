package dataproc

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	dataprocstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// newStateProvider returns a service whose cluster transitions are observable:
// a positive ready delay means a cluster stays in its transitional state until
// the (frozen) clock is advanced past StateStartTime + delay.
func newStateProvider(t *testing.T, delay time.Duration, opts ...Option) *Service {
	t.Helper()
	opts = append([]Option{WithClusterReadyDelay(delay)}, opts...)
	return NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(), opts...)
}

// freezeClock pins the global clock to t and restores real time on cleanup.
func freezeClock(t *testing.T, at time.Time) {
	t.Helper()
	clock.SetGlobalClock(clock.FixedClock{T: at})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
}

func opStatusState(t *testing.T, op dataprocstore.Operation) string {
	t.Helper()
	meta := mustJSONMap([]byte(op.Metadata))
	status, _ := meta["status"].(map[string]any)
	s, _ := status["state"].(string)
	return s
}

// TestClusterCreateLazyTransition verifies create returns an in-flight
// operation, the cluster stays CREATING until the delay elapses, and polling
// the operation settles it to RUNNING with the cluster as the response.
func TestClusterCreateLazyTransition(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	p := newStateProvider(t, 30*time.Second)
	ctx := context.Background()

	c, op, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if c.Status.State != "CREATING" {
		t.Fatalf("cluster state = %q, want CREATING", c.Status.State)
	}
	if op.Done {
		t.Fatal("create operation must not be done")
	}
	if got := opStatusState(t, op); got != "RUNNING" {
		t.Fatalf("operation metadata status = %q, want RUNNING", got)
	}
	if want := ClusterName("proj", "us-central1", "c1"); op.Target != want {
		t.Fatalf("operation target = %q, want %q", op.Target, want)
	}

	// Before the delay elapses, reads keep the cluster in CREATING and the
	// operation in flight.
	if got, err := p.GetCluster(ctx, "proj", "us-central1", "c1"); err != nil || got.Status.State != "CREATING" {
		t.Fatalf("GetCluster before delay = %+v, %v; want CREATING", got.Status.State, err)
	}
	if got, err := p.GetOperation(ctx, "proj", "us-central1", op.ID); err != nil || got.Done {
		t.Fatalf("GetOperation before delay done=%v err=%v; want in flight", got.Done, err)
	}

	// Past the delay, polling the operation settles the cluster and completes.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(31 * time.Second)})
	polled, err := p.GetOperation(ctx, "proj", "us-central1", op.ID)
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if !polled.Done || opStatusState(t, polled) != "DONE" {
		t.Fatalf("polled operation done=%v status=%q, want done/DONE", polled.Done, opStatusState(t, polled))
	}
	resp := mustJSONMap([]byte(polled.Response))
	if resp["clusterName"] != "c1" {
		t.Fatalf("operation response = %v, want the cluster", resp)
	}
	if status, _ := resp["status"].(map[string]any); status["state"] != "RUNNING" {
		t.Fatalf("operation response cluster state = %v, want RUNNING", resp["status"])
	}

	settled, err := p.GetCluster(ctx, "proj", "us-central1", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if settled.Status.State != "RUNNING" {
		t.Fatalf("settled state = %q, want RUNNING", settled.Status.State)
	}
	if len(settled.StatusHistory) == 0 || settled.StatusHistory[0].State != "CREATING" {
		t.Fatalf("status history lost CREATING: %+v", settled.StatusHistory)
	}
}

// TestClusterDeleteLazyTransition verifies delete keeps the DELETING record
// until the delay elapses, then removes it and completes the operation with an
// empty response.
func TestClusterDeleteLazyTransition(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	p := newStateProvider(t, 30*time.Second)
	ctx := context.Background()

	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(31 * time.Second)})
	if _, err := p.GetCluster(ctx, "proj", "us-central1", "c1"); err != nil {
		t.Fatalf("settle create: %v", err)
	}

	delOp, err := p.DeleteCluster(ctx, "proj", "us-central1", "c1")
	if err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if delOp.Done {
		t.Fatal("delete operation must not be done")
	}
	// The record is kept (and observable as DELETING) until the delay elapses.
	if got, err := p.GetCluster(ctx, "proj", "us-central1", "c1"); err != nil || got.Status.State != "DELETING" {
		t.Fatalf("GetCluster after delete = %+v, %v; want DELETING", got.Status.State, err)
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(62 * time.Second)})
	polled, err := p.GetOperation(ctx, "proj", "us-central1", delOp.ID)
	if err != nil {
		t.Fatalf("GetOperation delete: %v", err)
	}
	if !polled.Done {
		t.Fatal("delete operation should be done after the delay")
	}
	if got := mustJSONMap([]byte(polled.Response)); len(got) != 0 {
		t.Fatalf("delete response = %v, want empty", got)
	}
	if _, err := p.GetCluster(ctx, "proj", "us-central1", "c1"); err == nil {
		t.Fatal("expected NotFound after the delete transition")
	}
}

// TestClusterUpdateStartStopLazyTransitions verifies update/start/stop each move
// the cluster through their transitional state and settle on a poll.
func TestClusterUpdateStartStopLazyTransitions(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	p := newStateProvider(t, 30*time.Second)
	ctx := context.Background()

	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	advance := func(step int) time.Time {
		at := t0.Add(time.Duration(step) * 31 * time.Second)
		clock.SetGlobalClock(clock.FixedClock{T: at})
		return at
	}
	advance(1)
	if _, err := p.GetCluster(ctx, "proj", "us-central1", "c1"); err != nil {
		t.Fatalf("settle create: %v", err)
	}

	// Update: fields apply now, the state settles back to RUNNING on poll.
	_, updOp, err := p.UpdateCluster(ctx, "proj", "us-central1", "c1",
		ClusterInput{Labels: map[string]string{"env": "prod"}}, []string{"labels"})
	if err != nil {
		t.Fatalf("UpdateCluster: %v", err)
	}
	if got, _ := p.GetCluster(ctx, "proj", "us-central1", "c1"); got.Status.State != "UPDATING" {
		t.Fatalf("state during update = %q, want UPDATING", got.Status.State)
	}
	advance(2)
	upd, err := p.GetOperation(ctx, "proj", "us-central1", updOp.ID)
	if err != nil || !upd.Done {
		t.Fatalf("update op done=%v err=%v", upd.Done, err)
	}
	updated := mustJSONMap([]byte(upd.Response))
	if status, _ := updated["status"].(map[string]any); status["state"] != "RUNNING" {
		t.Fatalf("state after update = %v, want RUNNING", updated["status"])
	}
	if labels, _ := updated["labels"].(map[string]any); labels["env"] != "prod" {
		t.Fatalf("labels after update = %v, want env=prod", updated["labels"])
	}

	// Stop: STOPPING -> STOPPED.
	_, stopOp, err := p.StopCluster(ctx, "proj", "us-central1", "c1")
	if err != nil {
		t.Fatalf("StopCluster: %v", err)
	}
	if got, _ := p.GetCluster(ctx, "proj", "us-central1", "c1"); got.Status.State != "STOPPING" {
		t.Fatalf("state during stop = %q, want STOPPING", got.Status.State)
	}
	advance(3)
	stopped, err := p.GetOperation(ctx, "proj", "us-central1", stopOp.ID)
	if err != nil || !stopped.Done {
		t.Fatalf("stop op done=%v err=%v", stopped.Done, err)
	}
	if status, _ := mustJSONMap([]byte(stopped.Response))["status"].(map[string]any); status["state"] != "STOPPED" {
		t.Fatalf("state after stop = %v, want STOPPED", status["state"])
	}

	// Start: STARTING -> RUNNING.
	_, startOp, err := p.StartCluster(ctx, "proj", "us-central1", "c1")
	if err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	if got, _ := p.GetCluster(ctx, "proj", "us-central1", "c1"); got.Status.State != "STARTING" {
		t.Fatalf("state during start = %q, want STARTING", got.Status.State)
	}
	advance(4)
	started, err := p.GetOperation(ctx, "proj", "us-central1", startOp.ID)
	if err != nil || !started.Done {
		t.Fatalf("start op done=%v err=%v", started.Done, err)
	}
	if status, _ := mustJSONMap([]byte(started.Response))["status"].(map[string]any); status["state"] != "RUNNING" {
		t.Fatalf("state after start = %v, want RUNNING", status["state"])
	}
}

// TestClusterForcedError verifies a settling create can reach ERROR through the
// test hook, and the operation terminates carrying the ERROR cluster.
func TestClusterForcedError(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	p := newStateProvider(t, 30*time.Second, WithClusterErrorHook(func(string, string, string) bool { return true }))
	ctx := context.Background()

	_, op, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(31 * time.Second)})
	polled, err := p.GetOperation(ctx, "proj", "us-central1", op.ID)
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if !polled.Done {
		t.Fatal("forced-error operation should be done")
	}
	if status, _ := mustJSONMap([]byte(polled.Response))["status"].(map[string]any); status["state"] != "ERROR" {
		t.Fatalf("state = %v, want ERROR", status["state"])
	}
	if got, _ := p.GetCluster(ctx, "proj", "us-central1", "c1"); got.Status.State != "ERROR" {
		t.Fatalf("cluster state = %q, want ERROR", got.Status.State)
	}
}

// TestCreateClusterDuplicateAlreadyExists asserts the store's idempotency
// guarantee survives the async change.
func TestCreateClusterDuplicateAlreadyExists(t *testing.T) {
	p := newProvider(t)
	ctx := context.Background()
	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, _, err := p.CreateCluster(ctx, "proj", "us-central1", "c1", ClusterInput{})
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 409 {
		t.Fatalf("expected 409 AlreadyExists, got %v", err)
	}
}
