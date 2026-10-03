package workflows

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/lro"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
)

// freezeClock pins the global clock to at and restores real time on cleanup.
func freezeClock(t *testing.T, at time.Time) {
	t.Helper()
	clock.SetGlobalClock(clock.FixedClock{T: at})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
}

// TestWorkflowLROSynchronousDefault verifies the default (zero) mode is
// unchanged: create returns a done operation with an inline response.
func TestWorkflowLROSynchronousDefault(t *testing.T) {
	ctx := context.Background()
	s := NewService(workflowsstore.NewMemoryStore())

	_, op, err := s.CreateWorkflow(ctx, "proj", "us-central1", CreateInput{ID: "wf1", SourceContents: simpleSource})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !op.Done {
		t.Fatalf("default mode must return a done operation")
	}
	if op.EndTime.IsZero() {
		t.Fatalf("default mode must set endTime")
	}

	got, err := s.GetOperation(ctx, "proj", "us-central1", op.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if !got.Done || got.EndTime.IsZero() {
		t.Fatalf("default-mode read = done=%v endTime=%v; want done with endTime", got.Done, got.EndTime)
	}
}

// TestWorkflowLROAsyncSettlesOnRead verifies async mode stores an in-flight
// operation and settles it lazily once the delay elapses.
func TestWorkflowLROAsyncSettlesOnRead(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	s := NewService(workflowsstore.NewMemoryStore(),
		WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second}))

	_, op, err := s.CreateWorkflow(ctx, "proj", "us-central1", CreateInput{ID: "wf1", SourceContents: simpleSource})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if op.Done {
		t.Fatalf("async create must return an in-flight operation")
	}
	if !op.EndTime.IsZero() {
		t.Fatalf("in-flight operation endTime = %v, want zero", op.EndTime)
	}

	// The pending wire shape omits response and metadata.endTime.
	pendingJSON := OperationJSON(op, "proj")
	if pendingJSON["done"] != false {
		t.Fatalf("pending done = %v, want false", pendingJSON["done"])
	}
	if _, ok := pendingJSON["response"]; ok {
		t.Fatalf("pending operation must not include response: %v", pendingJSON)
	}
	if md, _ := pendingJSON["metadata"].(map[string]any); md != nil {
		if _, ok := md["endTime"]; ok {
			t.Fatalf("pending metadata must not include endTime: %v", md)
		}
	} else {
		t.Fatalf("pending metadata missing: %v", pendingJSON)
	}

	// Before the delay, a read stays in flight.
	if got, err := s.GetOperation(ctx, "proj", "us-central1", op.ID); err != nil || got.Done {
		t.Fatalf("read before delay = done=%v err=%v; want in flight", got.Done, err)
	}

	// Past the delay, the read settles deterministically at createTime+delay.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	got, err := s.GetOperation(ctx, "proj", "us-central1", op.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if !got.Done {
		t.Fatalf("read past delay must be done")
	}
	if want := t0.Add(30 * time.Second); !got.EndTime.Equal(want) {
		t.Fatalf("settled endTime = %v, want %v", got.EndTime, want)
	}

	doneJSON := OperationJSON(got, "proj")
	if doneJSON["done"] != true {
		t.Fatalf("settled done = %v, want true", doneJSON["done"])
	}
	if md, _ := doneJSON["metadata"].(map[string]any); md == nil || md["endTime"] == nil {
		t.Fatalf("settled metadata missing endTime: %v", doneJSON)
	}
	response, _ := doneJSON["response"].(map[string]any)
	if response == nil || response["name"] != "projects/proj/locations/us-central1/workflows/wf1" {
		t.Fatalf("settled response = %v", response)
	}

	// The persisted record is unchanged: settling is derived on read.
	stored, err := s.workflows.GetOperation(ctx, "proj", "us-central1", op.ID)
	if err != nil {
		t.Fatalf("store get: %v", err)
	}
	if stored.Done {
		t.Fatalf("settling must not mutate the persisted done flag")
	}
}

// TestWorkflowLROAsyncZeroDelaySettlesImmediately verifies a zero delay settles
// on the first read rather than leaving the operation in flight.
func TestWorkflowLROAsyncZeroDelaySettlesImmediately(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	s := NewService(workflowsstore.NewMemoryStore(),
		WithLROMode(lro.Mode{Enabled: true, Delay: 0}))

	_, op, err := s.CreateWorkflow(ctx, "proj", "us-central1", CreateInput{ID: "wf1", SourceContents: simpleSource})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if op.Done {
		t.Fatalf("create must still be in flight")
	}
	got, err := s.GetOperation(ctx, "proj", "us-central1", op.ID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if !got.Done {
		t.Fatalf("zero-delay read must be done")
	}
	if !got.EndTime.Equal(t0) {
		t.Fatalf("zero-delay endTime = %v, want createTime %v", got.EndTime, t0)
	}
	var resp map[string]any
	if err := json.Unmarshal([]byte(got.Response), &resp); err != nil || resp["name"] == nil {
		t.Fatalf("settled response = %q err=%v", got.Response, err)
	}
}
