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

// TestListOperationsPagesAndSettles verifies the location-scoped operations
// list: it pages over the location's persisted operations (the standard
// pageSize/pageToken cursor) and settles each returned operation against the
// timing mode, matching GetOperation.
func TestListOperationsPagesAndSettles(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	s := NewService(workflowsstore.NewMemoryStore(),
		WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second}))

	for _, id := range []string{"wf1", "wf2", "wf3"} {
		if _, _, err := s.CreateWorkflow(ctx, "proj", "us-central1", CreateInput{ID: id, SourceContents: simpleSource}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}

	// A pageSize of 2 yields two unsettled operations and a cursor.
	page, next, err := s.ListOperations(ctx, "proj", "us-central1", 2, "", "")
	if err != nil {
		t.Fatalf("list operations: %v", err)
	}
	if len(page) != 2 || next == "" {
		t.Fatalf("page = %d ops, next = %q; want 2 and a cursor", len(page), next)
	}
	for _, op := range page {
		if op.Done {
			t.Fatalf("in-flight operation %s settled early", op.ID)
		}
	}

	// The cursor resumes in operation-ID order (the deterministic key).
	rest, next2, err := s.ListOperations(ctx, "proj", "us-central1", 2, next, "")
	if err != nil {
		t.Fatalf("list operations page 2: %v", err)
	}
	if len(rest) != 1 || next2 != "" {
		t.Fatalf("page 2 = %d ops, next = %q; want 1 and no cursor", len(rest), next2)
	}
	if rest[0].ID <= page[1].ID {
		t.Fatalf("page 2 first id %q must sort after page 1 last id %q", rest[0].ID, page[1].ID)
	}

	// Past the delay the listed operations settle, like GetOperation.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	settled, _, err := s.ListOperations(ctx, "proj", "us-central1", 2, "", "")
	if err != nil {
		t.Fatalf("list after delay: %v", err)
	}
	for _, op := range settled {
		if !op.Done || !op.EndTime.Equal(t0.Add(30*time.Second)) {
			t.Fatalf("settled page op = done=%v endTime=%v", op.Done, op.EndTime)
		}
	}

	// An unknown location is an empty page, not an error.
	if empty, _, err := s.ListOperations(ctx, "proj", "europe-west1", 0, "", ""); err != nil || len(empty) != 0 {
		t.Fatalf("empty location = %d ops err=%v; want empty page", len(empty), err)
	}

	// A missing location fails loud; a valid filter is evaluated.
	if _, _, err := s.ListOperations(ctx, "proj", "", 0, "", ""); err == nil {
		t.Fatalf("expected InvalidArgument for a missing location")
	}
	if done, _, err := s.ListOperations(ctx, "proj", "us-central1", 0, "", "done=true"); err != nil || len(done) != 3 {
		t.Fatalf("done=true after the delay = %d ops, err=%v; want 3", len(done), err)
	}
	if _, _, err := s.ListOperations(ctx, "proj", "us-central1", 0, "", "bogus=1"); err == nil {
		t.Fatalf("expected InvalidArgument for an unknown operations filter field")
	}
}

// TestListOperationsFilter pins the shared AIP-160 operations filter over the
// Cloud Workflows operations surface: done/name/state/metadata fields,
// AND/OR/NOT, implicit AND, parentheses and comparisons.
func TestListOperationsFilter(t *testing.T) {
	ctx := context.Background()
	s := NewService(workflowsstore.NewMemoryStore())
	for _, id := range []string{"wf1", "wf2", "wf3"} {
		if _, _, err := s.CreateWorkflow(ctx, "proj", "us-central1", CreateInput{ID: id, SourceContents: simpleSource}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	all, _, err := s.ListOperations(ctx, "proj", "us-central1", 0, "", "")
	if err != nil || len(all) != 3 {
		t.Fatalf("unfiltered list = %d ops, err=%v; want 3", len(all), err)
	}
	first := all[0]

	cases := []struct {
		filter string
		want   int
	}{
		{"", 3},
		{"done=true", 3},
		{"done=false", 0},
		{"NOT done=true", 0},
		{"-done=true", 0},
		{"done=false OR done=true", 3},
		{"(done=true OR done=false) AND state=DONE", 3},
		{"state=RUNNING", 0},
		{"verb=create", 3},
		{"metadata.verb:create", 3},
		{`target:"wf1"`, 1},
		{`metadata.target:"wf2"`, 1},
		{`name:"operations/" AND done=true`, 3},
		{`name:"` + first.ID + `"`, 1},
		{`done=true done=true`, 3}, // implicit AND
		{`done=true AND (verb=create OR verb=update)`, 3},
	}
	for _, tc := range cases {
		page, _, err := s.ListOperations(ctx, "proj", "us-central1", 0, "", tc.filter)
		if err != nil {
			t.Errorf("filter %q: %v", tc.filter, err)
			continue
		}
		if len(page) != tc.want {
			t.Errorf("filter %q = %d ops, want %d", tc.filter, len(page), tc.want)
		}
	}

	for _, bad := range []string{
		"bogus=1",
		"done=maybe",
		"done=true AND",
		"name=\"unterminated",
		"(done=true",
		"done=true OR",
	} {
		if _, _, err := s.ListOperations(ctx, "proj", "us-central1", 0, "", bad); err == nil {
			t.Errorf("filter %q decoded, want InvalidArgument", bad)
		}
	}
}

// TestDeleteOperation verifies delete removes a persisted operation and that an
// unknown id is NotFound.
func TestDeleteOperation(t *testing.T) {
	ctx := context.Background()
	s := NewService(workflowsstore.NewMemoryStore())

	_, op, err := s.CreateWorkflow(ctx, "proj", "us-central1", CreateInput{ID: "wf1", SourceContents: simpleSource})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.DeleteOperation(ctx, "proj", "us-central1", op.ID); err != nil {
		t.Fatalf("delete operation: %v", err)
	}
	if _, err := s.GetOperation(ctx, "proj", "us-central1", op.ID); !IsNotFound(err) {
		t.Fatalf("get after delete = %v, want NotFound", err)
	}
	if err := s.DeleteOperation(ctx, "proj", "us-central1", op.ID); !IsNotFound(err) {
		t.Fatalf("delete absent = %v, want NotFound", err)
	}
	if err := s.DeleteOperation(ctx, "proj", "", op.ID); err == nil {
		t.Fatalf("expected InvalidArgument for a missing location")
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
