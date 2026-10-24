package workflows

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStoreWorkflowCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	w := Workflow{ID: "a", SourceContents: "main:\n  steps:\n    - r:\n        return: 1", State: "ACTIVE",
		Labels: map[string]string{"team": "x"}, RevisionID: "000001-a4d"}
	if err := s.CreateWorkflow(ctx, "proj", "us-central1", "a", w); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.CreateWorkflow(ctx, "proj", "us-central1", "a", Workflow{ID: "a"}); err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	got, err := s.GetWorkflow(ctx, "proj", "us-central1", "a")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.SourceContents != w.SourceContents || got.Labels["team"] != "x" {
		t.Fatalf("unexpected workflow: %+v", got)
	}

	// Same ID under a different location is independent.
	if err := s.CreateWorkflow(ctx, "proj", "europe-west1", "a", Workflow{ID: "a"}); err != nil {
		t.Fatalf("create other location: %v", err)
	}

	upd := got
	upd.Description = "updated"
	if err := s.UpdateWorkflow(ctx, "proj", "us-central1", "a", upd); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, _ = s.GetWorkflow(ctx, "proj", "us-central1", "a")
	if got.Description != "updated" {
		t.Fatalf("expected description update, got %+v", got)
	}

	s.CreateWorkflow(ctx, "proj", "us-central1", "c", Workflow{ID: "c"})
	s.CreateWorkflow(ctx, "proj", "us-central1", "b", Workflow{ID: "b"})
	all, err := s.ListWorkflows(ctx, "proj", "us-central1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 3 || all[0].ID != "a" || all[1].ID != "b" || all[2].ID != "c" {
		t.Fatalf("unexpected list order: %+v", all)
	}

	if err := s.DeleteWorkflow(ctx, "proj", "us-central1", "a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.GetWorkflow(ctx, "proj", "us-central1", "a"); err != ErrNoSuchWorkflow {
		t.Fatalf("expected ErrNoSuchWorkflow, got %v", err)
	}
	if err := s.DeleteWorkflow(ctx, "proj", "us-central1", "a"); err != ErrNoSuchWorkflow {
		t.Fatalf("expected ErrNoSuchWorkflow on delete missing, got %v", err)
	}
}

func TestMemoryStoreListWorkflowsByProject(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	_ = s.CreateWorkflow(ctx, "proj", "us-central1", "b", Workflow{ID: "b", Location: "us-central1"})
	_ = s.CreateWorkflow(ctx, "proj", "europe-west1", "a", Workflow{ID: "a", Location: "europe-west1"})
	_ = s.CreateWorkflow(ctx, "proj", "us-central1", "a", Workflow{ID: "a", Location: "us-central1"})
	// Another project must not leak into the aggregation.
	_ = s.CreateWorkflow(ctx, "other", "us-central1", "z", Workflow{ID: "z", Location: "us-central1"})

	got, err := s.ListWorkflowsByProject(ctx, "proj")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d workflows, want 3: %+v", len(got), got)
	}
	// Sorted by location, then ID.
	want := [][2]string{{"europe-west1", "a"}, {"us-central1", "a"}, {"us-central1", "b"}}
	for i, w := range want {
		if got[i].Location != w[0] || got[i].ID != w[1] {
			t.Fatalf("row %d = %s/%s, want %s/%s", i, got[i].Location, got[i].ID, w[0], w[1])
		}
	}
}

func TestMemoryStoreExecutionCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	if err := s.CreateWorkflow(ctx, "proj", "us-central1", "w1", Workflow{ID: "w1"}); err != nil {
		t.Fatalf("create workflow: %v", err)
	}

	start := time.Now().UTC().Truncate(time.Microsecond)
	e := Execution{ID: "e1", State: "SUCCEEDED", Argument: `{"a":1}`, Result: `42`,
		StartTime: start, EndTime: start, Duration: "0.5s"}
	if err := s.CreateExecution(ctx, "proj", "us-central1", "w1", "e1", e); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.CreateExecution(ctx, "proj", "us-central1", "w1", "e1", Execution{ID: "e1"}); err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	got, err := s.GetExecution(ctx, "proj", "us-central1", "w1", "e1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Result != "42" || got.Argument != `{"a":1}` {
		t.Fatalf("unexpected execution: %+v", got)
	}

	// List ordered by start time descending (newest first).
	later := start.Add(time.Second)
	s.CreateExecution(ctx, "proj", "us-central1", "w1", "e2", Execution{ID: "e2", StartTime: later})
	all, err := s.ListExecutions(ctx, "proj", "us-central1", "w1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 2 || all[0].ID != "e2" {
		t.Fatalf("unexpected list order: %+v", all)
	}

	// Update (cancel transition).
	got.State = "CANCELLED"
	if err := s.UpdateExecution(ctx, "proj", "us-central1", "w1", "e1", got); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, _ = s.GetExecution(ctx, "proj", "us-central1", "w1", "e1")
	if got.State != "CANCELLED" {
		t.Fatalf("expected CANCELLED, got %s", got.State)
	}

	// Deleting the workflow removes its executions.
	if err := s.DeleteWorkflow(ctx, "proj", "us-central1", "w1"); err != nil {
		t.Fatalf("delete workflow: %v", err)
	}
	if _, err := s.GetExecution(ctx, "proj", "us-central1", "w1", "e1"); err != ErrNoSuchExecution {
		t.Fatalf("expected ErrNoSuchExecution after workflow delete, got %v", err)
	}
}

func TestMemoryStoreOperations(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	op := Operation{ID: "op1", Done: true, Response: `{"name":"x"}`, Verb: "create", Target: "target"}
	if err := s.CreateOperation(ctx, "proj", "us-central1", op); err != nil {
		t.Fatalf("create op: %v", err)
	}
	got, err := s.GetOperation(ctx, "proj", "us-central1", "op1")
	if err != nil {
		t.Fatalf("get op: %v", err)
	}
	if got.Response != `{"name":"x"}` || got.Verb != "create" {
		t.Fatalf("unexpected op: %+v", got)
	}
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "missing"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation, got %v", err)
	}

	// ListOperations returns the location's operations in ID order and never
	// leaks another location's.
	for _, id := range []string{"op3", "op2"} {
		if err := s.CreateOperation(ctx, "proj", "us-central1", Operation{ID: id}); err != nil {
			t.Fatalf("create op %s: %v", id, err)
		}
	}
	if err := s.CreateOperation(ctx, "proj", "europe-west1", Operation{ID: "other"}); err != nil {
		t.Fatalf("create other-location op: %v", err)
	}
	ops, err := s.ListOperations(ctx, "proj", "us-central1")
	if err != nil {
		t.Fatalf("list ops: %v", err)
	}
	if len(ops) != 3 || ops[0].ID != "op1" || ops[1].ID != "op2" || ops[2].ID != "op3" {
		t.Fatalf("list ops = %+v, want op1,op2,op3", ops)
	}

	// Delete removes it, and a second delete is ErrNoSuchOperation.
	if err := s.DeleteOperation(ctx, "proj", "us-central1", "op2"); err != nil {
		t.Fatalf("delete op: %v", err)
	}
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "op2"); err != ErrNoSuchOperation {
		t.Fatalf("get deleted op = %v, want ErrNoSuchOperation", err)
	}
	if err := s.DeleteOperation(ctx, "proj", "us-central1", "op2"); err != ErrNoSuchOperation {
		t.Fatalf("re-delete = %v, want ErrNoSuchOperation", err)
	}
	if err := s.DeleteOperation(ctx, "proj", "us-central1", "absent"); err != ErrNoSuchOperation {
		t.Fatalf("delete absent = %v, want ErrNoSuchOperation", err)
	}
}

func TestMemoryStoreRevisions(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	base := Workflow{ID: "a", State: "ACTIVE", SourceContents: "s1", RevisionID: "000001-aaa"}
	if err := s.CreateWorkflow(ctx, "proj", "us-central1", "a", base); err != nil {
		t.Fatalf("create: %v", err)
	}
	revs, err := s.ListRevisions(ctx, "proj", "us-central1", "a")
	if err != nil {
		t.Fatalf("list revisions: %v", err)
	}
	if len(revs) != 1 || revs[0].RevisionID != "000001-aaa" || revs[0].SourceContents != "s1" {
		t.Fatalf("after create: %+v", revs)
	}

	// A non-revision update (same revision id) must not append.
	upd := base
	upd.Description = "d"
	if err := s.UpdateWorkflow(ctx, "proj", "us-central1", "a", upd); err != nil {
		t.Fatalf("update: %v", err)
	}
	if revs, _ = s.ListRevisions(ctx, "proj", "us-central1", "a"); len(revs) != 1 {
		t.Fatalf("non-revision update appended: %+v", revs)
	}

	// A revision-changing atomic update appends, newest first.
	if _, err := s.UpdateWorkflowAtomic(ctx, "proj", "us-central1", "a", func(w Workflow) (Workflow, error) {
		w.RevisionID = "000002-bbb"
		w.SourceContents = "s2"
		return w, nil
	}); err != nil {
		t.Fatalf("atomic update: %v", err)
	}
	revs, _ = s.ListRevisions(ctx, "proj", "us-central1", "a")
	if len(revs) != 2 || revs[0].RevisionID != "000002-bbb" || revs[1].RevisionID != "000001-aaa" {
		t.Fatalf("after revision update: %+v", revs)
	}

	got, err := s.GetRevision(ctx, "proj", "us-central1", "a", "000001-aaa")
	if err != nil {
		t.Fatalf("get revision: %v", err)
	}
	if got.SourceContents != "s1" {
		t.Fatalf("historical source = %q, want s1", got.SourceContents)
	}
	if _, err := s.GetRevision(ctx, "proj", "us-central1", "a", "missing"); err != ErrNoSuchRevision {
		t.Fatalf("expected ErrNoSuchRevision, got %v", err)
	}

	// Deleting the workflow cascades its revisions.
	if err := s.DeleteWorkflow(ctx, "proj", "us-central1", "a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if revs, _ = s.ListRevisions(ctx, "proj", "us-central1", "a"); len(revs) != 0 {
		t.Fatalf("revisions survived delete: %+v", revs)
	}
}

func TestMemoryStoreReset(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateWorkflow(ctx, "proj", "us-central1", "a", Workflow{ID: "a"})
	_ = s.CreateExecution(ctx, "proj", "us-central1", "a", "e1", Execution{ID: "e1"})
	_ = s.CreateOperation(ctx, "proj", "us-central1", Operation{ID: "op1"})
	s.Reset(ctx)
	if _, err := s.GetWorkflow(ctx, "proj", "us-central1", "a"); err != ErrNoSuchWorkflow {
		t.Fatalf("expected ErrNoSuchWorkflow after reset, got %v", err)
	}
	if _, err := s.GetExecution(ctx, "proj", "us-central1", "a", "e1"); err != ErrNoSuchExecution {
		t.Fatalf("expected ErrNoSuchExecution after reset, got %v", err)
	}
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "op1"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation after reset, got %v", err)
	}
}
