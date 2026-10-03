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
