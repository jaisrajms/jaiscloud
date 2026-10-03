package functions

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestMemoryStoreOperationCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	f := &Function{ID: "a", Runtime: "nodejs20", EntryPoint: "hello"}
	op := Operation{
		ID: "op1", Location: "us-central1", Done: true, Verb: "create",
		Target:   "projects/proj/locations/us-central1/functions/a",
		Function: f, CreateTime: time.Now(), EndTime: time.Now(),
	}
	if err := s.CreateOperation(ctx, "proj", "us-central1", op); err != nil {
		t.Fatalf("create op: %v", err)
	}

	got, err := s.GetOperation(ctx, "proj", "us-central1", "op1")
	if err != nil {
		t.Fatalf("get op: %v", err)
	}
	if !got.Done || got.Verb != "create" || got.Target != op.Target {
		t.Fatalf("unexpected operation: %+v", got)
	}
	if got.Function == nil || got.Function.Runtime != "nodejs20" || got.Function.EntryPoint != "hello" {
		t.Fatalf("response snapshot not preserved: %+v", got.Function)
	}

	// A delete mutation has no Function snapshot.
	if err := s.CreateOperation(ctx, "proj", "us-central1", Operation{ID: "op2", Verb: "delete", Done: true}); err != nil {
		t.Fatalf("create delete op: %v", err)
	}
	del, _ := s.GetOperation(ctx, "proj", "us-central1", "op2")
	if del.Function != nil {
		t.Fatalf("delete operation must carry no Function, got %+v", del.Function)
	}

	// Location- and project-scoped.
	if _, err := s.GetOperation(ctx, "proj", "europe-west1", "op1"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation for other location, got %v", err)
	}
	if _, err := s.GetOperation(ctx, "other", "us-central1", "op1"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation for other project, got %v", err)
	}

	// List is location-scoped and sorted by id.
	all, err := s.ListOperations(ctx, "proj", "us-central1")
	if err != nil {
		t.Fatalf("list ops: %v", err)
	}
	if len(all) != 2 || all[0].ID != "op1" || all[1].ID != "op2" {
		t.Fatalf("unexpected op list: %+v", all)
	}

	// Delete.
	if err := s.DeleteOperation(ctx, "proj", "us-central1", "op1"); err != nil {
		t.Fatalf("delete op: %v", err)
	}
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "op1"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation after delete, got %v", err)
	}
	if err := s.DeleteOperation(ctx, "proj", "us-central1", "op1"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation deleting missing, got %v", err)
	}

	// Reset clears operations too.
	s.Reset(ctx)
	if _, err := s.GetOperation(ctx, "proj", "us-central1", "op2"); err != ErrNoSuchOperation {
		t.Fatalf("expected ErrNoSuchOperation after reset, got %v", err)
	}
}

func TestMemoryStoreSnapshotKeepsOperations(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	f := &Function{ID: "a", Runtime: "python312", EntryPoint: "main.handler"}
	if err := s.CreateOperation(ctx, "proj", "us-central1", Operation{
		ID: "op1", Done: true, Verb: "create", Function: f, CreateTime: time.Now(),
	}); err != nil {
		t.Fatalf("create op: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	dst := NewMemoryStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := dst.GetOperation(ctx, "proj", "us-central1", "op1")
	if err != nil {
		t.Fatalf("get after restore: %v", err)
	}
	if got.Verb != "create" || got.Function == nil || got.Function.Runtime != "python312" {
		t.Fatalf("restored operation wrong: %+v", got)
	}
	empty, err := dst.IsEmpty(ctx)
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if empty {
		t.Fatalf("store with an operation reported empty")
	}
}
