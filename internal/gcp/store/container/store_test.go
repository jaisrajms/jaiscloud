package container

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestMemoryStoreClusterRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	c := Cluster{Name: "c1", Status: StatusRunning, CurrentMasterVersion: "1.30"}
	if err := s.CreateCluster(ctx, "p", "l", c); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if err := s.CreateCluster(ctx, "p", "l", c); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate err = %v, want ErrAlreadyExists", err)
	}
	got, err := s.GetCluster(ctx, "p", "l", "c1")
	if err != nil || got.Name != "c1" || got.ProjectID != "p" || got.Location != "l" {
		t.Fatalf("GetCluster = %+v, %v", got, err)
	}
	if list, err := s.ListClusters(ctx, "p", "l"); err != nil || len(list) != 1 {
		t.Fatalf("ListClusters = %d, %v", len(list), err)
	}
	if err := s.DeleteCluster(ctx, "p", "l", "c1"); err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if _, err := s.GetCluster(ctx, "p", "l", "c1"); !errors.Is(err, ErrNoSuchCluster) {
		t.Fatalf("after delete err = %v, want ErrNoSuchCluster", err)
	}
	if err := s.DeleteCluster(ctx, "p", "l", "c1"); !errors.Is(err, ErrNoSuchCluster) {
		t.Fatalf("delete missing err = %v, want ErrNoSuchCluster", err)
	}
}

func TestMemoryStoreOperationRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	op := Operation{Name: "operation-1", OperationType: OperationCreateCluster, Status: OperationStatusDone}
	if err := s.CreateOperation(ctx, "p", "l", op); err != nil {
		t.Fatalf("CreateOperation: %v", err)
	}
	if err := s.CreateOperation(ctx, "p", "l", op); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate op err = %v, want ErrAlreadyExists", err)
	}
	got, err := s.GetOperation(ctx, "p", "l", "operation-1")
	if err != nil || got.Name != "operation-1" || got.OperationType != OperationCreateCluster {
		t.Fatalf("GetOperation = %+v, %v", got, err)
	}
	if list, err := s.ListOperations(ctx, "p", "l"); err != nil || len(list) != 1 {
		t.Fatalf("ListOperations = %d, %v", len(list), err)
	}
	if _, err := s.GetOperation(ctx, "p", "l", "missing"); !errors.Is(err, ErrNoSuchOperation) {
		t.Fatalf("missing op err = %v, want ErrNoSuchOperation", err)
	}
}

func TestMemoryStoreSnapshotRestore(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if err := s.CreateCluster(ctx, "p", "l", Cluster{Name: "c1", Status: StatusRunning}); err != nil {
		t.Fatalf("seed cluster: %v", err)
	}
	if err := s.CreateOperation(ctx, "p", "l", Operation{Name: "operation-1", OperationType: OperationDeleteCluster}); err != nil {
		t.Fatalf("seed operation: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	dst := NewMemoryStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, err := dst.GetCluster(ctx, "p", "l", "c1"); err != nil {
		t.Fatalf("restored cluster: %v", err)
	}
	if _, err := dst.GetOperation(ctx, "p", "l", "operation-1"); err != nil {
		t.Fatalf("restored operation: %v", err)
	}
	if empty, err := dst.IsEmpty(ctx); err != nil || empty {
		t.Fatalf("IsEmpty after restore = %v, %v", empty, err)
	}
}

func TestMemoryStoreReset(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateCluster(ctx, "p", "l", Cluster{Name: "c1"})
	_ = s.CreateOperation(ctx, "p", "l", Operation{Name: "operation-1"})
	s.Reset(ctx)
	if empty, err := s.IsEmpty(ctx); err != nil || !empty {
		t.Fatalf("IsEmpty after reset = %v, %v", empty, err)
	}
}
