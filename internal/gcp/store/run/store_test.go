package run

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestMemoryStoreCRUDAndCascade(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if err := s.CreateService(ctx, "p", "l", Service{ID: "svc", Data: map[string]any{"description": "x"}}); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if _, err := s.GetService(ctx, "p", "l", "svc"); err != nil {
		t.Fatalf("GetService: %v", err)
	}
	if err := s.CreateService(ctx, "p", "l", Service{ID: "svc"}); !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("duplicate err = %v", err)
	}
	if err := s.CreateRevision(ctx, "p", "l", "svc", Revision{ID: "svc-00001"}); err != nil {
		t.Fatalf("CreateRevision: %v", err)
	}
	if _, err := s.GetRevision(ctx, "p", "l", "svc", "svc-00001"); err != nil {
		t.Fatalf("GetRevision: %v", err)
	}
	if err := s.DeleteService(ctx, "p", "l", "svc"); err != nil {
		t.Fatalf("DeleteService: %v", err)
	}
	if _, err := s.GetRevision(ctx, "p", "l", "svc", "svc-00001"); !errors.Is(err, ErrNoSuchRevision) {
		t.Errorf("revision not cascaded: %v", err)
	}
}

func TestMemoryStoreSnapshotRestore(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateService(ctx, "p", "l", Service{ID: "svc", Uri: "https://x"})
	_ = s.CreateRevision(ctx, "p", "l", "svc", Revision{ID: "svc-00001", Data: map[string]any{"containers": []any{}}})
	_ = s.CreateOperation(ctx, "p", "l", Operation{ID: "operation-run-1", Done: true, Service: &Service{ID: "svc"}})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	dst := NewMemoryStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if svc, err := dst.GetService(ctx, "p", "l", "svc"); err != nil || svc.Uri != "https://x" {
		t.Errorf("restored service = %+v, %v", svc, err)
	}
	if rev, err := dst.GetRevision(ctx, "p", "l", "svc", "svc-00001"); err != nil || rev.ID != "svc-00001" {
		t.Errorf("restored revision = %+v, %v", rev, err)
	}
	if op, err := dst.GetOperation(ctx, "p", "l", "operation-run-1"); err != nil || op.Service == nil || op.Service.ID != "svc" {
		t.Errorf("restored operation = %+v, %v", op, err)
	}
}

func TestMemoryStoreUpdateDelete(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateService(ctx, "p", "l", Service{ID: "svc", Data: map[string]any{"description": "a"}})
	if err := s.UpdateService(ctx, "p", "l", Service{ID: "svc", Data: map[string]any{"description": "b"}}); err != nil {
		t.Fatalf("UpdateService: %v", err)
	}
	svc, _ := s.GetService(ctx, "p", "l", "svc")
	if svc.Data["description"] != "b" {
		t.Errorf("description = %v", svc.Data["description"])
	}
	if err := s.UpdateService(ctx, "p", "l", Service{ID: "missing"}); !errors.Is(err, ErrNoSuchService) {
		t.Errorf("update missing err = %v", err)
	}
	if err := s.DeleteService(ctx, "p", "l", "missing"); !errors.Is(err, ErrNoSuchService) {
		t.Errorf("delete missing err = %v", err)
	}
}
