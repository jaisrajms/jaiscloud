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

func TestMemoryStoreDeleteRevision(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateService(ctx, "p", "l", Service{ID: "svc"})
	_ = s.CreateRevision(ctx, "p", "l", "svc", Revision{ID: "svc-00001"})
	_ = s.CreateRevision(ctx, "p", "l", "svc", Revision{ID: "svc-00002"})

	if err := s.DeleteRevision(ctx, "p", "l", "svc", "svc-00001"); err != nil {
		t.Fatalf("DeleteRevision: %v", err)
	}
	if _, err := s.GetRevision(ctx, "p", "l", "svc", "svc-00001"); !errors.Is(err, ErrNoSuchRevision) {
		t.Errorf("deleted revision still present: %v", err)
	}
	if _, err := s.GetRevision(ctx, "p", "l", "svc", "svc-00002"); err != nil {
		t.Errorf("sibling revision removed: %v", err)
	}
	if err := s.DeleteRevision(ctx, "p", "l", "svc", "svc-00001"); !errors.Is(err, ErrNoSuchRevision) {
		t.Errorf("delete missing err = %v", err)
	}
}

func TestMemoryStoreSnapshotRestore(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateService(ctx, "p", "l", Service{ID: "svc", Uri: "https://x"})
	_ = s.CreateRevision(ctx, "p", "l", "svc", Revision{ID: "svc-00001", Data: map[string]any{"containers": []any{}}})
	_ = s.CreateOperation(ctx, "p", "l", Operation{ID: "operation-run-1", Done: true, Service: &Service{ID: "svc"}})
	_ = s.CreateOperation(ctx, "p", "l", Operation{ID: "operation-run-2", Done: true, Revision: &Revision{ID: "svc-00001"}})

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
	if op, err := dst.GetOperation(ctx, "p", "l", "operation-run-2"); err != nil || op.Revision == nil || op.Revision.ID != "svc-00001" {
		t.Errorf("restored revision operation = %+v, %v", op, err)
	}
}

func TestMemoryStoreListServicesByProject(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.CreateService(ctx, "p", "us-central1", Service{ID: "b"})
	_ = s.CreateService(ctx, "p", "europe-west1", Service{ID: "a"})
	_ = s.CreateService(ctx, "other", "us-central1", Service{ID: "c"})

	got, err := s.ListServicesByProject(ctx, "p")
	if err != nil {
		t.Fatalf("ListServicesByProject: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d services, want 2", len(got))
	}
	if got[0].ID != "a" || got[0].Location != "europe-west1" || got[1].ID != "b" || got[1].Location != "us-central1" {
		t.Fatalf("unexpected order/scope: %+v", got)
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
