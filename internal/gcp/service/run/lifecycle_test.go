package run

import (
	"context"
	"testing"

	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/store"
)

// TestParkRemovesRevisionRuntimesKeepsRecords proves Park tears down every
// revision runtime but keeps the service and revision records, and Resume
// recreates only the latest ready revision.
func TestParkRemovesRevisionRuntimesKeepsRecords(t *testing.T) {
	ctx := context.Background()
	rt := &recordingRuntime{}
	s := NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithRuntimeManager(rt))

	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc", createRequest(), false); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	if _, err := s.UpdateService(ctx, "proj", "us-central1", "svc", createRequest(), "template", false, false); err != nil {
		t.Fatalf("UpdateService: %v", err)
	}
	if _, err := s.CreateService(ctx, "proj", "europe-west1", "other", createRequest(), false); err != nil {
		t.Fatalf("CreateService other: %v", err)
	}
	rt.ensured = nil
	rt.removed = nil

	res, err := s.Park(ctx, "proj")
	if err != nil {
		t.Fatalf("Park: %v", err)
	}
	if got := res["revisionsRemoved"]; got != 3 {
		t.Fatalf("revisionsRemoved = %v, want 3 (2 + 1 revisions)", got)
	}

	// Records survive the park.
	if _, err := s.GetService(ctx, "proj", "us-central1", "svc"); err != nil {
		t.Fatalf("service record lost by Park: %v", err)
	}
	revs, err := s.ListRevisions(ctx, "proj", "us-central1", "svc")
	if err != nil || len(revs) != 2 {
		t.Fatalf("ListRevisions = %d, %v; want 2 records intact", len(revs), err)
	}

	if err := s.Resume(ctx, "proj"); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	// One revision per service: the latest ready one.
	if len(rt.ensured) != 2 {
		t.Fatalf("EnsureRevision calls = %v, want 2 (one per service)", rt.ensured)
	}
	for _, id := range rt.ensured {
		if id != "svc-00002" && id != "other-00001" {
			t.Fatalf("Resume ensured an unexpected revision: %v", rt.ensured)
		}
	}
	if s.Name() != "cloudrun" {
		t.Fatalf("Name() = %q, want cloudrun", s.Name())
	}
}
