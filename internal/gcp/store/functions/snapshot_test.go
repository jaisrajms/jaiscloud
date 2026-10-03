package functions

import (
	"bytes"
	"context"
	"testing"
)

func TestMemoryStoreSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	f := Function{ID: "a", Runtime: "nodejs20", EntryPoint: "hello",
		EnvironmentVariables: map[string]string{"K": "V"}, Labels: map[string]string{"team": "x"},
		MinInstanceCount: 2, MaxInstanceCount: 10, MaxInstanceRequestConcurrency: 80, AvailableCPU: "0.5"}
	if err := s.CreateFunction(ctx, "proj", "us-central1", "a", f); err != nil {
		t.Fatalf("create: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	dst := NewMemoryStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := dst.GetFunction(ctx, "proj", "us-central1", "a")
	if err != nil {
		t.Fatalf("get after restore: %v", err)
	}
	if got.Runtime != "nodejs20" || got.EntryPoint != "hello" || got.EnvironmentVariables["K"] != "V" || got.Labels["team"] != "x" {
		t.Fatalf("restored function wrong: %+v", got)
	}
	if got.MinInstanceCount != 2 || got.MaxInstanceCount != 10 || got.MaxInstanceRequestConcurrency != 80 || got.AvailableCPU != "0.5" {
		t.Fatalf("instance config not restored: %+v", got)
	}
}

func TestMemoryStoreSnapshotKeepsSourceMetadata(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	f := Function{ID: "s", Runtime: "python312", EntryPoint: "main.handler",
		SourceSHA256: "abc123", SourceSize: 42, SourceBlobKey: "functions/p/us/s/abc123.zip"}
	if err := s.CreateFunction(ctx, "p", "us", "s", f); err != nil {
		t.Fatalf("create: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	dst := NewMemoryStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := dst.GetFunction(ctx, "p", "us", "s")
	if err != nil {
		t.Fatalf("get after restore: %v", err)
	}
	if got.SourceSHA256 != "abc123" || got.SourceSize != 42 || got.SourceBlobKey != "functions/p/us/s/abc123.zip" {
		t.Fatalf("source metadata not restored: %+v", got)
	}
}

// TestMemoryStoreSnapshotKeepsEventTriggerBacking covers the FP2 persisted
// surface: a storage event trigger's provisioned backing trigger name and
// dead-letter subscription survive a snapshot/restore cycle (they are stored as
// JSONB, so no migration is needed).
func TestMemoryStoreSnapshotKeepsEventTriggerBacking(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	f := Function{ID: "g", Runtime: "nodejs20", EntryPoint: "handler",
		EventTrigger: &EventTrigger{
			EventType:    "google.storage.object.finalize",
			Resource:     "projects/_/buckets/bkt",
			Trigger:      "projects/p/locations/us/triggers/functions-g",
			Subscription: "eventarc-us-functions-g",
		}}
	if err := s.CreateFunction(ctx, "p", "us", "g", f); err != nil {
		t.Fatalf("create: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	dst := NewMemoryStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := dst.GetFunction(ctx, "p", "us", "g")
	if err != nil {
		t.Fatalf("get after restore: %v", err)
	}
	if got.EventTrigger == nil || got.EventTrigger.Trigger == "" || got.EventTrigger.Subscription != "eventarc-us-functions-g" {
		t.Fatalf("event trigger backing not restored: %+v", got.EventTrigger)
	}
}
