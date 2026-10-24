package logging

import (
	"bytes"
	"context"
	"testing"
)

func TestMemoryStoreAdminSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if err := s.CreateBucket(ctx, "projects/p/locations/global", LogBucket{Name: "b1", Description: "d", RetentionDays: 30}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := s.CreateView(ctx, "projects/p/locations/global/buckets/b1", LogView{Name: "v1", Filter: "severity>=ERROR"}); err != nil {
		t.Fatalf("CreateView: %v", err)
	}
	if err := s.CreateLink(ctx, "projects/p/locations/global/buckets/b1", LogLink{Name: "l1", BigQueryDatasetID: "ds"}); err != nil {
		t.Fatalf("CreateLink: %v", err)
	}
	if err := s.CreateLogScope(ctx, "projects/p/locations/global", LogScope{Name: "s1", ResourceNames: []string{"projects/p"}}); err != nil {
		t.Fatalf("CreateLogScope: %v", err)
	}
	if err := s.SetSettings(ctx, "projects/p", LogSettings{StorageLocation: "us"}); err != nil {
		t.Fatalf("SetSettings: %v", err)
	}
	if err := s.SetCmekSettings(ctx, "projects/p", LogCmekSettings{KmsKeyName: "k"}); err != nil {
		t.Fatalf("SetCmekSettings: %v", err)
	}
	if empty, _ := s.IsEmpty(ctx); empty {
		t.Fatalf("IsEmpty = true after writing admin records")
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	restored := NewMemoryStore()
	if err := restored.Restore(ctx, &buf); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	b, err := restored.GetBucket(ctx, "projects/p/locations/global", "b1")
	if err != nil || b.Description != "d" {
		t.Fatalf("restored bucket = %+v, %v", b, err)
	}
	if v, err := restored.GetView(ctx, "projects/p/locations/global/buckets/b1", "v1"); err != nil || v.Filter != "severity>=ERROR" {
		t.Fatalf("restored view = %+v, %v", v, err)
	}
	if l, err := restored.GetLink(ctx, "projects/p/locations/global/buckets/b1", "l1"); err != nil || l.BigQueryDatasetID != "ds" {
		t.Fatalf("restored link = %+v, %v", l, err)
	}
	if ls, err := restored.GetLogScope(ctx, "projects/p/locations/global", "s1"); err != nil || len(ls.ResourceNames) != 1 {
		t.Fatalf("restored log scope = %+v, %v", ls, err)
	}
	if st, ok, err := restored.GetSettings(ctx, "projects/p"); err != nil || !ok || st.StorageLocation != "us" {
		t.Fatalf("restored settings = %+v, %v", st, err)
	}
	if cs, ok, err := restored.GetCmekSettings(ctx, "projects/p"); err != nil || !ok || cs.KmsKeyName != "k" {
		t.Fatalf("restored cmek = %+v, %v", cs, err)
	}

	restored.Reset(ctx)
	if empty, _ := restored.IsEmpty(ctx); !empty {
		t.Fatalf("IsEmpty = false after Reset")
	}
}
