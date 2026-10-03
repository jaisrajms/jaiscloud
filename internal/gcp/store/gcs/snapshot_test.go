package gcs

import (
	"bytes"
	"context"
	"testing"
)

func TestMemoryObjectStoreSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryObjectStore()
	if err := s.CreateBucket(ctx, "proj", "b", map[string]any{"projectId": "proj"}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if err := s.PutObjectMeta(ctx, "b", "o", ObjectMeta{Bucket: "b", Name: "o", Generation: "1"}); err != nil {
		t.Fatalf("put object: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	dst := NewMemoryObjectStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if _, err := dst.GetObjectMeta(ctx, "b", "o"); err != nil {
		t.Fatalf("restored object missing: %v", err)
	}
	if empty, _ := dst.IsEmpty(ctx); empty {
		t.Fatal("expected restored store non-empty")
	}
}
