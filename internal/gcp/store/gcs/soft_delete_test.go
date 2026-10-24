package gcs

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
)

func softDeleteFixture(t *testing.T) (*MemoryObjectStore, context.Context) {
	t.Helper()
	s := NewMemoryObjectStore()
	ctx := context.Background()
	if err := s.CreateBucket(ctx, "proj", "bkt", nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	return s, ctx
}

func putLive(t *testing.T, s *MemoryObjectStore, ctx context.Context, gen string) ObjectMeta {
	t.Helper()
	meta := ObjectMeta{Bucket: "bkt", Name: "o", Generation: gen, Metageneration: "1"}
	if err := s.PutObjectMeta(ctx, "bkt", "o", meta); err != nil {
		t.Fatalf("PutObjectMeta: %v", err)
	}
	return meta
}

// TestTombstoneOptionsRoundTrip covers the two non-live states: a noncurrent
// archive (versioning) and a soft-deleted copy with its purge deadline.
func TestTombstoneOptionsRoundTrip(t *testing.T) {
	s, ctx := softDeleteFixture(t)
	putLive(t, s, ctx, "1")

	deadline := clock.Now().Add(7 * 24 * time.Hour)
	got, err := s.TombstoneObjectMetaChecked(ctx, "bkt", "o", TombstoneOptions{SoftDeleted: true, HardDeleteTime: deadline}, nil)
	if err != nil {
		t.Fatalf("TombstoneObjectMetaChecked: %v", err)
	}
	if !got.SoftDeleted || got.TimeDeleted == nil {
		t.Fatalf("tombstone = %+v, want SoftDeleted with TimeDeleted set", got)
	}
	if !got.HardDeleteTime.Equal(deadline) {
		t.Errorf("HardDeleteTime = %v, want %v", got.HardDeleteTime, deadline)
	}

	// The live lookup must no longer resolve the soft-deleted generation.
	if _, err := s.GetObjectMeta(ctx, "bkt", "o"); err != ErrNoSuchObject {
		t.Fatalf("GetObjectMeta after soft delete = %v, want ErrNoSuchObject", err)
	}
	// ... but the generation itself is still addressable for restore.
	if _, err := s.GetObjectGeneration(ctx, "bkt", "o", "1"); err != nil {
		t.Fatalf("GetObjectGeneration: %v", err)
	}
}

// TestNoncurrentTombstoneHasNoSoftDeleteState pins the distinction restore
// relies on: a versioning archive is noncurrent, not soft-deleted.
func TestNoncurrentTombstoneHasNoSoftDeleteState(t *testing.T) {
	s, ctx := softDeleteFixture(t)
	putLive(t, s, ctx, "1")

	got, err := s.TombstoneObjectMeta(ctx, "bkt", "o", TombstoneOptions{})
	if err != nil {
		t.Fatalf("TombstoneObjectMeta: %v", err)
	}
	if got.SoftDeleted || !got.HardDeleteTime.IsZero() {
		t.Fatalf("noncurrent tombstone = %+v, want no soft-delete state", got)
	}
}

// TestReplaceKeepsSoftDeletedGenerations covers the overwrite rule: replacing
// the live generation in a non-versioned bucket must not drop retained copies.
func TestReplaceKeepsSoftDeletedGenerations(t *testing.T) {
	s, ctx := softDeleteFixture(t)
	putLive(t, s, ctx, "1")
	if _, err := s.TombstoneObjectMeta(ctx, "bkt", "o", TombstoneOptions{SoftDeleted: true, HardDeleteTime: clock.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	// A new live generation replaces nothing (there is no live row) and must
	// leave the soft-deleted copy alone.
	if err := s.PutObjectMeta(ctx, "bkt", "o", ObjectMeta{Bucket: "bkt", Name: "o", Generation: "2"}); err != nil {
		t.Fatalf("PutObjectMeta: %v", err)
	}
	if _, err := s.GetObjectGeneration(ctx, "bkt", "o", "1"); err != nil {
		t.Fatalf("soft-deleted generation lost on insert: %v", err)
	}
	// Keep the fixture honest: the live generation is the new one.
	live, err := s.GetObjectMeta(ctx, "bkt", "o")
	if err != nil || live.Generation != "2" {
		t.Fatalf("live generation = %+v, %v; want 2", live, err)
	}
}

// TestListObjectVersionsIncludesSoftDeleted pins the raw store contract: the
// caller filters (the versions listing hides soft-deleted copies, the
// ?softDeleted=true listing shows only them).
func TestListObjectVersionsIncludesSoftDeleted(t *testing.T) {
	s, ctx := softDeleteFixture(t)
	putLive(t, s, ctx, "1")
	if _, err := s.TombstoneObjectMeta(ctx, "bkt", "o", TombstoneOptions{SoftDeleted: true, HardDeleteTime: clock.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	vers, err := s.ListObjectVersions(ctx, "bkt")
	if err != nil {
		t.Fatalf("ListObjectVersions: %v", err)
	}
	if len(vers) != 1 || !vers[0].SoftDeleted {
		t.Fatalf("versions = %+v, want the soft-deleted generation", vers)
	}
	if objs, err := s.ListObjects(ctx, "bkt"); err != nil || len(objs) != 0 {
		t.Fatalf("ListObjects = %+v, %v; want no live objects", objs, err)
	}
}

// TestDeleteBucketWithOnlySoftDeletedObjects pins the documented bucket-delete
// rule: only live or noncurrent objects block it.
func TestDeleteBucketWithOnlySoftDeletedObjects(t *testing.T) {
	s, ctx := softDeleteFixture(t)
	putLive(t, s, ctx, "1")
	if _, err := s.TombstoneObjectMeta(ctx, "bkt", "o", TombstoneOptions{SoftDeleted: true, HardDeleteTime: clock.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	if err := s.DeleteBucket(ctx, "bkt"); err != nil {
		t.Fatalf("DeleteBucket with only soft-deleted objects: %v", err)
	}

	// A noncurrent generation still blocks deletion.
	if err := s.CreateBucket(ctx, "proj", "bkt2", nil); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if err := s.PutObjectMeta(ctx, "bkt2", "o", ObjectMeta{Bucket: "bkt2", Name: "o", Generation: "1"}); err != nil {
		t.Fatalf("PutObjectMeta: %v", err)
	}
	if _, err := s.TombstoneObjectMeta(ctx, "bkt2", "o", TombstoneOptions{}); err != nil {
		t.Fatalf("tombstone noncurrent: %v", err)
	}
	if err := s.DeleteBucket(ctx, "bkt2"); err != ErrBucketNotEmpty {
		t.Fatalf("DeleteBucket with a noncurrent generation = %v, want ErrBucketNotEmpty", err)
	}
}

// TestPutObjectMetaCheckedKeepsNonLiveRows covers the checked replace path used
// by an overwrite: deleting the live row must not remove retained generations.
func TestPutObjectMetaCheckedKeepsNonLiveRows(t *testing.T) {
	s, ctx := softDeleteFixture(t)
	putLive(t, s, ctx, "1")
	if _, err := s.TombstoneObjectMeta(ctx, "bkt", "o", TombstoneOptions{SoftDeleted: true, HardDeleteTime: clock.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	if err := s.PutObjectMetaChecked(ctx, "bkt", "o", ObjectMeta{Bucket: "bkt", Name: "o", Generation: "2"}, nil); err != nil {
		t.Fatalf("PutObjectMetaChecked: %v", err)
	}
	if _, err := s.GetObjectGeneration(ctx, "bkt", "o", "1"); err != nil {
		t.Fatalf("checked replace dropped the soft-deleted generation: %v", err)
	}
}
