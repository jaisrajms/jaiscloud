package gcs

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStoreVersioningGenerations(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryObjectStore()
	if err := s.CreateBucket(ctx, "proj", "bkt", nil); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	now := time.Now()
	gen1 := ObjectMeta{Bucket: "bkt", Name: "o", Generation: "10", Size: 1, TimeCreated: now, Updated: now}
	gen2 := ObjectMeta{Bucket: "bkt", Name: "o", Generation: "11", Size: 2, TimeCreated: now.Add(time.Second), Updated: now.Add(time.Second)}

	if err := s.PutObjectGeneration(ctx, "bkt", "o", gen1); err != nil {
		t.Fatalf("put gen1: %v", err)
	}
	if err := s.PutObjectGeneration(ctx, "bkt", "o", gen2); err != nil {
		t.Fatalf("put gen2: %v", err)
	}

	// Live generation is the newest.
	live, err := s.GetObjectMeta(ctx, "bkt", "o")
	if err != nil || live.Generation != "11" {
		t.Fatalf("expected live generation 11, got %+v / %v", live, err)
	}

	// The archived generation is readable by id and carries timeDeleted.
	old, err := s.GetObjectGeneration(ctx, "bkt", "o", "10")
	if err != nil || old.Generation != "10" || old.TimeDeleted == nil {
		t.Fatalf("expected archived generation 10 with timeDeleted, got %+v / %v", old, err)
	}

	// A missing generation returns ErrNoSuchObject.
	if _, err := s.GetObjectGeneration(ctx, "bkt", "o", "99"); !errors.Is(err, ErrNoSuchObject) {
		t.Fatalf("expected ErrNoSuchObject for missing generation, got %v", err)
	}

	// Live listing sees only the newest generation.
	liveList, err := s.ListObjects(ctx, "bkt")
	if err != nil || len(liveList) != 1 || liveList[0].Generation != "11" {
		t.Fatalf("expected 1 live object (gen 11), got %+v / %v", liveList, err)
	}

	// Versions listing sees both generations.
	vers, err := s.ListObjectVersions(ctx, "bkt")
	if err != nil || len(vers) != 2 {
		t.Fatalf("expected 2 versions, got %+v / %v", vers, err)
	}
}

// TestStoreUpdateObjectMetaKeepsGenerations verifies UpdateObjectMetaChecked
// mutates only the live generation in place: the live generation id is
// unchanged (only the metageneration bumps), every noncurrent generation
// survives, and a mismatch/absent live generation changes nothing.
func TestStoreUpdateObjectMetaKeepsGenerations(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryObjectStore()
	if err := s.CreateBucket(ctx, "proj", "bkt", nil); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	now := time.Now()
	gen1 := ObjectMeta{Bucket: "bkt", Name: "o", Generation: "10", Metageneration: "1", ContentType: "text/plain", Size: 1, TimeCreated: now, Updated: now}
	gen2 := ObjectMeta{
		Bucket: "bkt", Name: "o", Generation: "11", Metageneration: "1",
		ContentType: "text/plain", Size: 2, MD5Hash: "md5-original", CRC32C: "crc-original",
		KmsKeyName: "projects/p/locations/l/keyRings/r/cryptoKeys/k", WrappedDEK: []byte("wrapped-dek"), CSEKeySHA256: "cse-sha",
		TimeCreated: now.Add(time.Second), Updated: now.Add(time.Second),
	}
	if err := s.PutObjectGeneration(ctx, "bkt", "o", gen1); err != nil {
		t.Fatalf("put gen1: %v", err)
	}
	if err := s.PutObjectGeneration(ctx, "bkt", "o", gen2); err != nil {
		t.Fatalf("put gen2: %v", err)
	}

	// The caller supplies only the mutable fields (as the wire object does):
	// immutable fields — generation, size, checksums, creation time, and key
	// material incl. WrappedDEK — must be taken from storage, not the caller.
	update := ObjectMeta{
		Generation:     "999",
		Metageneration: "2",
		ContentType:    "application/json",
		Metadata:       map[string]string{"k": "v"},
		Updated:        now.Add(2 * time.Second),
	}
	if err := s.UpdateObjectMetaChecked(ctx, "bkt", "o", update, nil); err != nil {
		t.Fatalf("update: %v", err)
	}

	// Both generations survive, so the noncurrent revision was not discarded.
	vers, err := s.ListObjectVersions(ctx, "bkt")
	if err != nil || len(vers) != 2 {
		t.Fatalf("versions after update = %d, %v; want 2", len(vers), err)
	}
	got, err := s.GetObjectMeta(ctx, "bkt", "o")
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if got.Generation != "11" || got.Metageneration != "2" || got.ContentType != "application/json" || got.Metadata["k"] != "v" {
		t.Fatalf("live after update = %+v", got)
	}
	if got.Size != 2 {
		t.Fatalf("live size = %d, want 2 (immutable, in-place)", got.Size)
	}
	// Immutable fields (incl. the wrapped DEK, absent from the wire object)
	// must be preserved from storage.
	if string(got.WrappedDEK) != "wrapped-dek" || got.KmsKeyName != gen2.KmsKeyName ||
		got.MD5Hash != "md5-original" || got.CRC32C != "crc-original" || got.CSEKeySHA256 != "cse-sha" ||
		!got.TimeCreated.Equal(gen2.TimeCreated) {
		t.Fatalf("immutable fields not preserved after update: %+v", got)
	}
	old, err := s.GetObjectGeneration(ctx, "bkt", "o", "10")
	if err != nil || old.ContentType != "text/plain" || old.Metadata != nil || old.Metageneration != "1" {
		t.Fatalf("noncurrent gen 10 mutated: %+v / %v", old, err)
	}

	// A mismatching precondition leaves the live generation unchanged.
	bad := int64(99)
	cur, _ := s.GetObjectMeta(ctx, "bkt", "o")
	cur.ContentType = "application/xml"
	if err := s.UpdateObjectMetaChecked(ctx, "bkt", "o", cur, &Precondition{GenerationMatch: &bad}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
	}
	cur2, _ := s.GetObjectMeta(ctx, "bkt", "o")
	if cur2.ContentType != "application/json" {
		t.Fatalf("rejected update was applied: contentType = %q", cur2.ContentType)
	}

	// No live generation → ErrNoSuchObject.
	if err := s.UpdateObjectMetaChecked(ctx, "bkt", "missing", gen1, nil); !errors.Is(err, ErrNoSuchObject) {
		t.Fatalf("expected ErrNoSuchObject, got %v", err)
	}
}

func TestStoreObjectRetentionHoldsRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryObjectStore()
	if err := s.CreateBucket(ctx, "proj", "bkt", nil); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	until := time.Now().Add(time.Hour).UTC()
	o := ObjectMeta{
		Bucket: "bkt", Name: "r", Generation: "1",
		Retention:     &ObjectRetention{RetainUntilTime: until, Mode: "Unlocked"},
		TemporaryHold: true,
		TimeCreated:   time.Now(), Updated: time.Now(),
	}
	if err := s.PutObjectMeta(ctx, "bkt", "r", o); err != nil {
		t.Fatalf("put object: %v", err)
	}
	got, err := s.GetObjectMeta(ctx, "bkt", "r")
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	if got.Retention == nil || got.Retention.Mode != "Unlocked" || !got.Retention.RetainUntilTime.Equal(until) {
		t.Fatalf("retention not round-tripped: %+v", got.Retention)
	}
	if !got.TemporaryHold {
		t.Fatal("expected temporaryHold true")
	}
}
