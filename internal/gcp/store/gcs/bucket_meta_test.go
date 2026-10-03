package gcs

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
)

// bumpMetageneration applies the "N" → "N+1" increment a buckets.update would.
func bumpMetageneration(m map[string]any) {
	n, _ := strconv.Atoi(BucketMetageneration(m))
	m["metageneration"] = strconv.Itoa(n + 1)
}

func TestMemoryObjectStoreBucketMetagenerationAtomic(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryObjectStore()
	if err := s.CreateBucket(ctx, "proj", "bkt", nil); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	meta, err := s.GetBucket(ctx, "bkt")
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	if meta["metageneration"] != "1" {
		t.Fatalf("new bucket metageneration = %v, want \"1\"", meta["metageneration"])
	}

	// A successful atomic update bumps the metageneration and applies the field.
	updated, err := s.UpdateBucketMetaAtomic(ctx, "bkt", func(m map[string]any) (map[string]any, error) {
		bumpMetageneration(m)
		m["storageClass"] = "NEARLINE"
		return m, nil
	})
	if err != nil {
		t.Fatalf("atomic update: %v", err)
	}
	if updated["metageneration"] != "2" || updated["storageClass"] != "NEARLINE" {
		t.Fatalf("after update: %v", updated)
	}

	// A rejected precondition (sentinel) leaves the bucket untouched.
	tooNew := int64(999)
	if _, err := s.UpdateBucketMetaAtomic(ctx, "bkt", func(m map[string]any) (map[string]any, error) {
		if !BucketMetagenerationMatches(m, &Precondition{MetagenerationMatch: &tooNew}) {
			return nil, ErrPreconditionFailed
		}
		return m, nil
	}); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("expected ErrPreconditionFailed, got %v", err)
	}
	meta, _ = s.GetBucket(ctx, "bkt")
	if meta["metageneration"] != "2" || meta["storageClass"] != "NEARLINE" {
		t.Fatalf("rejected update must not apply: %v", meta)
	}

	// Matching precondition applies.
	one := int64(2)
	if _, err := s.UpdateBucketMetaAtomic(ctx, "bkt", func(m map[string]any) (map[string]any, error) {
		if !BucketMetagenerationMatches(m, &Precondition{MetagenerationMatch: &one}) {
			return nil, ErrPreconditionFailed
		}
		bumpMetageneration(m)
		return m, nil
	}); err != nil {
		t.Fatalf("matching update: %v", err)
	}
	if meta, _ = s.GetBucket(ctx, "bkt"); meta["metageneration"] != "3" {
		t.Fatalf("metageneration = %v after matching update, want \"3\"", meta["metageneration"])
	}

	// Missing bucket.
	if _, err := s.UpdateBucketMetaAtomic(ctx, "missing", func(m map[string]any) (map[string]any, error) {
		return m, nil
	}); !errors.Is(err, ErrNoSuchBucket) {
		t.Fatalf("expected ErrNoSuchBucket, got %v", err)
	}
}

// TestMemoryObjectStoreBucketMetagenerationNoLostUpdates is the regression test
// for the read-modify-write race AtomicUpdate closes: N concurrent updates each
// increment the metageneration; without a lock spanning the read and the write,
// increments would be lost and the final value would be less than N+1.
func TestMemoryObjectStoreBucketMetagenerationNoLostUpdates(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryObjectStore()
	if err := s.CreateBucket(ctx, "proj", "bkt", nil); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	const n = 64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.UpdateBucketMetaAtomic(ctx, "bkt", func(m map[string]any) (map[string]any, error) {
				bumpMetageneration(m)
				return m, nil
			}); err != nil {
				t.Errorf("atomic update: %v", err)
			}
		}()
	}
	wg.Wait()
	meta, err := s.GetBucket(ctx, "bkt")
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	if got := BucketMetageneration(meta); got != strconv.Itoa(n+1) {
		t.Fatalf("metageneration = %s after %d concurrent updates, want %d (lost updates)", got, n, n+1)
	}
}
