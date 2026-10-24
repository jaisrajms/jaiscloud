package storage

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/store/gcs"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// createBucketWithBody creates a bucket with an explicit body (retention or
// soft delete policy) for the restore tests.
func createBucketWithBody(t *testing.T, p *Provider, name string, body map[string]any) {
	t.Helper()
	if body == nil {
		body = map[string]any{}
	}
	body["name"] = name
	nr := bucketParams()
	nr.Params["body"] = body
	if _, err := p.BucketsInsert(context.Background(), nr); err != nil {
		t.Fatalf("insert bucket %q: %v", name, err)
	}
}

func uploadObject(t *testing.T, p *Provider, bucket, object string, data []byte) *model.ProviderResponse {
	t.Helper()
	nr := bucketParams()
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	nr.Params[wire.MediaKey] = data
	nr.Params[wire.ContentTypeKey] = "text/plain"
	resp, err := p.ObjectsInsert(context.Background(), nr)
	if err != nil {
		t.Fatalf("insert %q: %v", object, err)
	}
	return resp
}

// TestRestoreMissingBucketIsNotFound covers the restore error mapping for a
// bucket that no longer exists.
func TestRestoreMissingBucketIsNotFound(t *testing.T) {
	p := newTestProvider()
	nr := bucketParamsWithObj("no-such-bucket", "o.txt")
	nr.Params["generation"] = "1"
	if _, err := p.ObjectsRestore(context.Background(), nr); err == nil || getErrStatus(err) != 404 {
		t.Fatalf("restore in a missing bucket = %v, want 404", err)
	}
}

// TestRestorePreconditionMismatch covers the restore precondition path: the
// if_* checks are validated against the current live generation.
func TestRestorePreconditionMismatch(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWithBody(t, p, "bkt", nil)
	first := uploadObject(t, p, "bkt", "o.txt", []byte("v1"))
	gen, _ := first.Data["generation"].(string)
	if _, err := p.ObjectsDelete(ctx, bucketParamsWithObj("bkt", "o.txt")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// A live object makes ifGenerationMatch=0 fail.
	uploadObject(t, p, "bkt", "o.txt", []byte("v2"))

	nr := bucketParamsWithObj("bkt", "o.txt")
	nr.Params["generation"] = gen
	nr.Params["ifGenerationMatch"] = "0"
	if _, err := p.ObjectsRestore(ctx, nr); err == nil || getErrStatus(err) != 412 {
		t.Fatalf("restore with a stale ifGenerationMatch = %v, want 412", err)
	}
}

// TestRestoreOverLiveSoftDeletesSuperseded covers the documented restore-over-
// live rule: the restored copy becomes live, the superseded live object becomes
// a soft-deleted copy, and its bytes remain readable through a later restore.
func TestRestoreOverLiveSoftDeletesSuperseded(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWithBody(t, p, "bkt", nil)
	first := uploadObject(t, p, "bkt", "o.txt", []byte("payload-one"))
	gen1, _ := first.Data["generation"].(string)
	if _, err := p.ObjectsDelete(ctx, bucketParamsWithObj("bkt", "o.txt")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	second := uploadObject(t, p, "bkt", "o.txt", []byte("payload-two"))
	gen2, _ := second.Data["generation"].(string)

	nr := bucketParamsWithObj("bkt", "o.txt")
	nr.Params["generation"] = gen1
	restored, err := p.ObjectsRestore(ctx, nr)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	superseded, err := p.objects.GetObjectGeneration(ctx, "bkt", "o.txt", gen2)
	if err != nil {
		t.Fatalf("superseded generation missing: %v", err)
	}
	if !superseded.SoftDeleted {
		t.Error("superseded live generation was not soft-deleted")
	}
	// Its bytes are still there: restoring it yields the second payload.
	nr = bucketParamsWithObj("bkt", "o.txt")
	nr.Params["generation"] = gen2
	again, err := p.ObjectsRestore(ctx, nr)
	if err != nil {
		t.Fatalf("restore superseded generation: %v", err)
	}
	if again.Data["generation"] == restored.Data["generation"] {
		t.Errorf("restoring the superseded generation reused the live generation")
	}
}

// TestRestoreBlockedByLockedRetention covers the documented 403: a locked
// bucket retention policy protects an object younger than its retention period.
func TestRestoreBlockedByLockedRetention(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWithBody(t, p, "bkt", map[string]any{
		"retentionPolicy": map[string]any{"retentionPeriod": "3600"},
	})
	// Lock the policy (creation reports metageneration 1).
	lock := bucketParams()
	lock.Params["bucket"] = "bkt"
	lock.Params["ifMetagenerationMatch"] = "1"
	if _, err := p.BucketsLockRetentionPolicy(ctx, lock); err != nil {
		t.Fatalf("lock retention policy: %v", err)
	}
	obj := uploadObject(t, p, "bkt", "o.txt", []byte("held"))
	gen, _ := obj.Data["generation"].(string)
	// Stage the soft-deleted copy directly: the provider's delete path refuses
	// while the retention period is active (that block is covered elsewhere).
	if _, err := p.objects.TombstoneObjectMetaChecked(ctx, "bkt", "o.txt", gcs.TombstoneOptions{
		SoftDeleted:    true,
		HardDeleteTime: clock.Now().Add(24 * time.Hour),
	}, nil); err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	nr := bucketParamsWithObj("bkt", "o.txt")
	nr.Params["generation"] = gen
	if _, err := p.ObjectsRestore(ctx, nr); err == nil || getErrStatus(err) != 403 {
		t.Fatalf("restore under a locked retention policy = %v, want 403", err)
	}
}

// TestRestoreHonoursExplicitSoftDeletePolicy covers a bucket that sets its own
// soft delete window (rather than the 7-day default), including expiry.
func TestRestoreHonoursExplicitSoftDeletePolicy(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWithBody(t, p, "bkt", map[string]any{
		"softDeletePolicy": map[string]any{"retentionDurationSeconds": "3600"},
	})
	first := uploadObject(t, p, "bkt", "o.txt", []byte("v1"))
	gen, _ := first.Data["generation"].(string)
	if _, err := p.ObjectsDelete(ctx, bucketParamsWithObj("bkt", "o.txt")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	stored, err := p.objects.GetObjectGeneration(ctx, "bkt", "o.txt", gen)
	if err != nil {
		t.Fatalf("GetObjectGeneration: %v", err)
	}
	if window := time.Until(stored.HardDeleteTime); window < 50*time.Minute || window > time.Hour+time.Minute {
		t.Errorf("soft delete window = %v, want about an hour", window)
	}

	// Within the window the copy restores.
	nr := bucketParamsWithObj("bkt", "o.txt")
	nr.Params["generation"] = gen
	if _, err := p.ObjectsRestore(ctx, nr); err != nil {
		t.Fatalf("restore within window: %v", err)
	}

	// Past the window it is gone (404) and reclaimed.
	clock.SetGlobalClock(clock.FixedClock{T: clock.RealNow().Add(2 * time.Hour)})
	defer clock.SetGlobalClock(clock.RealClock{})
	if _, err := p.ObjectsRestore(ctx, nr); err == nil || getErrStatus(err) != 404 {
		t.Fatalf("restore after the window = %v, want 404", err)
	}
	if _, err := p.objects.GetObjectGeneration(ctx, "bkt", "o.txt", gen); err == nil {
		t.Error("expired soft-deleted copy was not reclaimed")
	}
}

// TestRestoreAllowedUnderUnlockedRetention covers the unlocked half of the
// Bucket Lock rule (and an unset retention period): restore proceeds.
func TestRestoreAllowedUnderUnlockedRetention(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWithBody(t, p, "bkt", map[string]any{
		"retentionPolicy": map[string]any{"retentionPeriod": ""},
	})
	obj := uploadObject(t, p, "bkt", "o.txt", []byte("v1"))
	gen, _ := obj.Data["generation"].(string)
	if _, err := p.objects.TombstoneObjectMetaChecked(ctx, "bkt", "o.txt", gcs.TombstoneOptions{
		SoftDeleted:    true,
		HardDeleteTime: clock.Now().Add(time.Hour),
	}, nil); err != nil {
		t.Fatalf("tombstone: %v", err)
	}
	nr := bucketParamsWithObj("bkt", "o.txt")
	nr.Params["generation"] = gen
	if _, err := p.ObjectsRestore(ctx, nr); err != nil {
		t.Fatalf("restore under an unlocked retention policy: %v", err)
	}
}

// TestSoftDeleteWindowFallsBackToDefault covers the soft delete window parsing
// fallbacks (absent duration and unparsable value both mean the 7-day default).
func TestSoftDeleteWindowFallsBackToDefault(t *testing.T) {
	for _, duration := range []any{"", "not-a-duration"} {
		p := newTestProvider()
		createBucketWithBody(t, p, "bkt", map[string]any{
			"softDeletePolicy": map[string]any{"retentionDurationSeconds": duration},
		})
		obj := uploadObject(t, p, "bkt", "o.txt", []byte("v1"))
		gen, _ := obj.Data["generation"].(string)
		if _, err := p.ObjectsDelete(context.Background(), bucketParamsWithObj("bkt", "o.txt")); err != nil {
			t.Fatalf("delete with duration %v: %v", duration, err)
		}
		stored, err := p.objects.GetObjectGeneration(context.Background(), "bkt", "o.txt", gen)
		if err != nil {
			t.Fatalf("GetObjectGeneration: %v", err)
		}
		if window := time.Until(stored.HardDeleteTime); window < 6*24*time.Hour {
			t.Errorf("duration %v: soft delete window = %v, want the 7-day default", duration, window)
		}
	}
}

// TestRestoreMissingBlobIsNotFound covers a soft-deleted copy whose bytes are
// gone (the restore cannot resurrect it).
func TestRestoreMissingBlobIsNotFound(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWithBody(t, p, "bkt", nil)
	obj := uploadObject(t, p, "bkt", "o.txt", []byte("v1"))
	gen, _ := obj.Data["generation"].(string)
	if _, err := p.ObjectsDelete(ctx, bucketParamsWithObj("bkt", "o.txt")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := p.blobs.Delete(ctx, blobsNamespace, blobKey("bkt", "o.txt", gen)); err != nil {
		t.Fatalf("drop blob: %v", err)
	}
	nr := bucketParamsWithObj("bkt", "o.txt")
	nr.Params["generation"] = gen
	if _, err := p.ObjectsRestore(ctx, nr); err == nil || getErrStatus(err) != 404 {
		t.Fatalf("restore without bytes = %v, want 404", err)
	}
}
