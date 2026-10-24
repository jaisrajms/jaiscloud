package storage

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

func insertVersionedBucket(t *testing.T, p *Provider, bucket string) {
	t.Helper()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{
		"name":       bucket,
		"versioning": map[string]any{"enabled": true},
	}
	if _, err := p.BucketsInsert(context.Background(), nr); err != nil {
		t.Fatalf("insert versioned bucket: %v", err)
	}
}

func getErrStatus(err error) int {
	if pe, ok := err.(*model.ProviderError); ok {
		return pe.HTTPStatus
	}
	return 0
}

// ─── objects.move ─────────────────────────────────────────────────────────────

func TestObjectsMoveCopiesAndRemovesSource(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "src.txt", "text/plain", map[string]any{"keep": "yes"})

	srcResp, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "src.txt"))
	if err != nil {
		t.Fatalf("get source: %v", err)
	}
	srcGen, _ := srcResp.Data["generation"].(string)

	nr := bucketParams()
	nr.Params["sourceBucket"] = "bkt"
	nr.Params["sourceObject"] = "src.txt"
	nr.Params["destinationBucket"] = "bkt"
	nr.Params["destinationObject"] = "dst.txt"
	resp, err := p.ObjectsMove(ctx, nr)
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	if resp.Data["name"] != "dst.txt" {
		t.Errorf("expected destination name dst.txt, got %v", resp.Data["name"])
	}
	dstGen, _ := resp.Data["generation"].(string)
	if dstGen == "" || dstGen == srcGen {
		t.Errorf("expected a new generation, got %q (source %q)", dstGen, srcGen)
	}
	md, _ := resp.Data["metadata"].(map[string]any)
	if md["keep"] != "yes" {
		t.Errorf("expected metadata copied, got %v", md)
	}

	media, err := p.ObjectsGetMedia(ctx, bucketParamsWithObj("bkt", "dst.txt"))
	if err != nil {
		t.Fatalf("get destination media: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "hello" {
		t.Fatalf("expected moved content 'hello', got %q", got)
	}

	if _, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "src.txt")); err == nil {
		t.Fatal("expected source to be removed after move")
	} else if getErrStatus(err) != 404 {
		t.Fatalf("expected 404 for removed source, got %v", err)
	}
}

func TestObjectsMoveCrossBucket(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "srcbkt", "src.txt", "text/plain", nil)

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "dstbkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert destination bucket: %v", err)
	}

	nr = bucketParams()
	nr.Params["sourceBucket"] = "srcbkt"
	nr.Params["sourceObject"] = "src.txt"
	nr.Params["destinationBucket"] = "dstbkt"
	nr.Params["destinationObject"] = "dst.txt"
	resp, err := p.ObjectsMove(ctx, nr)
	if err != nil {
		t.Fatalf("cross-bucket move: %v", err)
	}
	if resp.Data["bucket"] != "dstbkt" || resp.Data["name"] != "dst.txt" {
		t.Errorf("expected moved object in dstbkt/dst.txt, got %v/%v", resp.Data["bucket"], resp.Data["name"])
	}
	if _, err := p.ObjectsGet(ctx, bucketParamsWithObj("srcbkt", "src.txt")); err == nil {
		t.Fatal("expected source removed after cross-bucket move")
	}
	if _, err := p.ObjectsGet(ctx, bucketParamsWithObj("dstbkt", "dst.txt")); err != nil {
		t.Fatalf("expected destination present: %v", err)
	}
}

func TestObjectsMoveMissingSource(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	nr = bucketParams()
	nr.Params["sourceBucket"] = "bkt"
	nr.Params["sourceObject"] = "missing.txt"
	nr.Params["destinationBucket"] = "bkt"
	nr.Params["destinationObject"] = "dst.txt"
	if _, err := p.ObjectsMove(ctx, nr); err == nil {
		t.Fatal("expected 404 moving a missing source")
	} else if getErrStatus(err) != 404 {
		t.Fatalf("expected 404 ProviderError, got %v", err)
	}
}

// ─── objects.restore ──────────────────────────────────────────────────────────

func TestObjectsRestoreSoftDeletedGeneration(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	// Soft delete is a bucket policy (default on); the deleted generation must
	// stay restorable, and the restore produces a NEW object copy.
	ins := insertTestObject(t, p, "bkt", "o.txt", "text/plain", map[string]any{"k": "v"})
	gen, _ := ins.Data["generation"].(string)
	if gen == "" {
		t.Fatal("expected a generation on insert")
	}

	if _, err := p.ObjectsDelete(ctx, bucketParamsWithObj("bkt", "o.txt")); err != nil {
		t.Fatalf("delete object: %v", err)
	}
	if _, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "o.txt")); err == nil {
		t.Fatal("expected the deleted object to be gone from the live surface")
	}

	nr := bucketParamsWithObj("bkt", "o.txt")
	nr.Params["generation"] = gen
	resp, err := p.ObjectsRestore(ctx, nr)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	// Real GCS assigns a fresh generation and resets the metageneration.
	if resp.Data["generation"] == gen {
		t.Errorf("restore reused generation %q, want a new one", gen)
	}
	if resp.Data["metageneration"] != "1" {
		t.Errorf("metageneration = %v, want 1", resp.Data["metageneration"])
	}
	if _, present := resp.Data["softDeleteTime"]; present {
		t.Errorf("expected softDeleteTime cleared on restore, got %v", resp.Data["softDeleteTime"])
	}
	// Metadata is inherited from the soft-deleted object.
	meta, _ := resp.Data["metadata"].(map[string]any)
	if meta["k"] != "v" {
		t.Errorf("expected inherited metadata, got %v", resp.Data["metadata"])
	}

	live, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "o.txt"))
	if err != nil {
		t.Fatalf("expected restored object to be live: %v", err)
	}
	if live.Data["generation"] != resp.Data["generation"] {
		t.Errorf("live generation = %v, want %v", live.Data["generation"], resp.Data["generation"])
	}

	// The soft-deleted source is retained for its retention window, so restoring
	// it again creates another live copy (real GCS keeps the original).
	nr = bucketParamsWithObj("bkt", "o.txt")
	nr.Params["generation"] = gen
	again, err := p.ObjectsRestore(ctx, nr)
	if err != nil {
		t.Fatalf("second restore of a retained soft-deleted generation: %v", err)
	}
	if again.Data["generation"] == resp.Data["generation"] {
		t.Errorf("second restore reused generation %v, want a new one", again.Data["generation"])
	}

	// Soft-deleted generations are invisible to the live and versions listings,
	// and visible only through ?softDeleted=true.
	listParams := bucketParams()
	listParams.Params["bucket"] = "bkt"
	listed, err := p.ObjectsList(ctx, listParams)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := listed.Data["items"].([]any)
	for _, it := range items {
		if m, _ := it.(map[string]any); m["generation"] == gen {
			t.Errorf("soft-deleted generation appeared in the live listing")
		}
	}
	soft := bucketParams()
	soft.Params["bucket"] = "bkt"
	soft.Params["softDeleted"] = "true"
	listed, err = p.ObjectsList(ctx, soft)
	if err != nil {
		t.Fatalf("list softDeleted: %v", err)
	}
	items, _ = listed.Data["items"].([]any)
	if len(items) == 0 {
		t.Error("expected the soft-deleted generation in the ?softDeleted=true listing")
	}
}

// TestSoftDeletedGenerationIsInaccessible pins the documented restrictions: a
// soft-deleted generation cannot be read, downloaded, modified or permanently
// deleted through the normal object surface.
func TestSoftDeletedGenerationIsInaccessible(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	ins := insertTestObject(t, p, "bkt", "o.txt", "text/plain", nil)
	gen, _ := ins.Data["generation"].(string)
	if _, err := p.ObjectsDelete(ctx, bucketParamsWithObj("bkt", "o.txt")); err != nil {
		t.Fatalf("delete: %v", err)
	}

	get := bucketParamsWithObj("bkt", "o.txt")
	get.Params["generation"] = gen
	if _, err := p.ObjectsGet(ctx, get); err == nil || getErrStatus(err) != 404 {
		t.Fatalf("Get of a soft-deleted generation = %v, want 404", err)
	}
	show := bucketParamsWithObj("bkt", "o.txt")
	show.Params["generation"] = gen
	show.Params["softDeleted"] = "true"
	if _, err := p.ObjectsGet(ctx, show); err != nil {
		t.Fatalf("Get?softDeleted=true: %v", err)
	}
	media := bucketParamsWithObj("bkt", "o.txt")
	media.Params["generation"] = gen
	media.Params["softDeleted"] = "true"
	if _, err := p.ObjectsGetMedia(ctx, media); err == nil || getErrStatus(err) != 404 {
		t.Fatalf("media download of a soft-deleted generation = %v, want 404", err)
	}
	patch := bucketParamsWithObj("bkt", "o.txt")
	patch.Params["generation"] = gen
	patch.Params["body"] = map[string]any{"metadata": map[string]any{"k": "v"}}
	if _, err := p.ObjectsPatch(ctx, patch); err == nil || getErrStatus(err) != 404 {
		t.Fatalf("patch of a soft-deleted generation = %v, want 404", err)
	}
	// objects.delete?generation= cannot permanently delete a soft-deleted copy.
	del := bucketParamsWithObj("bkt", "o.txt")
	del.Params["generation"] = gen
	if _, err := p.ObjectsDelete(ctx, del); err == nil || getErrStatus(err) != 404 {
		t.Fatalf("delete?generation of a soft-deleted copy = %v, want 404", err)
	}
}

// TestOverwriteRetainsSoftDeletedPrior pins the soft-delete overwrite rule: a
// re-upload keeps the replaced generation as a soft-deleted copy, and a later
// upload must not destroy it.
func TestOverwriteRetainsSoftDeletedPrior(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	first := insertTestObject(t, p, "bkt", "o.txt", "text/plain", nil)
	oldGen, _ := first.Data["generation"].(string)

	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "o.txt"
	nr.Params[wire.MediaKey] = []byte("second")
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	retained, err := p.objects.GetObjectGeneration(ctx, "bkt", "o.txt", oldGen)
	if err != nil || !retained.SoftDeleted {
		t.Fatalf("overwritten generation not soft-deleted: meta=%+v err=%v", retained, err)
	}

	// Another upload of the same name must leave the retained copy alone.
	nr.Params[wire.MediaKey] = []byte("third")
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("second overwrite: %v", err)
	}
	if _, err := p.objects.GetObjectGeneration(ctx, "bkt", "o.txt", oldGen); err != nil {
		t.Fatalf("retained soft-deleted copy was destroyed by a later upload: %v", err)
	}
}

// TestSoftDeletedOnlyBucketCanBeDeleted pins buckets.delete: a bucket holding
// nothing but soft-deleted objects can be deleted (live/noncurrent block it).
func TestSoftDeletedOnlyBucketCanBeDeleted(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "o.txt", "text/plain", nil)
	if _, err := p.ObjectsDelete(ctx, bucketParamsWithObj("bkt", "o.txt")); err != nil {
		t.Fatalf("delete object: %v", err)
	}
	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	if _, err := p.BucketsDelete(ctx, nr); err != nil {
		t.Fatalf("delete bucket with only soft-deleted objects: %v", err)
	}
}

// TestRestoreExpiredSoftDeletedGeneration pins the documented expiry: a copy
// whose soft-delete window closed is gone (404) and is reclaimed.
func TestRestoreExpiredSoftDeletedGeneration(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	ins := insertTestObject(t, p, "bkt", "o.txt", "text/plain", nil)
	gen, _ := ins.Data["generation"].(string)
	if _, err := p.ObjectsDelete(ctx, bucketParamsWithObj("bkt", "o.txt")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Advance past the 7-day default soft-delete window the delete recorded.
	clock.SetGlobalClock(clock.FixedClock{T: clock.RealNow().Add(8 * 24 * time.Hour)})
	defer clock.SetGlobalClock(clock.RealClock{})
	nr := bucketParamsWithObj("bkt", "o.txt")
	nr.Params["generation"] = gen
	if _, err := p.ObjectsRestore(ctx, nr); err == nil || getErrStatus(err) != 404 {
		t.Fatalf("restore of an expired copy = %v, want 404", err)
	}
}

// TestObjectsRestoreRejectsNoncurrentGeneration pins the documented 412: a
// noncurrent (archived) version is not soft-deleted, so restore refuses it.
func TestObjectsRestoreRejectsNoncurrentGeneration(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertVersionedBucket(t, p, "bkt")

	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "o.txt"
	nr.Params[wire.MediaKey] = []byte("v1")
	first, err := p.ObjectsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("insert v1: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "o.txt"
	nr.Params[wire.MediaKey] = []byte("v2")
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("insert v2: %v", err)
	}

	// Versioned buckets archive the live generation on delete, which is a
	// noncurrent version, not a soft-deleted one.
	if _, err := p.ObjectsDelete(ctx, bucketParamsWithObj("bkt", "o.txt")); err != nil {
		t.Fatalf("delete object: %v", err)
	}
	nr = bucketParamsWithObj("bkt", "o.txt")
	nr.Params["generation"] = first.Data["generation"].(string)
	if _, err := p.ObjectsRestore(ctx, nr); err == nil || getErrStatus(err) != 412 {
		t.Fatalf("expected 412 restoring a noncurrent generation, got %v", err)
	}
}

// TestObjectsRestoreRejectedWhenSoftDeleteDisabled pins the documented 400: a
// bucket with soft delete turned off has nothing to restore.
func TestObjectsRestoreRejectedWhenSoftDeleteDisabled(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	bucket := bucketParams()
	bucket.Params["body"] = map[string]any{
		"name":             "bkt",
		"softDeletePolicy": map[string]any{"retentionDurationSeconds": "0"},
		"versioning":       map[string]any{"enabled": true},
	}
	if _, err := p.BucketsInsert(ctx, bucket); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "o.txt"
	nr.Params[wire.MediaKey] = []byte("v1")
	ins, err := p.ObjectsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := p.ObjectsDelete(ctx, bucketParamsWithObj("bkt", "o.txt")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	nr = bucketParamsWithObj("bkt", "o.txt")
	nr.Params["generation"] = ins.Data["generation"].(string)
	if _, err := p.ObjectsRestore(ctx, nr); err == nil || getErrStatus(err) != 400 {
		t.Fatalf("expected 400 with soft delete disabled, got %v", err)
	}
}

func TestObjectsRestoreMissingGeneration(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertVersionedBucket(t, p, "bkt")

	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "o.txt"
	nr.Params[wire.MediaKey] = []byte("hello")
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("insert object: %v", err)
	}

	nr = bucketParamsWithObj("bkt", "o.txt")
	nr.Params["generation"] = "999999"
	if _, err := p.ObjectsRestore(ctx, nr); err == nil {
		t.Fatal("expected 404 restoring a missing generation")
	} else if getErrStatus(err) != 404 {
		t.Fatalf("expected 404 ProviderError, got %v", err)
	}
}

func TestObjectsRestoreRequiresGeneration(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "o.txt", "text/plain", nil)

	if _, err := p.ObjectsRestore(ctx, bucketParamsWithObj("bkt", "o.txt")); err == nil {
		t.Fatal("expected InvalidRequest when generation is omitted")
	} else if getErrStatus(err) != 400 {
		t.Fatalf("expected 400 ProviderError, got %v", err)
	}
}

// ─── buckets.lockRetentionPolicy ──────────────────────────────────────────────

func TestBucketsLockRetentionPolicy(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{
		"name":            "bkt",
		"retentionPolicy": map[string]any{"retentionPeriod": "3600"},
	}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	resp, err := p.BucketsLockRetentionPolicy(ctx, nr)
	if err != nil {
		t.Fatalf("lock retention policy: %v", err)
	}
	rp, _ := resp.Data["retentionPolicy"].(map[string]any)
	if rp == nil {
		t.Fatal("expected retentionPolicy on locked bucket")
	}
	if locked, _ := rp["isLocked"].(bool); !locked {
		t.Errorf("expected isLocked=true, got %v", rp["isLocked"])
	}
	if et, _ := rp["effectiveTime"].(string); et == "" {
		t.Error("expected effectiveTime to be set on lock")
	}

	// Locking is idempotent.
	if _, err := p.BucketsLockRetentionPolicy(ctx, nr); err != nil {
		t.Fatalf("second lock should be idempotent: %v", err)
	}

	// Removing a locked policy must fail.
	removeNR := bucketParams()
	removeNR.Params["bucket"] = "bkt"
	removeNR.Params["body"] = map[string]any{"retentionPolicy": nil}
	if _, err := p.BucketsUpdate(ctx, removeNR); err == nil {
		t.Fatal("expected locked retention policy removal to fail")
	}

	// Shortening a locked policy must fail.
	shortenNR := bucketParams()
	shortenNR.Params["bucket"] = "bkt"
	shortenNR.Params["body"] = map[string]any{"retentionPolicy": map[string]any{"retentionPeriod": "60"}}
	if _, err := p.BucketsUpdate(ctx, shortenNR); err == nil {
		t.Fatal("expected locked retention policy shortening to fail")
	}

	// Extending a locked policy is allowed and preserves the lock.
	extendNR := bucketParams()
	extendNR.Params["bucket"] = "bkt"
	extendNR.Params["body"] = map[string]any{"retentionPolicy": map[string]any{"retentionPeriod": "7200"}}
	ext, err := p.BucketsUpdate(ctx, extendNR)
	if err != nil {
		t.Fatalf("extending a locked policy should succeed: %v", err)
	}
	extRP, _ := ext.Data["retentionPolicy"].(map[string]any)
	if locked, _ := extRP["isLocked"].(bool); !locked {
		t.Errorf("expected isLocked preserved after extension, got %v", extRP["isLocked"])
	}
	if extRP["retentionPeriod"] != "7200" {
		t.Errorf("expected retentionPeriod 7200 after extension, got %v", extRP["retentionPeriod"])
	}
}

func TestBucketsLockRetentionPolicyNoPolicy(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	if _, err := p.BucketsLockRetentionPolicy(ctx, nr); err == nil {
		t.Fatal("expected 404 for a bucket with no retention policy")
	} else if getErrStatus(err) != 404 {
		t.Fatalf("expected 404 ProviderError, got %v", err)
	}
}

// ─── Bucket.cors ──────────────────────────────────────────────────────────────

func testCORSConfig() []any {
	return []any{
		map[string]any{
			"origin":         []any{"https://example.com"},
			"method":         []any{"GET", "POST"},
			"responseHeader": []any{"Content-Type"},
			"maxAgeSeconds":  float64(3600),
		},
	}
}

func TestBucketCORSRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt", "cors": testCORSConfig()}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	resp, err := p.BucketsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	got, _ := resp.Data["cors"].([]any)
	if len(got) != 1 {
		t.Fatalf("expected 1 cors rule on get, got %v", resp.Data["cors"])
	}
	rule, _ := got[0].(map[string]any)
	if rule["maxAgeSeconds"] != float64(3600) {
		t.Errorf("expected maxAgeSeconds 3600, got %v", rule["maxAgeSeconds"])
	}

	// The gateway lookup sees the stored rules.
	rules := p.GetBucketCORSRules("bkt")
	if len(rules) != 1 {
		t.Fatalf("expected 1 stored CORS rule, got %d", len(rules))
	}

	// buckets.list returns it too.
	list, err := p.BucketsList(ctx, bucketParams())
	if err != nil {
		t.Fatalf("list buckets: %v", err)
	}
	items, _ := list.Data["items"].([]any)
	found := false
	for _, it := range items {
		m, _ := it.(map[string]any)
		if m["name"] == "bkt" {
			found = true
			if _, ok := m["cors"].([]any); !ok {
				t.Errorf("expected cors in bucket list, got %v", m["cors"])
			}
		}
	}
	if !found {
		t.Fatal("bucket not found in list")
	}

	// Updating replaces the CORS config.
	newCORS := []any{
		map[string]any{
			"origin":        []any{"*"},
			"method":        []any{"GET"},
			"maxAgeSeconds": float64(600),
		},
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["body"] = map[string]any{"cors": newCORS}
	upd, err := p.BucketsUpdate(ctx, nr)
	if err != nil {
		t.Fatalf("update bucket cors: %v", err)
	}
	gotUpd, _ := upd.Data["cors"].([]any)
	if len(gotUpd) != 1 {
		t.Fatalf("expected 1 cors rule after update, got %v", upd.Data["cors"])
	}
	if u, _ := gotUpd[0].(map[string]any); u["maxAgeSeconds"] != float64(600) {
		t.Errorf("expected maxAgeSeconds 600 after update, got %v", u["maxAgeSeconds"])
	}

	// Clearing CORS with an explicit null removes it.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["body"] = map[string]any{"cors": nil}
	cleared, err := p.BucketsUpdate(ctx, nr)
	if err != nil {
		t.Fatalf("clear bucket cors: %v", err)
	}
	if _, present := cleared.Data["cors"]; present {
		t.Errorf("expected cors cleared, got %v", cleared.Data["cors"])
	}
	if len(p.GetBucketCORSRules("bkt")) != 0 {
		t.Error("expected no stored CORS rules after clearing")
	}
}
