package storage

import (
	"context"
	"testing"

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
	insertVersionedBucket(t, p, "bkt")

	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "o.txt"
	nr.Params[wire.MediaKey] = []byte("hello")
	ins, err := p.ObjectsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("insert object: %v", err)
	}
	gen, _ := ins.Data["generation"].(string)
	if gen == "" {
		t.Fatal("expected a generation on insert")
	}

	// Deleting in a versioned bucket tombstones the live generation.
	if _, err := p.ObjectsDelete(ctx, bucketParamsWithObj("bkt", "o.txt")); err != nil {
		t.Fatalf("delete object: %v", err)
	}
	if _, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "o.txt")); err == nil {
		t.Fatal("expected object to be soft-deleted")
	}

	nr = bucketParamsWithObj("bkt", "o.txt")
	nr.Params["generation"] = gen
	resp, err := p.ObjectsRestore(ctx, nr)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if resp.Data["generation"] != gen {
		t.Errorf("expected restored generation %q, got %v", gen, resp.Data["generation"])
	}
	if _, present := resp.Data["timeDeleted"]; present {
		t.Errorf("expected timeDeleted cleared on restore, got %v", resp.Data["timeDeleted"])
	}

	if _, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "o.txt")); err != nil {
		t.Fatalf("expected restored object to be live: %v", err)
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
