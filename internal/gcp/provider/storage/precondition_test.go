package storage

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

func assertPrecondition412(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected a PreconditionFailed error, got nil")
	}
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.HTTPStatus != 412 {
		t.Fatalf("expected HTTP 412, got %v", err)
	}
}

// TestObjectsInsert_IfGenerationMatchZero_CreateOnlyIfAbsent verifies GCS's
// most common conditional-write idiom: ifGenerationMatch=0 means "only
// create if no live object currently exists at this name" — used for
// GCS-based locks/idempotent-create. Before this fix, the codec decoded the
// query param into nr.Params but the provider never read it, so this
// precondition was silently ignored and every insert always "succeeded"
// regardless of whether the object already existed.
func TestObjectsInsert_IfGenerationMatchZero_CreateOnlyIfAbsent(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	insert := func(data string) (*model.ProviderResponse, error) {
		nr := bucketParamsWithObj("bkt", "obj.txt")
		nr.Params[wire.MediaKey] = []byte(data)
		nr.Params["ifGenerationMatch"] = "0"
		return p.ObjectsInsert(ctx, nr)
	}

	// First insert: no live object exists, ifGenerationMatch=0 matches (0
	// live generations) — must succeed.
	first, err := insert("v1")
	if err != nil {
		t.Fatalf("first insert (should succeed, no live object yet): %v", err)
	}
	if first.Data["generation"] == "" {
		t.Fatal("expected a non-empty generation on the created object")
	}

	// Second insert: a live object now exists, ifGenerationMatch=0 no longer
	// matches — must be rejected with 412, and the original content must
	// survive untouched.
	if _, err := insert("v2-should-not-apply"); err == nil {
		t.Fatal("expected the second create-only-if-absent insert to fail, got nil error")
	} else {
		assertPrecondition412(t, err)
	}

	getNR := bucketParamsWithObj("bkt", "obj.txt")
	getResp, err := p.ObjectsGetMedia(ctx, getNR)
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	if got := streamBytes(t, getResp); string(got) != "v1" {
		t.Fatalf("object content = %q, want %q (the rejected second insert must not have applied)", got, "v1")
	}
}

// TestObjectsInsert_IfGenerationMatch_StaleGenerationRejected verifies the
// general (not just =0) case: a client overwriting an object it expects to
// still be at a specific generation must be rejected if that's no longer
// true.
func TestObjectsInsert_IfGenerationMatch_StaleGenerationRejected(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	resp := insertTestObject(t, p, "bkt", "obj.txt", "text/plain", nil)
	staleGen, _ := resp.Data["generation"].(string)
	if staleGen == "" {
		t.Fatal("expected a generation on the inserted object")
	}

	// A concurrent writer overwrites the object (no precondition), advancing
	// its generation.
	overwrite := bucketParamsWithObj("bkt", "obj.txt")
	overwrite.Params[wire.MediaKey] = []byte("concurrent-write")
	if _, err := p.ObjectsInsert(ctx, overwrite); err != nil {
		t.Fatalf("concurrent overwrite: %v", err)
	}

	// A stale writer, still holding the ORIGINAL generation, attempts a
	// conditional overwrite. Must be rejected.
	stale := bucketParamsWithObj("bkt", "obj.txt")
	stale.Params[wire.MediaKey] = []byte("stale-write-should-not-apply")
	stale.Params["ifGenerationMatch"] = staleGen
	if _, err := p.ObjectsInsert(ctx, stale); err == nil {
		t.Fatal("expected the stale-generation insert to fail, got nil error")
	} else {
		assertPrecondition412(t, err)
	}

	getResp, err := p.ObjectsGetMedia(ctx, bucketParamsWithObj("bkt", "obj.txt"))
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	if got := streamBytes(t, getResp); string(got) != "concurrent-write" {
		t.Fatalf("object content = %q, want %q (the stale write must not have applied)", got, "concurrent-write")
	}
}

// TestObjectsPatch_IfMetagenerationMatch verifies the metadata-mutation
// precondition on objects.patch: a stale metageneration is rejected, a
// matching one applies.
func TestObjectsPatch_IfMetagenerationMatch(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertResp := insertTestObject(t, p, "bkt", "obj.txt", "text/plain", nil)
	metagen, _ := insertResp.Data["metageneration"].(string)
	if metagen == "" {
		t.Fatal("expected a metageneration on the inserted object")
	}

	// Stale metageneration: rejected.
	stale := bucketParamsWithObj("bkt", "obj.txt")
	stale.Params["ifMetagenerationMatch"] = "999"
	stale.Params["body"] = map[string]any{"contentType": "application/json"}
	if _, err := p.ObjectsPatch(ctx, stale); err == nil {
		t.Fatal("expected a stale metageneration patch to fail, got nil error")
	} else {
		assertPrecondition412(t, err)
	}
	unchanged, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "obj.txt"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if unchanged.Data["contentType"] == "application/json" {
		t.Fatal("the rejected patch must not have applied")
	}

	// Matching metageneration: applies.
	match := bucketParamsWithObj("bkt", "obj.txt")
	match.Params["ifMetagenerationMatch"] = metagen
	match.Params["body"] = map[string]any{"contentType": "application/json"}
	resp, err := p.ObjectsPatch(ctx, match)
	if err != nil {
		t.Fatalf("matching metageneration patch: %v", err)
	}
	if resp.Data["contentType"] != "application/json" {
		t.Fatalf("expected contentType applied, got %v", resp.Data["contentType"])
	}
}

// TestObjectsDelete_IfGenerationMatch verifies the "safe delete" idiom: a
// delete conditioned on the generation the client last observed must be
// rejected if the object has since changed, and the object must survive.
func TestObjectsDelete_IfGenerationMatch(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	resp := insertTestObject(t, p, "bkt", "obj.txt", "text/plain", nil)
	staleGen, _ := resp.Data["generation"].(string)

	// Object changes underneath the client (overwrite advances generation).
	overwrite := bucketParamsWithObj("bkt", "obj.txt")
	overwrite.Params[wire.MediaKey] = []byte("new-content")
	if _, err := p.ObjectsInsert(ctx, overwrite); err != nil {
		t.Fatalf("overwrite: %v", err)
	}

	// Delete conditioned on the stale generation: rejected, object survives.
	del := bucketParamsWithObj("bkt", "obj.txt")
	del.Params["ifGenerationMatch"] = staleGen
	if _, err := p.ObjectsDelete(ctx, del); err == nil {
		t.Fatal("expected the stale-generation delete to fail, got nil error")
	} else {
		assertPrecondition412(t, err)
	}
	if _, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "obj.txt")); err != nil {
		t.Fatalf("object must still exist after a rejected conditional delete, got %v", err)
	}

	// Delete conditioned on the CURRENT generation: succeeds.
	current, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "obj.txt"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	currentGen, _ := current.Data["generation"].(string)
	del2 := bucketParamsWithObj("bkt", "obj.txt")
	del2.Params["ifGenerationMatch"] = currentGen
	if _, err := p.ObjectsDelete(ctx, del2); err != nil {
		t.Fatalf("delete with correct generation: %v", err)
	}
	if _, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "obj.txt")); err == nil {
		t.Fatal("expected the object to be gone after a matching conditional delete")
	}
}

// TestBucketsUpdate_MetagenerationPrecondition verifies buckets.update honors
// GCS's ifMetagenerationMatch/ifMetagenerationNotMatch preconditions atomically
// with the write: a stale match is rejected with 412 and the bucket is left
// unchanged, a matching one applies and advances the metageneration.
func TestBucketsUpdate_MetagenerationPrecondition(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	created, err := p.BucketsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	if got, _ := created.Data["metageneration"].(string); got != "1" {
		t.Fatalf("new bucket metageneration = %q, want \"1\"", got)
	}

	get := func() (string, string) {
		nr := bucketParams()
		nr.Params["bucket"] = "bkt"
		resp, err := p.BucketsGet(ctx, nr)
		if err != nil {
			t.Fatalf("get bucket: %v", err)
		}
		metagen, _ := resp.Data["metageneration"].(string)
		sc, _ := resp.Data["storageClass"].(string)
		return metagen, sc
	}
	update := func(params map[string]any, storageClass string) (*model.ProviderResponse, error) {
		nr := bucketParams()
		nr.Params["bucket"] = "bkt"
		for k, v := range params {
			nr.Params[k] = v
		}
		nr.Params["body"] = map[string]any{"storageClass": storageClass}
		return p.BucketsUpdate(ctx, nr)
	}

	// Stale ifMetagenerationMatch: rejected, bucket unchanged.
	if _, err := update(map[string]any{"ifMetagenerationMatch": "999"}, "NEARLINE"); err == nil {
		t.Fatal("expected a stale ifMetagenerationMatch update to fail")
	} else {
		assertPrecondition412(t, err)
	}
	if metagen, sc := get(); metagen != "1" || sc != "STANDARD" {
		t.Fatalf("rejected update must not apply: metageneration=%q storageClass=%q", metagen, sc)
	}

	// Matching ifMetagenerationMatch: applies and bumps 1 → 2.
	resp, err := update(map[string]any{"ifMetagenerationMatch": "1"}, "NEARLINE")
	if err != nil {
		t.Fatalf("matching metageneration update: %v", err)
	}
	if got, _ := resp.Data["metageneration"].(string); got != "2" {
		t.Fatalf("after update metageneration = %q, want \"2\"", got)
	}
	if metagen, sc := get(); metagen != "2" || sc != "NEARLINE" {
		t.Fatalf("matching update not applied: metageneration=%q storageClass=%q", metagen, sc)
	}

	// The now-stale value is rejected again.
	if _, err := update(map[string]any{"ifMetagenerationMatch": "1"}, "COLDLINE"); err == nil {
		t.Fatal("expected the same now-stale match to fail")
	} else {
		assertPrecondition412(t, err)
	}

	// ifMetagenerationNotMatch fails when it equals the current metageneration…
	if _, err := update(map[string]any{"ifMetagenerationNotMatch": "2"}, "COLDLINE"); err == nil {
		t.Fatal("expected ifMetagenerationNotMatch=2 to fail against current 2")
	} else {
		assertPrecondition412(t, err)
	}

	// …and applies against a different one, bumping 2 → 3.
	if _, err := update(map[string]any{"ifMetagenerationNotMatch": "1"}, "COLDLINE"); err != nil {
		t.Fatalf("ifMetagenerationNotMatch against a different value: %v", err)
	}
	if metagen, sc := get(); metagen != "3" || sc != "COLDLINE" {
		t.Fatalf("not-match update not applied: metageneration=%q storageClass=%q", metagen, sc)
	}

	// No precondition still bumps (3 → 4).
	if _, err := update(nil, "STANDARD"); err != nil {
		t.Fatalf("unconditional update: %v", err)
	}
	if metagen, _ := get(); metagen != "4" {
		t.Fatalf("unconditional update metageneration = %q, want \"4\"", metagen)
	}
}

// TestObjectsGet_ReadPreconditions verifies objects.get honors read
// preconditions: a stale ifGenerationMatch/ifMetagenerationMatch is 412, while
// an ifGenerationNotMatch matching the current generation is 304 Not Modified
// (the conditional-download idiom), not 412.
func TestObjectsGet_ReadPreconditions(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertResp := insertTestObject(t, p, "bkt", "obj.txt", "text/plain", nil)
	gen, _ := insertResp.Data["generation"].(string)
	if gen == "" {
		t.Fatal("expected a generation on the inserted object")
	}

	// Stale ifGenerationMatch on a metadata read: 412.
	stale := bucketParamsWithObj("bkt", "obj.txt")
	stale.Params["ifGenerationMatch"] = "999999"
	if _, err := p.ObjectsGet(ctx, stale); err == nil {
		t.Fatal("expected stale ifGenerationMatch read to fail, got nil")
	} else {
		assertPrecondition412(t, err)
	}

	// Stale ifMetagenerationMatch: 412.
	staleMeta := bucketParamsWithObj("bkt", "obj.txt")
	staleMeta.Params["ifMetagenerationMatch"] = "999"
	if _, err := p.ObjectsGet(ctx, staleMeta); err == nil {
		t.Fatal("expected stale ifMetagenerationMatch read to fail, got nil")
	} else {
		assertPrecondition412(t, err)
	}

	// ifGenerationNotMatch matching the current generation: 304, not 412.
	notMod := bucketParamsWithObj("bkt", "obj.txt")
	notMod.Params["ifGenerationNotMatch"] = gen
	resp, err := p.ObjectsGet(ctx, notMod)
	if err != nil {
		t.Fatalf("ifGenerationNotMatch read must not error, got %v", err)
	}
	if resp.HTTPStatus != 304 {
		t.Fatalf("ifGenerationNotMatch match = HTTP %d, want 304", resp.HTTPStatus)
	}

	// ifGenerationNotMatch against a different generation: normal 200 read.
	fresh := bucketParamsWithObj("bkt", "obj.txt")
	fresh.Params["ifGenerationNotMatch"] = "999999"
	resp, err = p.ObjectsGet(ctx, fresh)
	if err != nil {
		t.Fatalf("ifGenerationNotMatch mismatch read: %v", err)
	}
	if resp.HTTPStatus != 200 {
		t.Fatalf("ifGenerationNotMatch mismatch = HTTP %d, want 200", resp.HTTPStatus)
	}
}

// TestObjectsGetMedia_ReadPreconditions verifies the same read preconditions on
// the media (download) path: a not-match read returns 304 without a body, and a
// matching read returns the bytes.
func TestObjectsGetMedia_ReadPreconditions(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertResp := insertTestObject(t, p, "bkt", "obj.txt", "text/plain", nil)
	gen, _ := insertResp.Data["generation"].(string)

	notMod := bucketParamsWithObj("bkt", "obj.txt")
	notMod.Params["ifGenerationNotMatch"] = gen
	resp, err := p.ObjectsGetMedia(ctx, notMod)
	if err != nil {
		t.Fatalf("conditional media read must not error, got %v", err)
	}
	if resp.HTTPStatus != 304 {
		t.Fatalf("ifGenerationNotMatch media read = HTTP %d, want 304", resp.HTTPStatus)
	}

	ok := bucketParamsWithObj("bkt", "obj.txt")
	ok.Params["ifGenerationMatch"] = gen
	resp, err = p.ObjectsGetMedia(ctx, ok)
	if err != nil {
		t.Fatalf("matching media read: %v", err)
	}
	if resp.HTTPStatus != 200 {
		t.Fatalf("matching media read = HTTP %d, want 200", resp.HTTPStatus)
	}
	if got := string(streamBytes(t, resp)); got == "" {
		t.Fatal("expected media bytes on a matching read")
	}
}

// TestObjectsCopy_SourcePrecondition verifies objects.copy enforces the
// ifSourceGenerationMatch/NotMatch preconditions against the source object, as
// the GCS JSON API documents for rewrite/copy source preconditions.
func TestObjectsCopy_SourcePrecondition(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertResp := insertTestObject(t, p, "bkt", "src.txt", "text/plain", nil)
	gen, _ := insertResp.Data["generation"].(string)

	copy := func(srcGen string) (*model.ProviderResponse, error) {
		nr := bucketParams()
		nr.Params["sourceBucket"] = "bkt"
		nr.Params["sourceObject"] = "src.txt"
		nr.Params["destinationBucket"] = "bkt"
		nr.Params["destinationObject"] = "dst.txt"
		nr.Params["ifSourceGenerationMatch"] = srcGen
		return p.ObjectsCopy(ctx, nr)
	}

	// Stale source generation: 412, destination not created.
	if _, err := copy("999999"); err == nil {
		t.Fatal("expected stale ifSourceGenerationMatch copy to fail, got nil")
	} else {
		assertPrecondition412(t, err)
	}
	if _, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "dst.txt")); err == nil {
		t.Fatal("destination must not exist after a rejected copy")
	}

	// Matching source generation: copy succeeds.
	if _, err := copy(gen); err != nil {
		t.Fatalf("matching ifSourceGenerationMatch copy: %v", err)
	}
	if _, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "dst.txt")); err != nil {
		t.Fatalf("destination must exist after a successful copy: %v", err)
	}
}
