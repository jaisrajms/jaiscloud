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
