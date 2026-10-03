package storage

import (
	"context"
	"strings"
	"testing"

	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// providerErr asserts err is a *model.ProviderError with the given HTTP status
// and returns it for message checks.
func providerErr(t *testing.T, err error, status int) *model.ProviderError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected a %d provider error, got nil", status)
	}
	perr, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if perr.HTTPStatus != status {
		t.Fatalf("error status = %d, want %d (%v)", perr.HTTPStatus, status, err)
	}
	return perr
}

func createBucketWith(t *testing.T, p *Provider, name string, body map[string]any) {
	t.Helper()
	nr := bucketParams()
	b := map[string]any{"name": name}
	for k, v := range body {
		b[k] = v
	}
	nr.Params["body"] = b
	if _, err := p.BucketsInsert(context.Background(), nr); err != nil {
		t.Fatalf("insert bucket %s: %v", name, err)
	}
}

func putObject(t *testing.T, p *Provider, bucket, object string, body map[string]any) *model.ProviderResponse {
	t.Helper()
	nr := bucketParams()
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	nr.Params[wire.MediaKey] = []byte("hello")
	nr.Params[wire.ContentTypeKey] = "text/plain"
	if body != nil {
		nr.Params["body"] = body
	}
	resp, err := p.ObjectsInsert(context.Background(), nr)
	if err != nil {
		t.Fatalf("insert object %s/%s: %v", bucket, object, err)
	}
	return resp
}

func patchHold(t *testing.T, p *Provider, bucket, object, hold string, value bool) *model.ProviderResponse {
	t.Helper()
	nr := bucketParamsWithObj(bucket, object)
	nr.Params["body"] = map[string]any{hold: value}
	resp, err := p.ObjectsPatch(context.Background(), nr)
	if err != nil {
		t.Fatalf("patch %s=%v: %v", hold, value, err)
	}
	return resp
}

func TestTemporaryHoldBlocksDeleteAndOverwrite(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWith(t, p, "hold-tmp", nil)
	putObject(t, p, "hold-tmp", "obj.txt", nil)

	patched := patchHold(t, p, "hold-tmp", "obj.txt", "temporaryHold", true)
	if patched.Data["temporaryHold"] != true {
		t.Fatalf("patch did not return temporaryHold=true: %v", patched.Data)
	}
	got, err := p.ObjectsGet(ctx, bucketParamsWithObj("hold-tmp", "obj.txt"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Data["temporaryHold"] != true {
		t.Fatalf("get temporaryHold = %v, want true", got.Data["temporaryHold"])
	}

	_, err = p.ObjectsDelete(ctx, bucketParamsWithObj("hold-tmp", "obj.txt"))
	perr := providerErr(t, err, 403)
	if !strings.Contains(perr.Message, "temporary hold") {
		t.Fatalf("delete message = %q, want it to contain %q", perr.Message, "temporary hold")
	}

	// Overwriting a held object is rejected too.
	over := bucketParamsWithObj("hold-tmp", "obj.txt")
	over.Params[wire.MediaKey] = []byte("new")
	_, err = p.ObjectsInsert(ctx, over)
	perr = providerErr(t, err, 403)
	if !strings.Contains(perr.Message, "temporary hold") {
		t.Fatalf("overwrite message = %q, want it to contain %q", perr.Message, "temporary hold")
	}

	// Releasing the hold allows both.
	patchHold(t, p, "hold-tmp", "obj.txt", "temporaryHold", false)
	if _, err := p.ObjectsDelete(ctx, bucketParamsWithObj("hold-tmp", "obj.txt")); err != nil {
		t.Fatalf("delete after releasing hold: %v", err)
	}
}

func TestEventBasedHoldBlocksDeleteAndOverwrite(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWith(t, p, "hold-evt", nil)
	putObject(t, p, "hold-evt", "obj.txt", nil)

	patchHold(t, p, "hold-evt", "obj.txt", "eventBasedHold", true)

	_, err := p.ObjectsDelete(ctx, bucketParamsWithObj("hold-evt", "obj.txt"))
	perr := providerErr(t, err, 403)
	if !strings.Contains(perr.Message, "event-based hold") {
		t.Fatalf("delete message = %q, want it to contain %q", perr.Message, "event-based hold")
	}

	over := bucketParamsWithObj("hold-evt", "obj.txt")
	over.Params[wire.MediaKey] = []byte("new")
	_, err = p.ObjectsInsert(ctx, over)
	perr = providerErr(t, err, 403)
	if !strings.Contains(perr.Message, "event-based hold") {
		t.Fatalf("overwrite message = %q, want it to contain %q", perr.Message, "event-based hold")
	}
}

// TestBucketDefaultEventBasedHoldInherited verifies a new object inherits the
// bucket's defaultEventBasedHold unless the insert explicitly overrides it.
func TestBucketDefaultEventBasedHoldInherited(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWith(t, p, "hold-def", map[string]any{"defaultEventBasedHold": true})

	// The bucket returns the field.
	b, err := p.BucketsGet(ctx, func() *model.NormalizedRequest {
		nr := bucketParams()
		nr.Params["bucket"] = "hold-def"
		return nr
	}())
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	if b.Data["defaultEventBasedHold"] != true {
		t.Fatalf("bucket defaultEventBasedHold = %v, want true", b.Data["defaultEventBasedHold"])
	}

	inherited := putObject(t, p, "hold-def", "inherited.txt", nil)
	if inherited.Data["eventBasedHold"] != true {
		t.Fatalf("inherited eventBasedHold = %v, want true", inherited.Data["eventBasedHold"])
	}
	_, err = p.ObjectsDelete(ctx, bucketParamsWithObj("hold-def", "inherited.txt"))
	perr := providerErr(t, err, 403)
	if !strings.Contains(perr.Message, "event-based hold") {
		t.Fatalf("delete message = %q, want it to contain %q", perr.Message, "event-based hold")
	}

	// Overwriting the inherited-hold object is blocked too.
	over := bucketParamsWithObj("hold-def", "inherited.txt")
	over.Params[wire.MediaKey] = []byte("new")
	_, err = p.ObjectsInsert(ctx, over)
	perr = providerErr(t, err, 403)
	if !strings.Contains(perr.Message, "event-based hold") {
		t.Fatalf("overwrite message = %q, want it to contain %q", perr.Message, "event-based hold")
	}

	// An explicit eventBasedHold=false on the insert overrides the default.
	overridden := putObject(t, p, "hold-def", "overridden.txt", map[string]any{"eventBasedHold": false})
	if overridden.Data["eventBasedHold"] == true {
		t.Fatalf("explicit eventBasedHold=false was not honoured: %v", overridden.Data)
	}
	if _, err := p.ObjectsDelete(ctx, bucketParamsWithObj("hold-def", "overridden.txt")); err != nil {
		t.Fatalf("delete un-held object: %v", err)
	}
}

// TestRetentionPolicyDeleteMessage verifies an active bucket retention policy
// blocks the delete with a message that names the retention policy (distinct
// from the hold messages).
func TestRetentionPolicyDeleteMessage(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWith(t, p, "hold-ret", map[string]any{
		"retentionPolicy": map[string]any{"retentionPeriod": "3600"},
	})
	inserted := putObject(t, p, "hold-ret", "obj.txt", nil)
	if inserted.Data["retentionExpirationTime"] == nil {
		t.Fatalf("expected retentionExpirationTime on a retained object, got %v", inserted.Data)
	}

	_, err := p.ObjectsDelete(ctx, bucketParamsWithObj("hold-ret", "obj.txt"))
	perr := providerErr(t, err, 403)
	if !strings.Contains(perr.Message, "retention policy") {
		t.Fatalf("delete message = %q, want it to contain %q", perr.Message, "retention policy")
	}

	// Overwriting is blocked by the same active retention policy.
	over := bucketParamsWithObj("hold-ret", "obj.txt")
	over.Params[wire.MediaKey] = []byte("new")
	_, err = p.ObjectsInsert(ctx, over)
	perr = providerErr(t, err, 403)
	if !strings.Contains(perr.Message, "retention policy") {
		t.Fatalf("overwrite message = %q, want it to contain %q", perr.Message, "retention policy")
	}
}

// TestDeleteSpecificGeneration verifies objects.delete?generation= removes only
// the requested revision. Deleting the live revision must NOT promote a
// noncurrent survivor: a bare lookup by name stops resolving, the survivor is
// still reachable by generation, and removing the last generation lets the
// versioned bucket be deleted.
func TestDeleteSpecificGeneration(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWith(t, p, "ver-bkt", map[string]any{
		"versioning": map[string]any{"enabled": true},
	})

	v1 := putObject(t, p, "ver-bkt", "obj.txt", nil)
	v2 := putObject(t, p, "ver-bkt", "obj.txt", nil)
	gen1, _ := v1.Data["generation"].(string)
	gen2, _ := v2.Data["generation"].(string)
	if gen1 == "" || gen2 == "" || gen1 == gen2 {
		t.Fatalf("expected distinct generations, got %q and %q", gen1, gen2)
	}

	// Delete the live v2 by generation: v1 survives as a noncurrent revision.
	del := bucketParamsWithObj("ver-bkt", "obj.txt")
	del.Params["generation"] = gen2
	if _, err := p.ObjectsDelete(ctx, del); err != nil {
		t.Fatalf("delete v2: %v", err)
	}
	if _, err := p.ObjectsGet(ctx, bucketParamsWithObj("ver-bkt", "obj.txt")); err == nil {
		t.Fatal("expected bare get to fail after deleting the live generation (no promotion)")
	}
	byGen := func(gen string) *model.NormalizedRequest {
		nr := bucketParamsWithObj("ver-bkt", "obj.txt")
		nr.Params["generation"] = gen
		return nr
	}
	if _, err := p.ObjectsGet(ctx, byGen(gen1)); err != nil {
		t.Fatalf("noncurrent v1 must still be reachable by generation: %v", err)
	}

	// Delete the noncurrent v1 by generation; the object is then gone and the
	// bucket is empty and deletable.
	if _, err := p.ObjectsDelete(ctx, byGen(gen1)); err != nil {
		t.Fatalf("delete v1: %v", err)
	}
	if err := p.objects.DeleteBucket(ctx, "ver-bkt"); err != nil {
		t.Fatalf("bucket must be empty after deleting the last generation: %v", err)
	}
}

// TestDeleteNonLiveGenerationKeepsLive verifies deleting an older (noncurrent)
// generation leaves the live generation untouched.
func TestDeleteNonLiveGenerationKeepsLive(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWith(t, p, "ver-keep", map[string]any{
		"versioning": map[string]any{"enabled": true},
	})
	v1 := putObject(t, p, "ver-keep", "obj.txt", nil)
	v2 := putObject(t, p, "ver-keep", "obj.txt", nil)
	gen1, _ := v1.Data["generation"].(string)
	gen2, _ := v2.Data["generation"].(string)

	del := bucketParamsWithObj("ver-keep", "obj.txt")
	del.Params["generation"] = gen1
	if _, err := p.ObjectsDelete(ctx, del); err != nil {
		t.Fatalf("delete v1: %v", err)
	}
	live, err := p.ObjectsGet(ctx, bucketParamsWithObj("ver-keep", "obj.txt"))
	if err != nil {
		t.Fatalf("get live after deleting v1: %v", err)
	}
	if live.Data["generation"] != gen2 {
		t.Fatalf("live generation = %v, want %v", live.Data["generation"], gen2)
	}
}

func TestDeleteMissingGenerationIsNotFound(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWith(t, p, "ver-miss", map[string]any{
		"versioning": map[string]any{"enabled": true},
	})
	putObject(t, p, "ver-miss", "obj.txt", nil)

	del := bucketParamsWithObj("ver-miss", "obj.txt")
	del.Params["generation"] = "999999"
	_, err := p.ObjectsDelete(ctx, del)
	providerErr(t, err, 404)
}

// TestComposeInheritsDefaultEventBasedHold verifies the REST compose destination
// inherits the bucket's defaultEventBasedHold and that an explicit destination
// eventBasedHold overrides it (parity with the gRPC ComposeObject path).
func TestComposeInheritsDefaultEventBasedHold(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWith(t, p, "compose-hold", map[string]any{"defaultEventBasedHold": true})
	putObject(t, p, "compose-hold", "src.txt", nil)

	compose := func(object string, destination map[string]any) *model.ProviderResponse {
		t.Helper()
		nr := bucketParams()
		nr.Params["bucket"] = "compose-hold"
		nr.Params["object"] = object
		nr.Params["body"] = map[string]any{
			"destination":   destination,
			"sourceObjects": []any{map[string]any{"name": "src.txt"}},
		}
		resp, err := p.ObjectsCompose(ctx, nr)
		if err != nil {
			t.Fatalf("compose %s: %v", object, err)
		}
		return resp
	}

	inherited := compose("dst.txt", map[string]any{"name": "dst.txt"})
	if inherited.Data["eventBasedHold"] != true {
		t.Fatalf("compose destination eventBasedHold = %v, want true (bucket default)", inherited.Data["eventBasedHold"])
	}
	overridden := compose("dst2.txt", map[string]any{"name": "dst2.txt", "eventBasedHold": false})
	if overridden.Data["eventBasedHold"] == true {
		t.Fatalf("explicit eventBasedHold=false was not honoured: %v", overridden.Data)
	}
}
