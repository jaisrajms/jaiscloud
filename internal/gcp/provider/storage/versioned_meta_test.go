package storage

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
)

// versionsList returns the object maps from a ?versions=true listing.
func versionsList(t *testing.T, p *Provider, bucket string) []any {
	t.Helper()
	nr := bucketParams()
	nr.Params["bucket"] = bucket
	nr.Params["versions"] = "true"
	resp, err := p.ObjectsList(context.Background(), nr)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	items, _ := resp.Data["items"].([]any)
	return items
}

func getByGeneration(t *testing.T, p *Provider, bucket, object, generation string) *model.ProviderResponse {
	t.Helper()
	nr := bucketParamsWithObj(bucket, object)
	nr.Params["generation"] = generation
	resp, err := p.ObjectsGet(context.Background(), nr)
	if err != nil {
		t.Fatalf("get %s/%s@%s: %v", bucket, object, generation, err)
	}
	return resp
}

// TestPatchKeepsNoncurrentGenerations verifies objects.patch updates the live
// generation's metadata in place — the generation is unchanged (only the
// metageneration bumps) and every noncurrent generation survives. Before the
// fix, the replace-semantics PutObjectMeta* discarded them.
func TestPatchKeepsNoncurrentGenerations(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWith(t, p, "ver-patch", map[string]any{
		"versioning": map[string]any{"enabled": true},
	})

	v1 := putObject(t, p, "ver-patch", "obj.txt", nil)
	v2 := putObject(t, p, "ver-patch", "obj.txt", nil)
	gen1, _ := v1.Data["generation"].(string)
	gen2, _ := v2.Data["generation"].(string)
	if gen1 == "" || gen1 == gen2 {
		t.Fatalf("expected distinct generations, got %q and %q", gen1, gen2)
	}

	nr := bucketParamsWithObj("ver-patch", "obj.txt")
	nr.Params["body"] = map[string]any{
		"contentType": "application/json",
		"metadata":    map[string]any{"color": "black"},
	}
	patched, err := p.ObjectsPatch(ctx, nr)
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	if patched.Data["generation"] != gen2 {
		t.Fatalf("patch changed generation: got %v, want %v", patched.Data["generation"], gen2)
	}
	if patched.Data["contentType"] != "application/json" {
		t.Fatalf("patch contentType = %v, want application/json", patched.Data["contentType"])
	}
	if patched.Data["metageneration"] != "2" {
		t.Fatalf("patch metageneration = %v, want 2", patched.Data["metageneration"])
	}

	// Both generations remain listed.
	if items := versionsList(t, p, "ver-patch"); len(items) != 2 {
		t.Fatalf("?versions=true after patch = %d items, want 2 (noncurrent preserved)", len(items))
	}
	// The noncurrent generation is unchanged and still reachable by id.
	old := getByGeneration(t, p, "ver-patch", "obj.txt", gen1)
	if old.Data["contentType"] != "text/plain" || old.Data["metageneration"] != "1" {
		t.Fatalf("noncurrent generation was mutated: contentType=%v metageneration=%v",
			old.Data["contentType"], old.Data["metageneration"])
	}
}

// TestUpdateVersionedObjectKeepsNoncurrentGenerations covers the strict-replace
// (PUT) path, which shares the in-place store update.
func TestUpdateVersionedObjectKeepsNoncurrentGenerations(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucketWith(t, p, "ver-put", map[string]any{
		"versioning": map[string]any{"enabled": true},
	})

	v1 := putObject(t, p, "ver-put", "obj.txt", nil)
	v2 := putObject(t, p, "ver-put", "obj.txt", nil)
	gen1, _ := v1.Data["generation"].(string)
	gen2, _ := v2.Data["generation"].(string)

	nr := bucketParamsWithObj("ver-put", "obj.txt")
	nr.Params["body"] = map[string]any{"contentType": "text/csv"}
	updated, err := p.ObjectsUpdate(ctx, nr)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.Data["generation"] != gen2 {
		t.Fatalf("update changed generation: got %v, want %v", updated.Data["generation"], gen2)
	}
	if updated.Data["contentType"] != "text/csv" {
		t.Fatalf("update contentType = %v, want text/csv", updated.Data["contentType"])
	}
	if items := versionsList(t, p, "ver-put"); len(items) != 2 {
		t.Fatalf("?versions=true after update = %d items, want 2", len(items))
	}
	if old := getByGeneration(t, p, "ver-put", "obj.txt", gen1); old.Data["contentType"] != "text/plain" {
		t.Fatalf("noncurrent contentType = %v, want text/plain", old.Data["contentType"])
	}
}
