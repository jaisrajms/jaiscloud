package storage

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// insertDepthObject inserts an object via ObjectsInsert and returns its
// generation string.
func insertDepthObject(t *testing.T, p *Provider, bucket, object, data string, body map[string]any) string {
	t.Helper()
	nr := bucketParams()
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	nr.Params[wire.MediaKey] = []byte(data)
	nr.Params[wire.ContentTypeKey] = "text/plain"
	if body != nil {
		nr.Params["body"] = body
	}
	resp, err := p.ObjectsInsert(context.Background(), nr)
	if err != nil {
		t.Fatalf("insert %s: %v", object, err)
	}
	g, _ := resp.Data["generation"].(string)
	return g
}

func TestVersioningRetainReadGenerationAndListVersions(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt", "versioning": map[string]any{"enabled": true}}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	gen1 := insertDepthObject(t, p, "bkt", "v.txt", "one", nil)
	gen2 := insertDepthObject(t, p, "bkt", "v.txt", "two", nil)
	if gen1 == gen2 {
		t.Fatal("expected distinct generations on overwrite")
	}

	// Read the archived generation's metadata.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "v.txt"
	nr.Params["generation"] = gen1
	resp, err := p.ObjectsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get old generation: %v", err)
	}
	if resp.Data["generation"] != gen1 {
		t.Fatalf("expected generation %s, got %v", gen1, resp.Data["generation"])
	}

	// Read the archived generation's media bytes.
	media, err := p.ObjectsGetMedia(ctx, nr)
	if err != nil {
		t.Fatalf("get old media: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "one" {
		t.Fatalf("expected old media 'one', got %q", got)
	}

	// Live media still resolves to the newest generation.
	nr2 := bucketParams()
	nr2.Params["bucket"] = "bkt"
	nr2.Params["object"] = "v.txt"
	media2, err := p.ObjectsGetMedia(ctx, nr2)
	if err != nil {
		t.Fatalf("get live media: %v", err)
	}
	if got := string(streamBytes(t, media2)); got != "two" {
		t.Fatalf("expected live media 'two', got %q", got)
	}

	// Missing generation → 404.
	nr3 := bucketParams()
	nr3.Params["bucket"] = "bkt"
	nr3.Params["object"] = "v.txt"
	nr3.Params["generation"] = "999999"
	if _, err := p.ObjectsGet(ctx, nr3); err == nil {
		t.Fatal("expected 404 for missing generation")
	}

	// ?versions=true lists both generations; the archived one has timeDeleted.
	nr4 := bucketParams()
	nr4.Params["bucket"] = "bkt"
	nr4.Params["versions"] = "true"
	resp4, err := p.ObjectsList(ctx, nr4)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	items, _ := resp4.Data["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected 2 versions, got %d", len(items))
	}
	seenDeleted := false
	for _, it := range items {
		m := it.(map[string]any)
		if m["generation"] == gen1 && m["timeDeleted"] != "" && m["timeDeleted"] != nil {
			seenDeleted = true
		}
	}
	if !seenDeleted {
		t.Fatal("expected archived generation to carry timeDeleted")
	}

	// Default list (no versions) returns only the live object.
	nr5 := bucketParams()
	nr5.Params["bucket"] = "bkt"
	resp5, err := p.ObjectsList(ctx, nr5)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items5, _ := resp5.Data["items"].([]any)
	if len(items5) != 1 {
		t.Fatalf("expected 1 live object, got %d", len(items5))
	}
}

// TestObjectsListVersionsPaginationNoDataLoss verifies that walking every page
// of a ?versions=true listing with maxResults=1 returns each generation exactly
// once. A name's generations are ordered newest-first, so a name-only cursor
// (the pre-fix behaviour) skipped the remaining generations of the name whose
// generation straddled a page boundary — silent data loss.
func TestObjectsListVersionsPaginationNoDataLoss(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt", "versioning": map[string]any{"enabled": true}}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	gens := make([]string, 0, 3)
	for _, data := range []string{"one", "two", "three"} {
		gens = append(gens, insertDepthObject(t, p, "bkt", "v.txt", data, nil))
	}
	if len(gens) != 3 || gens[0] == gens[1] || gens[1] == gens[2] || gens[0] == gens[2] {
		t.Fatalf("expected 3 distinct generations, got %v", gens)
	}

	seen := map[string]int{}
	token := ""
	for page := 0; ; page++ {
		if page > 10 {
			t.Fatal("pagination did not terminate")
		}
		nr := bucketParams()
		nr.Params["bucket"] = "bkt"
		nr.Params["versions"] = "true"
		nr.Params["maxResults"] = "1"
		if token != "" {
			nr.Params["pageToken"] = token
		}
		resp, err := p.ObjectsList(ctx, nr)
		if err != nil {
			t.Fatalf("list page %d: %v", page, err)
		}
		items, _ := resp.Data["items"].([]any)
		if len(items) == 0 {
			t.Fatalf("page %d returned no items", page)
		}
		for _, it := range items {
			m := it.(map[string]any)
			g, _ := m["generation"].(string)
			seen[g]++
		}
		next, _ := resp.Data["nextPageToken"].(string)
		if next == "" {
			break
		}
		token = next
	}

	if len(seen) != 3 {
		t.Fatalf("expected 3 distinct generations across pages, got %d (%v)", len(seen), seen)
	}
	for _, g := range gens {
		if seen[g] != 1 {
			t.Errorf("generation %s returned %d times, want exactly 1", g, seen[g])
		}
	}
}

func TestRetentionDeleteForbidden(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt", "retentionPolicy": map[string]any{"retentionPeriod": "86400s"}}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	insertDepthObject(t, p, "bkt", "o.txt", "x", nil)

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "o.txt"
	if _, err := p.ObjectsDelete(ctx, nr); err == nil {
		t.Fatal("expected delete to be forbidden while retention is active")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 403 {
		t.Fatalf("expected 403 ProviderError, got %v", err)
	}
}

func TestTemporaryHoldBlocksDelete(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	insertDepthObject(t, p, "bkt", "held.txt", "x", map[string]any{"temporaryHold": true})

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "held.txt"
	if _, err := p.ObjectsDelete(ctx, nr); err == nil {
		t.Fatal("expected delete to be forbidden while held")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 403 {
		t.Fatalf("expected 403 ProviderError, got %v", err)
	}
}

func TestLifecycleConfigPersistAndLazyDelete(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{
		"name": "bkt",
		"lifecycle": map[string]any{"rule": []any{
			map[string]any{"action": map[string]any{"type": "Delete"}, "condition": map[string]any{"age": float64(1)}},
		}},
	}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// The lifecycle config round-trips through bucket get.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	resp, err := p.BucketsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	if lc, _ := resp.Data["lifecycle"].(map[string]any); lc == nil {
		t.Fatal("expected lifecycle config on bucket get")
	}

	// Lazy age-delete: insert under a frozen clock, advance past the age
	// threshold, and verify the object is dropped from the listing.
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	insertDepthObject(t, p, "bkt", "old.txt", "stale", nil)

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(48 * time.Hour)})

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	resp, err = p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := resp.Data["items"].([]any)
	if len(items) != 0 {
		t.Fatalf("expected expired object to be dropped, got %d items", len(items))
	}
}
