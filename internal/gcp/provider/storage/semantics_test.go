package storage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// insertObjectTyped inserts an object with an explicit contentType, which the
// shared insertObject helper (octet-stream default) does not allow.
func insertObjectTyped(t *testing.T, p *Provider, bucket, object, contentType string, data []byte) {
	t.Helper()
	nr := bucketParams()
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	nr.Params["body"] = map[string]any{"name": object, "contentType": contentType}
	nr.Params[wire.MediaKey] = data
	if _, err := p.ObjectsInsert(context.Background(), nr); err != nil {
		t.Fatalf("insert object %s: %v", object, err)
	}
}

// TestObjectsPatchMergePreservesContentTypeAndCopyInherits guards J6/R5: an
// objects.patch that only supplies metadata must keep the object's stored
// contentType, and a subsequent rewrite must inherit it. The Java client sends
// this PATCH as POST + X-HTTP-Method-Override (covered in the adapter tests);
// this exercises the provider-level merge + copy semantics directly.
func TestObjectsPatchMergePreservesContentTypeAndCopyInherits(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucket(t, p, "bkt")
	insertObjectTyped(t, p, "bkt", "src", "text/plain", []byte("x"))

	patch := bucketParamsWithObj("bkt", "src")
	patch.Params["body"] = map[string]any{"metadata": map[string]any{"tag": "keep-me"}}
	patched, err := p.ObjectsPatch(ctx, patch)
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	if got := patched.Data["contentType"]; got != "text/plain" {
		t.Fatalf("PATCH cleared contentType: got %v, want text/plain", got)
	}

	copyNR := bucketParams()
	copyNR.Params["sourceBucket"] = "bkt"
	copyNR.Params["sourceObject"] = "src"
	copyNR.Params["destinationBucket"] = "bkt"
	copyNR.Params["destinationObject"] = "dst"
	rewritten, err := p.ObjectsRewrite(ctx, copyNR)
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	resource, _ := rewritten.Data["resource"].(map[string]any)
	if got := resource["contentType"]; got != "text/plain" {
		t.Fatalf("copy did not inherit source contentType: got %v, want text/plain", got)
	}
	md, _ := resource["metadata"].(map[string]any)
	if md["tag"] != "keep-me" {
		t.Fatalf("copy did not inherit source metadata: got %#v", resource["metadata"])
	}
}

// TestObjectsGetMediaRangeNotSatisfiable guards J8/R7: a Range whose start is
// at or past the object size returns 416 + Content-Range: bytes */<size>, while
// valid ranges stay 206 and malformed ranges are ignored (200 full body).
func TestObjectsGetMediaRangeNotSatisfiable(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucket(t, p, "bkt")
	insertObjectTyped(t, p, "bkt", "obj", "text/plain", []byte("0123456789abcdef"))

	newReq := func(rng string) *model.NormalizedRequest {
		nr := bucketParamsWithObj("bkt", "obj")
		nr.Raw = httptest.NewRequest(http.MethodGet, "/bkt/obj", nil)
		if rng != "" {
			nr.Raw.Header.Set("Range", rng)
		}
		return nr
	}

	// Unsatisfiable: start == size.
	resp, err := p.ObjectsGetMedia(ctx, newReq("bytes=16-20"))
	if err != nil {
		t.Fatalf("media (unsatisfiable): %v", err)
	}
	if resp.HTTPStatus != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("unsatisfiable range status = %d, want 416", resp.HTTPStatus)
	}
	hdr, _ := resp.Data[wire.HeadersKey].(map[string]string)
	if got := hdr["Content-Range"]; got != "bytes */16" {
		t.Fatalf("Content-Range = %q, want %q", got, "bytes */16")
	}

	// Valid range still 206.
	resp, err = p.ObjectsGetMedia(ctx, newReq("bytes=4-7"))
	if err != nil {
		t.Fatalf("media (valid): %v", err)
	}
	if resp.HTTPStatus != http.StatusPartialContent {
		t.Fatalf("valid range status = %d, want 206", resp.HTTPStatus)
	}
	if got := string(streamBytes(t, resp)); got != "4567" {
		t.Fatalf("valid range body = %q, want %q", got, "4567")
	}

	// Malformed range is ignored → 200 with the full body.
	resp, err = p.ObjectsGetMedia(ctx, newReq("items=0-5"))
	if err != nil {
		t.Fatalf("media (malformed): %v", err)
	}
	if resp.HTTPStatus != http.StatusOK {
		t.Fatalf("malformed range status = %d, want 200", resp.HTTPStatus)
	}
	if got := string(streamBytes(t, resp)); got != "0123456789abcdef" {
		t.Fatalf("malformed range body = %q, want full object", got)
	}
}

// TestObjectsListStartOffset guards J9/R8: startOffset filters the listing to
// names lexicographically equal to or after it, composed with prefix.
func TestObjectsListStartOffset(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucket(t, p, "bkt")
	for _, name := range []string{"list-order-raw/C", "list-order-raw/A", "list-order-raw/B"} {
		insertObjectTyped(t, p, "bkt", name, "application/octet-stream", nil)
	}

	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["prefix"] = "list-order-raw/"
	nr.Params["startOffset"] = "list-order-raw/B"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []string{"list-order-raw/B", "list-order-raw/C"}
	if names := objectNames(t, resp); !reflect.DeepEqual(names, want) {
		t.Fatalf("startOffset listing = %v, want %v", names, want)
	}
}

// TestObjectsListStartOffsetWithDelimiter verifies startOffset filters object
// names before common prefixes are derived: a prefix survives when any of its
// objects is at or after the offset, even if the prefix string itself is before.
func TestObjectsListStartOffsetWithDelimiter(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucket(t, p, "bkt")
	insertObjectTyped(t, p, "bkt", "dir/a", "application/octet-stream", nil)
	insertObjectTyped(t, p, "bkt", "dir/z", "application/octet-stream", nil)

	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["delimiter"] = "/"
	nr.Params["startOffset"] = "dir/m"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	prefixes, _ := resp.Data["prefixes"].([]string)
	if !reflect.DeepEqual(prefixes, []string{"dir/"}) {
		t.Fatalf("prefixes = %v, want [dir/]", resp.Data["prefixes"])
	}
	if items, _ := resp.Data["items"].([]any); len(items) != 0 {
		t.Fatalf("expected no in-range items, got %v", items)
	}
}

// objectNames extracts item names from an ObjectsList response, in order.
func objectNames(t *testing.T, resp *model.ProviderResponse) []string {
	t.Helper()
	items, _ := resp.Data["items"].([]any)
	names := make([]string, 0, len(items))
	for _, it := range items {
		m, _ := it.(map[string]any)
		n, _ := m["name"].(string)
		names = append(names, n)
	}
	return names
}

// TestObjectsListEndOffset guards J33: endOffset filters the listing to names
// lexicographically before it, composed with prefix.
func TestObjectsListEndOffset(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucket(t, p, "bkt")
	for _, name := range []string{"end-order-raw/A", "end-order-raw/C", "end-order-raw/B"} {
		insertObjectTyped(t, p, "bkt", name, "application/octet-stream", nil)
	}

	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["prefix"] = "end-order-raw/"
	nr.Params["endOffset"] = "end-order-raw/C"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []string{"end-order-raw/A", "end-order-raw/B"}
	if got := objectNames(t, resp); !reflect.DeepEqual(got, want) {
		t.Fatalf("endOffset listing = %v, want %v", got, want)
	}
}

// TestObjectsListEndOffsetWithDelimiter verifies endOffset filters object names
// before common prefixes are derived: a prefix survives when any of its objects
// is before the offset, and disappears when none is.
func TestObjectsListEndOffsetWithDelimiter(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucket(t, p, "bkt")
	insertObjectTyped(t, p, "bkt", "dir/a", "application/octet-stream", nil)
	insertObjectTyped(t, p, "bkt", "dir/z", "application/octet-stream", nil)

	// dir/a is in range, so the "dir/" prefix survives.
	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["delimiter"] = "/"
	nr.Params["endOffset"] = "dir/m"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if prefixes, _ := resp.Data["prefixes"].([]string); !reflect.DeepEqual(prefixes, []string{"dir/"}) {
		t.Fatalf("prefixes = %v, want [dir/]", resp.Data["prefixes"])
	}
	if got := objectNames(t, resp); len(got) != 0 {
		t.Fatalf("expected no in-range items, got %v", got)
	}

	// No object is before "dir/0", so no prefix is derived at all.
	nr2 := bucketParams()
	nr2.Params["bucket"] = "bkt"
	nr2.Params["delimiter"] = "/"
	nr2.Params["endOffset"] = "dir/0"
	resp2, err := p.ObjectsList(ctx, nr2)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if prefixes, _ := resp2.Data["prefixes"].([]string); len(prefixes) != 0 {
		t.Fatalf("prefixes = %v, want none", prefixes)
	}
	if got := objectNames(t, resp2); len(got) != 0 {
		t.Fatalf("expected no items, got %v", got)
	}
}

// TestObjectsListEndOffsetWithVersions verifies endOffset applies to the
// versions listing too, keeping only generations of names before it.
func TestObjectsListEndOffsetWithVersions(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createVersionedBucket(t, p, "bkt")
	insertObjectTyped(t, p, "bkt", "v/A", "text/plain", []byte("a"))
	insertObjectTyped(t, p, "bkt", "v/B", "text/plain", []byte("b1"))
	insertObjectTyped(t, p, "bkt", "v/B", "text/plain", []byte("b2"))

	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["versions"] = "true"
	nr.Params["endOffset"] = "v/B"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if got, want := objectNames(t, resp), []string{"v/A"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("versions endOffset listing = %v, want %v", got, want)
	}
}

// TestObjectsListOffsetRange verifies startOffset (inclusive) and endOffset
// (exclusive) compose into a half-open range.
func TestObjectsListOffsetRange(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucket(t, p, "bkt")
	for _, name := range []string{"range-raw/A", "range-raw/B", "range-raw/C", "range-raw/D"} {
		insertObjectTyped(t, p, "bkt", name, "application/octet-stream", nil)
	}

	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["prefix"] = "range-raw/"
	nr.Params["startOffset"] = "range-raw/B"
	nr.Params["endOffset"] = "range-raw/D"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []string{"range-raw/B", "range-raw/C"}
	if got := objectNames(t, resp); !reflect.DeepEqual(got, want) {
		t.Fatalf("offset range listing = %v, want %v", got, want)
	}
}

// TestObjectsListEndOffsetPagination verifies endOffset bounds both pages: the
// pageToken cursor never walks past the offset.
func TestObjectsListEndOffsetPagination(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createBucket(t, p, "bkt")
	for _, name := range []string{"page-end/A", "page-end/B", "page-end/C"} {
		insertObjectTyped(t, p, "bkt", name, "application/octet-stream", nil)
	}

	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["prefix"] = "page-end/"
	nr.Params["endOffset"] = "page-end/C"
	nr.Params["maxResults"] = "1"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list page 1: %v", err)
	}
	if got, want := objectNames(t, resp), []string{"page-end/A"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("page 1 = %v, want %v", got, want)
	}
	tok, _ := resp.Data["nextPageToken"].(string)
	if tok == "" {
		t.Fatal("expected nextPageToken")
	}

	nr.Params["pageToken"] = tok
	resp2, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if got, want := objectNames(t, resp2), []string{"page-end/B"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("page 2 = %v, want %v", got, want)
	}
	if next, _ := resp2.Data["nextPageToken"].(string); next != "" {
		t.Fatalf("unexpected nextPageToken %q past endOffset", next)
	}
}

// TestObjectsListStartOffsetWithVersions verifies startOffset applies to the
// versions listing too, keeping only generations of names at or after it.
func TestObjectsListStartOffsetWithVersions(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	createVersionedBucket(t, p, "bkt")
	insertObjectTyped(t, p, "bkt", "v/A", "text/plain", []byte("a"))
	insertObjectTyped(t, p, "bkt", "v/B", "text/plain", []byte("b1"))
	insertObjectTyped(t, p, "bkt", "v/B", "text/plain", []byte("b2"))

	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["versions"] = "true"
	nr.Params["startOffset"] = "v/B"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	names := objectNames(t, resp)
	if len(names) == 0 {
		t.Fatal("expected at least one version at/after startOffset")
	}
	for _, name := range names {
		if name < "v/B" {
			t.Fatalf("version listing included %q before startOffset", name)
		}
	}
}
