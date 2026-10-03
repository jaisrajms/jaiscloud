package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/gcp/store/gcs"
	kms "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newTestProvider() *Provider {
	return New(gcs.NewMemoryObjectStore(), store.NewMemoryResourceStore(), blobfs.NewMemoryBlobStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))
}

// streamBytes reads the bytes from a "_stream" media response.
func streamBytes(t *testing.T, resp *model.ProviderResponse) []byte {
	t.Helper()
	rc, ok := resp.Data["_stream"].(io.ReadCloser)
	if !ok {
		t.Fatal("expected _stream io.ReadCloser in media response")
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	return b
}

func bucketParams() *model.NormalizedRequest {
	return &model.NormalizedRequest{AccountID: "proj", Params: map[string]any{}, ResourceID: resource.ResourceID("proj")}
}

// TestObjectsScopeToBucketProject verifies that object operations scope to the
// bucket's owning project rather than the request's default project (object
// requests carry no ?project=). Without this, a bucket created under
// ?project=proj would be invisible to an object op that falls back to the
// config default.
func TestObjectsScopeToBucketProject(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	// Create the bucket under project "proj".
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Object op whose request carries a DIFFERENT default project — it must
	// resolve the bucket's owning project and succeed.
	nr2 := bucketParams()
	nr2.AccountID = "other-project"
	nr2.Params["bucket"] = "bkt"
	nr2.Params["object"] = "o.txt"
	nr2.Params[wire.MediaKey] = []byte("hi")
	if _, err := p.ObjectsInsert(ctx, nr2); err != nil {
		t.Fatalf("expected object insert to resolve bucket project, got: %v", err)
	}

	// And a get via a different default project must also find it.
	nr3 := bucketParams()
	nr3.AccountID = "other-project"
	nr3.Params["bucket"] = "bkt"
	nr3.Params["object"] = "o.txt"
	if _, err := p.ObjectsGet(ctx, nr3); err != nil {
		t.Fatalf("expected object get to resolve bucket project, got: %v", err)
	}
}

func TestBucketRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "my-bucket", "location": "US"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "my-bucket"
	resp, err := p.BucketsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp.Data["name"] != "my-bucket" {
		t.Errorf("expected bucket name my-bucket, got %v", resp.Data["name"])
	}

	list, err := p.BucketsList(ctx, bucketParams())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := list.Data["items"].([]any)
	if len(items) != 1 {
		t.Errorf("expected 1 bucket, got %d", len(items))
	}
}

// TestBucketLabels verifies that GCS bucket labels round-trip through
// create/get/list and that a PATCH-style update replaces the label set. Bucket
// labels were previously dropped (missing from bucketMeta), which produced a
// perpetual terraform plan diff.
func TestBucketLabels(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{
		"name":   "labeled",
		"labels": map[string]any{"env": "compat-test", "team": "core"},
	}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "labeled"
	resp, err := p.BucketsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	labels, ok := resp.Data["labels"].(map[string]string)
	if !ok {
		t.Fatalf("expected labels map in GET response, got %T (%v)", resp.Data["labels"], resp.Data["labels"])
	}
	if labels["env"] != "compat-test" || labels["team"] != "core" {
		t.Fatalf("labels not preserved: %v", labels)
	}

	// A PATCH supplying labels replaces the whole set (real GCS semantics for a
	// map field): the omitted "team" label is removed.
	nr = bucketParams()
	nr.Params["bucket"] = "labeled"
	nr.Params["body"] = map[string]any{"labels": map[string]any{"env": "updated"}}
	if _, err := p.BucketsUpdate(ctx, nr); err != nil {
		t.Fatalf("update: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "labeled"
	resp, err = p.BucketsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	labels, _ = resp.Data["labels"].(map[string]string)
	if labels["env"] != "updated" {
		t.Fatalf("expected env=updated, got %v", labels)
	}
	if _, present := labels["team"]; present {
		t.Fatalf("expected team label removed by replacement, got %v", labels)
	}

	// Labels also appear in list output.
	list, err := p.BucketsList(ctx, bucketParams())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := list.Data["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(items))
	}
	item := items[0].(map[string]any)
	lm, _ := item["labels"].(map[string]string)
	if lm == nil || lm["env"] != "updated" {
		t.Fatalf("expected labels in list output, got %v", item["labels"])
	}
}

func TestObjectRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	// Create bucket first.
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Insert object with media bytes.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "dir/file.txt"
	nr.Params[wire.MediaKey] = []byte("hello world")
	nr.Params[wire.ContentTypeKey] = "text/plain"
	resp, err := p.ObjectsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("insert object: %v", err)
	}
	if resp.Data["size"] != "11" {
		t.Errorf("expected size 11, got %v", resp.Data["size"])
	}
	if resp.Data["md5Hash"] == "" {
		t.Error("expected md5Hash to be set")
	}

	// Get media back.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "dir/file.txt"
	media, err := p.ObjectsGetMedia(ctx, nr)
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "hello world" {
		t.Errorf("expected media bytes, got %q", got)
	}

	// List objects.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	list, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := list.Data["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 object, got %d", len(items))
	}
	obj := items[0].(map[string]any)
	if obj["name"] != "dir/file.txt" {
		t.Errorf("expected object name dir/file.txt, got %v", obj["name"])
	}

	// Delete object.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "dir/file.txt"
	if _, err := p.ObjectsDelete(ctx, nr); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "dir/file.txt")); err == nil {
		t.Error("expected NotFound after delete")
	}
}

func bucketParamsWithObj(bucket, object string) *model.NormalizedRequest {
	nr := bucketParams()
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	return nr
}

func extractUploadID(t *testing.T, resp *model.ProviderResponse) string {
	t.Helper()
	loc, _ := resp.Data[wire.LocationKey].(string)
	for _, q := range strings.Split(strings.SplitN(loc, "?", 2)[1], "&") {
		if k, v, ok := strings.Cut(q, "="); ok && k == "upload_id" {
			return v
		}
	}
	t.Fatalf("no upload_id in Location %q", loc)
	return ""
}

func TestObjectJSONRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Insert via JSON metadata body (no media) then round-trip through JSON.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["body"] = map[string]any{"name": "meta.json", "contentType": "application/json"}
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("insert object: %v", err)
	}

	resp, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "meta.json"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	// Ensure the response is JSON-serialisable without internal wire keys.
	b, _ := json.Marshal(resp.Data)
	if string(b) == "" {
		t.Error("expected non-empty JSON")
	}
	if resp.Data["contentType"] != "application/json" {
		t.Errorf("expected contentType application/json, got %v", resp.Data["contentType"])
	}
}

func TestObjectsListPagination(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		nr = bucketParams()
		nr.Params["bucket"] = "bkt"
		nr.Params["object"] = name
		nr.Params[wire.MediaKey] = []byte(name)
		if _, err := p.ObjectsInsert(ctx, nr); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}

	// Page 1: maxResults=2 → 2 items + nextPageToken.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["maxResults"] = "2"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := resp.Data["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected 2 items on page 1, got %d", len(items))
	}
	token, _ := resp.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatal("expected nextPageToken")
	}

	// Page 2: pageToken → 1 item.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["pageToken"] = token
	resp, err = p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	items, _ = resp.Data["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 item on page 2, got %d", len(items))
	}
	if _, hasNext := resp.Data["nextPageToken"]; hasNext {
		t.Error("did not expect nextPageToken on final page")
	}
}

// TestObjectsListMaxResultsCappedAt1000 verifies maxResults is capped at 1000:
// real GCS uses "this parameter or 1,000 items, whichever is smaller".
func TestObjectsListMaxResultsCappedAt1000(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	const total = 1001
	for i := 0; i < total; i++ {
		nr = bucketParams()
		nr.Params["bucket"] = "bkt"
		nr.Params["object"] = fmt.Sprintf("obj-%04d", i)
		nr.Params[wire.MediaKey] = []byte("x")
		if _, err := p.ObjectsInsert(ctx, nr); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["maxResults"] = "5000"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := resp.Data["items"].([]any)
	if len(items) != 1000 {
		t.Fatalf("expected 1000 items (maxResults clamp), got %d", len(items))
	}
	if token, _ := resp.Data["nextPageToken"].(string); token == "" {
		t.Error("expected nextPageToken when more than 1000 objects remain")
	}
}

func TestObjectsListPrefixFilter(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	for _, name := range []string{"logs/a", "logs/b", "data/c"} {
		nr = bucketParams()
		nr.Params["bucket"] = "bkt"
		nr.Params["object"] = name
		nr.Params[wire.MediaKey] = []byte(name)
		if _, err := p.ObjectsInsert(ctx, nr); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["prefix"] = "logs/"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := resp.Data["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected 2 prefixed items, got %d", len(items))
	}
}

func TestResumableMultiChunkUpload(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Start the resumable session.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "big.bin"
	nr.Params[wire.ContentTypeKey] = "application/octet-stream"
	start, err := p.ObjectsInsertStartResumable(ctx, nr)
	if err != nil {
		t.Fatalf("start resumable: %v", err)
	}
	loc, _ := start.Data[wire.LocationKey].(string)
	if loc == "" {
		t.Fatal("expected Location header")
	}
	// Extract upload_id from the Location query string.
	uploadID := ""
	for _, q := range strings.Split(strings.SplitN(loc, "?", 2)[1], "&") {
		if k, v, ok := strings.Cut(q, "="); ok && k == "upload_id" {
			uploadID = v
		}
	}
	if uploadID == "" {
		t.Fatalf("no upload_id in Location %q", loc)
	}

	// First chunk (unknown total) → 308 + Range.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-4/*"
	nr.Params[wire.MediaKey] = []byte("hello")
	resp, err := p.ObjectsInsertResumable(ctx, nr)
	if err != nil {
		t.Fatalf("chunk 1: %v", err)
	}
	if resp.HTTPStatus != 308 {
		t.Fatalf("expected 308, got %d", resp.HTTPStatus)
	}
	if rng, _ := resp.Data[wire.RangeKey].(string); rng != "bytes=0-4" {
		t.Fatalf("expected Range bytes=0-4, got %q", rng)
	}

	// Final chunk (known total) → 200, object stored.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 5-10/11"
	nr.Params[wire.MediaKey] = []byte(" world")
	resp, err = p.ObjectsInsertResumable(ctx, nr)
	if err != nil {
		t.Fatalf("final chunk: %v", err)
	}
	if resp.HTTPStatus != 200 {
		t.Fatalf("expected 200, got %d", resp.HTTPStatus)
	}

	// Verify the assembled object.
	media, err := p.ObjectsGetMedia(ctx, bucketParamsWithObj("bkt", "big.bin"))
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "hello world" {
		t.Fatalf("expected assembled 'hello world', got %q", got)
	}
}

func TestBucketIAMRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Default policy has bindings.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	resp, err := p.BucketsGetIamPolicy(ctx, nr)
	if err != nil {
		t.Fatalf("get iam: %v", err)
	}
	bindings, _ := resp.Data["bindings"].([]iamBinding)
	if len(bindings) == 0 {
		t.Fatal("expected default bindings")
	}

	// Set a new policy and read it back.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["body"] = map[string]any{
		"bindings": []any{
			map[string]any{"role": "roles/storage.objectViewer", "members": []any{"allUsers"}},
		},
	}
	if _, err := p.BucketsSetIamPolicy(ctx, nr); err != nil {
		t.Fatalf("set iam: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	resp, err = p.BucketsGetIamPolicy(ctx, nr)
	if err != nil {
		t.Fatalf("get iam after set: %v", err)
	}
	bindings, _ = resp.Data["bindings"].([]iamBinding)
	if len(bindings) != 1 || bindings[0].Role != "roles/storage.objectViewer" {
		t.Fatalf("unexpected bindings: %+v", bindings)
	}
}

func TestACLRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Default bucket ACL has entries.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	resp, err := p.BucketACLList(ctx, nr)
	if err != nil {
		t.Fatalf("acl list: %v", err)
	}
	if items, _ := resp.Data["items"].([]aclEntry); len(items) == 0 {
		t.Fatal("expected default ACL entries")
	}

	// Insert a new ACL entry and confirm it appears.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["body"] = map[string]any{"entity": "allUsers", "role": "READER"}
	if _, err := p.BucketACLInsert(ctx, nr); err != nil {
		t.Fatalf("acl insert: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	resp, err = p.BucketACLList(ctx, nr)
	if err != nil {
		t.Fatalf("acl list after insert: %v", err)
	}
	found := false
	for _, it := range resp.Data["items"].([]aclEntry) {
		if it.Entity == "allUsers" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected allUsers ACL entry after insert")
	}

	// A second insert must merge with the stored entries, not replace them.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["body"] = map[string]any{"entity": "allAuthenticatedUsers", "role": "READER"}
	if _, err := p.BucketACLInsert(ctx, nr); err != nil {
		t.Fatalf("second acl insert: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	resp, err = p.BucketACLList(ctx, nr)
	if err != nil {
		t.Fatalf("acl list after second insert: %v", err)
	}
	found = false
	for _, it := range resp.Data["items"].([]aclEntry) {
		if it.Entity == "allAuthenticatedUsers" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected allAuthenticatedUsers ACL entry after second insert")
	}
}

func TestObjectsListDelimiter(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	for _, name := range []string{"folder/a.txt", "folder/b.txt", "root.txt"} {
		nr = bucketParams()
		nr.Params["bucket"] = "bkt"
		nr.Params["object"] = name
		nr.Params[wire.MediaKey] = []byte(name)
		if _, err := p.ObjectsInsert(ctx, nr); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["delimiter"] = "/"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	prefixes, _ := resp.Data["prefixes"].([]string)
	if len(prefixes) != 1 || prefixes[0] != "folder/" {
		t.Errorf("expected prefixes [folder/], got %v", prefixes)
	}
	items, _ := resp.Data["items"].([]any)
	if len(items) != 1 {
		t.Errorf("expected 1 top-level item, got %d", len(items))
	}
}

func TestBucketDeleteNonEmpty(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "obj"
	nr.Params[wire.MediaKey] = []byte("x")
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("insert object: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	if _, err := p.BucketsDelete(ctx, nr); err == nil {
		t.Fatal("expected 409 on non-empty bucket delete")
	}
}

func TestResumableOffsetRepairAndStatusQuery(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Start with a content type.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "big.bin"
	nr.Params[wire.ContentTypeKey] = "application/octet-stream"
	start, err := p.ObjectsInsertStartResumable(ctx, nr)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	loc, _ := start.Data[wire.LocationKey].(string)
	uploadID := ""
	for _, q := range strings.Split(strings.SplitN(loc, "?", 2)[1], "&") {
		if k, v, ok := strings.Cut(q, "="); ok && k == "upload_id" {
			uploadID = v
		}
	}

	// Chunk 1.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-4/*"
	nr.Params[wire.MediaKey] = []byte("hello")
	resp, err := p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("chunk1: %v / %v", resp, err)
	}

	// Duplicate chunk retry — must NOT duplicate bytes.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-4/*"
	nr.Params[wire.MediaKey] = []byte("hello")
	resp, err = p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("dup chunk: %v / %v", resp, err)
	}
	if rng, _ := resp.Data[wire.RangeKey].(string); rng != "bytes=0-4" {
		t.Fatalf("dup chunk range = %q, want bytes=0-4", rng)
	}

	// Status query (bytes */11) — must not finalize.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes */11"
	resp, err = p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("status query: %v / %v", resp, err)
	}

	// Final chunk.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 5-10/11"
	nr.Params[wire.MediaKey] = []byte(" world")
	resp, err = p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 200 {
		t.Fatalf("final: %v / %v", resp, err)
	}
	if resp.Data["size"] != "11" {
		t.Fatalf("expected size 11, got %v", resp.Data["size"])
	}

	// Content type preserved from start.
	if resp.Data["contentType"] != "application/octet-stream" {
		t.Errorf("expected contentType application/octet-stream, got %v", resp.Data["contentType"])
	}
}

func TestResumableUploadSpillsToTempFile(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "large.bin"
	nr.Params[wire.ContentTypeKey] = "application/octet-stream"
	start, err := p.ObjectsInsertStartResumable(ctx, nr)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	loc, _ := start.Data[wire.LocationKey].(string)
	uploadID := ""
	for _, q := range strings.Split(strings.SplitN(loc, "?", 2)[1], "&") {
		if k, v, ok := strings.Cut(q, "="); ok && k == "upload_id" {
			uploadID = v
		}
	}

	// Chunk larger than the spill threshold forces file-backed accumulation.
	big := bytes.Repeat([]byte("x"), resumableSpillThreshold+1)
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = fmt.Sprintf("bytes 0-%d/*", len(big)-1)
	nr.Params[wire.MediaKey] = big
	resp, err := p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("big chunk: %v / %v", resp, err)
	}

	// Session must have spilled to a temp file, not held bytes in memory.
	p.mu.Lock()
	sess := p.uploads[uploadID]
	spilled := sess != nil && sess.tmpFile != nil && sess.buf == nil
	p.mu.Unlock()
	if !spilled {
		t.Fatal("expected session to spill to temp file after large chunk")
	}

	// Final chunk finalizes and the assembled object round-trips.
	tail := []byte("tail")
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = fmt.Sprintf("bytes %d-%d/%d", len(big), len(big)+len(tail)-1, len(big)+len(tail))
	nr.Params[wire.MediaKey] = tail
	resp, err = p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 200 {
		t.Fatalf("final chunk: %v / %v", resp, err)
	}

	media, err := p.ObjectsGetMedia(ctx, bucketParamsWithObj("bkt", "large.bin"))
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	got := streamBytes(t, media)
	if len(got) != len(big)+len(tail) {
		t.Fatalf("expected %d bytes, got %d", len(big)+len(tail), len(got))
	}
	if !bytes.Equal(got[:len(big)], big) || !bytes.Equal(got[len(big):], tail) {
		t.Fatal("assembled object bytes do not match")
	}
}

func TestResumableStatusQueryFinalizes(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "x.bin"
	nr.Params[wire.ContentTypeKey] = "application/octet-stream"
	start, err := p.ObjectsInsertStartResumable(ctx, nr)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	uploadID := extractUploadID(t, start)

	// Chunk with unknown total (the SDK sends the last chunk this way when it
	// has not yet observed EOF on the source).
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-4/*"
	nr.Params[wire.MediaKey] = []byte("hello")
	resp, err := p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("chunk: %v / %v", resp, err)
	}

	// Status query bytes */5 — all bytes received, must finalize with 200.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes */5"
	resp, err = p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 200 {
		t.Fatalf("status query: %v / %v", resp, err)
	}

	media, err := p.ObjectsGetMedia(ctx, bucketParamsWithObj("bkt", "x.bin"))
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "hello" {
		t.Fatalf("expected 'hello', got %q", got)
	}
}

func TestObjectChecksumAndIdentityFields(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "hello.txt"
	nr.Params[wire.MediaKey] = []byte("hello")
	nr.Params[wire.ContentTypeKey] = "text/plain"
	resp, err := p.ObjectsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("insert object: %v", err)
	}
	// crc32c (CRC32C-Castagnoli) must be present and base64.
	if c, _ := resp.Data["crc32c"].(string); c == "" {
		t.Error("expected crc32c field")
	}
	// id must be bucket/object/generation.
	gen, _ := resp.Data["generation"].(string)
	expectedID := "bkt/hello.txt/" + gen
	if id, _ := resp.Data["id"].(string); id != expectedID {
		t.Errorf("expected id %q, got %q", expectedID, id)
	}
	if e, _ := resp.Data["etag"].(string); e == "" {
		t.Error("expected etag field")
	}

	// Bucket must include id/metageneration/projectNumber.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	bresp, err := p.BucketsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get bucket: %v", err)
	}
	if id, _ := bresp.Data["id"].(string); id != "bkt" {
		t.Errorf("expected bucket id bkt, got %q", id)
	}
	if mg, _ := bresp.Data["metageneration"].(string); mg == "" {
		t.Error("expected bucket metageneration")
	}
	if pn, _ := bresp.Data["projectNumber"].(string); pn != resource.ProjectNumber("proj") {
		t.Errorf("expected synthesized projectNumber %q, got %q", resource.ProjectNumber("proj"), pn)
	}
	if pn, _ := bresp.Data["projectNumber"].(string); len(pn) != 12 || strings.Trim(pn, "0123456789") != "" {
		t.Errorf("expected 12-digit projectNumber, got %q", pn)
	}
	bresp2, err := p.BucketsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get bucket (second): %v", err)
	}
	if pn1, pn2 := bresp.Data["projectNumber"], bresp2.Data["projectNumber"]; pn1 != pn2 {
		t.Errorf("projectNumber not stable across reads: %v vs %v", pn1, pn2)
	}
}

// insertTestObject creates a bucket and inserts an object with the given
// contentType and metadata, returning the provider and the insertion response.
func insertTestObject(t *testing.T, p *Provider, bucket, object, contentType string, metadata map[string]any) *model.ProviderResponse {
	t.Helper()
	ctx := context.Background()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": bucket}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	nr.Params[wire.MediaKey] = []byte("hello")
	nr.Params[wire.ContentTypeKey] = contentType
	nr.Params["body"] = map[string]any{"metadata": metadata}
	resp, err := p.ObjectsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("insert object: %v", err)
	}
	return resp
}

func TestObjectsUpdateStrictReplace(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "obj.txt", "text/plain", map[string]any{"keep": "no"})

	// PUT with only contentType: omitted writable fields must be cleared.
	nr := bucketParamsWithObj("bkt", "obj.txt")
	nr.Params["body"] = map[string]any{"contentType": "application/json"}
	resp, err := p.ObjectsUpdate(ctx, nr)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if resp.Data["contentType"] != "application/json" {
		t.Errorf("expected contentType application/json, got %v", resp.Data["contentType"])
	}
	if resp.Data["metadata"] != nil {
		t.Errorf("expected metadata cleared by strict PUT, got %v", resp.Data["metadata"])
	}
	if resp.Data["storageClass"] != "STANDARD" {
		t.Errorf("expected storageClass reset to STANDARD, got %v", resp.Data["storageClass"])
	}
	// Immutable identity fields survive.
	if resp.Data["generation"] == "" || resp.Data["size"] == "" {
		t.Error("expected generation/size to be preserved")
	}
	if resp.Data["metageneration"] != "2" {
		t.Errorf("expected metageneration bumped to 2, got %v", resp.Data["metageneration"])
	}
}

func TestObjectsUpdateClearsOmittedContentType(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "obj.txt", "text/plain", nil)

	// Strict PUT with only metadata: contentType is omitted, so it is cleared.
	nr := bucketParamsWithObj("bkt", "obj.txt")
	nr.Params["body"] = map[string]any{"metadata": map[string]any{"new": "value"}}
	resp, err := p.ObjectsUpdate(ctx, nr)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, ok := resp.Data["contentType"]; ok {
		t.Errorf("expected contentType cleared by strict PUT, got %v", resp.Data["contentType"])
	}
	md, _ := resp.Data["metadata"].(map[string]any)
	if md["new"] != "value" || len(md) != 1 {
		t.Errorf("expected metadata replaced with {new:value}, got %v", resp.Data["metadata"])
	}
}

func TestObjectsPatchMetadataOnly(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "obj.txt", "text/plain", map[string]any{"keep": "yes"})

	// PATCH with only metadata: contentType is omitted, so it is preserved.
	nr := bucketParamsWithObj("bkt", "obj.txt")
	nr.Params["body"] = map[string]any{"metadata": map[string]any{"new": "value"}}
	resp, err := p.ObjectsPatch(ctx, nr)
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	if resp.Data["contentType"] != "text/plain" {
		t.Errorf("expected contentType preserved by PATCH, got %v", resp.Data["contentType"])
	}
	md, _ := resp.Data["metadata"].(map[string]any)
	if md["new"] != "value" {
		t.Errorf("expected metadata patched to {new:value}, got %v", resp.Data["metadata"])
	}
	if resp.Data["metageneration"] != "2" {
		t.Errorf("expected metageneration bumped to 2, got %v", resp.Data["metageneration"])
	}
}

func TestObjectsUpdatePatchNotFound(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	nr = bucketParamsWithObj("bkt", "missing.txt")
	nr.Params["body"] = map[string]any{"contentType": "text/plain"}
	if _, err := p.ObjectsUpdate(ctx, nr); err == nil {
		t.Error("expected NotFound from ObjectsUpdate on missing object")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 404 {
		t.Errorf("expected 404 ProviderError, got %v", err)
	}
	if _, err := p.ObjectsPatch(ctx, nr); err == nil {
		t.Error("expected NotFound from ObjectsPatch on missing object")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 404 {
		t.Errorf("expected 404 ProviderError, got %v", err)
	}
}

func TestBucketsUpdateNotFound(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["bucket"] = "missing-bucket"
	nr.Params["body"] = map[string]any{"location": "EU"}
	if _, err := p.BucketsUpdate(ctx, nr); err == nil {
		t.Error("expected NotFound from BucketsUpdate on missing bucket")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 404 {
		t.Errorf("expected 404 ProviderError, got %v", err)
	}
}

func TestObjectsSetIamPolicyEtagOCC(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "obj.txt", "text/plain", nil)

	bindings := []any{map[string]any{"role": "roles/storage.objectViewer", "members": []any{"allUsers"}}}

	// Mismatched etag → 409.
	nr := bucketParamsWithObj("bkt", "obj.txt")
	nr.Params["body"] = map[string]any{"bindings": bindings, "etag": "BOGUS="}
	_, err := p.ObjectsSetIamPolicy(ctx, nr)
	if err == nil {
		t.Fatal("expected 409 on object policy etag mismatch")
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != 409 {
		t.Fatalf("expected 409 ProviderError, got %v", err)
	}

	// No etag → accepted.
	nr = bucketParamsWithObj("bkt", "obj.txt")
	nr.Params["body"] = map[string]any{"bindings": bindings}
	if _, err := p.ObjectsSetIamPolicy(ctx, nr); err != nil {
		t.Fatalf("set without etag: %v", err)
	}
}

func TestObjectsIamPolicyNotFound(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	nr = bucketParamsWithObj("bkt", "missing.txt")
	if _, err := p.ObjectsGetIamPolicy(ctx, nr); err == nil {
		t.Error("expected NotFound from ObjectsGetIamPolicy on missing object")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 404 {
		t.Errorf("expected 404 ProviderError, got %v", err)
	}
	nr = bucketParamsWithObj("bkt", "missing.txt")
	nr.Params["body"] = map[string]any{"bindings": []any{}}
	if _, err := p.ObjectsSetIamPolicy(ctx, nr); err == nil {
		t.Error("expected NotFound from ObjectsSetIamPolicy on missing object")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 404 {
		t.Errorf("expected 404 ProviderError, got %v", err)
	}
}

func TestObjectACLRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "obj.txt", "text/plain", nil)

	// Default object ACL has entries scoped to the object.
	nr := bucketParamsWithObj("bkt", "obj.txt")
	resp, err := p.ObjectACLList(ctx, nr)
	if err != nil {
		t.Fatalf("object acl list: %v", err)
	}
	if items, _ := resp.Data["items"].([]aclEntry); len(items) == 0 {
		t.Fatal("expected default object ACL entries")
	} else {
		for _, it := range items {
			if it.Object != "obj.txt" {
				t.Errorf("expected object scoping on ACL entry, got %+v", it)
			}
		}
	}

	// Insert a new entry and confirm it appears with object scope.
	nr = bucketParamsWithObj("bkt", "obj.txt")
	nr.Params["body"] = map[string]any{"entity": "allUsers", "role": "READER"}
	ins, err := p.ObjectACLInsert(ctx, nr)
	if err != nil {
		t.Fatalf("object acl insert: %v", err)
	}
	if ins.Data["object"] != "obj.txt" {
		t.Errorf("expected object in ACL insert response, got %v", ins.Data["object"])
	}
	nr = bucketParamsWithObj("bkt", "obj.txt")
	resp, err = p.ObjectACLList(ctx, nr)
	if err != nil {
		t.Fatalf("object acl list after insert: %v", err)
	}
	found := false
	for _, it := range resp.Data["items"].([]aclEntry) {
		if it.Entity == "allUsers" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected allUsers object ACL entry after insert")
	}
}

func TestACLInsertValidation(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "obj.txt", "text/plain", nil)

	// Missing entity/role → 400.
	nr := bucketParamsWithObj("bkt", "obj.txt")
	nr.Params["body"] = map[string]any{"role": "READER"}
	if _, err := p.ObjectACLInsert(ctx, nr); err == nil {
		t.Error("expected InvalidRequest when entity is missing")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 400 {
		t.Errorf("expected 400 ProviderError, got %v", err)
	}
	nr = bucketParamsWithObj("bkt", "obj.txt")
	nr.Params["body"] = map[string]any{"entity": "allUsers"}
	if _, err := p.ObjectACLInsert(ctx, nr); err == nil {
		t.Error("expected InvalidRequest when role is missing")
	}
}

func TestBucketAndObjectErrorPaths(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	// Missing bucket on get/delete.
	nr := bucketParams()
	nr.Params["bucket"] = "missing"
	if _, err := p.BucketsGet(ctx, nr); err == nil {
		t.Error("expected NotFound from BucketsGet on missing bucket")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 404 {
		t.Errorf("expected 404 ProviderError, got %v", err)
	}
	if _, err := p.BucketsDelete(ctx, nr); err == nil {
		t.Error("expected NotFound from BucketsDelete on missing bucket")
	}

	insertTestObject(t, p, "bkt", "obj.txt", "text/plain", nil)

	// Missing object on delete/get-media.
	nr = bucketParamsWithObj("bkt", "missing.txt")
	if _, err := p.ObjectsDelete(ctx, nr); err == nil {
		t.Error("expected NotFound from ObjectsDelete on missing object")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 404 {
		t.Errorf("expected 404 ProviderError, got %v", err)
	}
	if _, err := p.ObjectsGetMedia(ctx, nr); err == nil {
		t.Error("expected NotFound from ObjectsGetMedia on missing object")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 404 {
		t.Errorf("expected 404 ProviderError, got %v", err)
	}
}

func TestBucketsInsertValidation(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	// Missing name → 400.
	nr := bucketParams()
	nr.Params["body"] = map[string]any{}
	if _, err := p.BucketsInsert(ctx, nr); err == nil {
		t.Error("expected InvalidRequest when bucket name is missing")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 400 {
		t.Errorf("expected 400 ProviderError, got %v", err)
	}

	// Duplicate → 409.
	nr = bucketParams()
	nr.Params["body"] = map[string]any{"name": "dup"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert: %v", err)
	}
	nr = bucketParams()
	nr.Params["body"] = map[string]any{"name": "dup"}
	if _, err := p.BucketsInsert(ctx, nr); err == nil {
		t.Error("expected Conflict on duplicate bucket")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 409 {
		t.Errorf("expected 409 ProviderError, got %v", err)
	}
}

func TestResumableSessionValidation(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	// Start without bucket/object → 400.
	nr := bucketParams()
	if _, err := p.ObjectsInsertStartResumable(ctx, nr); err == nil {
		t.Error("expected InvalidRequest when bucket/object missing")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 400 {
		t.Errorf("expected 400 ProviderError, got %v", err)
	}

	// Chunk for an unknown upload_id → 404.
	nr = bucketParams()
	nr.Params["upload_id"] = "nope"
	nr.Params["contentRange"] = "bytes 0-4/*"
	nr.Params[wire.MediaKey] = []byte("hello")
	if _, err := p.ObjectsInsertResumable(ctx, nr); err == nil {
		t.Error("expected NotFound on unknown upload_id")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 404 {
		t.Errorf("expected 404 ProviderError, got %v", err)
	}
}

func TestObjectsListInvalidPageToken(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "a.txt"
	nr.Params[wire.MediaKey] = []byte("a")
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// A malformed pageToken is ignored: the listing restarts from page 1.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["pageToken"] = "!!!not-base64!!!"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if items, _ := resp.Data["items"].([]any); len(items) != 1 {
		t.Fatalf("expected 1 item with invalid token, got %d", len(items))
	}
}

func TestBucketsListPagination(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	for _, name := range []string{"bkt-a", "bkt-b", "bkt-c"} {
		nr := bucketParams()
		nr.Params["body"] = map[string]any{"name": name}
		if _, err := p.BucketsInsert(ctx, nr); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}

	nr := bucketParams()
	nr.Params["maxResults"] = "2"
	resp, err := p.BucketsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := resp.Data["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected 2 buckets on page 1, got %d", len(items))
	}
	token, _ := resp.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatal("expected nextPageToken on page 1")
	}

	nr = bucketParams()
	nr.Params["maxResults"] = "2"
	nr.Params["pageToken"] = token
	resp, err = p.BucketsList(ctx, nr)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	items, _ = resp.Data["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 bucket on page 2, got %d", len(items))
	}
	if _, hasNext := resp.Data["nextPageToken"]; hasNext {
		t.Error("expected no nextPageToken on final page")
	}
}

func TestObjectsUpdateNoBody(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "obj.txt", "text/plain", map[string]any{"keep": "yes"})

	// Strict PUT without any body: every writable field is cleared.
	nr := bucketParamsWithObj("bkt", "obj.txt")
	resp, err := p.ObjectsUpdate(ctx, nr)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, ok := resp.Data["contentType"]; ok {
		t.Errorf("expected contentType cleared, got %v", resp.Data["contentType"])
	}
	if resp.Data["metadata"] != nil {
		t.Errorf("expected metadata cleared, got %v", resp.Data["metadata"])
	}
	if resp.Data["storageClass"] != "STANDARD" {
		t.Errorf("expected storageClass STANDARD, got %v", resp.Data["storageClass"])
	}
}

func TestGenerationSeededAcrossRestart(t *testing.T) {
	ctx := context.Background()
	objStore := gcs.NewMemoryObjectStore()
	blobs := blobfs.NewMemoryBlobStore()
	p1 := New(objStore, store.NewMemoryResourceStore(), blobs, crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p1.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "a.txt"
	nr.Params[wire.MediaKey] = []byte("a")
	first, err := p1.ObjectsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("insert object: %v", err)
	}

	// A "restarted" provider over the same object store must continue past the
	// stored generation (monotonicity across restarts, --dsn parity).
	p2 := New(objStore, store.NewMemoryResourceStore(), blobs, crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "b.txt"
	nr.Params[wire.MediaKey] = []byte("b")
	second, err := p2.ObjectsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("insert object after restart: %v", err)
	}
	g1, _ := first.Data["generation"].(string)
	g2, _ := second.Data["generation"].(string)
	n1, err1 := strconv.ParseInt(g1, 10, 64)
	n2, err2 := strconv.ParseInt(g2, 10, 64)
	if err1 != nil || err2 != nil || n2 <= n1 {
		t.Fatalf("expected generation after restart (%s) > stored generation (%s)", g2, g1)
	}
}

func TestResumableSessionEdgeCases(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Session start with the object name in the JSON body.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["body"] = map[string]any{"name": "x.bin"}
	start, err := p.ObjectsInsertStartResumable(ctx, nr)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	uploadID := extractUploadID(t, start)

	// Status query on an empty session → 308 with no Range header (real GCS
	// omits Range when no bytes are persisted).
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes */5"
	resp, err := p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("status query on empty session: %v / %v", resp, err)
	}
	if _, ok := resp.Data[wire.RangeKey]; ok {
		t.Fatalf("empty session must omit Range, got %v", resp.Data[wire.RangeKey])
	}

	// A chunk may update the session content type.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-4/*"
	nr.Params[wire.MediaKey] = []byte("hello")
	nr.Params[wire.ContentTypeKey] = "text/plain"
	if resp, err := p.ObjectsInsertResumable(ctx, nr); err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("chunk with content type: %v / %v", resp, err)
	}

	// Session cap: the earlier session still counts; fill to maxUploadSessions
	// and verify the next start is rejected with 429.
	for i := 0; i < maxUploadSessions-1; i++ {
		nr = bucketParams()
		nr.Params["bucket"] = "bkt"
		nr.Params["object"] = fmt.Sprintf("cap-%d.bin", i)
		if _, err := p.ObjectsInsertStartResumable(ctx, nr); err != nil {
			t.Fatalf("start %d: %v", i, err)
		}
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "overflow.bin"
	if _, err := p.ObjectsInsertStartResumable(ctx, nr); err == nil {
		t.Error("expected 429 when the resumable session cap is reached")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 429 {
		t.Errorf("expected 429 ProviderError, got %v", err)
	}
}

func TestResetCleansSpilledSessions(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "big.bin"
	start, err := p.ObjectsInsertStartResumable(ctx, nr)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	uploadID := extractUploadID(t, start)

	// A chunk past the spill threshold forces a temp file.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-" + fmt.Sprintf("%d", resumableSpillThreshold) + "/*"
	nr.Params[wire.MediaKey] = bytes.Repeat([]byte("x"), resumableSpillThreshold+1)
	if resp, err := p.ObjectsInsertResumable(ctx, nr); err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("spill chunk: %v / %v", resp, err)
	}
	p.mu.Lock()
	sess := p.uploads[uploadID]
	if sess == nil || sess.tmpFile == nil {
		p.mu.Unlock()
		t.Fatal("expected an active spilled session")
	}
	tmpPath := sess.tmpPath
	p.mu.Unlock()

	// Reset closes and removes the spill file.
	p.Reset(ctx)
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("expected spill file %s removed by Reset", tmpPath)
	}
	if _, ok := p.uploads[uploadID]; ok {
		t.Error("expected session map cleared by Reset")
	}
}

func TestResumableSessionTTLSweep(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Session 1 with a spilled temp file.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "stale.bin"
	start, err := p.ObjectsInsertStartResumable(ctx, nr)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	staleID := extractUploadID(t, start)
	nr = bucketParams()
	nr.Params["upload_id"] = staleID
	nr.Params["contentRange"] = "bytes 0-" + fmt.Sprintf("%d", resumableSpillThreshold) + "/*"
	nr.Params[wire.MediaKey] = bytes.Repeat([]byte("x"), resumableSpillThreshold+1)
	if resp, err := p.ObjectsInsertResumable(ctx, nr); err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("spill chunk: %v / %v", resp, err)
	}

	// Age the session past the TTL and capture its spill path.
	p.mu.Lock()
	sess := p.uploads[staleID]
	if sess == nil || sess.tmpFile == nil {
		p.mu.Unlock()
		t.Fatal("expected an active spilled session")
	}
	sess.lastAccess = clock.RealNow().Add(-2 * resumableSessionTTL)
	tmpPath := sess.tmpPath
	p.mu.Unlock()

	// Starting a new session triggers the sweep of the stale one.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "fresh.bin"
	if _, err := p.ObjectsInsertStartResumable(ctx, nr); err != nil {
		t.Fatalf("start fresh: %v", err)
	}
	p.mu.Lock()
	_, stale := p.uploads[staleID]
	p.mu.Unlock()
	if stale {
		t.Error("expected stale session to be swept")
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("expected spill file %s removed by sweep", tmpPath)
	}

	// The swept upload_id no longer accepts chunks.
	nr = bucketParams()
	nr.Params["upload_id"] = staleID
	nr.Params["contentRange"] = "bytes */1"
	if _, err := p.ObjectsInsertResumable(ctx, nr); err == nil {
		t.Error("expected NotFound on swept session")
	}
}

func TestObjectsGetMediaBlobMissing(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "obj.txt", "text/plain", nil)

	// Delete the blob behind the metadata: media reads must surface 404 (a
	// tombstoned/deleted version whose data was dropped), not silently return
	// nothing and not a 500.
	meta, err := p.objects.GetObjectMeta(ctx, "bkt", "obj.txt")
	if err != nil {
		t.Fatalf("get object meta: %v", err)
	}
	if err := p.blobs.Delete(ctx, blobsNamespace, blobKey("bkt", "obj.txt", meta.Generation)); err != nil {
		t.Fatalf("delete blob: %v", err)
	}
	if _, err := p.ObjectsGetMedia(ctx, bucketParamsWithObj("bkt", "obj.txt")); err == nil {
		t.Error("expected error when metadata is present but blob is missing")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 404 {
		t.Errorf("expected 404 ProviderError, got %v", err)
	}
}

func TestObjectsListPrefixDelimiterPagination(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	for _, name := range []string{"dir/a.txt", "dir/sub/b.txt", "other.txt"} {
		nr = bucketParams()
		nr.Params["bucket"] = "bkt"
		nr.Params["object"] = name
		nr.Params[wire.MediaKey] = []byte(name)
		if _, err := p.ObjectsInsert(ctx, nr); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}

	// prefix=dir/ + delimiter=/ → item "dir/a.txt" + prefix "dir/sub/".
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["prefix"] = "dir/"
	nr.Params["delimiter"] = "/"
	nr.Params["maxResults"] = "1"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := resp.Data["items"].([]any)
	prefixes, _ := resp.Data["prefixes"].([]string)
	if len(items) != 1 || len(prefixes) != 0 {
		t.Fatalf("page 1 expected 1 item, got %d items / %d prefixes", len(items), len(prefixes))
	}
	if items[0].(map[string]any)["name"] != "dir/a.txt" {
		t.Errorf("expected dir/a.txt, got %v", items[0].(map[string]any)["name"])
	}
	token, _ := resp.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatal("expected nextPageToken on page 1")
	}

	// Page 2: the dir/sub/ prefix, and nothing else.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["prefix"] = "dir/"
	nr.Params["delimiter"] = "/"
	nr.Params["maxResults"] = "1"
	nr.Params["pageToken"] = token
	resp, err = p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	items, _ = resp.Data["items"].([]any)
	prefixes, _ = resp.Data["prefixes"].([]string)
	if len(items) != 0 || len(prefixes) != 1 {
		t.Fatalf("page 2 expected 1 prefix, got %d items / %d prefixes", len(items), len(prefixes))
	}
	if prefixes[0] != "dir/sub/" {
		t.Errorf("expected prefix dir/sub/, got %v", prefixes)
	}
	if _, hasNext := resp.Data["nextPageToken"]; hasNext {
		t.Error("expected no nextPageToken on final page")
	}
}

func TestObjectsPatchMerges(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "obj.txt", "text/plain", map[string]any{"keep": "yes"})

	// PATCH with only contentType: omitted fields must be preserved.
	nr := bucketParamsWithObj("bkt", "obj.txt")
	nr.Params["body"] = map[string]any{"contentType": "application/json", "storageClass": "NEARLINE"}
	resp, err := p.ObjectsPatch(ctx, nr)
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	if resp.Data["contentType"] != "application/json" {
		t.Errorf("expected contentType application/json, got %v", resp.Data["contentType"])
	}
	if resp.Data["storageClass"] != "NEARLINE" {
		t.Errorf("expected storageClass NEARLINE, got %v", resp.Data["storageClass"])
	}
	md, _ := resp.Data["metadata"].(map[string]any)
	if md["keep"] != "yes" {
		t.Errorf("expected metadata preserved by PATCH, got %v", resp.Data["metadata"])
	}
}

func TestBucketsUpdatePreservesTimeCreated(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt", "location": "US"}
	created, err := p.BucketsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	timeCreated, _ := created.Data["timeCreated"].(string)

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["body"] = map[string]any{"location": "EU", "storageClass": "NEARLINE"}
	updated, err := p.BucketsUpdate(ctx, nr)
	if err != nil {
		t.Fatalf("update bucket: %v", err)
	}
	if updated.Data["location"] != "EU" {
		t.Errorf("expected location EU, got %v", updated.Data["location"])
	}
	if updated.Data["storageClass"] != "NEARLINE" {
		t.Errorf("expected storageClass NEARLINE, got %v", updated.Data["storageClass"])
	}
	if updated.Data["timeCreated"] != timeCreated {
		t.Errorf("expected timeCreated preserved, got %v", updated.Data["timeCreated"])
	}
}

func TestSetIamPolicyEtagOCC(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Current (default) policy etag.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	got, err := p.BucketsGetIamPolicy(ctx, nr)
	if err != nil {
		t.Fatalf("get iam: %v", err)
	}
	currentEtag, _ := got.Data["etag"].(string)

	bindings := []any{map[string]any{"role": "roles/storage.objectViewer", "members": []any{"allUsers"}}}

	// Mismatched etag → 409.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["body"] = map[string]any{"bindings": bindings, "etag": "BOGUS="}
	_, err = p.BucketsSetIamPolicy(ctx, nr)
	if err == nil {
		t.Fatal("expected 409 on etag mismatch")
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != 409 {
		t.Fatalf("expected 409 ProviderError, got %v", err)
	}

	// Matching etag → accepted, and the stored etag changes.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["body"] = map[string]any{"bindings": bindings, "etag": currentEtag, "version": 3.0}
	set, err := p.BucketsSetIamPolicy(ctx, nr)
	if err != nil {
		t.Fatalf("set with matching etag: %v", err)
	}
	newEtag, _ := set.Data["etag"].(string)
	if newEtag == "" || newEtag == currentEtag {
		t.Fatalf("expected a fresh etag after set, got %q", newEtag)
	}
	if v, _ := set.Data["version"].(int); v != 3 {
		t.Errorf("expected policy version 3, got %v", set.Data["version"])
	}

	// No etag → accepted (no OCC precondition).
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["body"] = map[string]any{"bindings": bindings}
	if _, err := p.BucketsSetIamPolicy(ctx, nr); err != nil {
		t.Fatalf("set without etag: %v", err)
	}
}

func TestObjectsIamPolicy(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "dir/obj.txt", "text/plain", nil)

	// Default object policy uses the object resourceId.
	nr := bucketParamsWithObj("bkt", "dir/obj.txt")
	got, err := p.ObjectsGetIamPolicy(ctx, nr)
	if err != nil {
		t.Fatalf("get object iam: %v", err)
	}
	if rid, _ := got.Data["resourceId"].(string); rid != "projects/_/buckets/bkt/objects/dir/obj.txt" {
		t.Errorf("expected object policy resourceId, got %q", rid)
	}
	if bindings, _ := got.Data["bindings"].([]iamBinding); len(bindings) == 0 {
		t.Error("expected default object policy bindings")
	}

	// Set + read back.
	nr = bucketParamsWithObj("bkt", "dir/obj.txt")
	nr.Params["body"] = map[string]any{
		"bindings": []any{
			map[string]any{"role": "roles/storage.objectViewer", "members": []any{"allUsers"}},
		},
	}
	if _, err := p.ObjectsSetIamPolicy(ctx, nr); err != nil {
		t.Fatalf("set object iam: %v", err)
	}
	nr = bucketParamsWithObj("bkt", "dir/obj.txt")
	got, err = p.ObjectsGetIamPolicy(ctx, nr)
	if err != nil {
		t.Fatalf("get object iam after set: %v", err)
	}
	bindings, _ := got.Data["bindings"].([]iamBinding)
	if len(bindings) != 1 || bindings[0].Role != "roles/storage.objectViewer" {
		t.Fatalf("unexpected bindings: %+v", bindings)
	}
}

func TestObjectsListDelimiterPagination(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	for _, name := range []string{"a-root.txt", "b-root.txt", "dir/a.txt", "dir/sub/b.txt", "zed.txt"} {
		nr = bucketParams()
		nr.Params["bucket"] = "bkt"
		nr.Params["object"] = name
		nr.Params[wire.MediaKey] = []byte(name)
		if _, err := p.ObjectsInsert(ctx, nr); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}

	// Combined result set sorted by name: a-root, b-root, dir/, zed.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["delimiter"] = "/"
	nr.Params["maxResults"] = "2"
	resp, err := p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := resp.Data["items"].([]any)
	prefixes, _ := resp.Data["prefixes"].([]string)
	if len(items) != 2 || len(prefixes) != 0 {
		t.Fatalf("page 1 expected 2 items, got %d items / %d prefixes", len(items), len(prefixes))
	}
	token, _ := resp.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatal("expected nextPageToken on page 1")
	}

	// Page 2: dir/ + zed, no further token.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["delimiter"] = "/"
	nr.Params["maxResults"] = "2"
	nr.Params["pageToken"] = token
	resp, err = p.ObjectsList(ctx, nr)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	items, _ = resp.Data["items"].([]any)
	prefixes, _ = resp.Data["prefixes"].([]string)
	if len(items) != 1 || len(prefixes) != 1 {
		t.Fatalf("page 2 expected 1 item + 1 prefix, got %d / %d", len(items), len(prefixes))
	}
	if _, hasNext := resp.Data["nextPageToken"]; hasNext {
		t.Error("expected no nextPageToken on final page")
	}
}

func TestParseByteRange(t *testing.T) {
	total := int64(100)
	cases := []struct {
		rng        string
		start, end int64
		res        rangeParse
	}{
		{"bytes=0-15", 0, 15, rangeOK},
		{"bytes=50-", 50, 99, rangeOK},
		{"bytes=-10", 90, 99, rangeOK},
		{"bytes=0-200", 0, 99, rangeOK},          // clamped to total
		{"bytes=100-", 0, 0, rangeUnsatisfiable}, // start at the end
		{"bytes=-0", 0, 0, rangeUnsatisfiable},   // zero-length suffix
		{"bytes=", 0, 0, rangeMalformed},         // empty
		{"items=0-5", 0, 0, rangeMalformed},      // wrong unit
		{"bytes=5-2", 0, 0, rangeMalformed},      // end < start
	}
	for _, c := range cases {
		s, e, res := parseByteRange(c.rng, total)
		if res != c.res || (res == rangeOK && (s != c.start || e != c.end)) {
			t.Errorf("parseByteRange(%q, %d) = (%d,%d,%v), want (%d,%d,%v)",
				c.rng, total, s, e, res, c.start, c.end, c.res)
		}
	}
}

// ─── resumable upload semantics (Gap 1–4) ───────────────────────────────────

func resumableStart(t *testing.T, p *Provider, bucket, object string) string {
	t.Helper()
	ctx := context.Background()
	nr := bucketParams()
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	nr.Params[wire.ContentTypeKey] = "application/octet-stream"
	start, err := p.ObjectsInsertStartResumable(ctx, nr)
	if err != nil {
		t.Fatalf("start resumable: %v", err)
	}
	return extractUploadID(t, start)
}

func TestResumableEmptyStatusQueryOmitsRange(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	uploadID := resumableStart(t, p, "bkt", "x.bin")

	// Empty-session status query -> 308 with no Range and no override.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes */5"
	resp, err := p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("status query: %v / %v", resp, err)
	}
	if _, ok := resp.Data[wire.RangeKey]; ok {
		t.Error("empty session must omit Range")
	}
	if _, ok := resp.Data[wire.StatusOverrideKey]; ok {
		t.Error("empty session must not carry a status override")
	}

	// length-0 + X-GUploader-No-308 -> 200 + override, no Range (Go SDK contract).
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes */5"
	nr.Params[wire.No308Key] = true
	resp, err = p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 200 {
		t.Fatalf("no-308 status query: %v / %v", resp, err)
	}
	if so, _ := resp.Data[wire.StatusOverrideKey].(string); so != "308" {
		t.Fatalf("expected status override 308, got %q", so)
	}
	if _, ok := resp.Data[wire.RangeKey]; ok {
		t.Error("empty no-308 session must omit Range")
	}
}

func TestResumableNonEmptyStatusKeepsRange(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	uploadID := resumableStart(t, p, "bkt", "x.bin")

	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-4/*"
	nr.Params[wire.MediaKey] = []byte("hello")
	if resp, err := p.ObjectsInsertResumable(ctx, nr); err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("chunk: %v / %v", resp, err)
	}

	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes */100"
	resp, err := p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("status: %v / %v", resp, err)
	}
	if rng, _ := resp.Data[wire.RangeKey].(string); rng != "bytes=0-4" {
		t.Fatalf("expected Range bytes=0-4, got %q", rng)
	}
}

func TestResumableOffsetGapReturnsPlain503(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	uploadID := resumableStart(t, p, "bkt", "x.bin")

	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 4-5/6"
	nr.Params[wire.MediaKey] = []byte("ok")
	_, err := p.ObjectsInsertResumable(ctx, nr)
	pe, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("expected ProviderError, got %v", err)
	}
	if pe.HTTPStatus != 503 {
		t.Fatalf("expected 503, got %d", pe.HTTPStatus)
	}
	if f, _ := pe.Data["errorFormat"].(string); f != "plain" {
		t.Fatalf("expected plain errorFormat, got %q", f)
	}
	if len(pe.Message) != 138 {
		t.Fatalf("expected 138-byte message, got %d", len(pe.Message))
	}
	if !strings.Contains(strings.ToLower(pe.Message), "content-range") || strings.Contains(strings.ToLower(pe.Message), "earlier") {
		t.Fatalf("unexpected message shape: %q", pe.Message)
	}
}

func TestResumableRewindIgnored(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	uploadID := resumableStart(t, p, "bkt", "x.bin")

	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-4/*"
	nr.Params[wire.MediaKey] = []byte("hello")
	if resp, err := p.ObjectsInsertResumable(ctx, nr); err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("chunk: %v / %v", resp, err)
	}

	// Re-send the same chunk (rewind) -> 308, bytes unchanged.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-4/*"
	nr.Params[wire.MediaKey] = []byte("hello")
	resp, err := p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("rewind chunk: %v / %v", resp, err)
	}
	if rng, _ := resp.Data[wire.RangeKey].(string); rng != "bytes=0-4" {
		t.Fatalf("expected Range bytes=0-4 after rewind, got %q", rng)
	}
}

func TestResumableUnknownSizeFinalChunk(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	uploadID := resumableStart(t, p, "bkt", "x.bin")

	body := []byte("seventeen bytes!!")
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-*/*"
	nr.Params[wire.MediaKey] = body
	resp, err := p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 200 {
		t.Fatalf("unknown-size final: %v / %v", resp, err)
	}
	if size, _ := resp.Data["size"].(string); size != strconv.Itoa(len(body)) {
		t.Fatalf("expected size %d, got %q", len(body), size)
	}

	media, err := p.ObjectsGetMedia(ctx, bucketParamsWithObj("bkt", "x.bin"))
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	if got := string(streamBytes(t, media)); got != string(body) {
		t.Fatalf("expected %q, got %q", body, got)
	}
}

func TestResumableUnknownSizeFinalKnownTotal(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	uploadID := resumableStart(t, p, "bkt", "x.bin")

	body := []byte("12345678901234567") // 17 bytes
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-*/17"
	nr.Params[wire.MediaKey] = body
	resp, err := p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 200 {
		t.Fatalf("known-total unknown-end final: %v / %v", resp, err)
	}

	// A mismatched declared total must 400.
	uploadID = resumableStart(t, p, "bkt", "y.bin")
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-*/16"
	nr.Params[wire.MediaKey] = body
	_, err = p.ObjectsInsertResumable(ctx, nr)
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 400 {
		t.Fatalf("expected 400 on size mismatch, got %v", err)
	}
}

func TestResumablePostCompletionReplay(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	uploadID := resumableStart(t, p, "bkt", "x.bin")

	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-4/*"
	nr.Params[wire.MediaKey] = []byte("hello")
	if resp, err := p.ObjectsInsertResumable(ctx, nr); err != nil || resp.HTTPStatus != 308 {
		t.Fatalf("chunk: %v / %v", resp, err)
	}
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 5-10/11"
	nr.Params[wire.MediaKey] = []byte(" world")
	resp, err := p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 200 {
		t.Fatalf("final: %v / %v", resp, err)
	}

	// Post-completion status query -> 200 + object size.
	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes */11"
	resp, err = p.ObjectsInsertResumable(ctx, nr)
	if err != nil || resp.HTTPStatus != 200 {
		t.Fatalf("post-completion status: %v / %v", resp, err)
	}
	if size, _ := resp.Data["size"].(string); size != "11" {
		t.Fatalf("expected size 11, got %q", size)
	}
}

func TestResumableTombstoneSweep(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	uploadID := resumableStart(t, p, "bkt", "x.bin")

	nr = bucketParams()
	nr.Params["upload_id"] = uploadID
	nr.Params["contentRange"] = "bytes 0-4/5"
	nr.Params[wire.MediaKey] = []byte("hello")
	if _, err := p.ObjectsInsertResumable(ctx, nr); err != nil {
		t.Fatalf("final: %v", err)
	}
	p.mu.Lock()
	if _, ok := p.completed[uploadID]; !ok {
		p.mu.Unlock()
		t.Fatal("expected a completion tombstone")
	}
	p.completed[uploadID].lastAccess = clock.RealNow().Add(-48 * time.Hour)
	p.sweepSessions()
	_, ok := p.completed[uploadID]
	p.mu.Unlock()
	if ok {
		t.Error("expected tombstone to be swept after TTL")
	}
}

// ─── V4 signed URLs (Gap 6) ────────────────────────────────────────────────

func signedRequest(bucket, object, expires, date string) *model.NormalizedRequest {
	nr := bucketParamsWithObj(bucket, object)
	nr.Params[wire.SignedURLKey] = true
	nr.Params["X-Goog-Algorithm"] = "GOOG4-RSA-SHA256"
	nr.Params["X-Goog-Credential"] = "test@test.iam.gserviceaccount.com/20260913/auto/storage/goog4_request"
	nr.Params["X-Goog-SignedHeaders"] = "host"
	nr.Params["X-Goog-Signature"] = "abc"
	nr.Params["X-Goog-Expires"] = expires
	nr.Params["X-Goog-Date"] = date
	return nr
}

func TestSignedURLExpiredToken(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := signedRequest("bkt", "o.txt", "60", "20200101T000000Z")
	_, err := p.ObjectsGetMedia(ctx, nr)
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 400 || pe.Code != "ExpiredToken" {
		t.Fatalf("expected ExpiredToken 400, got %v", err)
	}
	if f, _ := pe.Data["errorFormat"].(string); f != "xml" {
		t.Fatalf("expected xml errorFormat, got %q", f)
	}
}

func TestSignedURLMalformedDate(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := signedRequest("bkt", "o.txt", "3600", "not-a-date")
	_, err := p.ObjectsGetMedia(ctx, nr)
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 400 || pe.Code != "MalformedSecurityHeader" {
		t.Fatalf("expected MalformedSecurityHeader 400, got %v", err)
	}
	if param, _ := pe.Data["parameterName"].(string); param != "Date" {
		t.Fatalf("expected ParameterName Date, got %q", param)
	}
}

func TestSignedURLInvalidExpires(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := signedRequest("bkt", "o.txt", "604801", "20260913T000000Z")
	_, err := p.ObjectsGetMedia(ctx, nr)
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 400 || pe.Code != "MalformedSecurityHeader" {
		t.Fatalf("expected MalformedSecurityHeader 400, got %v", err)
	}
	if param, _ := pe.Data["parameterName"].(string); param != "Expires" {
		t.Fatalf("expected ParameterName Expires, got %q", param)
	}
}

func TestSignedURLLowercaseParams(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParamsWithObj("bkt", "o.txt")
	nr.Params[wire.SignedURLKey] = true
	nr.Params["X-Goog-Signature"] = "abc"
	nr.Params["x-goog-expires"] = "604801"
	nr.Params["x-goog-date"] = "20260913T000000Z"
	_, err := p.ObjectsGetMedia(ctx, nr)
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 400 || pe.Code != "MalformedSecurityHeader" {
		t.Fatalf("expected MalformedSecurityHeader 400, got %v", err)
	}
	if param, _ := pe.Data["parameterName"].(string); param != "Expires" {
		t.Fatalf("expected ParameterName Expires, got %q", param)
	}
}

func TestSignedURLPutStoresObject(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	date := clock.Now().Format("20060102T150405Z")
	nr = signedRequest("bkt", "o.txt", "3600", date)
	nr.Params[wire.MediaKey] = []byte("signed data")
	nr.Params[wire.ContentTypeKey] = "text/plain"
	resp, err := p.ObjectsInsert(ctx, nr)
	if err != nil || resp.HTTPStatus != 200 {
		t.Fatalf("signed PUT: %v / %v", resp, err)
	}

	media, err := p.ObjectsGetMedia(ctx, bucketParamsWithObj("bkt", "o.txt"))
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "signed data" {
		t.Fatalf("expected 'signed data', got %q", got)
	}
}

func TestSignedURLTamperedSignatureAccepted(t *testing.T) {
	// Records the deliberate auth bypass: a syntactically valid, unexpired
	// signed URL is accepted regardless of the signature value.
	ctx := context.Background()
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	nr = bucketParamsWithObj("bkt", "o.txt")
	nr.Params[wire.MediaKey] = []byte("payload")
	nr.Params[wire.ContentTypeKey] = "text/plain"
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("insert object: %v", err)
	}

	date := clock.Now().Format("20060102T150405Z")
	nr = signedRequest("bkt", "o.txt", "3600", date)
	nr.Params["X-Goog-Signature"] = "garbage-forged-signature"
	resp, err := p.ObjectsGetMedia(ctx, nr)
	if err != nil || resp.HTTPStatus != 200 {
		t.Fatalf("tampered signature must be accepted (200), got %v / %v", resp, err)
	}
}

// TestBucketSoftDeletePolicy verifies the default 7-day soft-delete policy is
// emitted with an effectiveTime, and that an explicit policy is preserved.
func TestBucketSoftDeletePolicy(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "sd-bkt"}
	resp, err := p.BucketsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	sd, _ := resp.Data["softDeletePolicy"].(map[string]any)
	if sd["retentionDurationSeconds"] != "604800" {
		t.Fatalf("softDeletePolicy = %#v, want retentionDurationSeconds 604800", resp.Data["softDeletePolicy"])
	}
	if sd["effectiveTime"] == nil || sd["effectiveTime"] == "" {
		t.Fatalf("softDeletePolicy missing effectiveTime: %#v", sd)
	}

	// An explicit policy wins over the default.
	nr = bucketParams()
	nr.Params["body"] = map[string]any{
		"name":             "sd-bkt-explicit",
		"softDeletePolicy": map[string]any{"retentionDurationSeconds": "3600"},
	}
	resp, err = p.BucketsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("insert explicit: %v", err)
	}
	sd, _ = resp.Data["softDeletePolicy"].(map[string]any)
	if sd["retentionDurationSeconds"] != "3600" {
		t.Fatalf("explicit softDeletePolicy overwritten: %#v", resp.Data["softDeletePolicy"])
	}
}

// TestObjectFinalizedTimes verifies object responses carry timeFinalized and
// timeStorageClassUpdated.
func TestObjectFinalizedTimes(t *testing.T) {
	now := clock.Now().UTC()
	m := gcs.ObjectMeta{
		Bucket:         "b",
		Name:           "o",
		Generation:     "1",
		Metageneration: "1",
		StorageClass:   "STANDARD",
		TimeCreated:    now,
		Updated:        now,
	}
	o := toMap(fromStoreObject(bucketParams(), m))
	if o["timeFinalized"] == nil || o["timeFinalized"] == "" {
		t.Fatalf("timeFinalized missing: %#v", o)
	}
	if o["timeStorageClassUpdated"] == nil || o["timeStorageClassUpdated"] == "" {
		t.Fatalf("timeStorageClassUpdated missing: %#v", o)
	}
}

// TestFetchObjectBytesRoundTrip verifies the server-side object read used by
// Cloud Functions source resolution returns the stored plaintext bytes, and
// reports gcs.ErrNoSuchObject for a missing object.
func TestFetchObjectBytesRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	want := []byte("PK\x03\x04 source archive")
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "src/o.zip"
	nr.Params[wire.MediaKey] = want
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("insert object: %v", err)
	}

	got, err := p.FetchObjectBytes(ctx, "bkt", "src/o.zip")
	if err != nil {
		t.Fatalf("FetchObjectBytes: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("FetchObjectBytes = %q, want %q", got, want)
	}

	if _, err := p.FetchObjectBytes(ctx, "bkt", "missing.zip"); !errors.Is(err, gcs.ErrNoSuchObject) {
		t.Fatalf("missing object err = %v, want gcs.ErrNoSuchObject", err)
	}
}

// TestPutObjectBytesRoundTrip verifies the server-side object write used by
// other emulated services (e.g. Dataproc driver output): the bucket is created
// lazily, the bytes round-trip through the encryption + blob path, and an
// overwrite replaces the live generation and cleans up the prior blob.
func TestPutObjectBytesRoundTrip(t *testing.T) {
	blobs := blobfs.NewMemoryBlobStore()
	p := New(gcs.NewMemoryObjectStore(), store.NewMemoryResourceStore(), blobs, crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))
	ctx := context.Background()

	if err := p.PutObjectBytes(ctx, "proj", "stage-bkt", "meta/jobs/j1/driveroutput", "text/plain", []byte("hello")); err != nil {
		t.Fatalf("PutObjectBytes: %v", err)
	}
	got, err := p.FetchObjectBytes(ctx, "stage-bkt", "meta/jobs/j1/driveroutput")
	if err != nil {
		t.Fatalf("FetchObjectBytes: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("round-trip = %q, want %q", got, "hello")
	}

	prev, err := p.objects.GetObjectMeta(ctx, "stage-bkt", "meta/jobs/j1/driveroutput")
	if err != nil {
		t.Fatalf("GetObjectMeta: %v", err)
	}
	if err := p.PutObjectBytes(ctx, "proj", "stage-bkt", "meta/jobs/j1/driveroutput", "text/plain", []byte("world")); err != nil {
		t.Fatalf("PutObjectBytes overwrite: %v", err)
	}
	got, err = p.FetchObjectBytes(ctx, "stage-bkt", "meta/jobs/j1/driveroutput")
	if err != nil {
		t.Fatalf("FetchObjectBytes after overwrite: %v", err)
	}
	if string(got) != "world" {
		t.Fatalf("overwrite round-trip = %q, want %q", got, "world")
	}
	if _, err := blobs.Get(ctx, blobsNamespace, blobKey("stage-bkt", "meta/jobs/j1/driveroutput", prev.Generation)); err == nil {
		t.Error("prior generation blob was not cleaned up on overwrite")
	}
}
