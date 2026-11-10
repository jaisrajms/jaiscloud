package storage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	gcpadapter "jaiscloud/internal/gcp/adapter"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
)

// bucketRawWrite drives a bucket write the way the wire does: the JSON body is
// decoded by the GCS codec (the only place that knows whether the request is a
// merge-patching PATCH or a replacing PUT) and the resulting request is
// dispatched to the provider handler the registry routes both verbs to.
func bucketRawWrite(t *testing.T, p *Provider, method, bucket, body string) *model.ProviderResponse {
	t.Helper()
	r := httptest.NewRequest(method, "/storage/v1/b/"+bucket, strings.NewReader(body))
	nr, err := (&gcpadapter.GCSCodec{}).Decode(r, []byte(body))
	if err != nil {
		t.Fatalf("%s decode: %v", method, err)
	}
	if nr.Action != "BucketsUpdate" {
		t.Fatalf("%s action = %q, want BucketsUpdate (both verbs share the core path)", method, nr.Action)
	}
	nr.AccountID = "proj"
	nr.ResourceID = resource.ResourceID("proj")
	resp, err := p.BucketsUpdate(context.Background(), nr)
	if err != nil {
		t.Fatalf("%s %s: %v", method, bucket, err)
	}
	return resp
}

// bucketRawLabels reads the bucket back and returns its served label map.
func bucketRawLabels(t *testing.T, p *Provider, bucket string) map[string]string {
	t.Helper()
	resp, err := p.BucketsGet(context.Background(), bucketParamsWithObj(bucket, ""))
	if err != nil {
		t.Fatalf("get %s: %v", bucket, err)
	}
	labels, _ := resp.Data["labels"].(map[string]string)
	return labels
}

// TestBucketsPatchMergesLabels pins TF5-2 on the raw REST path:
// buckets.patch is a JSON merge patch (RFC 7386). A key present in the request
// is set, a key sent as an explicit null is removed, and a key absent from the
// request is preserved — the shape hashicorp/google sends, since it clears a
// removed label through Bucket.NullFields["Labels.<key>"]. buckets.update (PUT)
// keeps replacing the whole map.
func TestBucketsPatchMergesLabels(t *testing.T) {
	p := newTestProvider()
	nr := bucketParams()
	nr.Params["body"] = map[string]any{
		"name":   "merge-labels",
		"labels": map[string]any{"a": "1", "b": "2"},
	}
	if _, err := p.BucketsInsert(context.Background(), nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	metaBefore := bucketMetageneration(t, p, "merge-labels")

	// PATCH {"labels":{"a":"9"}} keeps the absent b.
	resp := bucketRawWrite(t, p, http.MethodPatch, "merge-labels", `{"labels":{"a":"9"}}`)
	if got, _ := resp.Data["labels"].(map[string]string); got["a"] != "9" || got["b"] != "2" {
		t.Fatalf("patched response labels = %v, want a=9,b=2 (absent key preserved)", got)
	}
	labels := bucketRawLabels(t, p, "merge-labels")
	if labels["a"] != "9" || labels["b"] != "2" {
		t.Fatalf("labels after merge = %v, want a=9,b=2", labels)
	}

	// PATCH {"labels":{"b":null}} removes b and keeps a.
	bucketRawWrite(t, p, http.MethodPatch, "merge-labels", `{"labels":{"b":null}}`)
	labels = bucketRawLabels(t, p, "merge-labels")
	if labels["a"] != "9" {
		t.Fatalf("a after null-delete = %q, want 9", labels["a"])
	}
	if _, present := labels["b"]; present {
		t.Fatalf("b should be removed by the explicit null, got %v", labels)
	}

	// An empty value is a value: it stays in the map (the provider's
	// empty-label convergence caveat lives above the wire, not here).
	bucketRawWrite(t, p, http.MethodPatch, "merge-labels", `{"labels":{"team":""}}`)
	if v, present := bucketRawLabels(t, p, "merge-labels")["team"]; !present || v != "" {
		t.Fatalf("team = %q (present=%v), want the empty value preserved", v, present)
	}

	// Each PATCH bumps the metageneration.
	if meta := bucketMetageneration(t, p, "merge-labels"); meta != metaBefore+3 {
		t.Fatalf("metageneration = %d, want %d (one per patch)", meta, metaBefore+3)
	}

	// A null labels member clears the whole map (the provider sends this when
	// the last label is removed).
	bucketRawWrite(t, p, http.MethodPatch, "merge-labels", `{"labels":null}`)
	if labels := bucketRawLabels(t, p, "merge-labels"); len(labels) != 0 {
		t.Fatalf("labels after labels:null = %v, want none", labels)
	}

	// buckets.update (PUT) is still a full replace: b and team are re-added and
	// a PUT carrying only a clears them.
	bucketRawWrite(t, p, http.MethodPatch, "merge-labels", `{"labels":{"b":"2","team":"core"}}`)
	bucketRawWrite(t, p, http.MethodPut, "merge-labels", `{"labels":{"a":"1"}}`)
	labels = bucketRawLabels(t, p, "merge-labels")
	if len(labels) != 1 || labels["a"] != "1" {
		t.Fatalf("labels after PUT = %v, want only a=1 (PUT replaces)", labels)
	}
}

// bucketMetageneration parses the served metageneration as an int.
func bucketMetageneration(t *testing.T, p *Provider, bucket string) int {
	t.Helper()
	resp, err := p.BucketsGet(context.Background(), bucketParamsWithObj(bucket, ""))
	if err != nil {
		t.Fatalf("get %s: %v", bucket, err)
	}
	raw, _ := resp.Data["metageneration"].(string)
	if raw == "" {
		t.Fatalf("bucket %s has no metageneration", bucket)
	}
	n := 0
	for _, c := range raw {
		if c < '0' || c > '9' {
			t.Fatalf("metageneration %q is not numeric", raw)
		}
		n = n*10 + int(c-'0')
	}
	return n
}
