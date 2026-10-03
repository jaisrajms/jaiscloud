package storage

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
)

// TestBucketsGetStorageLayout covers buckets.getStorageLayout: the resource is
// returned for an existing bucket with hierarchical namespace disabled, and a
// missing bucket is 404.
func TestBucketsGetStorageLayout(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	create := bucketParams()
	create.Params["body"] = map[string]any{"name": "layout-bkt"}
	if _, err := p.BucketsInsert(ctx, create); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	get := bucketParams()
	get.Params["bucket"] = "layout-bkt"
	resp, err := p.BucketsGetStorageLayout(ctx, get)
	if err != nil {
		t.Fatalf("get storage layout: %v", err)
	}
	if resp.Data["kind"] != "storage#storageLayout" {
		t.Errorf("kind = %v, want storage#storageLayout", resp.Data["kind"])
	}
	if resp.Data["bucket"] != "layout-bkt" {
		t.Errorf("bucket = %v, want layout-bkt", resp.Data["bucket"])
	}
	hn, _ := resp.Data["hierarchicalNamespace"].(map[string]any)
	if hn == nil || hn["enabled"] != false {
		t.Errorf("hierarchicalNamespace = %v, want {enabled:false}", resp.Data["hierarchicalNamespace"])
	}

	missing := bucketParams()
	missing.Params["bucket"] = "no-such-bucket"
	_, err = p.BucketsGetStorageLayout(ctx, missing)
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 404 {
		t.Fatalf("missing bucket err = %v, want ProviderError 404", err)
	}
}
