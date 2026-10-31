package lambda

import (
	"context"
	"strings"
	"testing"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/model"
)

func TestStoreAndLoadCode(t *testing.T) {
	blobs := blobfs.NewMemoryBlobStore()
	p := &FunctionProvider{blobs: blobs}
	ctx := context.Background()

	original := []byte("fakecontent")
	sha, size, key, err := p.storeCode(ctx, "000", "foo", "$LATEST", original)
	if err != nil {
		t.Fatalf("storeCode: %v", err)
	}
	if len(sha) != 64 {
		t.Errorf("expected 64-char hex sha256, got %d chars", len(sha))
	}
	if size != int64(len(original)) {
		t.Errorf("size: got %d want %d", size, len(original))
	}
	if !strings.Contains(key, "foo") {
		t.Errorf("blobKey %q should contain function name", key)
	}

	loaded, err := p.loadCode(ctx, "000", "foo", "$LATEST")
	if err != nil {
		t.Fatalf("loadCode: %v", err)
	}
	if string(loaded) != string(original) {
		t.Errorf("loaded content mismatch: got %q want %q", loaded, original)
	}
}

func TestCodeHandlersServeStoredPackagesAsBody(t *testing.T) {
	p := &FunctionProvider{blobs: blobfs.NewMemoryBlobStore()}
	ctx := context.Background()
	if _, _, _, err := p.storeCode(ctx, "000", "foo", "$LATEST", []byte("fn")); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := p.storeLayerCode(ctx, "000", "lay", 2, []byte("ly")); err != nil {
		t.Fatal(err)
	}

	resp, err := p.getFunctionCodeZipHandler(ctx, &model.NormalizedRequest{AccountID: "000",
		Params: map[string]any{"functionName": "foo", "qualifier": "$LATEST"}})
	if err != nil || string(resp.Data["_body"].([]byte)) != "fn" {
		t.Fatalf("function code = %v, %v", resp, err)
	}
	// A layer version arrives as float64 when a plugin forwarded the request.
	resp, err = p.getLayerCodeZipHandler(ctx, &model.NormalizedRequest{AccountID: "000",
		Params: map[string]any{"layerName": "lay", "version": float64(2)}})
	if err != nil || string(resp.Data["_body"].([]byte)) != "ly" {
		t.Fatalf("layer code = %v, %v", resp, err)
	}
	_, err = p.getFunctionCodeZipHandler(ctx, &model.NormalizedRequest{AccountID: "000",
		Params: map[string]any{"functionName": "missing", "qualifier": "$LATEST"}})
	if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 404 {
		t.Fatalf("missing function error = %v", err)
	}
}
