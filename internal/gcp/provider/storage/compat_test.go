package storage

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"hash/crc32"
	"strings"
	"testing"

	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// crc32cB64 computes the GCS crc32c checksum (Castagnoli, big-endian, base64)
// the same way the provider does.
func crc32cB64(raw []byte) string {
	crc := crc32.Checksum(raw, crc32.MakeTable(crc32.Castagnoli))
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, crc)
	return base64.StdEncoding.EncodeToString(b)
}

func TestCustomMetadataRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "meta.txt", "text/plain", map[string]any{"env": "test", "owner": "sdk"})

	// objects.get returns the metadata object.
	resp, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "meta.txt"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	md, _ := resp.Data["metadata"].(map[string]any)
	if md["env"] != "test" || md["owner"] != "sdk" {
		t.Fatalf("expected metadata {env:test owner:sdk}, got %v", md)
	}

	// objects.list returns the metadata object too.
	list, err := p.ObjectsList(ctx, &model.NormalizedRequest{AccountID: "proj", Params: map[string]any{"bucket": "bkt"}})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := list.Data["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	obj := items[0].(map[string]any)
	lmd, _ := obj["metadata"].(map[string]any)
	if lmd["env"] != "test" || lmd["owner"] != "sdk" {
		t.Fatalf("expected list metadata {env:test owner:sdk}, got %v", lmd)
	}

	// Media download surfaces x-goog-meta-* headers (canonicalised key).
	media, err := p.ObjectsGetMedia(ctx, bucketParamsWithObj("bkt", "meta.txt"))
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	hdr, _ := media.Data[wire.HeadersKey].(map[string]string)
	if hdr["x-goog-meta-Env"] != "test" || hdr["x-goog-meta-Owner"] != "sdk" {
		t.Fatalf("expected x-goog-meta-* headers, got %v", hdr)
	}
}

func TestCustomMetadataFromHeadersRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// Upload with metadata supplied via x-goog-meta-* headers (simple upload).
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "hdr.txt"
	nr.Params[wire.MediaKey] = []byte("bytes")
	nr.Params[wire.MetaHeadersKey] = map[string]string{"originalname": "file.dat"}
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("insert: %v", err)
	}

	resp, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "hdr.txt"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	md, _ := resp.Data["metadata"].(map[string]any)
	if md["originalname"] != "file.dat" {
		t.Fatalf("expected metadata originalname=file.dat, got %v", md)
	}

	media, err := p.ObjectsGetMedia(ctx, bucketParamsWithObj("bkt", "hdr.txt"))
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	hdr, _ := media.Data[wire.HeadersKey].(map[string]string)
	if hdr["x-goog-meta-Originalname"] != "file.dat" {
		t.Fatalf("expected x-goog-meta-Originalname header, got %v", hdr)
	}
}

func TestObjectsRewriteCopiesContentAndNewGeneration(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "src.txt", "text/plain", map[string]any{"keep": "yes"})

	// Capture the source generation for comparison.
	srcResp, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "src.txt"))
	if err != nil {
		t.Fatalf("get source: %v", err)
	}
	srcGen, _ := srcResp.Data["generation"].(string)

	nr := bucketParams()
	nr.Params["sourceBucket"] = "bkt"
	nr.Params["sourceObject"] = "src.txt"
	nr.Params["destinationBucket"] = "bkt"
	nr.Params["destinationObject"] = "dst.txt"
	resp, err := p.ObjectsRewrite(ctx, nr)
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	if resp.Data["kind"] != "storage#rewriteResponse" {
		t.Errorf("expected kind storage#rewriteResponse, got %v", resp.Data["kind"])
	}
	if done, _ := resp.Data["done"].(bool); !done {
		t.Error("expected done=true")
	}
	if resp.Data["totalBytesRewritten"] != "5" || resp.Data["objectSize"] != "5" {
		t.Errorf("expected totalBytesRewritten/objectSize 5, got %v/%v",
			resp.Data["totalBytesRewritten"], resp.Data["objectSize"])
	}

	resource, _ := resp.Data["resource"].(map[string]any)
	if resource["name"] != "dst.txt" {
		t.Errorf("expected resource name dst.txt, got %v", resource["name"])
	}
	dstGen, _ := resource["generation"].(string)
	if dstGen == "" || dstGen == srcGen {
		t.Errorf("expected a new generation, got %q (source %q)", dstGen, srcGen)
	}
	rmd, _ := resource["metadata"].(map[string]any)
	if rmd["keep"] != "yes" {
		t.Errorf("expected metadata copied, got %v", rmd)
	}

	// Destination bytes round-trip.
	media, err := p.ObjectsGetMedia(ctx, bucketParamsWithObj("bkt", "dst.txt"))
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "hello" {
		t.Fatalf("expected copied content 'hello', got %q", got)
	}

	// Source survives the rewrite.
	if _, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "src.txt")); err != nil {
		t.Fatalf("source should remain after rewrite: %v", err)
	}
}

func TestObjectsRewriteMissingSource(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	nr = bucketParams()
	nr.Params["sourceBucket"] = "bkt"
	nr.Params["sourceObject"] = "missing.txt"
	nr.Params["destinationBucket"] = "bkt"
	nr.Params["destinationObject"] = "dst.txt"
	if _, err := p.ObjectsRewrite(ctx, nr); err == nil {
		t.Fatal("expected NotFound from rewrite of a missing source")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 404 {
		t.Fatalf("expected 404 ProviderError, got %v", err)
	}
}

func TestObjectsComposeConcatenates(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	for _, name := range []string{"a.txt", "b.txt"} {
		nr = bucketParams()
		nr.Params["bucket"] = "bkt"
		nr.Params["object"] = name
		nr.Params[wire.MediaKey] = []byte(name)
		if _, err := p.ObjectsInsert(ctx, nr); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}

	// a.txt="a.txt", b.txt="b.txt" → concatenated "a.txtb.txt" (10 bytes).
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "ab.txt"
	nr.Params["body"] = map[string]any{
		"sourceObjects": []any{
			map[string]any{"name": "a.txt"},
			map[string]any{"name": "b.txt"},
		},
	}
	resp, err := p.ObjectsCompose(ctx, nr)
	if err != nil {
		t.Fatalf("compose: %v", err)
	}

	if cc, _ := resp.Data["componentCount"].(float64); int64(cc) != 2 {
		t.Errorf("expected componentCount 2, got %v", resp.Data["componentCount"])
	}
	if _, present := resp.Data["md5Hash"]; present {
		t.Errorf("expected empty MD5, got %v", resp.Data["md5Hash"])
	}
	gotCRC, _ := resp.Data["crc32c"].(string)
	if gotCRC != crc32cB64([]byte("a.txtb.txt")) {
		t.Errorf("expected crc32c %q, got %q", crc32cB64([]byte("a.txtb.txt")), gotCRC)
	}
	if resp.Data["size"] != "10" {
		t.Errorf("expected size 10, got %v", resp.Data["size"])
	}

	// Bytes round-trip.
	media, err := p.ObjectsGetMedia(ctx, bucketParamsWithObj("bkt", "ab.txt"))
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "a.txtb.txt" {
		t.Fatalf("expected concatenated content 'a.txtb.txt', got %q", got)
	}
}

func TestObjectsComposeEmptySourceList(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "empty.txt"
	nr.Params["body"] = map[string]any{"sourceObjects": []any{}}
	if _, err := p.ObjectsCompose(ctx, nr); err == nil {
		t.Fatal("expected InvalidRequest when sourceObjects is empty")
	}
}

func TestMediaDownloadHeaders(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "obj.txt", "text/plain", map[string]any{"foo": "bar"})

	media, err := p.ObjectsGetMedia(ctx, bucketParamsWithObj("bkt", "obj.txt"))
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	hdr, _ := media.Data[wire.HeadersKey].(map[string]string)
	if hdr == nil {
		t.Fatal("expected media response headers")
	}

	if hdr["x-goog-generation"] == "" {
		t.Error("expected x-goog-generation header")
	}
	if hdr["x-goog-metageneration"] != "1" {
		t.Errorf("expected x-goog-metageneration 1, got %q", hdr["x-goog-metageneration"])
	}
	if hdr["x-goog-stored-content-length"] != "5" {
		t.Errorf("expected x-goog-stored-content-length 5, got %q", hdr["x-goog-stored-content-length"])
	}

	hash := hdr["x-goog-hash"]
	if !strings.Contains(hash, "crc32c=") || !strings.Contains(hash, "md5=") {
		t.Errorf("expected x-goog-hash with crc32c and md5, got %q", hash)
	}
	if hdr["x-goog-meta-Foo"] != "bar" {
		t.Errorf("expected x-goog-meta-Foo header, got %q", hdr["x-goog-meta-Foo"])
	}
}
