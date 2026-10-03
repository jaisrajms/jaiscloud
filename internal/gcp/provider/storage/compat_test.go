package storage

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"strings"
	"testing"

	"jaiscloud/internal/gcp/store/gcs"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// md5HexFromB64 decodes a base64 GCS md5Hash into the lowercase hex the XML API
// uses for its ETag.
func md5HexFromB64(t *testing.T, b64 string) string {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatalf("decode md5 %q: %v", b64, err)
	}
	return hex.EncodeToString(raw)
}

// gcsObjectMetaWithMD5 builds an ObjectMeta carrying the base64 MD5 of content.
func gcsObjectMetaWithMD5(content string) gcs.ObjectMeta {
	sum := md5.Sum([]byte(content))
	return gcs.ObjectMeta{MD5Hash: base64.StdEncoding.EncodeToString(sum[:])}
}

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

// TestObjectsCopyReturnsObject covers objects.copyTo: it copies like rewrite but
// returns the destination storage#object directly (not a rewrite envelope), and
// leaves the source in place.
func TestObjectsCopyReturnsObject(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "src.txt", "text/plain", map[string]any{"keep": "yes"})

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
	resp, err := p.ObjectsCopy(ctx, nr)
	if err != nil {
		t.Fatalf("copy: %v", err)
	}

	if resp.Data["kind"] != "storage#object" {
		t.Errorf("expected kind storage#object, got %v", resp.Data["kind"])
	}
	if resp.Data["name"] != "dst.txt" {
		t.Errorf("expected name dst.txt, got %v", resp.Data["name"])
	}
	dstGen, _ := resp.Data["generation"].(string)
	if dstGen == "" || dstGen == srcGen {
		t.Errorf("expected a new generation, got %q (source %q)", dstGen, srcGen)
	}

	media, err := p.ObjectsGetMedia(ctx, bucketParamsWithObj("bkt", "dst.txt"))
	if err != nil {
		t.Fatalf("get media: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "hello" {
		t.Fatalf("expected copied content 'hello', got %q", got)
	}
	if _, err := p.ObjectsGet(ctx, bucketParamsWithObj("bkt", "src.txt")); err != nil {
		t.Fatalf("source should remain after copy: %v", err)
	}
}

// TestMediaHeadersXMLEtag verifies the XML API ETag (quoted lowercase hex MD5)
// versus the JSON API constant etag, including the no-MD5 fallback.
func TestMediaHeadersXMLEtag(t *testing.T) {
	m := gcsObjectMetaWithMD5("hello")
	xml := mediaHeaders(m, true)
	if !strings.HasPrefix(xml["ETag"], `"`) || !strings.HasSuffix(xml["ETag"], `"`) {
		t.Fatalf("XML ETag not quoted: %q", xml["ETag"])
	}
	if xml["ETag"] != `"`+md5HexFromB64(t, m.MD5Hash)+`"` {
		t.Errorf("XML ETag = %q, want quoted hex of %q", xml["ETag"], m.MD5Hash)
	}
	if got := mediaHeaders(m, false)["ETag"]; got != "CAE=" {
		t.Errorf("JSON ETag = %q, want CAE=", got)
	}
	// No stored MD5 (composite objects): fall back to the quoted constant etag.
	noMD5 := gcsObjectMetaWithMD5("hello")
	noMD5.MD5Hash = ""
	if got := mediaHeaders(noMD5, true)["ETag"]; got != `"CAE="` {
		t.Errorf(`XML ETag without md5 = %q, want "CAE="`, got)
	}
}

// TestObjectsInsertXMLAPIHeaders verifies an XML API PUT attaches the object's
// response headers (J39) while the JSON API insert attaches none.
func TestObjectsInsertXMLAPIHeaders(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	// JSON API insert: no XML headers attached.
	jsonNr := bucketParamsWithObj("bkt", "json.txt")
	jsonNr.Params[wire.MediaKey] = []byte("hello")
	if resp, err := p.ObjectsInsert(ctx, jsonNr); err != nil {
		t.Fatalf("json insert: %v", err)
	} else if _, ok := resp.Data[wire.HeadersKey]; ok {
		t.Error("JSON insert must not attach XML response headers")
	}

	// XML API insert: header set present and correct.
	nr = bucketParamsWithObj("bkt", "xml.txt")
	nr.Params[wire.MediaKey] = []byte("hello")
	nr.Params[wire.XMLAPIKey] = true
	resp, err := p.ObjectsInsert(ctx, nr)
	if err != nil {
		t.Fatalf("xml insert: %v", err)
	}
	hdr, _ := resp.Data[wire.HeadersKey].(map[string]string)
	if hdr == nil {
		t.Fatal("expected XML API response headers")
	}
	md5b64, _ := resp.Data["md5Hash"].(string)
	if hdr["ETag"] != `"`+md5HexFromB64(t, md5b64)+`"` {
		t.Errorf("ETag = %q, want quoted hex of %q", hdr["ETag"], md5b64)
	}
	if hdr["x-goog-generation"] == "" || hdr["x-goog-generation"] != resp.Data["generation"] {
		t.Errorf("x-goog-generation = %q, generation = %v", hdr["x-goog-generation"], resp.Data["generation"])
	}
	if hdr["x-goog-stored-content-length"] != "5" {
		t.Errorf("x-goog-stored-content-length = %q, want 5", hdr["x-goog-stored-content-length"])
	}
	if !strings.Contains(hdr["x-goog-hash"], "crc32c=") || !strings.Contains(hdr["x-goog-hash"], "md5=") {
		t.Errorf("x-goog-hash = %q, want crc32c and md5", hdr["x-goog-hash"])
	}
}

// TestObjectsGetMediaXMLEtag verifies raw XML media downloads carry the quoted
// hex-MD5 ETag while JSON/download-path media reads keep the constant etag.
func TestObjectsGetMediaXMLEtag(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	insertTestObject(t, p, "bkt", "obj.txt", "text/plain", nil)

	nr := bucketParamsWithObj("bkt", "obj.txt")
	nr.Params[wire.XMLAPIKey] = true
	media, err := p.ObjectsGetMedia(ctx, nr)
	if err != nil {
		t.Fatalf("xml media: %v", err)
	}
	xmlHdr, _ := media.Data[wire.HeadersKey].(map[string]string)
	if xmlHdr == nil || !strings.HasPrefix(xmlHdr["ETag"], `"`) {
		t.Fatalf("expected quoted XML ETag, got %v", xmlHdr)
	}

	jsonMedia, err := p.ObjectsGetMedia(ctx, bucketParamsWithObj("bkt", "obj.txt"))
	if err != nil {
		t.Fatalf("json media: %v", err)
	}
	jsonHdr, _ := jsonMedia.Data[wire.HeadersKey].(map[string]string)
	if jsonHdr == nil || jsonHdr["ETag"] != "CAE=" {
		t.Fatalf("expected constant JSON ETag, got %v", jsonHdr)
	}
}
