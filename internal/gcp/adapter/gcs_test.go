package gcp

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"encoding/json"

	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// TestGCSCodecRawMediaMarksXMLAPI verifies requests on the raw XML path
// /{bucket}/{object} carry wire.XMLAPIKey (so reads/writes answer in the XML
// wire shape) while JSON API uploads do not.
func TestGCSCodecRawMediaMarksXMLAPI(t *testing.T) {
	c := &GCSCodec{}
	for _, method := range []string{"PUT", "GET", "HEAD"} {
		r := httptest.NewRequest(method, "/bkt/obj.txt", nil)
		nr, err := c.Decode(r, nil)
		if err != nil {
			t.Fatalf("%s decode: %v", method, err)
		}
		if v, _ := nr.Params[wire.XMLAPIKey].(bool); !v {
			t.Errorf("%s: expected wire.XMLAPIKey set", method)
		}
	}
	// A signed-URL raw PUT is likewise an XML API request (and carries the
	// signed-URL marker).
	sr := httptest.NewRequest("PUT", "/bkt/obj.txt?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=x&X-Goog-Date=20260930T160000Z&X-Goog-Expires=3600&X-Goog-Signature=abc", nil)
	snr, err := c.Decode(sr, nil)
	if err != nil {
		t.Fatalf("signed decode: %v", err)
	}
	if v, _ := snr.Params[wire.XMLAPIKey].(bool); !v {
		t.Error("signed PUT: expected wire.XMLAPIKey set")
	}
	if v, _ := snr.Params[wire.SignedURLKey].(bool); !v {
		t.Error("signed PUT: expected wire.SignedURLKey set")
	}
	// A JSON API media insert must NOT be marked as an XML request.
	r := httptest.NewRequest("POST", "/upload/storage/v1/b/bkt/o?uploadType=media&name=obj", strings.NewReader("x"))
	nr, err := c.Decode(r, []byte("x"))
	if err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if v, _ := nr.Params[wire.XMLAPIKey].(bool); v {
		t.Error("JSON API insert must not be marked XML")
	}
}

// TestGCSCodecEncodeXMLPutEmptyBody verifies an XML API PUT Object response is
// 200 with an empty body plus the object's ETag/generation/hash headers the
// provider attached — never the JSON storage#object body.
func TestGCSCodecEncodeXMLPutEmptyBody(t *testing.T) {
	c := &GCSCodec{}
	nr := &model.NormalizedRequest{Action: "ObjectsInsert", Params: map[string]any{wire.XMLAPIKey: true}}
	resp := &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"kind": "storage#object",
			"name": "obj.txt",
			wire.HeadersKey: map[string]string{
				"ETag":                  `"881f7881ac1bc144a2672e45babb8839"`,
				"x-goog-generation":     "123",
				"x-goog-metageneration": "1",
				"x-goog-hash":           "crc32c=abc,md5=def",
			},
		},
	}
	status, hdr, body := c.Encode(nr, resp)
	if status != 200 {
		t.Fatalf("expected 200, got %d", status)
	}
	if len(body) != 0 {
		t.Fatalf("expected empty body, got %q", body)
	}
	if got := hdr.Get("ETag"); got != `"881f7881ac1bc144a2672e45babb8839"` {
		t.Errorf("ETag = %q", got)
	}
	if got := hdr.Get("x-goog-generation"); got != "123" {
		t.Errorf("x-goog-generation = %q", got)
	}
	if got := hdr.Get("x-goog-hash"); got != "crc32c=abc,md5=def" {
		t.Errorf("x-goog-hash = %q", got)
	}
	if ct := hdr.Get("Content-Type"); !strings.HasPrefix(ct, "application/xml") {
		t.Errorf("Content-Type = %q, want application/xml", ct)
	}
}

// TestGCSCodecEncodeJSONInsertKeepsObject verifies the JSON API insert path still
// returns the storage#object body — only the XML path is reshaped.
func TestGCSCodecEncodeJSONInsertKeepsObject(t *testing.T) {
	c := &GCSCodec{}
	nr := &model.NormalizedRequest{Action: "ObjectsInsert", Params: map[string]any{}}
	resp := &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"kind": "storage#object", "name": "obj.txt"},
	}
	status, hdr, body := c.Encode(nr, resp)
	if status != 200 {
		t.Fatalf("expected 200, got %d", status)
	}
	if !strings.Contains(string(body), "storage#object") {
		t.Errorf("JSON insert should keep the object body, got %q", body)
	}
	if ct := hdr.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}

func TestGCSCodecDownloadForcesMedia(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("GET", "/download/storage/v1/b/bkt/o/obj", nil)
	nr, err := c.Decode(r, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsGetMedia" {
		t.Errorf("expected ObjectsGetMedia, got %q", nr.Action)
	}
}

func TestGCSCodecResumableStartCapturesContentType(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("POST", "/upload/storage/v1/b/bkt/o?uploadType=resumable&name=obj", nil)
	r.Header.Set("X-Upload-Content-Type", "text/plain")
	nr, err := c.Decode(r, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsInsertStartResumable" {
		t.Fatalf("expected ObjectsInsertStartResumable, got %q", nr.Action)
	}
	if ct, _ := nr.Params[wire.ContentTypeKey].(string); ct != "text/plain" {
		t.Errorf("expected content type text/plain, got %q", ct)
	}
}

func TestGCSCodecResumablePutNoContentTypeOverride(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("PUT", "/upload/storage/v1/b/bkt/o?uploadType=resumable&upload_id=1", nil)
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("Content-Range", "bytes 0-4/*")
	nr, err := c.Decode(r, []byte("chunk"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := nr.Params[wire.ContentTypeKey]; ok {
		t.Error("resumable PUT must not set object content type (chunk Content-Type is the media type)")
	}
	if cr, _ := nr.Params["contentRange"].(string); cr != "bytes 0-4/*" {
		t.Errorf("expected contentRange bytes 0-4/*, got %q", cr)
	}
}

func TestGCSCodecResumablePostChunk(t *testing.T) {
	// The Go SDK uploads chunks with POST (not PUT); a request carrying
	// upload_id must be treated as a chunk upload, not a session start.
	c := &GCSCodec{}
	r := httptest.NewRequest("POST", "/upload/storage/v1/b/bkt/o?uploadType=resumable&upload_id=42", nil)
	r.Header.Set("Content-Range", "bytes 0-4/*")
	r.Header.Set("X-GUploader-No-308", "yes")
	nr, err := c.Decode(r, []byte("chunk"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsInsertResumable" {
		t.Fatalf("expected ObjectsInsertResumable, got %q", nr.Action)
	}
	if no308, _ := nr.Params[wire.No308Key].(bool); !no308 {
		t.Error("expected No308Key to be set from X-GUploader-No-308 header")
	}
}

func TestGCSCodecRawMediaPath(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("GET", "/bkt/dir/obj.txt", nil)
	nr, err := c.Decode(r, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsGetMedia" {
		t.Fatalf("expected ObjectsGetMedia, got %q", nr.Action)
	}
	if b, _ := nr.Params["bucket"].(string); b != "bkt" {
		t.Errorf("expected bucket bkt, got %q", b)
	}
	if o, _ := nr.Params["object"].(string); o != "dir/obj.txt" {
		t.Errorf("expected object dir/obj.txt, got %q", o)
	}
}

func TestGCSCodecObjectUpdateVsPatch(t *testing.T) {
	c := &GCSCodec{}
	body := []byte(`{"contentType":"application/json"}`)

	// PUT → ObjectsUpdate (strict replacement semantics).
	r := httptest.NewRequest("PUT", "/storage/v1/b/bkt/o/obj.txt", nil)
	nr, err := c.Decode(r, body)
	if err != nil {
		t.Fatalf("decode PUT: %v", err)
	}
	if nr.Action != "ObjectsUpdate" {
		t.Fatalf("expected ObjectsUpdate for PUT, got %q", nr.Action)
	}

	// PATCH → ObjectsPatch (merge semantics).
	r = httptest.NewRequest("PATCH", "/storage/v1/b/bkt/o/obj.txt", nil)
	nr, err = c.Decode(r, body)
	if err != nil {
		t.Fatalf("decode PATCH: %v", err)
	}
	if nr.Action != "ObjectsPatch" {
		t.Fatalf("expected ObjectsPatch for PATCH, got %q", nr.Action)
	}
}

func TestGCSCodecObjectAclPath(t *testing.T) {
	c := &GCSCodec{}

	r := httptest.NewRequest("GET", "/storage/v1/b/bkt/o/dir/obj.txt/acl", nil)
	nr, err := c.Decode(r, nil)
	if err != nil {
		t.Fatalf("decode GET acl: %v", err)
	}
	if nr.Action != "ObjectACLList" {
		t.Fatalf("expected ObjectACLList, got %q", nr.Action)
	}
	if o, _ := nr.Params["object"].(string); o != "dir/obj.txt" {
		t.Errorf("expected object dir/obj.txt, got %q", o)
	}

	r = httptest.NewRequest("POST", "/storage/v1/b/bkt/o/dir/obj.txt/acl", nil)
	nr, err = c.Decode(r, []byte(`{"entity":"allUsers","role":"READER"}`))
	if err != nil {
		t.Fatalf("decode POST acl: %v", err)
	}
	if nr.Action != "ObjectACLInsert" {
		t.Fatalf("expected ObjectACLInsert, got %q", nr.Action)
	}
}

func TestGCSCodecObjectIamPath(t *testing.T) {
	c := &GCSCodec{}

	r := httptest.NewRequest("GET", "/storage/v1/b/bkt/o/dir/obj.txt/iam", nil)
	nr, err := c.Decode(r, nil)
	if err != nil {
		t.Fatalf("decode GET iam: %v", err)
	}
	if nr.Action != "ObjectsGetIamPolicy" {
		t.Fatalf("expected ObjectsGetIamPolicy, got %q", nr.Action)
	}
	if o, _ := nr.Params["object"].(string); o != "dir/obj.txt" {
		t.Errorf("expected object dir/obj.txt, got %q", o)
	}

	r = httptest.NewRequest("PUT", "/storage/v1/b/bkt/o/dir/obj.txt/iam", nil)
	nr, err = c.Decode(r, []byte(`{"bindings":[]}`))
	if err != nil {
		t.Fatalf("decode PUT iam: %v", err)
	}
	if nr.Action != "ObjectsSetIamPolicy" {
		t.Fatalf("expected ObjectsSetIamPolicy, got %q", nr.Action)
	}
}

func TestSplitEscapedPreservesEncodedSlash(t *testing.T) {
	// %2F within a segment (a slash in an object name) survives as part of the name.
	seg := splitEscaped("/storage/v1/b/bkt/o/a%2Fb/c")
	if len(seg) != 7 || seg[5] != "a/b" {
		t.Errorf("expected object name 'a/b' at seg[5], got %v", seg)
	}
}

func TestGCSCodecEncodeError(t *testing.T) {
	c := &GCSCodec{}
	perr := model.NewProviderError("NotFound", "object not found", 404)
	status, _, body := c.EncodeError(nil, perr)
	if status != 404 {
		t.Fatalf("expected status 404, got %d", status)
	}
	var env map[string]any
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	errObj, _ := env["error"].(map[string]any)
	errorsArr, ok := errObj["errors"].([]any)
	if !ok || len(errorsArr) == 0 {
		t.Fatal("expected non-empty errors[] array in GCS error envelope")
	}
	first := errorsArr[0].(map[string]any)
	if first["reason"] != "notFound" {
		t.Errorf("expected reason notFound, got %v", first["reason"])
	}
	if _, ok := errObj["status"]; ok {
		t.Error("GCS error envelope must not contain a 'status' field")
	}
}

// TestGCSCodecEncodeErrorReason verifies a provider-supplied documented reason
// (e.g. the CSEK customerEncryption* reasons) wins over the canonical-code
// mapping in the JSON error envelope.
func TestGCSCodecEncodeErrorReason(t *testing.T) {
	c := &GCSCodec{}
	perr := model.NewProviderError("InvalidArgument",
		"the resource is encrypted with a customer-supplied encryption key", 400).
		WithData(map[string]any{"reason": "resourceIsEncryptedWithCustomerEncryptionKey"})
	status, hdr, body := c.EncodeError(nil, perr)
	if status != 400 {
		t.Fatalf("expected 400, got %d", status)
	}
	if ct := hdr.Get("Content-Type"); ct != "application/json; charset=UTF-8" {
		t.Fatalf("expected json content type, got %q", ct)
	}
	var env map[string]any
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	errObj, _ := env["error"].(map[string]any)
	errorsArr, _ := errObj["errors"].([]any)
	if len(errorsArr) == 0 {
		t.Fatal("expected errors[] array")
	}
	first, _ := errorsArr[0].(map[string]any)
	if first["reason"] != "resourceIsEncryptedWithCustomerEncryptionKey" {
		t.Errorf("reason = %v, want resourceIsEncryptedWithCustomerEncryptionKey", first["reason"])
	}
}

func TestGCSCodecRewriteRouting(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("POST", "/storage/v1/b/srcbkt/o/dir/src.txt/rewriteTo/b/dstbkt/o/dir/dst.txt", nil)
	nr, err := c.Decode(r, []byte(`{"contentType":"text/plain"}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsRewrite" {
		t.Fatalf("expected ObjectsRewrite, got %q", nr.Action)
	}
	if b, _ := nr.Params["sourceBucket"].(string); b != "srcbkt" {
		t.Errorf("expected sourceBucket srcbkt, got %q", b)
	}
	if o, _ := nr.Params["sourceObject"].(string); o != "dir/src.txt" {
		t.Errorf("expected sourceObject dir/src.txt, got %q", o)
	}
	if b, _ := nr.Params["destinationBucket"].(string); b != "dstbkt" {
		t.Errorf("expected destinationBucket dstbkt, got %q", b)
	}
	if o, _ := nr.Params["destinationObject"].(string); o != "dir/dst.txt" {
		t.Errorf("expected destinationObject dir/dst.txt, got %q", o)
	}
	if _, ok := nr.Params["body"].(map[string]any); !ok {
		t.Error("expected rewrite request body to be parsed")
	}
}

func TestGCSCodecComposeRouting(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("POST", "/storage/v1/b/bkt/o/dir/dst.txt/compose", nil)
	nr, err := c.Decode(r, []byte(`{"sourceObjects":[{"name":"a.txt"}]}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsCompose" {
		t.Fatalf("expected ObjectsCompose, got %q", nr.Action)
	}
	if b, _ := nr.Params["bucket"].(string); b != "bkt" {
		t.Errorf("expected bucket bkt, got %q", b)
	}
	if o, _ := nr.Params["object"].(string); o != "dir/dst.txt" {
		t.Errorf("expected object dir/dst.txt, got %q", o)
	}
}

func TestGCSCodecMetadataHeaders(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("POST", "/upload/storage/v1/b/bkt/o?uploadType=media&name=obj", nil)
	r.Header.Set("x-goog-meta-originalname", "file.dat")
	r.Header.Set("x-goog-meta-env", "test")
	nr, err := c.Decode(r, []byte("bytes"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	md, _ := nr.Params[wire.MetaHeadersKey].(map[string]string)
	if md["originalname"] != "file.dat" || md["env"] != "test" {
		t.Fatalf("expected metadata headers captured, got %v", md)
	}
}

func TestGCSCodecEncodeForwardsHeaders(t *testing.T) {
	c := &GCSCodec{}
	nr := &model.NormalizedRequest{}
	resp := &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"_stream":           io.NopCloser(strings.NewReader("x")),
			wire.HeadersKey:     map[string]string{"x-goog-generation": "123", "x-goog-meta-Foo": "bar"},
			wire.ContentTypeKey: "text/plain",
		},
	}
	status, hdr, _ := c.Encode(nr, resp)
	if status != 200 {
		t.Fatalf("expected 200, got %d", status)
	}
	if hdr.Get("x-goog-generation") != "123" {
		t.Errorf("expected x-goog-generation header, got %q", hdr.Get("x-goog-generation"))
	}
	if hdr.Get("x-goog-meta-Foo") != "bar" {
		t.Errorf("expected x-goog-meta-Foo header, got %q", hdr.Get("x-goog-meta-Foo"))
	}
}

func TestGCSCodecCopyToRouting(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("POST", "/storage/v1/b/srcbkt/o/dir/src.txt/copyTo/b/dstbkt/o/dir/dst.txt", nil)
	nr, err := c.Decode(r, []byte(`{"contentType":"text/plain"}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsCopy" {
		t.Fatalf("expected ObjectsCopy, got %q", nr.Action)
	}
	if b, _ := nr.Params["sourceBucket"].(string); b != "srcbkt" {
		t.Errorf("expected sourceBucket srcbkt, got %q", b)
	}
	if o, _ := nr.Params["sourceObject"].(string); o != "dir/src.txt" {
		t.Errorf("expected sourceObject dir/src.txt, got %q", o)
	}
	if b, _ := nr.Params["destinationBucket"].(string); b != "dstbkt" {
		t.Errorf("expected destinationBucket dstbkt, got %q", b)
	}
	if o, _ := nr.Params["destinationObject"].(string); o != "dir/dst.txt" {
		t.Errorf("expected destinationObject dir/dst.txt, got %q", o)
	}
	if _, ok := nr.Params["body"].(map[string]any); !ok {
		t.Error("expected copy request body to be parsed")
	}
}

func TestGCSCodecRestoreRouting(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("POST", "/storage/v1/b/bkt/o/dir/obj.txt/restore?generation=1234", nil)
	nr, err := c.Decode(r, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsRestore" {
		t.Fatalf("expected ObjectsRestore, got %q", nr.Action)
	}
	if b, _ := nr.Params["bucket"].(string); b != "bkt" {
		t.Errorf("expected bucket bkt, got %q", b)
	}
	if o, _ := nr.Params["object"].(string); o != "dir/obj.txt" {
		t.Errorf("expected object dir/obj.txt, got %q", o)
	}
	if g, _ := nr.Params["generation"].(string); g != "1234" {
		t.Errorf("expected generation 1234, got %q", g)
	}
}

func TestGCSCodecMoveRoutingSameBucket(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("POST", "/storage/v1/b/bkt/o/dir/src.txt/moveTo/o/dir/dst.txt", nil)
	nr, err := c.Decode(r, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsMove" {
		t.Fatalf("expected ObjectsMove, got %q", nr.Action)
	}
	if b, _ := nr.Params["sourceBucket"].(string); b != "bkt" {
		t.Errorf("expected sourceBucket bkt, got %q", b)
	}
	if o, _ := nr.Params["sourceObject"].(string); o != "dir/src.txt" {
		t.Errorf("expected sourceObject dir/src.txt, got %q", o)
	}
	if b, _ := nr.Params["destinationBucket"].(string); b != "bkt" {
		t.Errorf("expected destinationBucket bkt, got %q", b)
	}
	if o, _ := nr.Params["destinationObject"].(string); o != "dir/dst.txt" {
		t.Errorf("expected destinationObject dir/dst.txt, got %q", o)
	}
}

func TestGCSCodecMoveRoutingCrossBucket(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("POST", "/storage/v1/b/srcbkt/o/src.txt/moveTo/b/dstbkt/o/dst.txt", nil)
	nr, err := c.Decode(r, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsMove" {
		t.Fatalf("expected ObjectsMove, got %q", nr.Action)
	}
	if b, _ := nr.Params["sourceBucket"].(string); b != "srcbkt" {
		t.Errorf("expected sourceBucket srcbkt, got %q", b)
	}
	if o, _ := nr.Params["sourceObject"].(string); o != "src.txt" {
		t.Errorf("expected sourceObject src.txt, got %q", o)
	}
	if b, _ := nr.Params["destinationBucket"].(string); b != "dstbkt" {
		t.Errorf("expected destinationBucket dstbkt, got %q", b)
	}
	if o, _ := nr.Params["destinationObject"].(string); o != "dst.txt" {
		t.Errorf("expected destinationObject dst.txt, got %q", o)
	}
}

func TestGCSCodecLockRetentionPolicyRouting(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("POST", "/storage/v1/b/bkt/lockRetentionPolicy?ifMetagenerationMatch=1", nil)
	nr, err := c.Decode(r, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "BucketsLockRetentionPolicy" {
		t.Fatalf("expected BucketsLockRetentionPolicy, got %q", nr.Action)
	}
	if b, _ := nr.Params["bucket"].(string); b != "bkt" {
		t.Errorf("expected bucket bkt, got %q", b)
	}
	if m, _ := nr.Params["ifMetagenerationMatch"].(string); m != "1" {
		t.Errorf("expected ifMetagenerationMatch 1, got %q", m)
	}
}

func TestGCSCodecResumableUnknownEndContentRange(t *testing.T) {
	// "bytes 0-*/*" (terminal unknown-end chunk) must pass through verbatim.
	c := &GCSCodec{}
	r := httptest.NewRequest("PUT", "/upload/storage/v1/b/bkt/o?uploadType=resumable&upload_id=1", nil)
	r.Header.Set("Content-Range", "bytes 0-*/*")
	nr, err := c.Decode(r, []byte("hello"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsInsertResumable" {
		t.Fatalf("expected ObjectsInsertResumable, got %q", nr.Action)
	}
	if cr, _ := nr.Params["contentRange"].(string); cr != "bytes 0-*/*" {
		t.Errorf("expected contentRange bytes 0-*/*, got %q", cr)
	}
	if _, ok := nr.Params[wire.MediaKey].([]byte); !ok {
		t.Error("expected media bytes captured")
	}
}

func TestGCSCodecStoragePathResumableChunk(t *testing.T) {
	// A rewritten session URI on the JSON path must decode as a chunk.
	c := &GCSCodec{}
	r := httptest.NewRequest("PUT", "/storage/v1/b/bkt/o?uploadType=resumable&upload_id=7", nil)
	r.Header.Set("Content-Range", "bytes 0-4/*")
	nr, err := c.Decode(r, []byte("hello"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsInsertResumable" {
		t.Fatalf("expected ObjectsInsertResumable, got %q", nr.Action)
	}
	if cr, _ := nr.Params["contentRange"].(string); cr != "bytes 0-4/*" {
		t.Errorf("expected contentRange bytes 0-4/*, got %q", cr)
	}
	if b, _ := nr.Params["bucket"].(string); b != "bkt" {
		t.Errorf("expected bucket bkt, got %q", b)
	}
}

func TestGCSCodecStoragePathResumableStart(t *testing.T) {
	// Initiation on the JSON path (emulator accommodation) decodes to start.
	c := &GCSCodec{}
	r := httptest.NewRequest("POST", "/storage/v1/b/bkt/o?uploadType=resumable&name=obj", nil)
	nr, err := c.Decode(r, []byte(`{"name":"obj"}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsInsertStartResumable" {
		t.Fatalf("expected ObjectsInsertStartResumable, got %q", nr.Action)
	}
	if b, _ := nr.Params["bucket"].(string); b != "bkt" {
		t.Errorf("expected bucket bkt, got %q", b)
	}
	if o, _ := nr.Params["object"].(string); o != "obj" {
		t.Errorf("expected object obj, got %q", o)
	}
}

func TestGCSCodecResumableUploadPathStart(t *testing.T) {
	// Discovery resumable-initiation path /resumable/upload/storage/v1/...
	c := &GCSCodec{}
	r := httptest.NewRequest("POST", "/resumable/upload/storage/v1/b/bkt/o?uploadType=resumable&name=obj", nil)
	nr, err := c.Decode(r, []byte(`{"name":"obj"}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsInsertStartResumable" {
		t.Fatalf("expected ObjectsInsertStartResumable, got %q", nr.Action)
	}
}

func TestGCSCodecResumableUploadPathChunk(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("PUT", "/resumable/upload/storage/v1/b/bkt/o?uploadType=resumable&upload_id=9", nil)
	r.Header.Set("Content-Range", "bytes 0-3/*")
	nr, err := c.Decode(r, []byte("data"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsInsertResumable" {
		t.Fatalf("expected ObjectsInsertResumable, got %q", nr.Action)
	}
}

func TestDetectServiceResumableUploadPath(t *testing.T) {
	r := httptest.NewRequest("POST", "/resumable/upload/storage/v1/b/bkt/o?uploadType=resumable", nil)
	svc, _ := DetectService(r)
	if svc != "storage" {
		t.Fatalf("expected storage for /resumable/upload/..., got %q", svc)
	}
}

func TestGCSCodecSignedGetDetected(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("GET", "/bkt/obj.txt?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Signature=abc&X-Goog-Expires=300&X-Goog-Date=20260913T000000Z", nil)
	nr, err := c.Decode(r, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsGetMedia" {
		t.Fatalf("expected ObjectsGetMedia, got %q", nr.Action)
	}
	if signed, _ := nr.Params[wire.SignedURLKey].(bool); !signed {
		t.Error("expected SignedURLKey set for signed GET")
	}
}

func TestGCSCodecSignedPutDetected(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("PUT", "/bkt/obj.txt?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Signature=abc&X-Goog-Expires=300&X-Goog-Date=20260913T000000Z", nil)
	r.Header.Set("Content-Type", "text/plain")
	nr, err := c.Decode(r, []byte("data"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsInsert" {
		t.Fatalf("expected ObjectsInsert, got %q", nr.Action)
	}
	if signed, _ := nr.Params[wire.SignedURLKey].(bool); !signed {
		t.Error("expected SignedURLKey set for signed PUT")
	}
	if b, _ := nr.Params[wire.MediaKey].([]byte); string(b) != "data" {
		t.Errorf("expected media data, got %q", b)
	}
}

func TestDetectServiceSignedPut(t *testing.T) {
	r := httptest.NewRequest("PUT", "/bkt/obj.txt?X-Goog-Signature=abc", nil)
	svc, _ := DetectService(r)
	if svc != "storage" {
		t.Fatalf("expected storage for signed PUT, got %q", svc)
	}
}

func TestGCSCodecEncodeOverrideWithoutRange(t *testing.T) {
	c := &GCSCodec{}
	nr := &model.NormalizedRequest{}
	resp := &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{wire.StatusOverrideKey: "308"},
	}
	status, hdr, body := c.Encode(nr, resp)
	if status != 200 {
		t.Fatalf("expected 200, got %d", status)
	}
	if hdr.Get("X-Http-Status-Code-Override") != "308" {
		t.Fatalf("expected X-Http-Status-Code-Override 308, got %q", hdr.Get("X-Http-Status-Code-Override"))
	}
	if hdr.Get("Range") != "" {
		t.Fatalf("expected no Range header, got %q", hdr.Get("Range"))
	}
	if len(body) != 0 {
		t.Fatalf("expected empty body, got %q", body)
	}
}

func TestGCSCodecEncodePlain308EmptyBody(t *testing.T) {
	c := &GCSCodec{}
	nr := &model.NormalizedRequest{}
	resp := &model.ProviderResponse{HTTPStatus: 308, Data: map[string]any{}}
	status, hdr, body := c.Encode(nr, resp)
	if status != 308 {
		t.Fatalf("expected 308, got %d", status)
	}
	if hdr.Get("Range") != "" {
		t.Fatalf("expected no Range, got %q", hdr.Get("Range"))
	}
	if len(body) != 0 {
		t.Fatalf("expected empty body, got %q", body)
	}
}

func TestGCSCodecEncodeErrorPlain(t *testing.T) {
	c := &GCSCodec{}
	perr := model.NewProviderError("InvalidRequest", "Invalid request.  offset", 503).
		WithData(map[string]any{"errorFormat": "plain"})
	status, hdr, body := c.EncodeError(nil, perr)
	if status != 503 {
		t.Fatalf("expected 503, got %d", status)
	}
	if ct := hdr.Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Fatalf("expected text/plain, got %q", ct)
	}
	if string(body) != "Invalid request.  offset" {
		t.Fatalf("expected exact plain body, got %q", body)
	}
}

func TestGCSCodecEncodeErrorXML(t *testing.T) {
	c := &GCSCodec{}
	perr := model.NewProviderError("ExpiredToken", "The provided token has expired.", 400).
		WithData(map[string]any{"errorFormat": "xml"})
	status, hdr, body := c.EncodeError(nil, perr)
	if status != 400 {
		t.Fatalf("expected 400, got %d", status)
	}
	if ct := hdr.Get("Content-Type"); ct != "application/xml" {
		t.Fatalf("expected application/xml, got %q", ct)
	}
	s := string(body)
	if !strings.Contains(s, "<Code>ExpiredToken</Code>") {
		t.Fatalf("expected ExpiredToken code, got %q", s)
	}
	if strings.Contains(s, "<ParameterName>") {
		t.Fatalf("expected no ParameterName, got %q", s)
	}

	perr = model.NewProviderError("MalformedSecurityHeader", "Your request has a malformed header.", 400).
		WithData(map[string]any{"errorFormat": "xml", "parameterName": "Expires"})
	_, _, body = c.EncodeError(nil, perr)
	if s := string(body); !strings.Contains(s, "<ParameterName>Expires</ParameterName>") {
		t.Fatalf("expected ParameterName Expires, got %q", s)
	}
}

func TestGCSCodecEncodeErrorXMLAPI(t *testing.T) {
	c := &GCSCodec{}
	xmlReq := func(method string) *model.NormalizedRequest {
		r := httptest.NewRequest(method, "/bkt/obj.txt", nil)
		return &model.NormalizedRequest{Params: map[string]any{wire.XMLAPIKey: true}, Raw: r}
	}

	// Missing object on the raw path → NoSuchKey XML document.
	status, hdr, body := c.EncodeError(xmlReq("GET"),
		model.NewProviderError("NotFound", "object not found", 404))
	if status != 404 {
		t.Fatalf("status = %d, want 404", status)
	}
	if ct := hdr.Get("Content-Type"); ct != "application/xml; charset=UTF-8" {
		t.Fatalf("Content-Type = %q, want application/xml; charset=UTF-8", ct)
	}
	if s := string(body); !strings.Contains(s, "<Code>NoSuchKey</Code>") {
		t.Fatalf("expected NoSuchKey, got %q", s)
	}

	// Missing bucket → NoSuchBucket (signalled by the provider's xmlCode).
	_, _, body = c.EncodeError(xmlReq("GET"),
		model.NewProviderError("NotFound", "bucket not found", 404).
			WithData(map[string]any{"xmlCode": "NoSuchBucket"}))
	if s := string(body); !strings.Contains(s, "<Code>NoSuchBucket</Code>") {
		t.Fatalf("expected NoSuchBucket, got %q", s)
	}

	// A CSEK failure maps the JSON reason to the capitalized XML code.
	_, _, body = c.EncodeError(xmlReq("GET"),
		model.NewProviderError("InvalidRequest", "bad key", 400).
			WithData(map[string]any{"reason": "customerEncryptionKeyIsIncorrect"}))
	if s := string(body); !strings.Contains(s, "<Code>CustomerEncryptionKeyIsIncorrect</Code>") {
		t.Fatalf("expected CustomerEncryptionKeyIsIncorrect, got %q", s)
	}

	// HEAD carries the status and Content-Type but no body.
	status, hdr, body = c.EncodeError(xmlReq("HEAD"),
		model.NewProviderError("NotFound", "object not found", 404))
	if status != 404 || len(body) != 0 {
		t.Fatalf("HEAD error: status=%d body=%q, want 404/empty", status, body)
	}
	if hdr.Get("Content-Type") == "" {
		t.Fatal("HEAD error should still set Content-Type")
	}
}

func TestGCSCodecParseMultipartSingleQuotedBoundary(t *testing.T) {
	// gcloud/apitools emits `boundary='...=='` (single-quoted, with tspecial '='
	// chars) — mime.ParseMediaType rejects this, so the boundary is extracted
	// manually.
	body := "--BND==\r\nContent-Type: application/json\r\n\r\n{\"name\":\"o.txt\"}\r\n--BND==\r\nContent-Type: text/plain\r\n\r\nhello\r\n--BND==--\r\n"
	r := httptest.NewRequest("POST", "/upload/storage/v1/b/bkt/o?uploadType=multipart", strings.NewReader(body))
	r.Header.Set("Content-Type", "multipart/related; boundary='BND=='")
	nr := &model.NormalizedRequest{Params: map[string]any{}}
	if err := parseMultipart(r, []byte(body), nr.Params); err != nil {
		t.Fatalf("parseMultipart: %v", err)
	}
	if m, _ := nr.Params["body"].(map[string]any); m == nil || m["name"] != "o.txt" {
		t.Fatalf("expected metadata name o.txt, got %v", m)
	}
	rc, ok := nr.Params[wire.StreamKey].(io.Reader)
	if !ok {
		t.Fatal("expected stream media part")
	}
	b, err := io.ReadAll(rc)
	if err != nil || string(b) != "hello" {
		t.Fatalf("expected media hello, got %q / %v", b, err)
	}
}

func TestGCSCodecStorageLayoutRouting(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("GET", "/storage/v1/b/bkt/storageLayout?alt=json", nil)
	nr, err := c.Decode(r, nil)
	if err != nil {
		t.Fatalf("decode storageLayout: %v", err)
	}
	if nr.Action != "BucketsGetStorageLayout" {
		t.Fatalf("action = %q, want BucketsGetStorageLayout", nr.Action)
	}
	if got, _ := nr.Params["bucket"].(string); got != "bkt" {
		t.Fatalf("bucket = %q, want bkt", got)
	}
}

func TestGCSCodecNotificationConfigsRouting(t *testing.T) {
	c := &GCSCodec{}

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantAction string
		wantNotif  string
	}{
		{"insert", "POST", "/storage/v1/b/bkt/notificationConfigs",
			`{"topic":"projects/p/topics/t"}`, "NotificationsInsert", ""},
		{"list", "GET", "/storage/v1/b/bkt/notificationConfigs", "", "NotificationsList", ""},
		{"get", "GET", "/storage/v1/b/bkt/notificationConfigs/42", "", "NotificationsGet", "42"},
		{"delete", "DELETE", "/storage/v1/b/bkt/notificationConfigs/42", "", "NotificationsDelete", "42"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var body []byte
			if tc.body != "" {
				body = []byte(tc.body)
			}
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			nr, err := c.Decode(r, body)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if nr.Action != tc.wantAction {
				t.Fatalf("action = %q, want %q", nr.Action, tc.wantAction)
			}
			if got, _ := nr.Params["bucket"].(string); got != "bkt" {
				t.Errorf("bucket = %q, want bkt", got)
			}
			if got, _ := nr.Params["notification"].(string); got != tc.wantNotif {
				t.Errorf("notification = %q, want %q", got, tc.wantNotif)
			}
		})
	}
}
