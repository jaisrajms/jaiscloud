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
