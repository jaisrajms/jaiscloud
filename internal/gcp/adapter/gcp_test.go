package gcp_test

import (
	"bytes"
	"compress/gzip"
	"net/http/httptest"
	"strings"
	"testing"

	"jaiscloud/internal/gcp/adapter"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

func gzipBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func TestGCPAdapter_GzipJSONBody(t *testing.T) {
	a := gcp.New()
	payload := []byte(`{"name":"gzip-bucket"}`)
	r := httptest.NewRequest("POST", "/storage/v1/b?project=test-project", bytes.NewReader(payload))
	r.Header.Set("Content-Encoding", "gzip")

	nr, _, err := a.DetectAndDecode(r, gzipBytes(t, payload))
	if err != nil {
		t.Fatalf("DetectAndDecode: %v", err)
	}
	body, ok := nr.Params["body"].(map[string]any)
	if !ok {
		t.Fatalf("expected decoded JSON body, got %#v", nr.Params["body"])
	}
	if body["name"] != "gzip-bucket" {
		t.Errorf("expected name gzip-bucket, got %v", body["name"])
	}
	if got := r.Header.Get("Content-Encoding"); got != "" {
		t.Errorf("expected transport Content-Encoding cleared, got %q", got)
	}
}

func TestGCPAdapter_GzipMediaUploadNotDecoded(t *testing.T) {
	a := gcp.New()
	r := httptest.NewRequest("POST", "/upload/storage/v1/b/bkt/o?uploadType=media", nil)
	r.Header.Set("Content-Encoding", "gzip")

	// uploadType=media treats Content-Encoding as the object's own encoding, so
	// the body must be left untouched and the header preserved.
	_, _, _ = a.DetectAndDecode(r, gzipBytes(t, []byte("object bytes")))
	if got := r.Header.Get("Content-Encoding"); got != "gzip" {
		t.Errorf("expected media Content-Encoding preserved, got %q", got)
	}
}

func TestGCPAdapter_GzipMalformedBody(t *testing.T) {
	a := gcp.New()
	r := httptest.NewRequest("POST", "/storage/v1/b?project=test-project", nil)
	r.Header.Set("Content-Encoding", "gzip")

	_, _, err := a.DetectAndDecode(r, []byte("not actually gzip"))
	if err == nil {
		t.Fatal("expected malformed gzip error")
	}
	pe, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("expected *model.ProviderError, got %T (%v)", err, err)
	}
	if pe.HTTPStatus != 400 {
		t.Errorf("expected 400, got %d", pe.HTTPStatus)
	}
}

// TestGCPAdapter_XMLPutKeepsObjectEncoding covers J69: a raw XML API object PUT
// with Content-Encoding describes the object's own encoding, not the transport,
// so the bytes must be stored as-is and the header captured as contentEncoding.
func TestGCPAdapter_XMLPutKeepsObjectEncoding(t *testing.T) {
	a := gcp.New()
	gz := gzipBytes(t, []byte("object bytes"))
	r := httptest.NewRequest("PUT", "/bkt/obj.txt", bytes.NewReader(gz))
	r.Header.Set("Content-Encoding", "gzip")

	nr, _, err := a.DetectAndDecode(r, gz)
	if err != nil {
		t.Fatalf("DetectAndDecode: %v", err)
	}
	if got, _ := nr.Params["contentEncoding"].(string); got != "gzip" {
		t.Errorf("contentEncoding = %q, want gzip", got)
	}
	// The stored bytes must be the gzip body, not a transport-decoded plaintext.
	if got, _ := nr.Params[wire.MediaKey].([]byte); !bytes.Equal(got, gz) {
		t.Errorf("media bytes were altered: got %d bytes, want %d", len(got), len(gz))
	}
}

// TestGCPAdapter_GzipJSONPutNotMistakenForRawMedia guards the raw-media gzip
// exemption: a JSON API object PUT (/storage/v1/...) must still have its gzip
// transport body decoded, even though the path shape resembles /{a}/{b}.
func TestGCPAdapter_GzipJSONPutNotMistakenForRawMedia(t *testing.T) {
	a := gcp.New()
	payload := []byte(`{"contentType":"application/json"}`)
	r := httptest.NewRequest("PUT", "/storage/v1/b/bkt/o/obj.txt", bytes.NewReader(payload))
	r.Header.Set("Content-Encoding", "gzip")

	nr, _, err := a.DetectAndDecode(r, gzipBytes(t, payload))
	if err != nil {
		t.Fatalf("DetectAndDecode: %v", err)
	}
	body, ok := nr.Params["body"].(map[string]any)
	if !ok {
		t.Fatalf("expected decoded JSON body, got %#v", nr.Params["body"])
	}
	if body["contentType"] != "application/json" {
		t.Errorf("contentType = %v, want application/json", body["contentType"])
	}
}

func TestGCPAdapter_GzipStreamingMultipartWrapped(t *testing.T) {
	a := gcp.New()
	r := httptest.NewRequest("POST", "/upload/storage/v1/b/bkt/o?uploadType=multipart", bytes.NewReader(gzipBytes(t, []byte("x"))))
	r.Header.Set("Content-Encoding", "gzip")

	// The codec may reject the tiny body, but the transport must already be
	// unwrapped: header cleared and length unknown.
	_, _, _ = a.DetectAndDecode(r, nil)
	if got := r.Header.Get("Content-Encoding"); got != "" {
		t.Errorf("expected multipart Content-Encoding cleared, got %q", got)
	}
	if r.ContentLength != -1 {
		t.Errorf("expected ContentLength -1 for streamed body, got %d", r.ContentLength)
	}
}

func TestGCPAdapter_Cloud(t *testing.T) {
	a := gcp.New()
	if a.Cloud() != model.CloudGCP {
		t.Errorf("expected CloudGCP, got %s", a.Cloud())
	}
}

func TestGCPAdapter_DetectAndDecode_Storage(t *testing.T) {
	a := gcp.New()
	r := httptest.NewRequest("GET", "/storage/v1/b/my-bucket/o", nil)
	nr, codec, err := a.DetectAndDecode(r, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if nr.Service != "storage" {
		t.Errorf("expected service storage, got %q", nr.Service)
	}
	if nr.Action != "ObjectsList" {
		t.Errorf("expected action ObjectsList, got %q", nr.Action)
	}
	if codec == nil {
		t.Fatal("expected non-nil codec")
	}
	if got := a.ServiceToProvider("storage"); got != "Storage" {
		t.Errorf("expected provider prefix Storage, got %q", got)
	}
}

// TestGCPAdapter_MethodOverride verifies the Java google-http-client shape:
// a PATCH tunnelled as POST with X-HTTP-Method-Override must route to the
// merge (ObjectsPatch) handler, not the strict-replace ObjectsUpdate.
func TestGCPAdapter_MethodOverride(t *testing.T) {
	a := gcp.New()
	body := []byte(`{"metadata":{"tag":"keep-me"}}`)

	r := httptest.NewRequest("POST", "/storage/v1/b/bkt/o/obj.txt", bytes.NewReader(body))
	r.Header.Set("X-HTTP-Method-Override", "PATCH")
	nr, _, err := a.DetectAndDecode(r, body)
	if err != nil {
		t.Fatalf("DetectAndDecode: %v", err)
	}
	if nr.Action != "ObjectsPatch" {
		t.Fatalf("expected ObjectsPatch for overridden PATCH, got %q", nr.Action)
	}
	if r.Method != "PATCH" {
		t.Errorf("expected effective method PATCH, got %q", r.Method)
	}

	// Without the override header a POST on an object resource stays the
	// defensive objects.update route.
	r = httptest.NewRequest("POST", "/storage/v1/b/bkt/o/obj.txt", bytes.NewReader(body))
	nr, _, err = a.DetectAndDecode(r, body)
	if err != nil {
		t.Fatalf("DetectAndDecode (no override): %v", err)
	}
	if nr.Action != "ObjectsUpdate" {
		t.Fatalf("expected ObjectsUpdate for plain POST, got %q", nr.Action)
	}
}

func TestGCPAdapter_DetectAndDecode_Unknown(t *testing.T) {
	a := gcp.New()
	r := httptest.NewRequest("POST", "/", nil)
	_, _, err := a.DetectAndDecode(r, nil)
	if err == nil {
		t.Fatal("expected UnknownService error")
	}
	if !strings.Contains(err.Error(), "GCP") {
		t.Errorf("expected GCP detection error, got %v", err)
	}
}

// TestGCPAdapter_FunctionTriggerHost verifies the adapter selects the raw-HTTP
// trigger codec (not the control-plane JSON codec) when the request is
// addressed to a deployed function's HTTPS-trigger host.
func TestGCPAdapter_FunctionTriggerHost(t *testing.T) {
	a := gcp.New()
	r := httptest.NewRequest("POST", "/hello", strings.NewReader("payload"))
	r.Host = "us-central1-proj.cloudfunctions.net"

	nr, _, err := a.DetectAndDecode(r, []byte("payload"))
	if err != nil {
		t.Fatalf("DetectAndDecode: %v", err)
	}
	if nr.Service != "functions" || nr.Action != "InvokeTrigger" {
		t.Fatalf("got service=%q action=%q, want functions/InvokeTrigger", nr.Service, nr.Action)
	}
	if nr.Params["project"] != "proj" || nr.Params["location"] != "us-central1" || nr.Params["functionId"] != "hello" {
		t.Errorf("params = %v", nr.Params)
	}
	if nr.Params["payload"] != "payload" {
		t.Errorf("payload = %v, want payload", nr.Params["payload"])
	}
}
