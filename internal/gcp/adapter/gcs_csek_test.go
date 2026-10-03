package gcp

import (
	"net/http/httptest"
	"testing"

	"jaiscloud/internal/gcp/wire"
)

// TestGCSCodecRawMediaPut verifies an unsigned XML API PUT /{bucket}/{object}
// decodes as an object insert (with CSEK material and metadata captured), not
// as an unsupported path. Real GCS supports plain XML PUT uploads; the emulator
// does not enforce auth, so the signed-URL restriction is lifted.
func TestGCSCodecRawMediaPut(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("PUT", "/bkt/dir/obj.txt", nil)
	r.Header.Set("Content-Type", "text/plain")
	r.Header.Set("x-goog-encryption-algorithm", "AES256")
	r.Header.Set("x-goog-encryption-key", "a2V5")
	r.Header.Set("x-goog-encryption-key-sha256", "c2hh")
	r.Header.Set("x-goog-meta-color", "blue")

	nr, err := c.Decode(r, []byte("payload"))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsInsert" {
		t.Fatalf("expected ObjectsInsert, got %q", nr.Action)
	}
	if b, _ := nr.Params["bucket"].(string); b != "bkt" {
		t.Errorf("expected bucket bkt, got %q", b)
	}
	if o, _ := nr.Params["object"].(string); o != "dir/obj.txt" {
		t.Errorf("expected object dir/obj.txt, got %q", o)
	}
	if m, _ := nr.Params[wire.MediaKey].([]byte); string(m) != "payload" {
		t.Errorf("expected media payload, got %q", m)
	}
	if ct, _ := nr.Params[wire.ContentTypeKey].(string); ct != "text/plain" {
		t.Errorf("expected content type text/plain, got %q", ct)
	}
	if alg, _ := nr.Params[wire.CSEKAlgorithm].(string); alg != "AES256" {
		t.Errorf("expected CSEK algorithm AES256, got %q", alg)
	}
	if k, _ := nr.Params[wire.CSEKKey].(string); k != "a2V5" {
		t.Errorf("expected CSEK key captured, got %q", k)
	}
	if sha, _ := nr.Params[wire.CSEKKeySHA256].(string); sha != "c2hh" {
		t.Errorf("expected CSEK sha captured, got %q", sha)
	}
	md, _ := nr.Params[wire.MetaHeadersKey].(map[string]string)
	if md["color"] != "blue" {
		t.Errorf("expected x-goog-meta-color captured, got %#v", md)
	}
}

// TestGCSCodecCopySourceCSEKHeaders verifies the copy-source CSEK headers are
// captured distinctly from the destination key headers on the rewrite path.
func TestGCSCodecCopySourceCSEKHeaders(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("POST", "/storage/v1/b/bkt/o/dst.txt/rewriteTo/b/bkt/o/dst2.txt", nil)
	r.Header.Set("x-goog-encryption-key", "ZK")
	r.Header.Set("x-goog-encryption-key-sha256", "ZS")
	r.Header.Set("x-goog-copy-source-encryption-algorithm", "AES256")
	r.Header.Set("x-goog-copy-source-encryption-key", "SK")
	r.Header.Set("x-goog-copy-source-encryption-key-sha256", "SS")

	nr, err := c.Decode(r, nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsRewrite" {
		t.Fatalf("expected ObjectsRewrite, got %q", nr.Action)
	}
	if alg, _ := nr.Params[wire.CopySourceCSEKAlgorithm].(string); alg != "AES256" {
		t.Errorf("expected copy-source algorithm AES256, got %q", alg)
	}
	if dk, _ := nr.Params[wire.CSEKKey].(string); dk != "ZK" {
		t.Errorf("expected destination key ZK, got %q", dk)
	}
	if sk, _ := nr.Params[wire.CopySourceCSEKKey].(string); sk != "SK" {
		t.Errorf("expected copy-source key SK, got %q", sk)
	}
	if ss, _ := nr.Params[wire.CopySourceCSEKKeySHA256].(string); ss != "SS" {
		t.Errorf("expected copy-source sha SS, got %q", ss)
	}
}

// TestGCSCodecRawMediaPutRejectsXMLVariants verifies the newly-allowed unsigned
// XML PUT does not silently swallow other XML API PUT operations (copy via
// x-goog-copy-source, compose, etc.) as plain object uploads.
func TestGCSCodecRawMediaPutRejectsXMLVariants(t *testing.T) {
	c := &GCSCodec{}

	r := httptest.NewRequest("PUT", "/bkt/dst.txt", nil)
	r.Header.Set("x-goog-copy-source", "/bkt/src.txt")
	if _, err := c.Decode(r, nil); err == nil {
		t.Error("expected error for XML copy PUT (x-goog-copy-source)")
	}

	for _, sub := range []string{"compose", "acl", "retention", "encryption", "uploadId"} {
		r = httptest.NewRequest("PUT", "/bkt/dst.txt?"+sub+"=1", nil)
		if _, err := c.Decode(r, nil); err == nil {
			t.Errorf("expected error for XML PUT with ?%s", sub)
		}
	}
}

// TestDetectServiceUnsignedRawPut verifies an unsigned raw PUT is routed to
// storage (previously only signed PUTs were).
func TestDetectServiceUnsignedRawPut(t *testing.T) {
	r := httptest.NewRequest("PUT", "/bkt/obj.txt", nil)
	svc, _ := DetectService(r)
	if svc != "storage" {
		t.Fatalf("expected storage for unsigned raw PUT, got %q", svc)
	}
}

// TestGCSCodecComposeCSEKHeaders verifies an objects.compose request routes to
// ObjectsCompose and captures the x-goog-encryption-* headers, which the
// provider applies to both the source reads and the destination write (J38).
func TestGCSCodecComposeCSEKHeaders(t *testing.T) {
	c := &GCSCodec{}
	r := httptest.NewRequest("POST", "/storage/v1/b/bkt/o/dst.txt/compose", nil)
	r.Header.Set("x-goog-encryption-algorithm", "AES256")
	r.Header.Set("x-goog-encryption-key", "CK")
	r.Header.Set("x-goog-encryption-key-sha256", "CS")

	nr, err := c.Decode(r, []byte(`{"sourceObjects":[{"name":"src.txt"}]}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Action != "ObjectsCompose" {
		t.Fatalf("expected ObjectsCompose, got %q", nr.Action)
	}
	if b, _ := nr.Params["bucket"].(string); b != "bkt" {
		t.Errorf("expected bucket bkt, got %q", b)
	}
	if o, _ := nr.Params["object"].(string); o != "dst.txt" {
		t.Errorf("expected object dst.txt, got %q", o)
	}
	if alg, _ := nr.Params[wire.CSEKAlgorithm].(string); alg != "AES256" {
		t.Errorf("expected CSEK algorithm AES256, got %q", alg)
	}
	if k, _ := nr.Params[wire.CSEKKey].(string); k != "CK" {
		t.Errorf("expected CSEK key CK, got %q", k)
	}
	if sha, _ := nr.Params[wire.CSEKKeySHA256].(string); sha != "CS" {
		t.Errorf("expected CSEK sha CS, got %q", sha)
	}
}
