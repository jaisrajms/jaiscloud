package storage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"testing"

	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// csekMaterial returns the base64 key and base64 SHA-256 for a 32-byte AES-256
// key, matching the x-goog-encryption-key / -key-sha256 header values.
func csekMaterial(key []byte) (keyB64, shaB64 string) {
	sum := sha256.Sum256(key)
	return base64.StdEncoding.EncodeToString(key), base64.StdEncoding.EncodeToString(sum[:])
}

// newCSEKBucket creates a bucket and an object encrypted with the given CSEK
// material, returning the provider and the object's key/SHA headers.
func newCSEKBucket(t *testing.T, p *Provider, keyB64, shaB64 string) {
	t.Helper()
	ctx := context.Background()

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "csek.txt"
	nr.Params[wire.MediaKey] = []byte("classified")
	nr.Params[wire.ContentTypeKey] = "text/plain"
	nr.Params[wire.CSEKAlgorithm] = "AES256"
	nr.Params[wire.CSEKKey] = keyB64
	nr.Params[wire.CSEKKeySHA256] = shaB64
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("insert csek object: %v", err)
	}
}

func TestGCSCSEKMetadataAndMedia(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	key := []byte("0123456789abcdef0123456789abcdef")
	keyB64, shaB64 := csekMaterial(key)
	newCSEKBucket(t, p, keyB64, shaB64)

	// Metadata surfaces the CSEK descriptor (JSON API customerEncryption).
	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "csek.txt"
	get, err := p.ObjectsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	ce, _ := get.Data["customerEncryption"].(map[string]any)
	if ce == nil {
		t.Fatalf("expected customerEncryption on CSEK metadata, got %#v", get.Data)
	}
	if alg, _ := ce["encryptionAlgorithm"].(string); alg != "AES256" {
		t.Errorf("expected AES256, got %q", alg)
	}
	if sha, _ := ce["keySha256"].(string); sha != shaB64 {
		t.Errorf("expected keySha256 %q, got %q", shaB64, sha)
	}

	// Read without the key → 400 (real GCS: resourceIsEncryptedWithCustomerEncryptionKey).
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "csek.txt"
	if _, err := p.ObjectsGetMedia(ctx, nr); err == nil {
		t.Fatal("expected error reading CSEK object without key")
	} else {
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
			t.Fatalf("expected 400 ProviderError, got %v", err)
		}
	}

	// Read with the key → original bytes, plus the XML CSEK response headers.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "csek.txt"
	nr.Params[wire.CSEKAlgorithm] = "AES256"
	nr.Params[wire.CSEKKey] = keyB64
	nr.Params[wire.CSEKKeySHA256] = shaB64
	media, err := p.ObjectsGetMedia(ctx, nr)
	if err != nil {
		t.Fatalf("get media with key: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "classified" {
		t.Fatalf("expected %q, got %q", "classified", got)
	}
	if hdr, _ := media.Data[wire.HeadersKey].(map[string]string); hdr != nil {
		if hdr["x-goog-encryption-algorithm"] != "AES256" {
			t.Errorf("expected x-goog-encryption-algorithm AES256, got %q", hdr["x-goog-encryption-algorithm"])
		}
		if hdr["x-goog-encryption-key-sha256"] != shaB64 {
			t.Errorf("expected x-goog-encryption-key-sha256 %q, got %q", shaB64, hdr["x-goog-encryption-key-sha256"])
		}
	} else {
		t.Error("expected CSEK response headers on media download")
	}
}

func TestGCSCSEKReadWrongSHARejected(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	keyB64, shaB64 := csekMaterial([]byte("0123456789abcdef0123456789abcdef"))
	newCSEKBucket(t, p, keyB64, shaB64)

	// Correct key but a sha256 that does not belong to it → 400.
	otherKey := []byte("fedcba9876543210fedcba9876543210")
	_, otherSHA := csekMaterial(otherKey)

	nr := bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "csek.txt"
	nr.Params[wire.CSEKAlgorithm] = "AES256"
	nr.Params[wire.CSEKKey] = keyB64
	nr.Params[wire.CSEKKeySHA256] = otherSHA
	if _, err := p.ObjectsGetMedia(ctx, nr); err == nil {
		t.Fatal("expected error for mismatched caller-supplied sha256")
	} else if got := csekErrorReason(t, err); got != csekReasonKeySha256Invalid {
		t.Fatalf("reason = %q, want %q", got, csekReasonKeySha256Invalid)
	}
}

func TestGCSCopySourceInvalidAlgorithmRejected(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	keyB64, shaB64 := csekMaterial([]byte("0123456789abcdef0123456789abcdef"))
	newCSEKBucket(t, p, keyB64, shaB64)

	nr := bucketParams()
	nr.Params["sourceBucket"] = "bkt"
	nr.Params["sourceObject"] = "csek.txt"
	nr.Params["destinationBucket"] = "bkt"
	nr.Params["destinationObject"] = "copy.txt"
	nr.Params[wire.CopySourceCSEKAlgorithm] = "AES128"
	nr.Params[wire.CopySourceCSEKKey] = keyB64
	nr.Params[wire.CopySourceCSEKKeySHA256] = shaB64
	if _, err := p.ObjectsCopy(ctx, nr); err == nil {
		t.Fatal("expected error for non-AES256 copy-source algorithm")
	} else if got := csekErrorReason(t, err); got != csekReasonAlgorithmInvalid {
		t.Fatalf("reason = %q, want %q", got, csekReasonAlgorithmInvalid)
	}
}

func TestGCSCSEKInvalidAlgorithmRejected(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	keyB64, shaB64 := csekMaterial([]byte("0123456789abcdef0123456789abcdef"))

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "x.txt"
	nr.Params[wire.MediaKey] = []byte("data")
	nr.Params[wire.CSEKAlgorithm] = "AES128"
	nr.Params[wire.CSEKKey] = keyB64
	nr.Params[wire.CSEKKeySHA256] = shaB64
	if _, err := p.ObjectsInsert(ctx, nr); err == nil {
		t.Fatal("expected error for non-AES256 algorithm")
	} else {
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
			t.Fatalf("expected 400 ProviderError, got %v", err)
		}
	}
}

func TestGCSCSEKMissingSHARejected(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	keyB64, _ := csekMaterial([]byte("0123456789abcdef0123456789abcdef"))

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "x.txt"
	nr.Params[wire.MediaKey] = []byte("data")
	nr.Params[wire.CSEKKey] = keyB64
	if _, err := p.ObjectsInsert(ctx, nr); err == nil {
		t.Fatal("expected error for CSEK key without sha256")
	} else {
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
			t.Fatalf("expected 400 ProviderError, got %v", err)
		}
	}
}

// TestGCSCopyCSEKSourceRequiresSourceKey verifies a copy of a CSEK source
// without the copy-source key fails with 400 — the source cannot be decrypted.
func TestGCSCopyCSEKSourceRequiresSourceKey(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	keyB64, shaB64 := csekMaterial([]byte("0123456789abcdef0123456789abcdef"))
	newCSEKBucket(t, p, keyB64, shaB64)

	nr := bucketParams()
	nr.Params["sourceBucket"] = "bkt"
	nr.Params["sourceObject"] = "csek.txt"
	nr.Params["destinationBucket"] = "bkt"
	nr.Params["destinationObject"] = "plain.txt"
	if _, err := p.ObjectsCopy(ctx, nr); err == nil {
		t.Fatal("expected error copying CSEK source without the source key")
	} else if got := csekErrorReason(t, err); got != csekReasonResourceEncrypted {
		t.Fatalf("reason = %q, want %q", got, csekReasonResourceEncrypted)
	}
}

// TestGCSCopyDropsSourceCSEKWhenNoDestinationKey verifies a copy of a CSEK
// source (with the copy-source key) re-encrypts with server-DEK and clears the
// source's customerEncryption metadata, so the destination is readable without
// a key.
func TestGCSCopyDropsSourceCSEKWhenNoDestinationKey(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	keyB64, shaB64 := csekMaterial([]byte("0123456789abcdef0123456789abcdef"))
	newCSEKBucket(t, p, keyB64, shaB64)

	nr := bucketParams()
	nr.Params["sourceBucket"] = "bkt"
	nr.Params["sourceObject"] = "csek.txt"
	nr.Params["destinationBucket"] = "bkt"
	nr.Params["destinationObject"] = "plain.txt"
	nr.Params[wire.CopySourceCSEKAlgorithm] = "AES256"
	nr.Params[wire.CopySourceCSEKKey] = keyB64
	nr.Params[wire.CopySourceCSEKKeySHA256] = shaB64
	if _, err := p.ObjectsCopy(ctx, nr); err != nil {
		t.Fatalf("copy: %v", err)
	}

	// Destination metadata must not claim CSEK.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "plain.txt"
	get, err := p.ObjectsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get destination: %v", err)
	}
	if ce, _ := get.Data["customerEncryption"].(map[string]any); ce != nil {
		t.Fatalf("destination must not inherit source customerEncryption: %#v", ce)
	}

	// Destination is readable without any key.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "plain.txt"
	media, err := p.ObjectsGetMedia(ctx, nr)
	if err != nil {
		t.Fatalf("get destination media: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "classified" {
		t.Fatalf("expected %q, got %q", "classified", got)
	}
}

// TestGCSCopyCSEKSourceWithKeys verifies a copy of a CSEK source decrypts the
// source with the copy-source key and re-encrypts the destination under the
// destination key.
func TestGCSCopyCSEKSourceWithKeys(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	srcKey := []byte("0123456789abcdef0123456789abcdef")
	srcB64, srcSHA := csekMaterial(srcKey)
	newCSEKBucket(t, p, srcB64, srcSHA)

	dstKey := []byte("fedcba9876543210fedcba9876543210")
	dstB64, dstSHA := csekMaterial(dstKey)

	nr := bucketParams()
	nr.Params["sourceBucket"] = "bkt"
	nr.Params["sourceObject"] = "csek.txt"
	nr.Params["destinationBucket"] = "bkt"
	nr.Params["destinationObject"] = "copy.txt"
	nr.Params[wire.CopySourceCSEKAlgorithm] = "AES256"
	nr.Params[wire.CopySourceCSEKKey] = srcB64
	nr.Params[wire.CopySourceCSEKKeySHA256] = srcSHA
	nr.Params[wire.CSEKAlgorithm] = "AES256"
	nr.Params[wire.CSEKKey] = dstB64
	nr.Params[wire.CSEKKeySHA256] = dstSHA
	if _, err := p.ObjectsCopy(ctx, nr); err != nil {
		t.Fatalf("copy: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "copy.txt"
	get, err := p.ObjectsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get destination: %v", err)
	}
	ce, _ := get.Data["customerEncryption"].(map[string]any)
	if ce == nil || ce["keySha256"] != dstSHA {
		t.Fatalf("expected destination keySha256 %q, got %#v", dstSHA, ce)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "copy.txt"
	nr.Params[wire.CSEKAlgorithm] = "AES256"
	nr.Params[wire.CSEKKey] = dstB64
	nr.Params[wire.CSEKKeySHA256] = dstSHA
	media, err := p.ObjectsGetMedia(ctx, nr)
	if err != nil {
		t.Fatalf("get destination media: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "classified" {
		t.Fatalf("expected %q, got %q", "classified", got)
	}
}

// csekErrorReason returns the documented CSEK reason carried on a 400
// ProviderError, failing the test if the error is not a CSEK ProviderError.
func csekErrorReason(t *testing.T, err error) string {
	t.Helper()
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected ProviderError, got %v", err)
	}
	if pe.HTTPStatus != 400 {
		t.Fatalf("expected HTTP 400, got %d (%v)", pe.HTTPStatus, err)
	}
	r, _ := pe.Data["reason"].(string)
	if r == "" {
		t.Fatalf("expected a reason in ProviderError.Data, got %v", err)
	}
	return r
}

// TestGCSCSEKErrorReasons verifies each CSEK failure carries the documented
// GCS reason (J35), and that a key on a non-CSEK object is rejected rather than
// silently ignored (J36).
func TestGCSCSEKErrorReasons(t *testing.T) {
	ctx := context.Background()
	key := []byte("0123456789abcdef0123456789abcdef")
	keyB64, shaB64 := csekMaterial(key)
	otherB64, otherSHA := csekMaterial([]byte("fedcba9876543210fedcba9876543210"))

	setup := func(t *testing.T) *Provider {
		t.Helper()
		p := newTestProvider()
		newCSEKBucket(t, p, keyB64, shaB64) // bkt + csek.txt
		nr := bucketParams()
		nr.Params["bucket"] = "bkt"
		nr.Params["object"] = "plain.txt"
		nr.Params[wire.MediaKey] = []byte("plain")
		if _, err := p.ObjectsInsert(ctx, nr); err != nil {
			t.Fatalf("insert plain object: %v", err)
		}
		return p
	}
	insert := func(p *Provider, mut func(map[string]any)) error {
		nr := bucketParams()
		nr.Params["bucket"] = "bkt"
		nr.Params["object"] = "new.txt"
		nr.Params[wire.MediaKey] = []byte("data")
		mut(nr.Params)
		_, err := p.ObjectsInsert(ctx, nr)
		return err
	}
	read := func(p *Provider, object string, mut func(map[string]any)) error {
		nr := bucketParams()
		nr.Params["bucket"] = "bkt"
		nr.Params["object"] = object
		if mut != nil {
			mut(nr.Params)
		}
		_, err := p.ObjectsGetMedia(ctx, nr)
		return err
	}

	shortKey := base64.StdEncoding.EncodeToString([]byte("short"))
	tests := []struct {
		name   string
		reason string
		run    func(p *Provider) error
	}{
		{"insert algorithm invalid", csekReasonAlgorithmInvalid, func(p *Provider) error {
			return insert(p, func(m map[string]any) {
				m[wire.CSEKAlgorithm] = "AES128"
				m[wire.CSEKKey] = keyB64
				m[wire.CSEKKeySHA256] = shaB64
			})
		}},
		{"insert algorithm missing", csekReasonAlgorithmInvalid, func(p *Provider) error {
			return insert(p, func(m map[string]any) {
				m[wire.CSEKKey] = keyB64
				m[wire.CSEKKeySHA256] = shaB64
			})
		}},
		{"insert key missing", csekReasonKeyFormatInvalid, func(p *Provider) error {
			return insert(p, func(m map[string]any) {
				m[wire.CSEKAlgorithm] = "AES256"
				m[wire.CSEKKeySHA256] = shaB64
			})
		}},
		{"insert key malformed", csekReasonKeyFormatInvalid, func(p *Provider) error {
			return insert(p, func(m map[string]any) {
				m[wire.CSEKAlgorithm] = "AES256"
				m[wire.CSEKKey] = shortKey
				m[wire.CSEKKeySHA256] = shaB64
			})
		}},
		{"insert sha missing", csekReasonKeySha256Invalid, func(p *Provider) error {
			return insert(p, func(m map[string]any) {
				m[wire.CSEKAlgorithm] = "AES256"
				m[wire.CSEKKey] = keyB64
			})
		}},
		{"insert sha mismatch", csekReasonKeySha256Invalid, func(p *Provider) error {
			return insert(p, func(m map[string]any) {
				m[wire.CSEKAlgorithm] = "AES256"
				m[wire.CSEKKey] = keyB64
				m[wire.CSEKKeySHA256] = otherSHA
			})
		}},
		{"read encrypted without key", csekReasonResourceEncrypted, func(p *Provider) error {
			return read(p, "csek.txt", nil)
		}},
		{"read encrypted wrong key", csekReasonKeyIncorrect, func(p *Provider) error {
			return read(p, "csek.txt", func(m map[string]any) {
				m[wire.CSEKAlgorithm] = "AES256"
				m[wire.CSEKKey] = otherB64
				m[wire.CSEKKeySHA256] = otherSHA
			})
		}},
		{"read encrypted algorithm invalid", csekReasonAlgorithmInvalid, func(p *Provider) error {
			return read(p, "csek.txt", func(m map[string]any) {
				m[wire.CSEKAlgorithm] = "AES128"
				m[wire.CSEKKey] = keyB64
				m[wire.CSEKKeySHA256] = shaB64
			})
		}},
		{"read encrypted key malformed", csekReasonKeyFormatInvalid, func(p *Provider) error {
			return read(p, "csek.txt", func(m map[string]any) {
				m[wire.CSEKAlgorithm] = "AES256"
				m[wire.CSEKKey] = shortKey
				m[wire.CSEKKeySHA256] = shaB64
			})
		}},
		{"read encrypted algorithm missing", csekReasonAlgorithmInvalid, func(p *Provider) error {
			return read(p, "csek.txt", func(m map[string]any) {
				m[wire.CSEKKey] = keyB64
				m[wire.CSEKKeySHA256] = shaB64
			})
		}},
		{"read encrypted sha missing", csekReasonKeySha256Invalid, func(p *Provider) error {
			return read(p, "csek.txt", func(m map[string]any) {
				m[wire.CSEKAlgorithm] = "AES256"
				m[wire.CSEKKey] = keyB64
			})
		}},
		{"read encrypted sha mismatch", csekReasonKeySha256Invalid, func(p *Provider) error {
			return read(p, "csek.txt", func(m map[string]any) {
				m[wire.CSEKAlgorithm] = "AES256"
				m[wire.CSEKKey] = keyB64
				m[wire.CSEKKeySHA256] = otherSHA
			})
		}},
		{"read plain with key", csekReasonResourceNotEncrypted, func(p *Provider) error {
			return read(p, "plain.txt", func(m map[string]any) {
				m[wire.CSEKAlgorithm] = "AES256"
				m[wire.CSEKKey] = keyB64
				m[wire.CSEKKeySHA256] = shaB64
			})
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run(setup(t))
			if err == nil {
				t.Fatal("expected an error")
			}
			if got := csekErrorReason(t, err); got != tc.reason {
				t.Errorf("reason = %q, want %q", got, tc.reason)
			}
		})
	}
}

// TestGCSComposeCSEKSources verifies objects.compose reads its source objects
// with the customer-supplied encryption key (CSEK) supplied on the compose
// request and encrypts the resulting composite object with that same key (J38).
// Real GCS requires every CSEK component to use the same key and encrypts the
// composite with it.
func TestGCSComposeCSEKSources(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	key := []byte("0123456789abcdef0123456789abcdef")
	keyB64, shaB64 := csekMaterial(key)
	otherB64, otherSHA := csekMaterial([]byte("fedcba9876543210fedcba9876543210"))

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	for name, content := range map[string]string{"a.txt": "aaa", "b.txt": "bbb"} {
		nr = bucketParams()
		nr.Params["bucket"] = "bkt"
		nr.Params["object"] = name
		nr.Params[wire.MediaKey] = []byte(content)
		nr.Params[wire.CSEKAlgorithm] = "AES256"
		nr.Params[wire.CSEKKey] = keyB64
		nr.Params[wire.CSEKKeySHA256] = shaB64
		if _, err := p.ObjectsInsert(ctx, nr); err != nil {
			t.Fatalf("insert %s: %v", name, err)
		}
	}

	withKey := func(b64, sha string) func(map[string]any) {
		return func(m map[string]any) {
			m[wire.CSEKAlgorithm] = "AES256"
			m[wire.CSEKKey] = b64
			m[wire.CSEKKeySHA256] = sha
		}
	}
	compose := func(mut func(map[string]any)) (*model.ProviderResponse, error) {
		nr := bucketParams()
		nr.Params["bucket"] = "bkt"
		nr.Params["object"] = "ab.txt"
		nr.Params["body"] = map[string]any{
			"sourceObjects": []any{
				map[string]any{"name": "a.txt"},
				map[string]any{"name": "b.txt"},
			},
		}
		if mut != nil {
			mut(nr.Params)
		}
		return p.ObjectsCompose(ctx, nr)
	}

	// Without the key a CSEK source cannot be read.
	if _, err := compose(nil); csekErrorReason(t, err) != csekReasonResourceEncrypted {
		t.Fatalf("compose without key: got %v", err)
	}
	// A different key is an incorrect source key.
	if _, err := compose(withKey(otherB64, otherSHA)); csekErrorReason(t, err) != csekReasonKeyIncorrect {
		t.Fatalf("compose wrong key: got %v", err)
	}

	// The correct key composes the sources and encrypts the destination with it.
	resp, err := compose(withKey(keyB64, shaB64))
	if err != nil {
		t.Fatalf("compose CSEK sources: %v", err)
	}
	if cc, _ := resp.Data["componentCount"].(float64); int64(cc) != 2 {
		t.Errorf("componentCount = %v, want 2", resp.Data["componentCount"])
	}
	ce, _ := resp.Data["customerEncryption"].(map[string]any)
	if ce == nil || ce["keySha256"] != shaB64 {
		t.Fatalf("destination customerEncryption = %#v, want keySha256 %q", resp.Data["customerEncryption"], shaB64)
	}

	// The composite reads back with the key and is unreadable without it.
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "ab.txt"
	withKey(keyB64, shaB64)(nr.Params)
	media, err := p.ObjectsGetMedia(ctx, nr)
	if err != nil {
		t.Fatalf("get composite with key: %v", err)
	}
	if got := string(streamBytes(t, media)); got != "aaabbb" {
		t.Fatalf("composite content = %q, want %q", got, "aaabbb")
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "ab.txt"
	if _, err := p.ObjectsGetMedia(ctx, nr); err == nil {
		t.Fatal("expected composite to be unreadable without the key")
	}
}

// TestGCSComposeCSEKRejectsPlainSource verifies that supplying a CSEK key to a
// compose whose source is not CSEK-encrypted is rejected (J36 semantics), since
// real GCS requires every component of a CSEK compose to use the same key.
func TestGCSComposeCSEKRejectsPlainSource(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	keyB64, shaB64 := csekMaterial([]byte("0123456789abcdef0123456789abcdef"))

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "bkt"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "plain.txt"
	nr.Params[wire.MediaKey] = []byte("plain")
	if _, err := p.ObjectsInsert(ctx, nr); err != nil {
		t.Fatalf("insert plain object: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "bkt"
	nr.Params["object"] = "out.txt"
	nr.Params["body"] = map[string]any{
		"sourceObjects": []any{map[string]any{"name": "plain.txt"}},
	}
	nr.Params[wire.CSEKAlgorithm] = "AES256"
	nr.Params[wire.CSEKKey] = keyB64
	nr.Params[wire.CSEKKeySHA256] = shaB64
	_, err := p.ObjectsCompose(ctx, nr)
	if got := csekErrorReason(t, err); got != csekReasonResourceNotEncrypted {
		t.Fatalf("reason = %q, want %q", got, csekReasonResourceNotEncrypted)
	}
}
