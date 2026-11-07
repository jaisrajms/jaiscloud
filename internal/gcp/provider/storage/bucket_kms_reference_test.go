package storage

import (
	"context"
	"errors"
	"testing"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/store/gcs"
	kms "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// newCMEKProvider returns a provider wired to a fresh KMS store holding a
// symmetric key in location "us" (the GCS US multi-region's KMS location).
func newCMEKProvider(t *testing.T) (*Provider, string) {
	t.Helper()
	ctx := context.Background()
	keys := kms.NewMemoryStore()
	p := New(gcs.NewMemoryObjectStore(), store.NewMemoryResourceStore(), blobfs.NewMemoryBlobStore(),
		crypto.NewEnvelopeEncryptor(keys))
	p.SetKMSStore(keys)
	if err := keys.CreateKeyRing(ctx, "proj", "us", "ring", kms.KeyRing{ID: "ring"}); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if err := keys.CreateCryptoKey(ctx, "proj", "us", "ring", "key",
		kms.CryptoKey{ID: "key", Purpose: "ENCRYPT_DECRYPT"}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	return p, "projects/proj/locations/us/keyRings/ring/cryptoKeys/key"
}

func insertBucketWithCMEK(t *testing.T, p *Provider, name, location, key string) error {
	t.Helper()
	nr := bucketParams()
	body := map[string]any{"name": name}
	if location != "" {
		body["location"] = location
	}
	if key != "" {
		body["encryption"] = map[string]any{"defaultKmsKeyName": key}
	}
	nr.Params["body"] = body
	_, err := p.BucketsInsert(context.Background(), nr)
	return err
}

func requireInvalidArgument(t *testing.T, err error, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s = nil, want INVALID_ARGUMENT", what)
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.HTTPStatus != 400 {
		t.Fatalf("%s = %v, want HTTP 400 INVALID_ARGUMENT", what, err)
	}
}

// TestBucketsInsertCMEKReference covers FC4-1 on the REST create path: a
// bucket's encryption.defaultKmsKeyName is resolved against the KMS store, so
// an existing same-location ENCRYPT_DECRYPT key is accepted while a missing,
// cross-location or non-encrypting key is INVALID_ARGUMENT.
func TestBucketsInsertCMEKReference(t *testing.T) {
	ctx := context.Background()
	p, key := newCMEKProvider(t)

	if err := insertBucketWithCMEK(t, p, "cmek-ok", "US", key); err != nil {
		t.Fatalf("accept: %v", err)
	}
	requireInvalidArgument(t, insertBucketWithCMEK(t, p, "cmek-missing", "US",
		"projects/proj/locations/us/keyRings/ring/cryptoKeys/absent"), "missing key")
	requireInvalidArgument(t, insertBucketWithCMEK(t, p, "cmek-wrong-loc", "US",
		"projects/proj/locations/europe/keyRings/ring/cryptoKeys/key"), "cross-location key")

	// A signing key cannot encrypt a bucket.
	if err := p.kms.CreateCryptoKey(ctx, "proj", "us", "ring", "signer",
		kms.CryptoKey{ID: "signer", Purpose: "ASYMMETRIC_SIGN"}); err != nil {
		t.Fatalf("CreateCryptoKey(signer): %v", err)
	}
	requireInvalidArgument(t, insertBucketWithCMEK(t, p, "cmek-purpose", "US",
		"projects/proj/locations/us/keyRings/ring/cryptoKeys/signer"), "wrong-purpose key")
}

// TestBucketsUpdateCMEKReference is the update half: a PATCH that introduces a
// nonexistent default key is rejected and leaves the stored bucket untouched.
func TestBucketsUpdateCMEKReference(t *testing.T) {
	ctx := context.Background()
	p, key := newCMEKProvider(t)
	if err := insertBucketWithCMEK(t, p, "cmek-update", "US", key); err != nil {
		t.Fatalf("insert: %v", err)
	}

	nr := bucketParams()
	nr.Params["bucket"] = "cmek-update"
	nr.Params["body"] = map[string]any{
		"encryption": map[string]any{"defaultKmsKeyName": "projects/proj/locations/us/keyRings/ring/cryptoKeys/absent"},
	}
	_, err := p.BucketsUpdate(ctx, nr)
	requireInvalidArgument(t, err, "update with missing key")

	nr = bucketParams()
	nr.Params["bucket"] = "cmek-update"
	got, err := p.BucketsGet(ctx, nr)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	enc, _ := got.Data["encryption"].(map[string]any)
	if enc["defaultKmsKeyName"] != key {
		t.Fatalf("stored key = %v, want the original %q", enc["defaultKmsKeyName"], key)
	}
}
