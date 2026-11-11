package storage

import (
	"context"
	"errors"
	"testing"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/store/gcs"
	kms "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// cmekFixture builds a provider with one key ring and the keys the contract
// table needs: a healthy US key, an asymmetric key, and a key in `global` (which
// a US bucket must refuse).
func cmekFixture(t *testing.T) (*Provider, kms.Store, string) {
	t.Helper()
	ctx := context.Background()
	keys := kms.NewMemoryStore()
	p := New(gcs.NewMemoryObjectStore(), store.NewMemoryResourceStore(), blobfs.NewMemoryBlobStore(), crypto.NewEnvelopeEncryptor(keys))
	p.SetKMSStore(keys)
	for _, kr := range []struct{ loc, id string }{{"us", "kr"}, {"global", "gr"}} {
		if err := keys.CreateKeyRing(ctx, "proj", kr.loc, kr.id, kms.KeyRing{ID: kr.id}); err != nil {
			t.Fatalf("CreateKeyRing %s: %v", kr.id, err)
		}
	}
	if err := keys.CreateCryptoKey(ctx, "proj", "us", "kr", "ck", kms.CryptoKey{ID: "ck", Purpose: "ENCRYPT_DECRYPT"}); err != nil {
		t.Fatal(err)
	}
	if err := keys.CreateCryptoKey(ctx, "proj", "us", "kr", "signer", kms.CryptoKey{ID: "signer", Purpose: "ASYMMETRIC_SIGN"}); err != nil {
		t.Fatal(err)
	}
	if err := keys.CreateCryptoKey(ctx, "proj", "global", "gr", "gk", kms.CryptoKey{ID: "gk", Purpose: "ENCRYPT_DECRYPT"}); err != nil {
		t.Fatal(err)
	}
	return p, keys, "projects/proj/locations/us/keyRings/kr/cryptoKeys/ck"
}

func cmekProviderError(t *testing.T, err error) *model.ProviderError {
	t.Helper()
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v (%T), want *model.ProviderError", err, err)
	}
	return pe
}

func wantCMEKError(t *testing.T, err error, reason, message string) {
	t.Helper()
	pe := cmekProviderError(t, err)
	if pe.HTTPStatus != 400 {
		t.Fatalf("status = %d, want 400 (%v)", pe.HTTPStatus, err)
	}
	if got, _ := pe.Data["reason"].(string); got != reason {
		t.Fatalf("reason = %q, want %q (%v)", got, reason, err)
	}
	if pe.Message != message {
		t.Fatalf("message = %q, want %q", pe.Message, message)
	}
}

// TestGCSBucketCMEKReferenceErrors pins the three bucket-write refusals to the
// messages real GCS returns (verified against parity-diff-jaiscloud on
// 2026-11-11), all 400 with errors[].reason "invalid" rather than the
// "internalError" the generic path produced.
func TestGCSBucketCMEKReferenceErrors(t *testing.T) {
	ctx := context.Background()
	p, _, _ := cmekFixture(t)

	cases := []struct {
		name, key, message string
	}{
		{
			name:    "missing key",
			key:     "projects/proj/locations/us/keyRings/kr/cryptoKeys/nope",
			message: "Cannot find the requested Cloud KMS encryption key.",
		},
		{
			name:    "wrong location",
			key:     "projects/proj/locations/global/keyRings/gr/cryptoKeys/gk",
			message: "Cloud KMS region 'global' not available for use with GCS region 'US'.",
		},
		{
			name:    "version-qualified name",
			key:     "projects/proj/locations/us/keyRings/kr/cryptoKeys/ck/cryptoKeyVersions/1",
			message: "Malformed Cloud KMS crypto key: projects/proj/locations/us/keyRings/kr/cryptoKeys/ck/cryptoKeyVersions/1",
		},
	}
	for _, tc := range cases {
		nr := bucketParams()
		nr.Params["body"] = map[string]any{
			"name":       "b",
			"location":   "US",
			"encryption": map[string]any{"defaultKmsKeyName": tc.key},
		}
		_, err := p.BucketsInsert(ctx, nr)
		wantCMEKError(t, err, "invalid", tc.message)
	}
}

// TestGCSObjectCMEKReferenceErrors covers the upload-time refusals: a
// per-object kmsKeyName is validated on every write, so a bad one fails before
// any bytes are stored.
func TestGCSObjectCMEKReferenceErrors(t *testing.T) {
	ctx := context.Background()
	p, _, healthy := cmekFixture(t)

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "b", "location": "US"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}

	upload := func(key string) error {
		nr := bucketParams()
		nr.Params["bucket"] = "b"
		nr.Params["object"] = "o.txt"
		nr.Params["kmsKeyName"] = key
		nr.Params[wire.MediaKey] = []byte("hi")
		_, err := p.ObjectsInsert(ctx, nr)
		return err
	}

	wantCMEKError(t, upload("projects/proj/locations/us/keyRings/kr/cryptoKeys/nope"),
		"invalid", "Cannot find the requested Cloud KMS encryption key.")
	wantCMEKError(t, upload("projects/proj/locations/global/keyRings/gr/cryptoKeys/gk"),
		"invalid", "Cloud KMS region 'global' not available for use with GCS region 'US'.")
	wantCMEKError(t, upload("projects/proj/locations/us/keyRings/kr/cryptoKeys/ck/cryptoKeyVersions/1"),
		"invalid", "Malformed Cloud KMS crypto key: projects/proj/locations/us/keyRings/kr/cryptoKeys/ck/cryptoKeyVersions/1")

	if err := upload(healthy); err != nil {
		t.Fatalf("healthy key upload: %v", err)
	}
}

// TestGCSObjectCMEKDisabledPrimary pins the state refusal: the write is refused
// (the CMEK guide's primary-version rule) with the documented
// `cloudKmsDisabledKey` wording and reason "invalid".
func TestGCSObjectCMEKDisabledPrimary(t *testing.T) {
	ctx := context.Background()
	p, keys, healthy := cmekFixture(t)

	nr := bucketParams()
	nr.Params["body"] = map[string]any{"name": "b", "location": "US"}
	if _, err := p.BucketsInsert(ctx, nr); err != nil {
		t.Fatalf("insert bucket: %v", err)
	}
	if err := keys.UpdateVersionState(ctx, "proj", "us", "kr", "ck", "1", "DISABLED"); err != nil {
		t.Fatalf("disable primary: %v", err)
	}

	nr = bucketParams()
	nr.Params["bucket"] = "b"
	nr.Params["object"] = "o.txt"
	nr.Params["kmsKeyName"] = healthy
	nr.Params[wire.MediaKey] = []byte("hi")
	_, err := p.ObjectsInsert(ctx, nr)
	wantCMEKError(t, err, "invalid", "Cloud KMS key "+healthy+" is disabled, destroyed, or scheduled to be destroyed.")
}
