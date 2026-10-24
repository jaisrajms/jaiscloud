package crypto

import (
	"bytes"
	"context"
	"testing"

	"jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
)

func newTestEncryptor() (*encryptor, *kms.MemoryStore) {
	store := kms.NewMemoryStore()
	return &encryptor{kmsStore: store}, store
}

func createTestKey(t *testing.T, store *kms.MemoryStore, project, loc, ring, key string) {
	t.Helper()
	ctx := context.Background()
	if err := store.CreateKeyRing(ctx, project, loc, ring, kms.KeyRing{Location: loc, ID: ring}); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if err := store.CreateCryptoKey(ctx, project, loc, ring, key, kms.CryptoKey{
		Location:  loc,
		KeyRingID: ring,
		ID:        key,
		Purpose:   "ENCRYPT_DECRYPT",
	}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
}

func keyName(project, loc, ring, key string) string {
	return "projects/" + project + "/locations/" + loc + "/keyRings/" + ring + "/cryptoKeys/" + key
}

func TestWrapUnwrap_ServerDEK(t *testing.T) {
	enc, _ := newTestEncryptor()
	ctx := context.Background()

	rawDEK, wrappedDEK, err := enc.Wrap(ctx, "acct", "")
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	if len(rawDEK) == 0 {
		t.Fatal("expected non-empty server DEK")
	}
	if wrappedDEK != nil {
		t.Fatalf("expected nil wrappedDEK for server DEK, got %v", wrappedDEK)
	}

	unwrapped, err := enc.Unwrap(ctx, "acct", "", nil)
	if err != nil {
		t.Fatalf("Unwrap: %v", err)
	}
	if !bytes.Equal(rawDEK, unwrapped) {
		t.Fatalf("server DEK mismatch: wrap=%x unwrap=%x", rawDEK, unwrapped)
	}
}

func TestWrapUnwrap_KMSKey_RoundTrip(t *testing.T) {
	enc, store := newTestEncryptor()
	ctx := context.Background()
	createTestKey(t, store, "proj1", "global", "ring1", "key1")
	name := keyName("proj1", "global", "ring1", "key1")

	rawDEK, wrappedDEK, err := enc.Wrap(ctx, "acct", name)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	if len(rawDEK) != 32 {
		t.Fatalf("expected 32-byte DEK, got %d bytes", len(rawDEK))
	}
	if len(wrappedDEK) == 0 {
		t.Fatal("expected non-empty wrappedDEK for a KMS-backed key")
	}

	unwrapped, err := enc.Unwrap(ctx, "acct", name, wrappedDEK)
	if err != nil {
		t.Fatalf("Unwrap: %v", err)
	}
	if !bytes.Equal(rawDEK, unwrapped) {
		t.Fatalf("round-trip DEK mismatch: wrap=%x unwrap=%x", rawDEK, unwrapped)
	}
}

// TestUnwrap_SurvivesPrimaryVersionRotation verifies that rotating a
// CryptoKey's primary version after Wrap does not strand data encrypted
// under the version that was primary at wrap time — Unwrap must use the
// version tagged onto the wrapped blob, not whichever version is currently
// primary.
func TestUnwrap_SurvivesPrimaryVersionRotation(t *testing.T) {
	enc, store := newTestEncryptor()
	ctx := context.Background()
	createTestKey(t, store, "proj1", "global", "ring1", "key1")
	name := keyName("proj1", "global", "ring1", "key1")

	// Wrap while version "1" is primary.
	rawDEK, wrappedDEK, err := enc.Wrap(ctx, "acct", name)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}

	// Rotate the primary version to a freshly created version "2".
	newVersion, err := store.CreateVersion(ctx, "proj1", "global", "ring1", "key1", kms.Version{})
	if err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}
	if err := store.UpdatePrimaryVersion(ctx, "proj1", "global", "ring1", "key1", newVersion); err != nil {
		t.Fatalf("UpdatePrimaryVersion: %v", err)
	}

	// Unwrap must still succeed, using version "1"'s key material, even
	// though version "2" is now primary.
	unwrapped, err := enc.Unwrap(ctx, "acct", name, wrappedDEK)
	if err != nil {
		t.Fatalf("Unwrap after rotation: %v", err)
	}
	if !bytes.Equal(rawDEK, unwrapped) {
		t.Fatalf("DEK mismatch after rotation: wrap=%x unwrap=%x", rawDEK, unwrapped)
	}

	// A DEK wrapped AFTER rotation should be tagged with the new version and
	// still round-trip correctly too.
	rawDEK2, wrappedDEK2, err := enc.Wrap(ctx, "acct", name)
	if err != nil {
		t.Fatalf("Wrap after rotation: %v", err)
	}
	unwrapped2, err := enc.Unwrap(ctx, "acct", name, wrappedDEK2)
	if err != nil {
		t.Fatalf("Unwrap of post-rotation DEK: %v", err)
	}
	if !bytes.Equal(rawDEK2, unwrapped2) {
		t.Fatalf("post-rotation DEK mismatch: wrap=%x unwrap=%x", rawDEK2, unwrapped2)
	}
}

func TestWrapUnwrap_DifferentKeysProduceDifferentDEKs(t *testing.T) {
	enc, store := newTestEncryptor()
	ctx := context.Background()
	createTestKey(t, store, "proj1", "global", "ring1", "key1")
	name := keyName("proj1", "global", "ring1", "key1")

	dek1, _, err := enc.Wrap(ctx, "acct", name)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	dek2, _, err := enc.Wrap(ctx, "acct", name)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	if bytes.Equal(dek1, dek2) {
		t.Fatal("expected each Wrap call to generate a fresh random DEK")
	}
}

func TestWrap_InvalidKeyNameFormat(t *testing.T) {
	enc, _ := newTestEncryptor()
	ctx := context.Background()

	_, _, err := enc.Wrap(ctx, "acct", "not-a-valid-resource-name")
	assertInvalidArgument(t, err)
}

func TestWrap_NoSuchCryptoKey(t *testing.T) {
	enc, _ := newTestEncryptor()
	ctx := context.Background()

	_, _, err := enc.Wrap(ctx, "acct", keyName("proj1", "global", "ring1", "missing"))
	assertInvalidArgument(t, err)
}

// TestWrap_DisabledVersion: a key whose primary version is disabled cannot
// encrypt. Wrap keeps the shared consumers' historical INVALID_ARGUMENT;
// ValidateKey (Secret Manager's create/patch probe) answers the
// FAILED_PRECONDITION real GCP reports instead — see TestValidateKey.
func TestWrap_DisabledVersion(t *testing.T) {
	enc, store := newTestEncryptor()
	ctx := context.Background()
	createTestKey(t, store, "proj1", "global", "ring1", "key1")
	if err := store.UpdateVersionState(ctx, "proj1", "global", "ring1", "key1", "1", "DISABLED"); err != nil {
		t.Fatalf("UpdateVersionState: %v", err)
	}

	_, _, err := enc.Wrap(ctx, "acct", keyName("proj1", "global", "ring1", "key1"))
	assertInvalidArgument(t, err)
}

func TestUnwrap_InvalidKeyNameFormat(t *testing.T) {
	enc, _ := newTestEncryptor()
	ctx := context.Background()

	_, err := enc.Unwrap(ctx, "acct", "not-a-valid-resource-name", []byte("blob"))
	assertInvalidArgument(t, err)
}

func TestUnwrap_NoSuchCryptoKey(t *testing.T) {
	enc, _ := newTestEncryptor()
	ctx := context.Background()

	_, err := enc.Unwrap(ctx, "acct", keyName("proj1", "global", "ring1", "missing"), []byte("blob"))
	assertInvalidArgument(t, err)
}

func TestUnwrap_DisabledVersion(t *testing.T) {
	enc, store := newTestEncryptor()
	ctx := context.Background()
	createTestKey(t, store, "proj1", "global", "ring1", "key1")
	if err := store.UpdateVersionState(ctx, "proj1", "global", "ring1", "key1", "1", "DISABLED"); err != nil {
		t.Fatalf("UpdateVersionState: %v", err)
	}

	_, err := enc.Unwrap(ctx, "acct", keyName("proj1", "global", "ring1", "key1"), []byte("blob"))
	assertInvalidArgument(t, err)
}

func TestUnwrap_TamperedCiphertext(t *testing.T) {
	enc, store := newTestEncryptor()
	ctx := context.Background()
	createTestKey(t, store, "proj1", "global", "ring1", "key1")
	name := keyName("proj1", "global", "ring1", "key1")

	_, wrappedDEK, err := enc.Wrap(ctx, "acct", name)
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	tampered := append([]byte(nil), wrappedDEK...)
	tampered[len(tampered)-1] ^= 0xFF

	_, err = enc.Unwrap(ctx, "acct", name, tampered)
	assertInvalidArgument(t, err)
}

func assertFailedPrecondition(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	perr, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if perr.HTTPStatus != 400 || perr.Status != "FAILED_PRECONDITION" {
		t.Fatalf("expected FAILED_PRECONDITION/400, got %q/%d (%s)", perr.Status, perr.HTTPStatus, perr.Message)
	}
}

func assertInvalidArgument(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	perr, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if perr.Code != "InvalidArgument" {
		t.Fatalf("expected code InvalidArgument, got %q", perr.Code)
	}
	if perr.HTTPStatus != 400 {
		t.Fatalf("expected HTTP 400, got %d", perr.HTTPStatus)
	}
}

func TestParseCryptoKeyName(t *testing.T) {
	tests := []struct {
		name                               string
		wantProj, wantLoc, wantKR, wantKey string
	}{
		{
			name:     "projects/p1/locations/us/keyRings/r1/cryptoKeys/k1",
			wantProj: "p1", wantLoc: "us", wantKR: "r1", wantKey: "k1",
		},
		{name: "not-a-valid-name"},
		{name: "projects/p1/locations/us/keyRings/r1"},
		{name: ""},
	}
	for _, tt := range tests {
		proj, loc, kr, key := parseCryptoKeyName(tt.name)
		if proj != tt.wantProj || loc != tt.wantLoc || kr != tt.wantKR || key != tt.wantKey {
			t.Errorf("parseCryptoKeyName(%q) = (%q,%q,%q,%q), want (%q,%q,%q,%q)",
				tt.name, proj, loc, kr, key, tt.wantProj, tt.wantLoc, tt.wantKR, tt.wantKey)
		}
	}
}

// TestWrapKeys_PerReplica wraps one DEK under two keys (DC57): both blobs unwrap
// to the same DEK, each tagged with its own key's primary CryptoKeyVersion.
func TestWrapKeys_PerReplica(t *testing.T) {
	enc, store := newTestEncryptor()
	ctx := context.Background()
	createTestKey(t, store, "proj1", "us-east1", "ring1", "east")
	createTestKey(t, store, "proj1", "us-central1", "ring1", "central")

	east := keyName("proj1", "us-east1", "ring1", "east")
	central := keyName("proj1", "us-central1", "ring1", "central")
	rawDEK, wrapped, err := enc.WrapKeys(ctx, "proj1", []string{east, central})
	if err != nil {
		t.Fatalf("WrapKeys: %v", err)
	}
	if len(wrapped) != 2 {
		t.Fatalf("wrapped = %d blobs, want 2", len(wrapped))
	}
	for i, name := range []string{east, central} {
		got, err := enc.Unwrap(ctx, "proj1", name, wrapped[i])
		if err != nil {
			t.Fatalf("unwrap %s: %v", name, err)
		}
		if !bytes.Equal(got, rawDEK) {
			t.Fatalf("%s unwrapped a different DEK", name)
		}
	}
	// No CMEK bindings is the server-DEK path: no wrapped blobs.
	serverDEK, wrapped, err := enc.WrapKeys(ctx, "proj1", nil)
	if err != nil || len(serverDEK) == 0 || wrapped != nil {
		t.Fatalf("WrapKeys(nil) = %x, %v, %v; want the server DEK and no blobs", serverDEK, wrapped, err)
	}
}

// TestValidateKey pins create/patch-time key resolution (DC58): an existing
// enabled key resolves, a missing key is INVALID_ARGUMENT, a disabled primary is
// FAILED_PRECONDITION, and a non-ENCRYPT_DECRYPT key is INVALID_ARGUMENT.
func TestValidateKey(t *testing.T) {
	enc, store := newTestEncryptor()
	ctx := context.Background()
	createTestKey(t, store, "proj1", "global", "ring1", "key1")
	if err := enc.ValidateKey(ctx, "proj1", keyName("proj1", "global", "ring1", "key1")); err != nil {
		t.Fatalf("ValidateKey(enabled) = %v, want nil", err)
	}
	assertInvalidArgument(t, enc.ValidateKey(ctx, "proj1", keyName("proj1", "global", "ring1", "missing")))
	assertInvalidArgument(t, enc.ValidateKey(ctx, "proj1", "not-a-key"))

	if err := store.UpdateVersionState(ctx, "proj1", "global", "ring1", "key1", "1", "DISABLED"); err != nil {
		t.Fatalf("UpdateVersionState: %v", err)
	}
	assertFailedPrecondition(t, enc.ValidateKey(ctx, "proj1", keyName("proj1", "global", "ring1", "key1")))

	// A key that cannot encrypt a DEK is INVALID_ARGUMENT.
	if err := store.CreateCryptoKey(ctx, "proj1", "global", "ring1", "signing", kms.CryptoKey{
		Location: "global", KeyRingID: "ring1", ID: "signing", Purpose: "ASYMMETRIC_SIGN",
	}); err != nil {
		t.Fatalf("CreateCryptoKey(signing): %v", err)
	}
	assertInvalidArgument(t, enc.ValidateKey(ctx, "proj1", keyName("proj1", "global", "ring1", "signing")))
}
