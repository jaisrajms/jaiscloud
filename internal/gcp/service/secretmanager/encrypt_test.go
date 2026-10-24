package secretmanager

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/model"
)

// newKey creates an ENCRYPT_DECRYPT CryptoKey and returns its resource name.
func newKey(t *testing.T, store *kms.MemoryStore, project, location, ring, id string) string {
	t.Helper()
	ctx := context.Background()
	if err := store.CreateKeyRing(ctx, project, location, ring, kms.KeyRing{Location: location, ID: ring}); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if err := store.CreateCryptoKey(ctx, project, location, ring, id, kms.CryptoKey{
		Location: location, KeyRingID: ring, ID: id, Purpose: "ENCRYPT_DECRYPT",
	}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	return "projects/" + project + "/locations/" + location + "/keyRings/" + ring + "/cryptoKeys/" + id
}

// keysVersion returns a key's primary CryptoKeyVersion number.
func keysVersion(t *testing.T, store *kms.MemoryStore, project, location, ring, id string) string {
	t.Helper()
	ck, err := store.GetCryptoKey(context.Background(), project, location, ring, id)
	if err != nil {
		t.Fatalf("GetCryptoKey %s/%s: %v", location, id, err)
	}
	return ck.PrimaryVersion
}

// providerErr returns err as a *model.ProviderError, or nil.
func providerErr(err error) *model.ProviderError {
	var pe *model.ProviderError
	if errors.As(err, &pe) {
		return pe
	}
	return nil
}

// TestValidateReplicationKeys pins the create/patch-time CMEK resolution (DC58):
// a policy naming a missing key is INVALID_ARGUMENT, a key whose primary version
// is disabled is FAILED_PRECONDITION, and a key in another project is accepted
// (Secret Manager supports keys and secrets in separate projects).
func TestValidateReplicationKeys(t *testing.T) {
	ctx := context.Background()
	store := kms.NewMemoryStore()
	enc := crypto.NewEnvelopeEncryptor(store)

	usEastKey := newKey(t, store, "p", "us-east1", "kr", "k")
	usCentralKey := newKey(t, store, "p", "us-central1", "kr", "k")
	foreignKey := newKey(t, store, "keys-project", "us-east1", "kr", "k")

	// A well-formed policy naming existing, enabled keys is accepted — including
	// two different per-replica keys and a key from another project.
	globalKey := newKey(t, store, "p", "global", "kr", "k")
	cases := []*secretmanagerstore.Replication{
		auto(globalKey),
		userManaged(replica("us-east1", usEastKey), replica("us-central1", usCentralKey)),
		userManaged(replica("us-east1", foreignKey)),
	}
	for i, repl := range cases {
		if err := ValidateReplicationKeys(ctx, enc, "p", repl); err != nil {
			t.Errorf("case %d: ValidateReplicationKeys = %v, want nil", i, err)
		}
	}

	// Missing key: INVALID_ARGUMENT, naming which replica's key failed.
	missing := "projects/p/locations/us-east1/keyRings/kr/cryptoKeys/nope"
	err := ValidateReplicationKeys(ctx, enc, "p", userManaged(replica("us-east1", usEastKey), replica("us-central1", missing)))
	if pe := providerErr(err); pe == nil || pe.HTTPStatus != 400 || pe.Code != "InvalidArgument" {
		t.Fatalf("missing key error = %v, want INVALID_ARGUMENT/400", err)
	}

	// Malformed key: INVALID_ARGUMENT.
	if err := ValidateReplicationKeys(ctx, enc, "p", auto("not-a-key")); err == nil {
		t.Fatal("malformed key accepted")
	}

	// Disabled primary version: FAILED_PRECONDITION (the state real GCP reports
	// when the CMEK cannot encrypt), on both arms.
	if err := store.UpdateVersionState(ctx, "p", "us-central1", "kr", "k", "1", "DISABLED"); err != nil {
		t.Fatalf("UpdateVersionState: %v", err)
	}
	for _, repl := range []*secretmanagerstore.Replication{
		userManaged(replica("us-central1", usCentralKey)),
	} {
		err := ValidateReplicationKeys(ctx, enc, "p", repl)
		pe := providerErr(err)
		if pe == nil || pe.HTTPStatus != 400 || pe.Status != "FAILED_PRECONDITION" {
			t.Errorf("disabled key error = %v, want FAILED_PRECONDITION/400", err)
		}
	}

	// The shape rules still apply: an unknown replica location is rejected
	// before any key is resolved.
	if err := ValidateReplicationKeys(ctx, enc, "p", userManaged(replica("us-east-1", ""))); err == nil {
		t.Error("unknown replica location accepted")
	}
}

// TestEncryptPayloadPerReplica wraps one DEK per CMEK replica (DC57): every
// replica's wrapped copy unwraps to the same DEK, which decrypts the single
// stored payload ciphertext.
func TestEncryptPayloadPerReplica(t *testing.T) {
	ctx := context.Background()
	store := kms.NewMemoryStore()
	enc := crypto.NewEnvelopeEncryptor(store)
	usEastKey := newKey(t, store, "p", "us-east1", "kr", "k")
	usCentralKey := newKey(t, store, "p", "us-central1", "kr", "k")
	repl := userManaged(replica("us-east1", usEastKey), replica("us-central1", usCentralKey), replica("us-west1", ""))

	wrap, err := EncryptPayload(ctx, enc, "p", repl, []byte("s3cret"))
	if err != nil {
		t.Fatalf("EncryptPayload: %v", err)
	}
	if wrap.KmsKeyName != usEastKey {
		t.Errorf("primary key = %q, want %q", wrap.KmsKeyName, usEastKey)
	}
	if len(wrap.ReplicaDEKs) != 2 {
		t.Fatalf("replica wraps = %d, want 2 (one per CMEK replica)", len(wrap.ReplicaDEKs))
	}
	if wrap.ReplicaDEKs[0].Location != "us-east1" || wrap.ReplicaDEKs[0].KmsKeyName != usEastKey {
		t.Errorf("first replica wrap = %#v", wrap.ReplicaDEKs[0])
	}
	if wrap.ReplicaDEKs[1].Location != "us-central1" || wrap.ReplicaDEKs[1].KmsKeyName != usCentralKey {
		t.Errorf("second replica wrap = %#v", wrap.ReplicaDEKs[1])
	}

	ciphertext, err := base64.StdEncoding.DecodeString(wrap.Data)
	if err != nil {
		t.Fatalf("payload is not base64: %v", err)
	}
	var firstDEK []byte
	for i, rep := range wrap.ReplicaDEKs {
		rawDEK, err := enc.Unwrap(ctx, "p", rep.KmsKeyName, rep.WrappedDEK)
		if err != nil {
			t.Fatalf("unwrap replica %d: %v", i, err)
		}
		if i == 0 {
			firstDEK = rawDEK
		} else if string(rawDEK) != string(firstDEK) {
			t.Errorf("replica %d unwrapped a different DEK", i)
		}
		plain, err := kms.DecryptData(rawDEK, ciphertext, nil)
		if err != nil || string(plain) != "s3cret" {
			t.Errorf("replica %d decrypt = %q, %v", i, plain, err)
		}
	}
}

// TestEncryptPayloadAutomaticAndServerDEK covers the two non-user-managed
// shapes: an automatic CMEK policy records the primary wrap only, and a
// plaintext (Google-managed) policy records no wraps.
func TestEncryptPayloadAutomaticAndServerDEK(t *testing.T) {
	ctx := context.Background()
	store := kms.NewMemoryStore()
	enc := crypto.NewEnvelopeEncryptor(store)
	globalKey := newKey(t, store, "p", "global", "kr", "k")

	wrap, err := EncryptPayload(ctx, enc, "p", auto(globalKey), []byte("hi"))
	if err != nil {
		t.Fatalf("EncryptPayload(automatic): %v", err)
	}
	if wrap.KmsKeyName != globalKey || len(wrap.WrappedDEK) == 0 {
		t.Errorf("automatic wrap = %#v, want the global key and a wrapped DEK", wrap)
	}
	if len(wrap.ReplicaDEKs) != 0 {
		t.Errorf("automatic replica wraps = %#v, want none", wrap.ReplicaDEKs)
	}

	plain, err := EncryptPayload(ctx, enc, "p", nil, []byte("hi"))
	if err != nil {
		t.Fatalf("EncryptPayload(server DEK): %v", err)
	}
	if plain.KmsKeyName != "" || plain.WrappedDEK != nil || len(plain.ReplicaDEKs) != 0 {
		t.Errorf("server-DEK wrap = %#v, want no wraps", plain)
	}
}

// TestDecryptVersion covers the access path the two transports share: the
// primary wrap, the fallback to another replica's wrap when that key version is
// unavailable, the error when no wrap works, and a corrupt payload.
func TestDecryptVersion(t *testing.T) {
	ctx := context.Background()
	store := kms.NewMemoryStore()
	enc := crypto.NewEnvelopeEncryptor(store)
	usEastKey := newKey(t, store, "p", "us-east1", "kr", "k")
	usCentralKey := newKey(t, store, "p", "us-central1", "kr", "k")
	repl := userManaged(replica("us-east1", usEastKey), replica("us-central1", usCentralKey))

	wrap, err := EncryptPayload(ctx, enc, "p", repl, []byte("payload"))
	if err != nil {
		t.Fatalf("EncryptPayload: %v", err)
	}
	version := secretmanagerstore.Version{
		SecretID: "s", VersionID: "1", State: "ENABLED", Data: wrap.Data,
		KmsKeyName: wrap.KmsKeyName, WrappedDEK: wrap.WrappedDEK, ReplicaDEKs: wrap.ReplicaDEKs,
	}
	plain, err := DecryptVersion(ctx, enc, "p", version)
	if err != nil || string(plain) != "payload" {
		t.Fatalf("DecryptVersion = %q, %v", plain, err)
	}

	// WrapOf carries the same wrap data the renderers use.
	if got := WrapOf(version); got.KmsKeyName != version.KmsKeyName || len(got.ReplicaDEKs) != len(version.ReplicaDEKs) {
		t.Fatalf("WrapOf = %#v, want the version's wraps", got)
	}

	// Disable the primary wrap's key version: the other replica's wrap still
	// decrypts the single payload ciphertext.
	primary := keysVersion(t, store, "p", "us-east1", "kr", "k")
	if err := store.UpdateVersionState(ctx, "p", "us-east1", "kr", "k", primary, "DISABLED"); err != nil {
		t.Fatalf("disable primary key version: %v", err)
	}
	plain, err = DecryptVersion(ctx, enc, "p", version)
	if err != nil || string(plain) != "payload" {
		t.Fatalf("DecryptVersion via the fallback replica = %q, %v", plain, err)
	}

	// With both key versions disabled no wrap works, and the primary error is
	// returned.
	central := keysVersion(t, store, "p", "us-central1", "kr", "k")
	if err := store.UpdateVersionState(ctx, "p", "us-central1", "kr", "k", central, "DISABLED"); err != nil {
		t.Fatalf("disable second key version: %v", err)
	}
	if _, err := DecryptVersion(ctx, enc, "p", version); providerErr(err) == nil {
		t.Fatalf("DecryptVersion with every key disabled = %v, want an error", err)
	}

	// A corrupt payload is a decode error, not a panic.
	bad := version
	bad.Data = "not base64!"
	if _, err := DecryptVersion(ctx, enc, "p", bad); err == nil {
		t.Fatal("DecryptVersion accepted a corrupt payload")
	}
}

// TestParentReplication covers the shared version-status parent lookup.
func TestParentReplication(t *testing.T) {
	ctx := context.Background()
	secrets := secretmanagerstore.NewMemoryStore()
	repl := userManaged(replica("us-east1", ""))
	sec := secretmanagerstore.Secret{ID: "s"}
	sec.SetReplication(repl)
	if err := secrets.CreateSecret(ctx, "p", "s", sec); err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}
	got, err := ParentReplication(ctx, secrets, "p", "s")
	if err != nil || got.UserManaged == nil || got.UserManaged.Replicas[0].Location != "us-east1" {
		t.Fatalf("ParentReplication = %#v, %v", got, err)
	}
	if _, err := ParentReplication(ctx, secrets, "p", "missing"); err == nil {
		t.Fatal("ParentReplication accepted a missing secret")
	}
}
