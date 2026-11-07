package functions

import (
	"context"
	"testing"

	functionsstore "jaiscloud/internal/gcp/store/functions"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/store"
)

// kmsFunctionProvider returns a functions core wired to a KMS store holding a
// symmetric key in us-central1 plus the key's resource name.
func kmsFunctionProvider(t *testing.T) (*Service, string) {
	t.Helper()
	ctx := context.Background()
	keys := kmsstore.NewMemoryStore()
	if err := keys.CreateKeyRing(ctx, "proj", "us-central1", "ring", kmsstore.KeyRing{ID: "ring"}); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if err := keys.CreateCryptoKey(ctx, "proj", "us-central1", "ring", "key",
		kmsstore.CryptoKey{ID: "key", Purpose: "ENCRYPT_DECRYPT"}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	s := NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore())
	s.SetKMSStore(keys)
	return s, "projects/proj/locations/us-central1/keyRings/ring/cryptoKeys/key"
}

func functionWithKMS(key string) FunctionInput {
	return FunctionInputFromMap(map[string]any{
		"buildConfig": map[string]any{"runtime": "nodejs20"},
		"kmsKeyName":  key,
	}, V2)
}

// TestFunctionKMSReference covers FC4-1 for Cloud Functions: kmsKeyName is
// resolved against the KMS store at create/update. An existing same-region
// ENCRYPT_DECRYPT key is accepted; a missing or cross-region key is
// INVALID_ARGUMENT.
func TestFunctionKMSReference(t *testing.T) {
	ctx := context.Background()
	s, key := kmsFunctionProvider(t)

	if _, _, err := s.CreateFunction(ctx, "proj", "us-central1", "fn-ok", functionWithKMS(key), V2); err != nil {
		t.Fatalf("accept: %v", err)
	}
	if _, _, err := s.CreateFunction(ctx, "proj", "us-central1", "fn-missing", functionWithKMS(
		"projects/proj/locations/us-central1/keyRings/ring/cryptoKeys/absent"), V2); err == nil {
		t.Fatal("missing key: create succeeded, want INVALID_ARGUMENT")
	}
	if _, _, err := s.CreateFunction(ctx, "proj", "us-central1", "fn-region", functionWithKMS(
		"projects/proj/locations/europe-west1/keyRings/ring/cryptoKeys/key"), V2); err == nil {
		t.Fatal("cross-region key: create succeeded, want INVALID_ARGUMENT")
	}

	// An update that introduces a bad key is rejected before it is persisted.
	if _, _, err := s.UpdateFunction(ctx, "proj", "us-central1", "fn-ok",
		functionWithKMS("projects/proj/locations/us-central1/keyRings/ring/cryptoKeys/absent"),
		[]string{"kmsKeyName"}, V2); err == nil {
		t.Fatal("update with missing key succeeded, want INVALID_ARGUMENT")
	}
	stored, err := s.GetFunction(ctx, "proj", "us-central1", "fn-ok")
	if err != nil {
		t.Fatalf("GetFunction: %v", err)
	}
	if stored.KmsKeyName != key {
		t.Fatalf("stored kmsKeyName = %q, want the original %q", stored.KmsKeyName, key)
	}
}
