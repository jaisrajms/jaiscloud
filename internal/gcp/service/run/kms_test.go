package run

import (
	"context"
	"testing"

	kmsstore "jaiscloud/internal/gcp/store/kms"
)

// kmsRunService returns a Cloud Run core wired to a KMS store holding a
// symmetric key in us-central1 plus the key's resource name.
func kmsRunService(t *testing.T) (*Service, string) {
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
	s := newTestService()
	s.SetKMSStore(keys)
	return s, "projects/proj/locations/us-central1/keyRings/ring/cryptoKeys/key"
}

func runRequestWithKMS(key string) map[string]any {
	body := createRequest()
	body["template"].(map[string]any)["encryptionKey"] = key
	return body
}

// TestRunServiceKMSReference covers FC4-1 for Cloud Run: template.encryptionKey
// is resolved against the KMS store at create/update, so an existing
// same-region key is accepted and a missing or cross-region key is
// INVALID_ARGUMENT.
func TestRunServiceKMSReference(t *testing.T) {
	ctx := context.Background()
	s, key := kmsRunService(t)

	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc-ok", runRequestWithKMS(key), false); err != nil {
		t.Fatalf("accept: %v", err)
	}
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc-missing", runRequestWithKMS(
		"projects/proj/locations/us-central1/keyRings/ring/cryptoKeys/absent"), false); err == nil {
		t.Fatal("missing key: create succeeded, want INVALID_ARGUMENT")
	}
	if _, err := s.CreateService(ctx, "proj", "us-central1", "svc-region", runRequestWithKMS(
		"projects/proj/locations/europe-west1/keyRings/ring/cryptoKeys/key"), false); err == nil {
		t.Fatal("cross-region key: create succeeded, want INVALID_ARGUMENT")
	}

	// A template update that introduces a bad key is rejected before the store
	// write.
	if _, err := s.UpdateService(ctx, "proj", "us-central1", "svc-ok",
		runRequestWithKMS("projects/proj/locations/us-central1/keyRings/ring/cryptoKeys/absent"),
		"template", false, false); err == nil {
		t.Fatal("update with missing key succeeded, want INVALID_ARGUMENT")
	}
	svc, err := s.GetService(ctx, "proj", "us-central1", "svc-ok")
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}
	if got := encryptionKeyFromService(svc.Data); got != key {
		t.Fatalf("stored encryptionKey = %q, want the original %q", got, key)
	}
}
