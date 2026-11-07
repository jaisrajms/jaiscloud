package dataproc

import (
	"context"
	"testing"

	kmsstore "jaiscloud/internal/gcp/store/kms"
)

// kmsProvider returns a Dataproc core wired to a KMS store holding a symmetric
// key in us-central1 plus the key's resource name.
func kmsProvider(t *testing.T) (*Service, string) {
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
	p := newProvider(t)
	p.SetKMSStore(keys)
	return p, "projects/proj/locations/us-central1/keyRings/ring/cryptoKeys/key"
}

func clusterWithKMS(key string) ClusterInput {
	return ClusterInputFromMap(map[string]any{
		"config": map[string]any{"encryptionConfig": map[string]any{"kmsKey": key}},
	})
}

// TestCreateClusterKMSReference covers FC4-1 for Dataproc: a cluster's
// encryptionConfig.kmsKey is resolved against the KMS store (both transports
// share this core), so an existing same-region ENCRYPT_DECRYPT key is accepted
// and a missing, cross-region or signing key is INVALID_ARGUMENT.
func TestCreateClusterKMSReference(t *testing.T) {
	ctx := context.Background()
	p, key := kmsProvider(t)

	if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "cmek-ok", clusterWithKMS(key)); err != nil {
		t.Fatalf("accept: %v", err)
	}
	for name, k := range map[string]string{
		"missing":      "projects/proj/locations/us-central1/keyRings/ring/cryptoKeys/absent",
		"other-region": "projects/proj/locations/europe-west1/keyRings/ring/cryptoKeys/key",
	} {
		if _, _, err := p.CreateCluster(ctx, "proj", "us-central1", "cmek-"+name, clusterWithKMS(k)); err == nil {
			t.Fatalf("%s key: create succeeded, want INVALID_ARGUMENT", name)
		} else {
			wantProviderStatus(t, err, 400)
		}
	}
}

// TestWorkflowTemplateKMSReference is the WorkflowTemplate half of FC4-1.
func TestWorkflowTemplateKMSReference(t *testing.T) {
	ctx := context.Background()
	p, key := kmsProvider(t)

	def := validTemplateDef("t1")
	def["encryptionConfig"] = map[string]any{"kmsKey": key}
	if _, err := p.CreateWorkflowTemplate(ctx, "proj", "us-central1", WorkflowTemplateInput{ID: "t1", Definition: def}); err != nil {
		t.Fatalf("accept: %v", err)
	}

	bad := validTemplateDef("t2")
	bad["encryptionConfig"] = map[string]any{"kmsKey": "projects/proj/locations/us-central1/keyRings/ring/cryptoKeys/absent"}
	if _, err := p.CreateWorkflowTemplate(ctx, "proj", "us-central1", WorkflowTemplateInput{ID: "t2", Definition: bad}); err == nil {
		t.Fatal("missing key: create succeeded, want INVALID_ARGUMENT")
	} else {
		wantProviderStatus(t, err, 400)
	}
}
