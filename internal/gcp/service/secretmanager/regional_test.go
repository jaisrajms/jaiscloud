package secretmanager

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
)

func TestValidateRegionalCMEK(t *testing.T) {
	cases := []struct {
		name     string
		location string
		cme      *secretmanagerstore.CustomerManagedEncryption
		wantErr  bool
	}{
		{"nil is google-managed", "us-central1", nil, false},
		{"key in the secret's location", "us-central1",
			&secretmanagerstore.CustomerManagedEncryption{KmsKeyName: "projects/p/locations/us-central1/keyRings/kr/cryptoKeys/k"}, false},
		{"missing key", "us-central1", &secretmanagerstore.CustomerManagedEncryption{}, true},
		{"not a crypto key", "us-central1",
			&secretmanagerstore.CustomerManagedEncryption{KmsKeyName: "projects/p/keyRings/kr/cryptoKeys/k"}, true},
		{"key in another location", "us-central1",
			&secretmanagerstore.CustomerManagedEncryption{KmsKeyName: "projects/p/locations/europe-west1/keyRings/kr/cryptoKeys/k"}, true},
	}
	for _, tc := range cases {
		err := ValidateRegionalCMEK(tc.location, tc.cme)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: ValidateRegionalCMEK = %v, wantErr %v", tc.name, err, tc.wantErr)
		}
	}
}

func TestRegionalReplicationUnsupported(t *testing.T) {
	err := RegionalReplicationUnsupported()
	if err == nil {
		t.Fatal("want an error")
	}
	if got := err.Error(); got == "" {
		t.Error("want a non-empty message")
	}
}

func TestProjectCMEKUnsupported(t *testing.T) {
	if err := ProjectCMEKUnsupported(); err == nil {
		t.Fatal("want an error")
	}
}

func TestValidateRegionalCMEKKey(t *testing.T) {
	ctx := context.Background()
	kmsStore := kms.NewMemoryStore()
	enc := crypto.NewEnvelopeEncryptor(kmsStore)

	const project, location, ring, key = "proj", "us-central1", "kr", "k"
	if err := kmsStore.CreateKeyRing(ctx, project, location, ring, kms.KeyRing{ID: ring}); err != nil {
		t.Fatal(err)
	}
	if err := kmsStore.CreateCryptoKey(ctx, project, location, ring, key, kms.CryptoKey{ID: key, Purpose: "ENCRYPT_DECRYPT"}); err != nil {
		t.Fatal(err)
	}
	v, err := kmsStore.CreateVersion(ctx, project, location, ring, key, kms.Version{State: "ENABLED"})
	if err != nil {
		t.Fatal(err)
	}
	if err := kmsStore.UpdatePrimaryVersion(ctx, project, location, ring, key, v); err != nil {
		t.Fatal(err)
	}
	keyName := "projects/proj/locations/us-central1/keyRings/kr/cryptoKeys/k"

	if err := ValidateRegionalCMEKKey(ctx, enc, project, location, nil); err != nil {
		t.Errorf("nil CMEK: %v", err)
	}
	if err := ValidateRegionalCMEKKey(ctx, enc, project, location, &secretmanagerstore.CustomerManagedEncryption{KmsKeyName: keyName}); err != nil {
		t.Errorf("resolvable key: %v", err)
	}
	if err := ValidateRegionalCMEKKey(ctx, enc, project, location, &secretmanagerstore.CustomerManagedEncryption{
		KmsKeyName: "projects/proj/locations/us-central1/keyRings/kr/cryptoKeys/missing",
	}); err == nil {
		t.Error("missing key: want an error")
	}
}
