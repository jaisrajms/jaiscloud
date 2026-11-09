package secretmanager

import (
	"context"
	"testing"

	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"

	"jaiscloud/internal/gcp/store/kms"
)

// TestRegionalSecretGRPC covers the same RPCs with a location-scoped parent and
// resource names: the official clients reach regional secrets by pointing at the
// regional endpoint, but the messages carry the location.
func TestRegionalSecretGRPC(t *testing.T) {
	ctx := context.Background()
	client, kmsStore, cleanup := secretTestServiceWithKMS(t)
	defer cleanup()

	const project, location, ring, key = "test", "us-central1", "kr", "k"
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
	keyName := "projects/test/locations/us-central1/keyRings/kr/cryptoKeys/k"
	parent := "projects/test/locations/us-central1"
	wantName := parent + "/secrets/regional"

	created, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   parent,
		SecretId: "regional",
		Secret: &secretmanagerpb.Secret{
			Labels:                    map[string]string{"env": "conf"},
			CustomerManagedEncryption: &secretmanagerpb.CustomerManagedEncryption{KmsKeyName: keyName},
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.GetName() != wantName {
		t.Fatalf("name = %q, want %q", created.GetName(), wantName)
	}
	if created.GetReplication() != nil {
		t.Error("regional Secret must not carry a replication policy")
	}
	if created.GetCustomerManagedEncryption().GetKmsKeyName() != keyName {
		t.Errorf("customerManagedEncryption = %v, want %q", created.GetCustomerManagedEncryption(), keyName)
	}

	got, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: wantName})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.GetName() != wantName {
		t.Errorf("get name = %q, want %q", got.GetName(), wantName)
	}

	version, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent:  wantName,
		Payload: &secretmanagerpb.SecretPayload{Data: []byte("hello")},
	})
	if err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	if version.GetName() != wantName+"/versions/1" {
		t.Fatalf("version name = %q", version.GetName())
	}
	if version.GetReplicationStatus() != nil {
		t.Error("regional SecretVersion must not carry replicationStatus")
	}
	if version.GetCustomerManagedEncryption().GetKmsKeyVersionName() == "" {
		t.Error("regional SecretVersion customerManagedEncryption.kmsKeyVersionName is empty")
	}

	access, err := client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: wantName + "/versions/1"})
	if err != nil {
		t.Fatalf("access: %v", err)
	}
	if string(access.GetPayload().GetData()) != "hello" {
		t.Errorf("payload = %q, want hello", access.GetPayload().GetData())
	}

	// The project-level list must not include the regional secret; the
	// location-scoped list must.
	global, err := client.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{Parent: "projects/test"})
	if err != nil {
		t.Fatalf("global list: %v", err)
	}
	if len(global.GetSecrets()) != 0 {
		t.Errorf("global list returned %d regional secrets, want 0", len(global.GetSecrets()))
	}
	regional, err := client.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{Parent: parent})
	if err != nil {
		t.Fatalf("regional list: %v", err)
	}
	if len(regional.GetSecrets()) != 1 || regional.GetSecrets()[0].GetName() != wantName {
		t.Fatalf("regional list = %v, want [%s]", regional.GetSecrets(), wantName)
	}

	// A replication policy is rejected on a regional secret.
	if _, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent: parent, SecretId: "bad",
		Secret: &secretmanagerpb.Secret{Replication: &secretmanagerpb.Replication{
			Replication: &secretmanagerpb.Replication_Automatic_{Automatic: &secretmanagerpb.Replication_Automatic{}},
		}},
	}); err == nil {
		t.Error("regional create with replication: want InvalidArgument")
	}

	if _, err := client.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{Name: wantName}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: wantName}); err == nil {
		t.Error("get after delete: want NotFound")
	}
}
