package secretmanager

import (
	"context"
	"testing"

	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"jaiscloud/internal/gcp/store/kms"
)

// newGRPCTestKey creates an enabled CryptoKey with one primary version and
// returns its resource name plus that version number.
func newGRPCTestKey(t *testing.T, ks kms.Store, project, location, ring, keyID string) (string, string) {
	t.Helper()
	ctx := context.Background()
	if err := ks.CreateKeyRing(ctx, project, location, ring, kms.KeyRing{ID: ring}); err != nil && err != kms.ErrAlreadyExists {
		t.Fatalf("create key ring %s/%s: %v", location, ring, err)
	}
	if err := ks.CreateCryptoKey(ctx, project, location, ring, keyID, kms.CryptoKey{ID: keyID, Purpose: "ENCRYPT_DECRYPT"}); err != nil {
		t.Fatalf("create crypto key %s: %v", keyID, err)
	}
	version, err := ks.CreateVersion(ctx, project, location, ring, keyID, kms.Version{State: "ENABLED"})
	if err != nil {
		t.Fatalf("create key version %s: %v", keyID, err)
	}
	if err := ks.UpdatePrimaryVersion(ctx, project, location, ring, keyID, version); err != nil {
		t.Fatalf("set primary %s: %v", keyID, err)
	}
	return "projects/" + project + "/locations/" + location + "/keyRings/" + ring + "/cryptoKeys/" + keyID, version
}

// TestSecretManagerUserManagedReplication covers DC45 on the gRPC surface: a
// secret created with a user-managed replication policy must echo that policy
// back through the official client types (arm, replica locations and
// per-replica keys) on create/get/list/update, and its versions must report a
// user-managed replicationStatus. Both transports share the store, so a policy
// written over REST has to read back identically here.
func TestSecretManagerUserManagedReplication(t *testing.T) {
	client, ks, cleanup := secretTestServiceWithKMS(t)
	defer cleanup()
	ctx := context.Background()

	const (
		name    = "projects/test/secrets/um-secret"
		east    = "us-east1"
		central = "us-central1"
	)
	keyEast, versionEast := newGRPCTestKey(t, ks, "test", east, "kr", "key-east")
	keyEast2, _ := newGRPCTestKey(t, ks, "test", east, "kr", "key-east-2")

	policy := func(eastKey string) *secretmanagerpb.Replication {
		first := &secretmanagerpb.Replication_UserManaged_Replica{Location: east}
		if eastKey != "" {
			first.CustomerManagedEncryption = &secretmanagerpb.CustomerManagedEncryption{KmsKeyName: eastKey}
		}
		return &secretmanagerpb.Replication{
			Replication: &secretmanagerpb.Replication_UserManaged_{
				UserManaged: &secretmanagerpb.Replication_UserManaged{
					Replicas: []*secretmanagerpb.Replication_UserManaged_Replica{
						first,
						{Location: central},
					},
				},
			},
		}
	}
	assertPolicy := func(what string, sec *secretmanagerpb.Secret, eastKey string) {
		t.Helper()
		replicas := sec.GetReplication().GetUserManaged().GetReplicas()
		if len(replicas) != 2 {
			t.Fatalf("%s: replicas = %v, want 2", what, replicas)
		}
		if replicas[0].GetLocation() != east || replicas[1].GetLocation() != central {
			t.Fatalf("%s: locations = [%q %q], want [%q %q]", what, replicas[0].GetLocation(), replicas[1].GetLocation(), east, central)
		}
		if got := replicas[0].GetCustomerManagedEncryption().GetKmsKeyName(); got != eastKey {
			t.Fatalf("%s: %s key = %q, want %q", what, east, got, eastKey)
		}
		if replicas[1].GetCustomerManagedEncryption() != nil {
			t.Fatalf("%s: %s cmek = %v, want none", what, central, replicas[1].GetCustomerManagedEncryption())
		}
	}

	created, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent: "projects/test", SecretId: "um-secret", Secret: &secretmanagerpb.Secret{Replication: policy(keyEast)},
	})
	if err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}
	assertPolicy("CreateSecret", created, keyEast)

	got, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: name})
	if err != nil {
		t.Fatalf("GetSecret: %v", err)
	}
	assertPolicy("GetSecret", got, keyEast)

	listed, err := client.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{Parent: "projects/test"})
	if err != nil {
		t.Fatalf("ListSecrets: %v", err)
	}
	assertPolicy("ListSecrets", listed.GetSecrets()[0], keyEast)

	// A version reports one status per replica; the CryptoKeyVersion is on the
	// replica whose location wrapped the payload.
	added, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent: name, Payload: &secretmanagerpb.SecretPayload{Data: []byte("payload")},
	})
	if err != nil {
		t.Fatalf("AddSecretVersion: %v", err)
	}
	assertVersionReplicas := func(what string, v *secretmanagerpb.SecretVersion, wantKeyVersion string) {
		t.Helper()
		replicas := v.GetReplicationStatus().GetUserManaged().GetReplicas()
		if len(replicas) != 2 {
			t.Fatalf("%s: replicationStatus replicas = %v, want 2", what, replicas)
		}
		if replicas[0].GetLocation() != east || replicas[1].GetLocation() != central {
			t.Fatalf("%s: replicationStatus locations = [%q %q]", what, replicas[0].GetLocation(), replicas[1].GetLocation())
		}
		if got := replicas[0].GetCustomerManagedEncryption().GetKmsKeyVersionName(); got != wantKeyVersion {
			t.Fatalf("%s: %s kmsKeyVersionName = %q, want %q", what, east, got, wantKeyVersion)
		}
		if replicas[1].GetCustomerManagedEncryption() != nil {
			t.Fatalf("%s: %s status cmek = %v, want none", what, central, replicas[1].GetCustomerManagedEncryption())
		}
	}
	wantKeyVersion := keyEast + "/versions/" + versionEast
	assertVersionReplicas("AddSecretVersion", added, wantKeyVersion)
	read, err := client.GetSecretVersion(ctx, &secretmanagerpb.GetSecretVersionRequest{Name: added.GetName()})
	if err != nil {
		t.Fatalf("GetSecretVersion: %v", err)
	}
	assertVersionReplicas("GetSecretVersion", read, wantKeyVersion)

	// Changing a replica's key is the only supported policy update; it is echoed
	// and leaves existing versions naming the key that wrapped them.
	updated, err := client.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{
		Secret:     &secretmanagerpb.Secret{Name: name, Replication: policy(keyEast2)},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"replication"}},
	})
	if err != nil {
		t.Fatalf("UpdateSecret: %v", err)
	}
	assertPolicy("UpdateSecret", updated, keyEast2)
	read, err = client.GetSecretVersion(ctx, &secretmanagerpb.GetSecretVersionRequest{Name: added.GetName()})
	if err != nil {
		t.Fatalf("GetSecretVersion after update: %v", err)
	}
	assertVersionReplicas("GetSecretVersion after update", read, wantKeyVersion)

	// Switching the arm is rejected: the policy's shape is immutable.
	if _, err := client.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{
		Secret: &secretmanagerpb.Secret{Name: name, Replication: &secretmanagerpb.Replication{
			Replication: &secretmanagerpb.Replication_Automatic_{Automatic: &secretmanagerpb.Replication_Automatic{}},
		}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"replication"}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("switching to automatic = %v, want InvalidArgument", err)
	}

	// The documented negatives are surfaced through the typed status.
	for _, tc := range []struct {
		id   string
		what string
		repl *secretmanagerpb.Replication
	}{
		{"empty-replicas", "empty replicas", &secretmanagerpb.Replication{
			Replication: &secretmanagerpb.Replication_UserManaged_{UserManaged: &secretmanagerpb.Replication_UserManaged{}},
		}},
		{"replica-key-location", "replica key outside the replica location", &secretmanagerpb.Replication{
			Replication: &secretmanagerpb.Replication_UserManaged_{UserManaged: &secretmanagerpb.Replication_UserManaged{
				Replicas: []*secretmanagerpb.Replication_UserManaged_Replica{{
					Location:                  east,
					CustomerManagedEncryption: &secretmanagerpb.CustomerManagedEncryption{KmsKeyName: "projects/test/locations/" + central + "/keyRings/kr/cryptoKeys/k"},
				}},
			}},
		}},
		{"automatic-key-global", "automatic key outside global", &secretmanagerpb.Replication{
			Replication: &secretmanagerpb.Replication_Automatic_{Automatic: &secretmanagerpb.Replication_Automatic{
				CustomerManagedEncryption: &secretmanagerpb.CustomerManagedEncryption{KmsKeyName: "projects/test/locations/" + east + "/keyRings/kr/cryptoKeys/k"},
			}},
		}},
	} {
		_, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
			Parent: "projects/test", SecretId: tc.id, Secret: &secretmanagerpb.Secret{Replication: tc.repl},
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: CreateSecret = %v, want InvalidArgument", tc.what, err)
		}
	}
}

// TestSecretManagerPerReplicaDEKsGRPC covers DC57 on the gRPC surface: a
// version added to a user-managed secret whose replicas name different CMEK
// keys reports each replica's own CryptoKeyVersion (the emulator used to wrap
// only under the first replica's key), and the payload stays readable.
func TestSecretManagerPerReplicaDEKsGRPC(t *testing.T) {
	client, ks, cleanup := secretTestServiceWithKMS(t)
	defer cleanup()
	ctx := context.Background()

	const (
		name    = "projects/test/secrets/per-replica"
		east    = "us-east1"
		central = "us-central1"
	)
	keyEast, versionEast := newGRPCTestKey(t, ks, "test", east, "kr", "key-east")
	keyCentral, versionCentral := newGRPCTestKey(t, ks, "test", central, "kr", "key-central")

	if _, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent: "projects/test", SecretId: "per-replica",
		Secret: &secretmanagerpb.Secret{Replication: &secretmanagerpb.Replication{
			Replication: &secretmanagerpb.Replication_UserManaged_{
				UserManaged: &secretmanagerpb.Replication_UserManaged{
					Replicas: []*secretmanagerpb.Replication_UserManaged_Replica{
						{Location: east, CustomerManagedEncryption: &secretmanagerpb.CustomerManagedEncryption{KmsKeyName: keyEast}},
						{Location: central, CustomerManagedEncryption: &secretmanagerpb.CustomerManagedEncryption{KmsKeyName: keyCentral}},
						{Location: "us-west1"},
					},
				},
			},
		}},
	}); err != nil {
		t.Fatalf("CreateSecret: %v", err)
	}

	added, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent: name, Payload: &secretmanagerpb.SecretPayload{Data: []byte("payload")},
	})
	if err != nil {
		t.Fatalf("AddSecretVersion: %v", err)
	}
	replicas := added.GetReplicationStatus().GetUserManaged().GetReplicas()
	if len(replicas) != 3 {
		t.Fatalf("replicationStatus replicas = %v, want 3", replicas)
	}
	if got, want := replicas[0].GetCustomerManagedEncryption().GetKmsKeyVersionName(), keyEast+"/versions/"+versionEast; got != want {
		t.Fatalf("%s kmsKeyVersionName = %q, want %q", east, got, want)
	}
	if got, want := replicas[1].GetCustomerManagedEncryption().GetKmsKeyVersionName(), keyCentral+"/versions/"+versionCentral; got != want {
		t.Fatalf("%s kmsKeyVersionName = %q, want %q", central, got, want)
	}
	if replicas[2].GetCustomerManagedEncryption() != nil {
		t.Fatalf("Google-managed replica status = %v, want none", replicas[2].GetCustomerManagedEncryption())
	}

	accessed, err := client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: added.GetName()})
	if err != nil {
		t.Fatalf("AccessSecretVersion: %v", err)
	}
	if string(accessed.GetPayload().GetData()) != "payload" {
		t.Fatalf("payload = %q, want %q", accessed.GetPayload().GetData(), "payload")
	}
}

// TestSecretManagerCreateValidateCMEKGRPC covers DC58 on the gRPC surface:
// CreateSecret/UpdateSecret resolve every CMEK key, so a missing key answers
// InvalidArgument and a disabled primary version FailedPrecondition at the
// secret write, not only at AddSecretVersion.
func TestSecretManagerCreateValidateCMEKGRPC(t *testing.T) {
	client, ks, cleanup := secretTestServiceWithKMS(t)
	defer cleanup()
	ctx := context.Background()

	globalKey, _ := newGRPCTestKey(t, ks, "test", "global", "kr", "global-key")
	disabledKey, _ := newGRPCTestKey(t, ks, "test", "global", "kr", "disabled-key")
	disabled, err := ks.GetCryptoKey(ctx, "test", "global", "kr", "disabled-key")
	if err != nil {
		t.Fatalf("GetCryptoKey: %v", err)
	}
	if err := ks.UpdateVersionState(ctx, "test", "global", "kr", "disabled-key", disabled.PrimaryVersion, "DISABLED"); err != nil {
		t.Fatalf("disable key version: %v", err)
	}

	auto := func(key string) *secretmanagerpb.Secret {
		return &secretmanagerpb.Secret{Replication: &secretmanagerpb.Replication{
			Replication: &secretmanagerpb.Replication_Automatic_{
				Automatic: &secretmanagerpb.Replication_Automatic{
					CustomerManagedEncryption: &secretmanagerpb.CustomerManagedEncryption{KmsKeyName: key},
				},
			},
		}}
	}

	missing := "projects/test/locations/global/keyRings/kr/cryptoKeys/nope"
	if _, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent: "projects/test", SecretId: "missing-key", Secret: auto(missing),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("create with a missing key = %v, want InvalidArgument", err)
	}
	if _, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent: "projects/test", SecretId: "disabled-key", Secret: auto(disabledKey),
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("create with a disabled key = %v, want FailedPrecondition", err)
	}

	if _, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent: "projects/test", SecretId: "ok", Secret: auto(globalKey),
	}); err != nil {
		t.Fatalf("create with a valid key: %v", err)
	}
	if _, err := client.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{
		Secret:     &secretmanagerpb.Secret{Name: "projects/test/secrets/ok", Replication: auto(missing).Replication},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"replication"}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("patch to a missing key = %v, want InvalidArgument", err)
	}
	got, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: "projects/test/secrets/ok"})
	if err != nil {
		t.Fatalf("GetSecret: %v", err)
	}
	if key := got.GetReplication().GetAutomatic().GetCustomerManagedEncryption().GetKmsKeyName(); key != globalKey {
		t.Fatalf("stored key after rejected patch = %q, want %q", key, globalKey)
	}
}
