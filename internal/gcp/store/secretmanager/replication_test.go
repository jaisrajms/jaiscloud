package secretmanager

import (
	"bytes"
	"context"
	"reflect"
	"testing"
)

// userManagedPolicy is a two-replica policy with one CMEK key, used by the
// snapshot and store tests.
func userManagedPolicy() *Replication {
	return &Replication{UserManaged: &UserManagedReplication{Replicas: []Replica{
		{Location: "us-east1", CustomerManagedEncryption: &CustomerManagedEncryption{
			KmsKeyName: "projects/p/locations/us-east1/keyRings/kr/cryptoKeys/k",
		}},
		{Location: "us-central1"},
	}}}
}

// TestSecretReplicationPolicy pins the store-side helpers: an explicit policy is
// the source of truth and SetReplication derives the effective wrapping key,
// while a legacy record (only kms_key_name) reads back as the automatic arm.
func TestSecretReplicationPolicy(t *testing.T) {
	policy := userManagedPolicy()

	sec := Secret{ID: "s"}
	sec.SetReplication(policy)
	if sec.Replication != policy {
		t.Fatalf("SetReplication stored %#v, want the policy", sec.Replication)
	}
	if want := "projects/p/locations/us-east1/keyRings/kr/cryptoKeys/k"; sec.KmsKeyName != want {
		t.Fatalf("effective key = %q, want %q", sec.KmsKeyName, want)
	}
	if got := sec.ReplicationPolicy(); got != policy {
		t.Fatalf("ReplicationPolicy = %#v, want the stored policy", got)
	}

	// Clearing the policy clears the effective key too: the default, automatic
	// replication with Google-managed encryption.
	sec.SetReplication(nil)
	if sec.Replication != nil || sec.KmsKeyName != "" {
		t.Fatalf("cleared secret = %+v, want no policy and no key", sec)
	}
	if got := sec.ReplicationPolicy(); got == nil || got.Automatic == nil || got.Automatic.CustomerManagedEncryption != nil {
		t.Fatalf("ReplicationPolicy after clear = %#v, want an empty automatic arm", got)
	}

	// A record written before the policy was modelled carries only the key.
	legacy := Secret{ID: "legacy", KmsKeyName: "projects/p/locations/global/keyRings/kr/cryptoKeys/k"}
	got := legacy.ReplicationPolicy()
	if got.Automatic == nil || got.Automatic.CustomerManagedEncryption == nil ||
		got.Automatic.CustomerManagedEncryption.KmsKeyName != legacy.KmsKeyName {
		t.Fatalf("legacy ReplicationPolicy = %#v, want the automatic arm with the stored key", got)
	}
	if empty := (Secret{ID: "empty"}).ReplicationPolicy(); empty.Automatic == nil || empty.Automatic.CustomerManagedEncryption != nil {
		t.Fatalf("keyless ReplicationPolicy = %#v, want an empty automatic arm", empty)
	}
}

// TestMemoryStoreSnapshotPreservesReplication verifies that the replication
// policy survives a Snapshot/Restore round trip, so a restored emulator does
// not rewrite a user-managed policy as automatic (DC45).
func TestMemoryStoreSnapshotPreservesReplication(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	policy := userManagedPolicy()
	um := Secret{ID: "um"}
	um.SetReplication(policy)
	if err := s.CreateSecret(ctx, "proj", "um", um); err != nil {
		t.Fatalf("create secret: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	dst := NewMemoryStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	restored, err := dst.GetSecret(ctx, "proj", "um")
	if err != nil {
		t.Fatalf("get restored secret: %v", err)
	}
	if !reflect.DeepEqual(restored.Replication, policy) {
		t.Fatalf("restored replication = %#v, want %#v", restored.Replication, policy)
	}
	if restored.KmsKeyName != policy.WrapKey() {
		t.Fatalf("restored effective key = %q, want %q", restored.KmsKeyName, policy.WrapKey())
	}
}
