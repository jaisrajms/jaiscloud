package secretmanager

import (
	"strings"
	"testing"

	"jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
)

// versionedDEK builds a wrapped-DEK blob tagged with a CryptoKeyVersion, the
// shape the encryptor stores.
func versionedDEK(t *testing.T, version string) []byte {
	t.Helper()
	return kms.EncodeVersionedCiphertext(version, []byte("ciphertext"))
}

func cme(key string) *secretmanagerstore.CustomerManagedEncryption {
	if key == "" {
		return nil
	}
	return &secretmanagerstore.CustomerManagedEncryption{KmsKeyName: key}
}

func auto(key string) *secretmanagerstore.Replication {
	return &secretmanagerstore.Replication{
		Automatic: &secretmanagerstore.AutomaticReplication{CustomerManagedEncryption: cme(key)},
	}
}

func userManaged(replicas ...secretmanagerstore.Replica) *secretmanagerstore.Replication {
	return &secretmanagerstore.Replication{
		UserManaged: &secretmanagerstore.UserManagedReplication{Replicas: replicas},
	}
}

func replica(location, key string) secretmanagerstore.Replica {
	return secretmanagerstore.Replica{Location: location, CustomerManagedEncryption: cme(key)}
}

// TestValidateReplication pins the documented shape of the replication oneof:
// one arm only, non-empty and distinct user-managed locations, and a CMEK key
// that resides where the arm requires (global for automatic, the replica's own
// location for user-managed). Both transports call it, so this is the single
// contract.
func TestValidateReplication(t *testing.T) {
	const (
		globalKey    = "projects/p/locations/global/keyRings/kr/cryptoKeys/k"
		usEastKey    = "projects/p/locations/us-east1/keyRings/kr/cryptoKeys/k"
		usCentralKey = "projects/p/locations/us-central1/keyRings/kr/cryptoKeys/k"
	)

	valid := []struct {
		name string
		repl *secretmanagerstore.Replication
	}{
		{"nil defaults to automatic", nil},
		{"automatic without cmek", auto("")},
		{"automatic with global cmek", auto(globalKey)},
		{"one user-managed replica without cmek", userManaged(replica("us-east1", ""))},
		{"replica cmek in the replica location", userManaged(replica("us-east1", usEastKey))},
		{"mixed replicas", userManaged(replica("us-east1", usEastKey), replica("us-central1", ""))},
	}
	for _, tc := range valid {
		if err := ValidateReplication(tc.repl); err != nil {
			t.Errorf("%s: ValidateReplication = %v, want nil", tc.name, err)
		}
	}

	both := auto(globalKey)
	both.UserManaged = &secretmanagerstore.UserManagedReplication{Replicas: []secretmanagerstore.Replica{replica("us-east1", "")}}

	invalid := []struct {
		name string
		repl *secretmanagerstore.Replication
		want string
	}{
		{"both arms", both, "exactly one of automatic or userManaged"},
		{"no arm", &secretmanagerstore.Replication{}, "exactly one of automatic or userManaged"},
		{"automatic cmek without key", &secretmanagerstore.Replication{Automatic: &secretmanagerstore.AutomaticReplication{CustomerManagedEncryption: &secretmanagerstore.CustomerManagedEncryption{}}}, "kmsKeyName is required"},
		{"automatic cmek outside global", auto(usCentralKey), "must reside in global"},
		{"automatic cmek not a crypto key name", auto("not-a-key"), "is not a Cloud KMS CryptoKey name"},
		{"user-managed with no replicas", userManaged(), "cannot be empty"},
		{"replica without location", userManaged(replica("", "")), "location is required"},
		{"replica in an unknown location", userManaged(replica("us-east-1", "")), "is not a supported location"},
		{"replica in global", userManaged(replica("global", "")), "is not a supported location"},
		{"duplicate location", userManaged(replica("us-east1", ""), replica("us-east1", usEastKey)), "duplicate location"},
		{
			"replica cmek without key",
			userManaged(secretmanagerstore.Replica{
				Location:                  "us-east1",
				CustomerManagedEncryption: &secretmanagerstore.CustomerManagedEncryption{},
			}),
			"kmsKeyName is required",
		},
		{"replica cmek in another location", userManaged(replica("us-east1", usCentralKey)), "must reside in replica location"},
		{"replica cmek not a crypto key name", userManaged(replica("us-east1", "not-a-key")), "is not a Cloud KMS CryptoKey name"},
	}
	for _, tc := range invalid {
		err := ValidateReplication(tc.repl)
		if err == nil {
			t.Errorf("%s: ValidateReplication = nil, want an error", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error = %q, want it to contain %q", tc.name, err.Error(), tc.want)
		}
	}
}

// TestMergeReplication pins the update semantics: the oneof arm and the
// user-managed replica locations are immutable, only the CMEK configuration
// changes (including clearing it), and a nil incoming policy is a no-op.
func TestMergeReplication(t *testing.T) {
	const (
		globalKey  = "projects/p/locations/global/keyRings/kr/cryptoKeys/k"
		globalKey2 = "projects/p/locations/global/keyRings/kr/cryptoKeys/k2"
		usEastKey  = "projects/p/locations/us-east1/keyRings/kr/cryptoKeys/k"
		usEastKey2 = "projects/p/locations/us-east1/keyRings/kr/cryptoKeys/k2"
	)
	storedAuto := auto(globalKey)
	storedUser := userManaged(replica("us-east1", usEastKey), replica("us-central1", ""))

	// No incoming policy leaves the stored one alone.
	if got, err := MergeReplication(storedAuto, nil); err != nil || got != storedAuto {
		t.Fatalf("MergeReplication(stored, nil) = %v, %v; want the stored policy", got, err)
	}
	// A record written before the policy was modelled is replaced wholesale.
	if got, err := MergeReplication(nil, storedAuto); err != nil || got != storedAuto {
		t.Fatalf("MergeReplication(nil, incoming) = %v, %v; want incoming", got, err)
	}

	// Automatic: the key is replaced, including clearing it (GCP's documented
	// "remove CMEK configuration" form).
	got, err := MergeReplication(storedAuto, auto(globalKey2))
	if err != nil {
		t.Fatalf("automatic key update: %v", err)
	}
	if key := got.Automatic.CustomerManagedEncryption; key == nil || key.KmsKeyName != globalKey2 {
		t.Fatalf("automatic key = %#v, want %q", key, globalKey2)
	}
	got, err = MergeReplication(storedAuto, auto(""))
	if err != nil {
		t.Fatalf("automatic cmek clear: %v", err)
	}
	if got.Automatic.CustomerManagedEncryption != nil {
		t.Fatalf("automatic cmek clear = %#v, want nil", got.Automatic.CustomerManagedEncryption)
	}

	// The arm itself is immutable.
	if _, err := MergeReplication(storedAuto, userManaged(replica("us-east1", ""))); err == nil || !strings.Contains(err.Error(), "cannot be changed") {
		t.Fatalf("automatic -> user-managed = %v, want an immutability error", err)
	}
	if _, err := MergeReplication(storedUser, auto("")); err == nil || !strings.Contains(err.Error(), "cannot be changed") {
		t.Fatalf("user-managed -> automatic = %v, want an immutability error", err)
	}

	// User-managed: the replica key changes in place, the stored order is kept,
	// and the replica that had no CMEK stays Google-managed.
	got, err = MergeReplication(storedUser, userManaged(replica("us-central1", ""), replica("us-east1", usEastKey2)))
	if err != nil {
		t.Fatalf("user-managed key update: %v", err)
	}
	if len(got.UserManaged.Replicas) != 2 || got.UserManaged.Replicas[0].Location != "us-east1" {
		t.Fatalf("user-managed replicas = %#v, want the stored order (us-east1 first)", got.UserManaged.Replicas)
	}
	if key := got.UserManaged.Replicas[0].CustomerManagedEncryption; key == nil || key.KmsKeyName != usEastKey2 {
		t.Fatalf("us-east1 key = %#v, want %q", key, usEastKey2)
	}
	if key := got.UserManaged.Replicas[1].CustomerManagedEncryption; key != nil {
		t.Fatalf("us-central1 key = %#v, want nil", key)
	}

	// The replica set is immutable: renaming or adding a location is rejected.
	if _, err := MergeReplication(storedUser, userManaged(replica("us-west1", ""), replica("us-central1", ""))); err == nil || !strings.Contains(err.Error(), "replica locations") {
		t.Fatalf("location rename = %v, want a locations error", err)
	}
	if _, err := MergeReplication(storedUser, userManaged(replica("us-east1", usEastKey), replica("us-central1", ""), replica("us-west1", ""))); err == nil || !strings.Contains(err.Error(), "replica locations") {
		t.Fatalf("location add = %v, want a locations error", err)
	}

	// Validation still runs on the incoming policy.
	if _, err := MergeReplication(storedUser, userManaged()); err == nil {
		t.Fatal("MergeReplication accepted an empty replica list")
	}
}

// TestReplicationWrapKey pins which key a new version is wrapped with: the
// automatic arm's, or the first user-managed replica that names one.
func TestReplicationWrapKey(t *testing.T) {
	const (
		key1 = "projects/p/locations/us-east1/keyRings/kr/cryptoKeys/k1"
		key2 = "projects/p/locations/us-central1/keyRings/kr/cryptoKeys/k2"
	)
	if got := (*secretmanagerstore.Replication)(nil).WrapKey(); got != "" {
		t.Errorf("nil policy WrapKey = %q, want empty", got)
	}
	if got := auto("").WrapKey(); got != "" {
		t.Errorf("automatic without cmek WrapKey = %q, want empty", got)
	}
	if got := auto(key1).WrapKey(); got != key1 {
		t.Errorf("automatic WrapKey = %q, want %q", got, key1)
	}
	if got := userManaged(replica("us-east1", ""), replica("us-central1", key2)).WrapKey(); got != key2 {
		t.Errorf("user-managed WrapKey = %q, want %q", got, key2)
	}
	if got := userManaged(replica("us-east1", ""), replica("us-central1", "")).WrapKey(); got != "" {
		t.Errorf("user-managed without cmek WrapKey = %q, want empty", got)
	}
}

// TestVersionReplicaStatuses pins the per-replica status: every location is
// reported, and every CMEK replica carries the CryptoKeyVersion that wrapped
// its own copy of the DEK (DC57). A legacy version with no per-replica wraps
// falls back to naming the primary wrap's location only.
func TestVersionReplicaStatuses(t *testing.T) {
	const (
		usEastKey    = "projects/p/locations/us-east1/keyRings/kr/cryptoKeys/k"
		usCentralKey = "projects/p/locations/us-central1/keyRings/kr/cryptoKeys/k"
	)
	usEastWrapped := versionedDEK(t, "7")
	usCentralWrapped := versionedDEK(t, "3")
	repl := userManaged(replica("us-east1", usEastKey), replica("us-central1", usCentralKey), replica("us-west1", ""))

	if got := VersionReplicaStatuses(VersionWrap{KmsKeyName: usEastKey, WrappedDEK: usEastWrapped}, auto("")); got != nil {
		t.Fatalf("automatic policy statuses = %#v, want nil", got)
	}

	// A version that recorded one wrap per CMEK replica: both CMEK replicas
	// report their own key version, the Google-managed replica reports none.
	got := VersionReplicaStatuses(VersionWrap{
		KmsKeyName: usEastKey, WrappedDEK: usEastWrapped,
		ReplicaDEKs: []secretmanagerstore.ReplicaDEK{
			{Location: "us-east1", KmsKeyName: usEastKey, WrappedDEK: usEastWrapped},
			{Location: "us-central1", KmsKeyName: usCentralKey, WrappedDEK: usCentralWrapped},
		},
	}, repl)
	if len(got) != 3 {
		t.Fatalf("statuses = %#v, want 3 replicas", got)
	}
	if got[0].Location != "us-east1" || got[0].KmsKeyVersionName != usEastKey+"/versions/7" {
		t.Errorf("us-east1 status = %#v, want the us-east1 key version", got[0])
	}
	if got[1].Location != "us-central1" || got[1].KmsKeyVersionName != usCentralKey+"/versions/3" {
		t.Errorf("us-central1 status = %#v, want the us-central1 key version", got[1])
	}
	if got[2].KmsKeyVersionName != "" {
		t.Errorf("Google-managed replica status = %#v, want no kmsKeyVersionName", got[2])
	}

	// A version written before per-replica wraps existed: only the replica in
	// the primary wrap's location carries a status.
	legacy := VersionReplicaStatuses(VersionWrap{KmsKeyName: usCentralKey, WrappedDEK: usCentralWrapped}, repl)
	if legacy[0].KmsKeyVersionName != "" || legacy[1].KmsKeyVersionName != usCentralKey+"/versions/3" || legacy[2].KmsKeyVersionName != "" {
		t.Errorf("legacy statuses = %#v, want only us-central1", legacy)
	}
}

// TestReplicationCMEKBindings pins the per-replica key bindings a version is
// wrapped under: the automatic arm is one global binding, a user-managed policy
// yields one binding per CMEK replica in replica order.
func TestReplicationCMEKBindings(t *testing.T) {
	const (
		globalKey    = "projects/p/locations/global/keyRings/kr/cryptoKeys/k"
		usEastKey    = "projects/p/locations/us-east1/keyRings/kr/cryptoKeys/k"
		usCentralKey = "projects/p/locations/us-central1/keyRings/kr/cryptoKeys/k"
	)
	if got := (*secretmanagerstore.Replication)(nil).CMEKBindings(); got != nil {
		t.Errorf("nil policy bindings = %#v, want nil", got)
	}
	if got := auto("").CMEKBindings(); got != nil {
		t.Errorf("automatic without cmek bindings = %#v, want nil", got)
	}
	if got := auto(globalKey).CMEKBindings(); len(got) != 1 || got[0].Location != "global" || got[0].KmsKeyName != globalKey {
		t.Errorf("automatic bindings = %#v, want one global binding", got)
	}
	got := userManaged(replica("us-east1", usEastKey), replica("us-central1", ""), replica("us-west1", "")).CMEKBindings()
	if len(got) != 1 || got[0].Location != "us-east1" || got[0].KmsKeyName != usEastKey {
		t.Errorf("user-managed bindings = %#v, want the us-east1 binding", got)
	}
	// CMEK replicas that are not first are still bound.
	got = userManaged(replica("us-east1", ""), replica("us-central1", usCentralKey)).CMEKBindings()
	if len(got) != 1 || got[0].Location != "us-central1" || got[0].KmsKeyName != usCentralKey {
		t.Errorf("user-managed non-first bindings = %#v, want the us-central1 binding", got)
	}
}
