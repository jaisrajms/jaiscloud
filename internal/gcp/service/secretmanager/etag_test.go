package secretmanager

import (
	"errors"
	"testing"
	"time"

	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/model"
)

func baseSecret() secretmanagerstore.Secret {
	return secretmanagerstore.Secret{
		ID:         "s",
		CreateTime: time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC),
		NextVer:    1,
	}
}

func aborted(t *testing.T, err error) {
	t.Helper()
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		t.Fatalf("err = %v, want a ProviderError", err)
	}
	if perr.HTTPStatus != 409 || perr.Status != "ABORTED" {
		t.Fatalf("err = %d/%s, want 409 ABORTED", perr.HTTPStatus, perr.Status)
	}
}

func TestSecretEtagStableAndSensitiveToMetadata(t *testing.T) {
	const name = "projects/p/secrets/s"
	s := baseSecret()
	base := SecretEtag(name, s)

	if got := SecretEtag(name, s); got != base {
		t.Fatalf("etag not stable across calls: %q vs %q", got, base)
	}
	// NextVer is the version counter, not Secret metadata: adding a version
	// must not invalidate a caller's secret etag (real GCP's Secret etag
	// covers the Secret object only).
	s.NextVer = 7
	if got := SecretEtag(name, s); got != base {
		t.Errorf("NextVer changed the secret etag; it should not")
	}

	changes := []func(*secretmanagerstore.Secret){
		func(s *secretmanagerstore.Secret) { s.Labels = map[string]string{"env": "prod"} },
		func(s *secretmanagerstore.Secret) { s.Annotations = map[string]string{"a": "b"} },
		func(s *secretmanagerstore.Secret) {
			s.Rotation = &secretmanagerstore.Rotation{NextRotationTime: "2026-10-17T00:00:00Z", RotationPeriod: "86400s"}
		},
		func(s *secretmanagerstore.Secret) { s.VersionAliases = map[string]int{"stable": 1} },
		func(s *secretmanagerstore.Secret) {
			s.KmsKeyName = "projects/p/locations/global/keyRings/r/cryptoKeys/k"
		},
		func(s *secretmanagerstore.Secret) {
			s.SetReplication(&secretmanagerstore.Replication{UserManaged: &secretmanagerstore.UserManagedReplication{
				Replicas: []secretmanagerstore.Replica{{Location: "us-east1"}},
			}})
		},
	}
	for i, mutate := range changes {
		next := baseSecret()
		mutate(&next)
		if got := SecretEtag(name, next); got == base {
			t.Errorf("mutation %d did not change the secret etag", i)
		}
	}

	// A user-managed policy's per-replica CMEK key is metadata too: changing it
	// (the one policy update real GCP supports) must move the etag.
	um := func(key string) secretmanagerstore.Secret {
		s := baseSecret()
		s.SetReplication(&secretmanagerstore.Replication{UserManaged: &secretmanagerstore.UserManagedReplication{
			Replicas: []secretmanagerstore.Replica{
				{Location: "us-east1", CustomerManagedEncryption: &secretmanagerstore.CustomerManagedEncryption{KmsKeyName: key}},
				{Location: "us-central1"},
			},
		}})
		return s
	}
	k1 := "projects/p/locations/us-east1/keyRings/r/cryptoKeys/k"
	k2 := "projects/p/locations/us-east1/keyRings/r/cryptoKeys/k2"
	if SecretEtag(name, um(k1)) == SecretEtag(name, um(k2)) {
		t.Error("a per-replica CMEK key change did not change the secret etag")
	}

	// A legacy record (only the key name) and its explicit automatic equivalent
	// are the same visible object, so they share an etag: the upgrade a patch
	// performs must not invalidate a caller's token.
	if SecretEtag(name, baseSecret()) != SecretEtag(name, func() secretmanagerstore.Secret {
		s := baseSecret()
		s.Replication = &secretmanagerstore.Replication{Automatic: &secretmanagerstore.AutomaticReplication{}}
		return s
	}()) {
		t.Error("legacy and explicit default policies should share an etag")
	}
	if got := SecretEtag("projects/other/secrets/s", baseSecret()); got == base {
		t.Errorf("a different resource name must not share the etag")
	}
}

func TestVersionEtagFollowsState(t *testing.T) {
	const name = "projects/p/secrets/s/versions/1"
	v := secretmanagerstore.Version{
		SecretID:   "s",
		VersionID:  "1",
		State:      "ENABLED",
		CreateTime: time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC),
	}
	enabled := VersionEtag(name, timestamp.Canonical(v.CreateTime), v.State, "")
	if got := VersionEtag(name, timestamp.Canonical(v.CreateTime), v.State, ""); got != enabled {
		t.Fatalf("version etag not stable across calls")
	}
	v.State = "DISABLED"
	disabled := VersionEtag(name, timestamp.Canonical(v.CreateTime), v.State, "")
	if disabled == enabled {
		t.Fatalf("version etag did not change on a state transition")
	}
	v.State = "DESTROYED"
	if VersionEtag(name, timestamp.Canonical(v.CreateTime), v.State, "") == disabled {
		t.Fatalf("version etag did not change on destroy")
	}
}

func TestCheckEtagSemantics(t *testing.T) {
	const name = "projects/p/secrets/s"
	s := baseSecret()
	current := SecretEtag(name, s)

	if err := CheckSecretEtag(name, s, ""); err != nil {
		t.Errorf("absent etag should be a no-op, got %v", err)
	}
	if err := CheckSecretEtag(name, s, current); err != nil {
		t.Errorf("current etag should pass, got %v", err)
	}
	aborted(t, CheckSecretEtag(name, s, "BOGUS="))

	v := secretmanagerstore.Version{SecretID: "s", VersionID: "1", State: "ENABLED", CreateTime: s.CreateTime}
	vname := "projects/p/secrets/s/versions/1"
	if err := CheckVersionEtag(vname, v, ""); err != nil {
		t.Errorf("absent version etag should be a no-op, got %v", err)
	}
	if err := CheckVersionEtag(vname, v, VersionEtag(vname, timestamp.Canonical(v.CreateTime), v.State, "")); err != nil {
		t.Errorf("current version etag should pass, got %v", err)
	}
	aborted(t, CheckVersionEtag(vname, v, "BOGUS="))
}
