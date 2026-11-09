package secretmanager

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/store"
)

// newRegionalProvider returns a provider backed by an in-memory KMS store with a
// usable ENCRYPT_DECRYPT key in keyLocation, plus that key's resource name.
func newRegionalProvider(t *testing.T, keyLocation string) (*Provider, string) {
	t.Helper()
	ctx := context.Background()
	kmsStore := kms.NewMemoryStore()
	const ring, key = "kr", "k"
	if err := kmsStore.CreateKeyRing(ctx, "proj", keyLocation, ring, kms.KeyRing{ID: ring}); err != nil {
		t.Fatal(err)
	}
	if err := kmsStore.CreateCryptoKey(ctx, "proj", keyLocation, ring, key, kms.CryptoKey{ID: key, Purpose: "ENCRYPT_DECRYPT"}); err != nil {
		t.Fatal(err)
	}
	v, err := kmsStore.CreateVersion(ctx, "proj", keyLocation, ring, key, kms.Version{State: "ENABLED"})
	if err != nil {
		t.Fatal(err)
	}
	if err := kmsStore.UpdatePrimaryVersion(ctx, "proj", keyLocation, ring, key, v); err != nil {
		t.Fatal(err)
	}
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kmsStore))
	return p, "projects/proj/locations/" + keyLocation + "/keyRings/" + ring + "/cryptoKeys/" + key
}

func TestRegionalSecretRoundTrip(t *testing.T) {
	ctx := context.Background()
	p, keyName := newRegionalProvider(t, "us-central1")

	// Create: the parent carries the location, the body the regional CMEK.
	nr := newNR(map[string]any{
		"name":     "locations/us-central1/secrets",
		"secretId": "regional",
		"body": map[string]any{
			"labels":      map[string]any{"env": "conf"},
			"annotations": map[string]any{"owner": "tf"},
			"customerManagedEncryption": map[string]any{
				"kmsKeyName": keyName,
			},
		},
	})
	resp, err := p.Create(ctx, nr)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	wantName := "projects/proj/locations/us-central1/secrets/regional"
	if resp.Data["name"] != wantName {
		t.Fatalf("name = %v, want %s", resp.Data["name"], wantName)
	}
	if _, ok := resp.Data["replication"]; ok {
		t.Error("regional Secret must not carry a replication policy")
	}
	cme, _ := resp.Data["customerManagedEncryption"].(map[string]any)
	if cme["kmsKeyName"] != keyName {
		t.Errorf("customerManagedEncryption.kmsKeyName = %v, want %s", cme["kmsKeyName"], keyName)
	}

	// Read by the regional name.
	nr = newNR(map[string]any{"name": "locations/us-central1/secrets/regional"})
	resp, err = p.Get(ctx, nr)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp.Data["name"] != wantName {
		t.Errorf("get name = %v, want %s", resp.Data["name"], wantName)
	}
	labels, _ := resp.Data["labels"].(map[string]string)
	if labels["env"] != "conf" {
		t.Errorf("labels = %v, want env=conf", resp.Data["labels"])
	}

	// A version of a regional secret reports top-level customerManagedEncryption
	// and no replicationStatus.
	nr = newNR(map[string]any{
		"name": "locations/us-central1/secrets/regional",
		"body": map[string]any{"payload": map[string]any{"data": "aGVsbG8="}},
	})
	resp, err = p.AddVersion(ctx, nr)
	if err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	wantVersion := wantName + "/versions/1"
	if resp.Data["name"] != wantVersion {
		t.Fatalf("version name = %v, want %s", resp.Data["name"], wantVersion)
	}
	if _, ok := resp.Data["replicationStatus"]; ok {
		t.Error("regional SecretVersion must not carry replicationStatus")
	}
	vcme, _ := resp.Data["customerManagedEncryption"].(map[string]any)
	if vcme["kmsKeyVersionName"] == "" || vcme["kmsKeyVersionName"] == nil {
		t.Errorf("version customerManagedEncryption = %v, want a kmsKeyVersionName", resp.Data["customerManagedEncryption"])
	}

	nr = newNR(map[string]any{"name": "locations/us-central1/secrets/regional/versions/1"})
	resp, err = p.Access(ctx, nr)
	if err != nil {
		t.Fatalf("access: %v", err)
	}
	payload, _ := resp.Data["payload"].(map[string]any)
	if payload["data"] != "aGVsbG8=" {
		t.Errorf("payload = %v, want aGVsbG8=", payload["data"])
	}

	// Listing is scoped: the regional list sees the secret, the project list
	// does not.
	nr = newNR(map[string]any{"name": "locations/us-central1/secrets"})
	resp, err = p.List(ctx, nr)
	if err != nil {
		t.Fatalf("regional list: %v", err)
	}
	items, _ := resp.Data["secrets"].([]any)
	if len(items) != 1 {
		t.Fatalf("regional list returned %d secrets, want 1", len(items))
	}
	nr = newNR(map[string]any{"name": "secrets"})
	resp, err = p.List(ctx, nr)
	if err != nil {
		t.Fatalf("project list: %v", err)
	}
	if items, _ := resp.Data["secrets"].([]any); len(items) != 0 {
		t.Errorf("project list returned %d regional secrets, want 0", len(items))
	}

	// A labels-only patch preserves the stored CMEK configuration.
	nr = newNR(map[string]any{
		"name":       "locations/us-central1/secrets/regional",
		"updateMask": "labels",
		"body":       map[string]any{"labels": map[string]any{"env": "prod"}},
	})
	resp, err = p.Update(ctx, nr)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if cme, _ := resp.Data["customerManagedEncryption"].(map[string]any); cme["kmsKeyName"] != keyName {
		t.Errorf("update dropped the CMEK key: %v", resp.Data["customerManagedEncryption"])
	}
	if labels, _ := resp.Data["labels"].(map[string]string); labels["env"] != "prod" {
		t.Errorf("labels = %v, want env=prod", resp.Data["labels"])
	}

	// IAM is served over the shared policy plane, keyed by the regional name.
	nr = newNR(map[string]any{"name": "locations/us-central1/secrets/regional"})
	resp, err = p.GetIamPolicy(ctx, nr)
	if err != nil {
		t.Fatalf("getIamPolicy: %v", err)
	}
	if resp.Data["etag"] == nil {
		t.Error("expected an etag on the regional secret policy")
	}

	// Deleting the regional secret removes it.
	nr = newNR(map[string]any{"name": "locations/us-central1/secrets/regional"})
	if _, err := p.Delete(ctx, nr); err != nil {
		t.Fatalf("delete: %v", err)
	}
	nr = newNR(map[string]any{"name": "locations/us-central1/secrets/regional"})
	if _, err := p.Get(ctx, nr); err == nil {
		t.Fatal("get after delete: want NotFound")
	}
}

func TestRegionalSecretIsIsolatedFromProjectSecrets(t *testing.T) {
	ctx := context.Background()
	p, _ := newRegionalProvider(t, "us-central1")

	// The same secret ID in the global and a regional namespace.
	if _, err := p.Create(ctx, newNR(map[string]any{
		"name": "secrets", "secretId": "shared",
		"body": map[string]any{"replication": map[string]any{"automatic": map[string]any{}}},
	})); err != nil {
		t.Fatalf("create global: %v", err)
	}
	if _, err := p.Create(ctx, newNR(map[string]any{
		"name": "locations/us-central1/secrets", "secretId": "shared", "body": map[string]any{},
	})); err != nil {
		t.Fatalf("create regional: %v", err)
	}

	for name, want := range map[string]string{
		"secrets/shared":                                     "projects/proj/secrets/shared",
		"locations/us-central1/secrets/shared":               "projects/proj/locations/us-central1/secrets/shared",
		"projects/proj/secrets/shared":                       "projects/proj/secrets/shared",
		"projects/proj/locations/us-central1/secrets/shared": "projects/proj/locations/us-central1/secrets/shared",
	} {
		resp, err := p.Get(ctx, newNR(map[string]any{"name": name}))
		if err != nil {
			t.Fatalf("get %q: %v", name, err)
		}
		if resp.Data["name"] != want {
			t.Errorf("get %q name = %v, want %s", name, resp.Data["name"], want)
		}
	}

	// A regional GET in another location is a 404, never the us-central1 secret.
	if _, err := p.Get(ctx, newNR(map[string]any{"name": "locations/europe-west1/secrets/shared"})); err == nil {
		t.Fatal("cross-location get: want NotFound")
	}
}

func TestRegionalSecretRejectsReplication(t *testing.T) {
	ctx := context.Background()
	p, _ := newRegionalProvider(t, "us-central1")
	nr := newNR(map[string]any{
		"name": "locations/us-central1/secrets", "secretId": "r",
		"body": map[string]any{"replication": map[string]any{"automatic": map[string]any{}}},
	})
	if _, err := p.Create(ctx, nr); err == nil {
		t.Fatal("regional create with replication: want InvalidArgument")
	}
}

func TestProjectSecretRejectsRegionalCMEK(t *testing.T) {
	ctx := context.Background()
	p, keyName := newRegionalProvider(t, "us-central1")
	nr := newNR(map[string]any{
		"name": "secrets", "secretId": "g",
		"body": map[string]any{"customerManagedEncryption": map[string]any{"kmsKeyName": keyName}},
	})
	if _, err := p.Create(ctx, nr); err == nil {
		t.Fatal("project-level create with customerManagedEncryption: want InvalidArgument")
	}
}

func TestRegionalSecretCMEKMustBeInTheSecretLocation(t *testing.T) {
	ctx := context.Background()
	p, keyName := newRegionalProvider(t, "us-central1")
	nr := newNR(map[string]any{
		"name": "locations/europe-west1/secrets", "secretId": "r",
		"body": map[string]any{"customerManagedEncryption": map[string]any{"kmsKeyName": keyName}},
	})
	if _, err := p.Create(ctx, nr); err == nil {
		t.Fatal("regional create with a key in another location: want InvalidArgument")
	}
}

func TestRegionalSecretUpdateCMEK(t *testing.T) {
	ctx := context.Background()
	p, keyName := newRegionalProvider(t, "us-central1")
	if _, err := p.Create(ctx, newNR(map[string]any{
		"name": "locations/us-central1/secrets", "secretId": "r", "body": map[string]any{},
	})); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Setting the key echoes it.
	resp, err := p.Update(ctx, newNR(map[string]any{
		"name":       "locations/us-central1/secrets/r",
		"updateMask": "customerManagedEncryption",
		"body":       map[string]any{"customerManagedEncryption": map[string]any{"kmsKeyName": keyName}},
	}))
	if err != nil {
		t.Fatalf("update set CMEK: %v", err)
	}
	if cme, _ := resp.Data["customerManagedEncryption"].(map[string]any); cme["kmsKeyName"] != keyName {
		t.Fatalf("customerManagedEncryption = %v, want %s", resp.Data["customerManagedEncryption"], keyName)
	}

	// A mask naming the field without carrying it clears the configuration.
	resp, err = p.Update(ctx, newNR(map[string]any{
		"name":       "locations/us-central1/secrets/r",
		"updateMask": "customerManagedEncryption",
		"body":       map[string]any{},
	}))
	if err != nil {
		t.Fatalf("update clear CMEK: %v", err)
	}
	if _, ok := resp.Data["customerManagedEncryption"]; ok {
		t.Errorf("customerManagedEncryption = %v, want it cleared", resp.Data["customerManagedEncryption"])
	}
}
