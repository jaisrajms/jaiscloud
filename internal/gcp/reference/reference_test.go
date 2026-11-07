package reference

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/resource"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/store"
)

func TestKMSLocationMatches(t *testing.T) {
	cases := []struct {
		resource, key string
		want          bool
	}{
		{"US", "us", true},
		{"EU", "europe", true},
		{"ASIA", "asia", true},
		{"us-central1", "US-CENTRAL1", true},
		{"us-central1", "europe-west1", false},
		{"US", "us-central1", false},
		{"", "europe-west1", true},
	}
	for _, c := range cases {
		if got := KMSLocationMatches(c.resource, c.key); got != c.want {
			t.Errorf("KMSLocationMatches(%q, %q) = %v, want %v", c.resource, c.key, got, c.want)
		}
	}
}

func TestSplitCryptoKeyName(t *testing.T) {
	p, l, r, k, ok := SplitCryptoKeyName("projects/p/locations/us/keyRings/ring/cryptoKeys/key")
	if !ok || p != "p" || l != "us" || r != "ring" || k != "key" {
		t.Fatalf("split = (%q,%q,%q,%q,%v)", p, l, r, k, ok)
	}
	for _, bad := range []string{"", "projects/p/locations/us/keyRings/ring", "p/l/us/ring/key/key"} {
		if _, _, _, _, ok := SplitCryptoKeyName(bad); ok {
			t.Errorf("SplitCryptoKeyName(%q) accepted a malformed name", bad)
		}
	}
}

func TestValidateCryptoKey(t *testing.T) {
	ctx := context.Background()
	keys := kmsstore.NewMemoryStore()
	if err := keys.CreateKeyRing(ctx, "p", "us-central1", "ring", kmsstore.KeyRing{ID: "ring"}); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	for id, purpose := range map[string]string{"key": "ENCRYPT_DECRYPT", "sign": "ASYMMETRIC_SIGN"} {
		if err := keys.CreateCryptoKey(ctx, "p", "us-central1", "ring", id,
			kmsstore.CryptoKey{ID: id, Purpose: purpose}); err != nil {
			t.Fatalf("CreateCryptoKey(%s): %v", id, err)
		}
	}
	good := "projects/p/locations/us-central1/keyRings/ring/cryptoKeys/key"

	if err := ValidateCryptoKey(ctx, keys, "field", good, "us-central1"); err != nil {
		t.Fatalf("accept: %v", err)
	}
	if err := ValidateCryptoKey(ctx, nil, "field", good, "us-central1"); err != nil {
		t.Fatalf("nil store accept: %v", err)
	}
	for name, c := range map[string]struct {
		store kmsstore.Store
		name  string
		loc   string
	}{
		"malformed":     {keys, "not-a-key", "us-central1"},
		"cross-loc":     {keys, good, "europe-west1"},
		"missing":       {keys, "projects/p/locations/us-central1/keyRings/ring/cryptoKeys/absent", "us-central1"},
		"missing-ring":  {keys, "projects/p/locations/us-central1/keyRings/absent/cryptoKeys/key", "us-central1"},
		"wrong-purpose": {keys, "projects/p/locations/us-central1/keyRings/ring/cryptoKeys/sign", "us-central1"},
		"nil-malformed": {nil, "not-a-key", "us-central1"},
	} {
		if err := ValidateCryptoKey(ctx, c.store, "field", c.name, c.loc); err == nil {
			t.Errorf("%s: ValidateCryptoKey accepted %q", name, c.name)
		}
	}
}

func TestSplitComputeNetworkName(t *testing.T) {
	p, n, ok := SplitComputeNetworkName("projects/123/global/networks/net1")
	if !ok || p != "123" || n != "net1" {
		t.Fatalf("split = (%q,%q,%v)", p, n, ok)
	}
	if p, n, ok := SplitComputeNetworkName("net1"); !ok || p != "" || n != "net1" {
		t.Fatalf("bare split = (%q,%q,%v)", p, n, ok)
	}
	for _, bad := range []string{"", "projects/123/regions/us/networks/net1", "projects/123/global/networks"} {
		if _, _, ok := SplitComputeNetworkName(bad); ok {
			t.Errorf("SplitComputeNetworkName(%q) accepted a malformed name", bad)
		}
	}
}

func TestValidateComputeNetwork(t *testing.T) {
	ctx := context.Background()
	res := store.NewMemoryResourceStore()
	if err := res.Create(ctx, "p", store.GlobalRegion, store.ResourceEntry{
		Type: ComputeNetworkType, ID: "net1", Data: []byte(`{"name":"net1"}`),
	}); err != nil {
		t.Fatalf("seed network: %v", err)
	}
	byNumber := "projects/" + resource.ProjectNumber("p") + "/global/networks/net1"

	if err := ValidateComputeNetwork(ctx, res, "p", byNumber); err != nil {
		t.Fatalf("accept by number: %v", err)
	}
	if err := ValidateComputeNetwork(ctx, res, "p", "projects/p/global/networks/net1"); err != nil {
		t.Fatalf("accept by id: %v", err)
	}
	if err := ValidateComputeNetwork(ctx, nil, "p", byNumber); err != nil {
		t.Fatalf("nil store accept: %v", err)
	}
	for name, c := range map[string]struct {
		resources store.ResourceStore
		name      string
	}{
		"malformed":     {res, "projects/p/global/networks"},
		"other-proj":    {res, "projects/other/global/networks/net1"},
		"missing":       {res, "projects/p/global/networks/absent"},
		"nil-bad-shape": {nil, "projects/p/global/networks"},
	} {
		if err := ValidateComputeNetwork(ctx, c.resources, "p", c.name); err == nil {
			t.Errorf("%s: ValidateComputeNetwork accepted %q", name, c.name)
		}
	}
}
