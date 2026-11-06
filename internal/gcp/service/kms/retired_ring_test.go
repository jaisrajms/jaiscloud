package kms

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// TestCryptoKeyRetirementIsRingScoped pins FC5-3: a crypto key id is only
// unique within its key ring, so deleting k in r1 must not retire k in r2 of
// the same location (the store key used to be "location/k").
func TestCryptoKeyRetirementIsRingScoped(t *testing.T) {
	ctx := context.Background()
	core, _ := newCore()
	for _, ring := range []string{"r1", "r2"} {
		if _, err := core.CreateKeyRing(ctx, "proj", "global", ring); err != nil {
			t.Fatalf("CreateKeyRing(%s): %v", ring, err)
		}
	}

	// A key with no versions can be deleted immediately (the destroy/delete
	// version dance is irrelevant to retirement scoping).
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "r1", "k", CryptoKeyInput{SkipInitialVersion: true}); err != nil {
		t.Fatalf("create r1/k: %v", err)
	}
	if _, err := core.DeleteCryptoKey(ctx, "proj", "global", "r1", "k"); err != nil {
		t.Fatalf("delete r1/k: %v", err)
	}

	// r2 never had a k: creating it must succeed.
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "r2", "k", CryptoKeyInput{SkipInitialVersion: true}); err != nil {
		t.Fatalf("create r2/k after r1/k was deleted = %v, want success (retirement leaked across rings)", err)
	}
	// r1's name stays retired.
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "r1", "k", CryptoKeyInput{SkipInitialVersion: true}); !isAlreadyExistsErr(err) {
		t.Fatalf("re-create r1/k = %v, want AlreadyExists", err)
	}

	// Retire r2's key too: both records must be addressable by their own
	// (ring-scoped, opaque) public name.
	if _, err := core.DeleteCryptoKey(ctx, "proj", "global", "r2", "k"); err != nil {
		t.Fatalf("delete r2/k: %v", err)
	}
	for _, ring := range []string{"r1", "r2"} {
		rr, err := core.GetRetiredResource(ctx, "proj", "global", ring+"~k")
		if err != nil {
			t.Fatalf("GetRetiredResource(%s~k): %v", ring, err)
		}
		if want := CryptoKeyName("proj", "global", ring, "k"); rr.OriginalResource != want {
			t.Fatalf("%s~k originalResource = %q, want %q", ring, rr.OriginalResource, want)
		}
	}
	list, err := core.ListRetiredResources(ctx, "proj", "global")
	if err != nil {
		t.Fatalf("ListRetiredResources: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("ListRetiredResources = %d records, want 2", len(list))
	}
	names := map[string]bool{}
	for _, rr := range list {
		if names[rr.Name] {
			t.Fatalf("duplicate RetiredResource name %q", rr.Name)
		}
		names[rr.Name] = true
	}
}

// TestCryptoKeyRetirementLegacyRowIsRingScoped pins the dual-read of the old
// store key ("location/key"): a pre-fix row still retires the key it recorded
// (matched by originalResource), and must not block a same-id key in another
// ring. No migration is needed because the row already carries its ring.
func TestCryptoKeyRetirementLegacyRowIsRingScoped(t *testing.T) {
	ctx := context.Background()
	core, _ := newCore()
	for _, ring := range []string{"r1", "r2"} {
		if _, err := core.CreateKeyRing(ctx, "proj", "global", ring); err != nil {
			t.Fatalf("CreateKeyRing(%s): %v", ring, err)
		}
	}
	legacy := RetiredResource{
		Name:             RetiredResourceName("proj", "global", "k"),
		OriginalResource: CryptoKeyName("proj", "global", "r1", "k"),
		ResourceType:     "CRYPTO_KEY",
	}
	data, _ := json.Marshal(legacy)
	if err := core.resources.Upsert(ctx, "proj", store.GlobalRegion, store.ResourceEntry{
		Type: rtRetiredResource, ID: legacyRetiredResourceID("global", "k"), Data: data,
	}); err != nil {
		t.Fatalf("seed legacy retirement row: %v", err)
	}

	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "r2", "k", CryptoKeyInput{SkipInitialVersion: true}); err != nil {
		t.Fatalf("create r2/k while only a legacy r1 retirement exists = %v, want success", err)
	}
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "r1", "k", CryptoKeyInput{SkipInitialVersion: true}); !isAlreadyExistsErr(err) {
		t.Fatalf("create r1/k against its legacy retirement row = %v, want AlreadyExists", err)
	}
	// The pre-fix public name is still addressable (the record carries it).
	if _, err := core.GetRetiredResource(ctx, "proj", "global", "k"); err != nil {
		t.Fatalf("legacy retired-resource name is no longer readable: %v", err)
	}
}

func isAlreadyExistsErr(err error) bool {
	var pe *model.ProviderError
	return errors.As(err, &pe) && pe.Code == "AlreadyExists"
}
