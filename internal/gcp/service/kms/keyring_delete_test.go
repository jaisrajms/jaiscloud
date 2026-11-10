package kms

import (
	"context"
	"errors"
	"testing"

	"jaiscloud/internal/model"
)

// providerErrCode unwraps a provider error's code, failing on any other error.
func providerErrCode(t *testing.T, err error) string {
	t.Helper()
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v (%T), want *model.ProviderError", err, err)
	}
	return pe.Code
}

// TestDeleteKeyRing pins the core half of TF5-4: an empty key ring is deleted
// (a get is NOT_FOUND afterwards) while a ring that still holds a crypto key is
// FAILED_PRECONDITION — the documented "all child resources ... must have been
// previously deleted" precondition, in the same shape DeleteCryptoKey uses for
// versions. Retirement is key-scoped: the ring's own name is reusable, and a
// retired crypto-key record left behind by a deleted key does not block the
// ring's delete.
func TestDeleteKeyRing(t *testing.T) {
	ctx := context.Background()
	core, _ := newCore()

	if err := core.DeleteKeyRing(ctx, "proj", "global", "absent"); providerErrCode(t, err) != "NotFound" {
		t.Fatalf("DeleteKeyRing(absent) = %v, want NotFound", err)
	}

	for _, ring := range []string{"empty", "busy", "retired"} {
		if _, err := core.CreateKeyRing(ctx, "proj", "global", ring); err != nil {
			t.Fatalf("CreateKeyRing(%s): %v", ring, err)
		}
	}

	// An empty ring deletes.
	if err := core.DeleteKeyRing(ctx, "proj", "global", "empty"); err != nil {
		t.Fatalf("DeleteKeyRing(empty): %v", err)
	}
	if _, err := core.GetKeyRing(ctx, "proj", "global", "empty"); providerErrCode(t, err) != "NotFound" {
		t.Fatalf("GetKeyRing(deleted) = %v, want NotFound", err)
	}
	// ... and its name can be used again (the ring is not retired).
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "empty"); err != nil {
		t.Fatalf("re-create the deleted ring name = %v, want success", err)
	}

	// A ring holding a crypto key does not.
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "busy", "k", CryptoKeyInput{SkipInitialVersion: true}); err != nil {
		t.Fatalf("CreateCryptoKey(busy/k): %v", err)
	}
	if err := core.DeleteKeyRing(ctx, "proj", "global", "busy"); providerErrCode(t, err) != "FailedPrecondition" {
		t.Fatalf("DeleteKeyRing(busy) = %v, want FailedPrecondition", err)
	}

	// A retired crypto-key record is a key-level record, not a live child.
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "retired", "k", CryptoKeyInput{SkipInitialVersion: true}); err != nil {
		t.Fatalf("CreateCryptoKey(retired/k): %v", err)
	}
	if _, err := core.DeleteCryptoKey(ctx, "proj", "global", "retired", "k"); err != nil {
		t.Fatalf("DeleteCryptoKey(retired/k): %v", err)
	}
	if err := core.DeleteKeyRing(ctx, "proj", "global", "retired"); err != nil {
		t.Fatalf("DeleteKeyRing after every key was deleted = %v, want success", err)
	}
	// The retired key name stays unusable after the ring is re-created.
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "retired"); err != nil {
		t.Fatalf("re-create ring: %v", err)
	}
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "retired", "k", CryptoKeyInput{SkipInitialVersion: true}); providerErrCode(t, err) != "AlreadyExists" {
		t.Fatalf("re-create the retired key name = %v, want AlreadyExists", err)
	}
}
