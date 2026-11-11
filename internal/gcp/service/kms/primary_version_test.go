package kms

import (
	"context"
	"errors"
	"testing"

	"jaiscloud/internal/model"
)

// providerError asserts err is a *model.ProviderError with the given
// HTTP status and code.
func providerError(t *testing.T, err error, status int, code string) *model.ProviderError {
	t.Helper()
	if err == nil {
		t.Fatalf("err = nil, want %d/%s", status, code)
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("error is %T, want *model.ProviderError: %v", err, err)
	}
	if pe.HTTPStatus != status || pe.Code != code {
		t.Fatalf("status/code = %d/%s, want %d/%s (%v)", pe.HTTPStatus, pe.Code, status, code, err)
	}
	return pe
}

// TestCoreSetPrimaryVersionRejectsNonSymmetricKey pins the documented refusal:
// "Returns an error if called on a key whose purpose is not ENCRYPT_DECRYPT."
// Real Cloud KMS answers 400 FAILED_PRECONDITION with
// "Keys with purpose ASYMMETRIC_SIGN do not have a primary version." — verified
// against real GCP (parity-diff-jaiscloud, 2026-11-11).
func TestCoreSetPrimaryVersionRejectsNonSymmetricKey(t *testing.T) {
	ctx := context.Background()
	core, _ := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "signer", CryptoKeyInput{
		Purpose:   "ASYMMETRIC_SIGN",
		Algorithm: "RSA_SIGN_PKCS1_2048_SHA256",
	}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}

	_, err := core.SetPrimaryVersion(ctx, "proj", "global", "kr", "signer", "1")
	pe := providerError(t, err, 400, "FailedPrecondition")
	if want := "Keys with purpose ASYMMETRIC_SIGN do not have a primary version."; pe.Message != want {
		t.Fatalf("message = %q, want %q", pe.Message, want)
	}

	// The rejected call must not have made the requested version primary.
	if _, err := core.CreateVersion(ctx, "proj", "global", "kr", "signer", CreateVersionInput{}); err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}
	if _, err := core.SetPrimaryVersion(ctx, "proj", "global", "kr", "signer", "2"); err == nil {
		t.Fatal("SetPrimaryVersion on an ASYMMETRIC_SIGN key succeeded, want FAILED_PRECONDITION")
	}
	ck, err := core.GetCryptoKey(ctx, "proj", "global", "kr", "signer")
	if err != nil {
		t.Fatalf("GetCryptoKey: %v", err)
	}
	if ck.PrimaryVersion != "1" {
		t.Fatalf("primary = %q after rejected calls, want the untouched value 1", ck.PrimaryVersion)
	}
}

// TestCoreSetPrimaryVersionSymmetricStillWorks keeps the positive path: a
// symmetric key rotates its primary version as before.
func TestCoreSetPrimaryVersionSymmetricStillWorks(t *testing.T) {
	ctx := context.Background()
	core, _ := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyInput{}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	if _, err := core.CreateVersion(ctx, "proj", "global", "kr", "sym", CreateVersionInput{}); err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}
	ck, err := core.SetPrimaryVersion(ctx, "proj", "global", "kr", "sym", "2")
	if err != nil {
		t.Fatalf("SetPrimaryVersion: %v", err)
	}
	if ck.PrimaryVersion != "2" {
		t.Fatalf("primary = %q, want 2", ck.PrimaryVersion)
	}
}

// TestCoreSetPrimaryVersionMissingCases keeps the 404s distinct from the
// purpose refusal.
func TestCoreSetPrimaryVersionMissingCases(t *testing.T) {
	ctx := context.Background()
	core, _ := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyInput{}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}

	_, err := core.SetPrimaryVersion(ctx, "proj", "global", "kr", "nope", "1")
	providerError(t, err, 404, "NotFound")
	_, err = core.SetPrimaryVersion(ctx, "proj", "global", "kr", "sym", "99")
	providerError(t, err, 404, "NotFound")
}
