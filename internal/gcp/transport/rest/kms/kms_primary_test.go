package kms

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
)

// TestCryptoKeyUpdatePrimaryVersionRejectsNonSymmetricKey pins the REST answer
// real Cloud KMS gives for an ASYMMETRIC_SIGN key: 400 FAILED_PRECONDITION
// "Keys with purpose ASYMMETRIC_SIGN do not have a primary version."
// (verified against real GCP, parity-diff-jaiscloud, 2026-11-11).
func TestCryptoKeyUpdatePrimaryVersionRejectsNonSymmetricKey(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("create keyring: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name":        "locations/global/keyRings/kr",
		"cryptoKeyId": "signer",
		"body":        map[string]any{"purpose": "ASYMMETRIC_SIGN", "versionTemplate": map[string]any{"algorithm": "RSA_SIGN_PKCS1_2048_SHA256"}},
	})); err != nil {
		t.Fatalf("create asymmetric key: %v", err)
	}

	_, err := p.CryptoKeyUpdatePrimaryVersion(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/cryptoKeys/signer",
		"body": map[string]any{"cryptoKeyVersionId": "1"},
	}))
	pe, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("err = %v (%T), want *model.ProviderError", err, err)
	}
	if pe.HTTPStatus != 400 || pe.Code != "FailedPrecondition" {
		t.Fatalf("status/code = %d/%s, want 400/FailedPrecondition", pe.HTTPStatus, pe.Code)
	}
	if want := "Keys with purpose ASYMMETRIC_SIGN do not have a primary version."; pe.Message != want {
		t.Fatalf("message = %q, want %q", pe.Message, want)
	}

	// A symmetric key still rotates.
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name":        "locations/global/keyRings/kr",
		"cryptoKeyId": "sym",
		"body":        map[string]any{"purpose": "ENCRYPT_DECRYPT"},
	})); err != nil {
		t.Fatalf("create symmetric key: %v", err)
	}
	if _, err := p.CryptoKeyUpdatePrimaryVersion(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/cryptoKeys/sym",
		"body": map[string]any{"cryptoKeyVersionId": "1"},
	})); err != nil {
		t.Fatalf("update primary on symmetric key: %v", err)
	}
}
