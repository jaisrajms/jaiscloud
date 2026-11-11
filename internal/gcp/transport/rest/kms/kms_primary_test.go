package kms

import (
	"context"
	"strings"
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

// TestCryptoKeyUpdatePrimaryVersionAcceptsNonEnabledVersion pins the wire shape
// real KMS returns: 200 with the requested version as primary and that state
// echoed (verified against parity-diff-jaiscloud, 2026-11-11). The CMEK
// consumer — not Cloud KMS — is what refuses to encrypt without an ENABLED
// primary, so this must not grow a state guard.
func TestCryptoKeyUpdatePrimaryVersionAcceptsNonEnabledVersion(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("create keyring: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name":        "locations/global/keyRings/kr",
		"cryptoKeyId": "sym",
		"body":        map[string]any{"purpose": "ENCRYPT_DECRYPT"},
	})); err != nil {
		t.Fatalf("create key: %v", err)
	}
	if _, err := p.CryptoKeyVersionCreate(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/cryptoKeys/sym/cryptoKeyVersions",
	})); err != nil {
		t.Fatalf("create version: %v", err)
	}
	if _, err := p.CryptoKeyVersionDestroy(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/cryptoKeys/sym/cryptoKeyVersions/2",
	})); err != nil {
		t.Fatalf("destroy version: %v", err)
	}

	resp, err := p.CryptoKeyUpdatePrimaryVersion(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/cryptoKeys/sym",
		"body": map[string]any{"cryptoKeyVersionId": "2"},
	}))
	if err != nil {
		t.Fatalf("update primary to a DESTROY_SCHEDULED version = %v, want 200", err)
	}
	primary, _ := resp.Data["primary"].(map[string]any)
	if got := primary["state"]; got != "DESTROY_SCHEDULED" {
		t.Fatalf("primary.state = %v, want DESTROY_SCHEDULED", got)
	}
	if name, _ := primary["name"].(string); !strings.HasSuffix(name, "/cryptoKeyVersions/2") {
		t.Fatalf("primary.name = %q, want the requested version", name)
	}
}
