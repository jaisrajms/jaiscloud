package kms

import (
	"context"
	"encoding/base64"
	"testing"
)

const (
	testKR   = "locations/global/keyRings/kr"
	testKey  = testKR + "/cryptoKeys/aes"
	testName = testKey + "/cryptoKeyVersions/1"
)

// newAESProvider returns a provider with a keyring and an AES-256-GCM
// RAW_ENCRYPT_DECRYPT crypto key (version "1").
func newAESProvider(t *testing.T) *Provider {
	t.Helper()
	ctx := context.Background()
	p := newTestProvider()
	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("KeyRingCreate: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": testKR, "cryptoKeyId": "aes",
		"body": map[string]any{
			"purpose":         "RAW_ENCRYPT_DECRYPT",
			"versionTemplate": map[string]any{"algorithm": "AES_256_GCM"},
		},
	})); err != nil {
		t.Fatalf("CryptoKeyCreate: %v", err)
	}
	return p
}

// TestRESTRawEncryptDecrypt covers cryptoKeyVersions.rawEncrypt/rawDecrypt over
// REST (the core methods existed for gRPC only).
func TestRESTRawEncryptDecrypt(t *testing.T) {
	ctx := context.Background()
	p := newAESProvider(t)

	resp, err := p.CryptoKeyVersionRawEncrypt(ctx, newNR(map[string]any{
		"name": testName,
		"body": map[string]any{"plaintext": base64.StdEncoding.EncodeToString([]byte("hello raw"))},
	}))
	if err != nil {
		t.Fatalf("RawEncrypt: %v", err)
	}
	ctB64, _ := resp.Data["ciphertext"].(string)
	ivB64, _ := resp.Data["initializationVector"].(string)
	if ctB64 == "" || ivB64 == "" {
		t.Fatalf("RawEncrypt response missing ciphertext/iv: %v", resp.Data)
	}

	plain, err := p.CryptoKeyVersionRawDecrypt(ctx, newNR(map[string]any{
		"name": testName,
		"body": map[string]any{
			"ciphertext":                  ctB64,
			"initializationVector":        ivB64,
			"additionalAuthenticatedData": "",
		},
	}))
	if err != nil {
		t.Fatalf("RawDecrypt: %v", err)
	}
	got, _ := plain.Data["plaintext"].(string)
	if got != base64.StdEncoding.EncodeToString([]byte("hello raw")) {
		t.Fatalf("round-trip plaintext = %q, want %q", got, base64.StdEncoding.EncodeToString([]byte("hello raw")))
	}

	// A mismatched CRC is rejected.
	badCRC := "12345"
	_, err = p.CryptoKeyVersionRawEncrypt(ctx, newNR(map[string]any{
		"name": testName,
		"body": map[string]any{
			"plaintext":       base64.StdEncoding.EncodeToString([]byte("hello raw")),
			"plaintextCrc32c": badCRC,
		},
	}))
	if err == nil || errStatus(err) != 400 {
		t.Fatalf("RawEncrypt with bad crc: err = %v, want 400", err)
	}
}

// TestRESTGenerateRandomBytes covers locations.generateRandomBytes (the core
// method is new; the gRPC transport previously owned it inline).
func TestRESTGenerateRandomBytes(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	resp, err := p.GenerateRandomBytes(ctx, newNR(map[string]any{
		"name": "locations/global",
		"body": map[string]any{"lengthBytes": 32},
	}))
	if err != nil {
		t.Fatalf("GenerateRandomBytes: %v", err)
	}
	data, _ := resp.Data["data"].(string)
	raw, err := base64.StdEncoding.DecodeString(data)
	if err != nil || len(raw) != 32 {
		t.Fatalf("data = %q (len %d), want 32 random bytes", data, len(raw))
	}
	if resp.Data["dataCrc32c"] == nil {
		t.Fatal("dataCrc32c missing")
	}

	// The documented bounds are enforced (min 8, max 1024).
	for _, n := range []int{0, 7, 1025} {
		_, err := p.GenerateRandomBytes(ctx, newNR(map[string]any{
			"name": "locations/global",
			"body": map[string]any{"lengthBytes": n},
		}))
		if err == nil || errStatus(err) != 400 {
			t.Errorf("length %d: err = %v, want 400", n, err)
		}
	}
}

// TestRESTImportJobIAM covers the importJobs IAM trio. importJobs had IAM in
// neither transport before this change (the core rejected the resource name).
func TestRESTImportJobIAM(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("KeyRingCreate: %v", err)
	}
	if _, err := p.ImportJobCreate(ctx, newNR(map[string]any{
		"name": testKR, "importJobId": "job",
		"body": map[string]any{"importMethod": "RSA_OAEP_3072_SHA256_AES_256"},
	})); err != nil {
		t.Fatalf("ImportJobCreate: %v", err)
	}
	job := testKR + "/importJobs/job"

	if _, err := p.GetIamPolicy(ctx, newNR(map[string]any{"name": job})); err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if _, err := p.SetIamPolicy(ctx, newNR(map[string]any{
		"name": job,
		"body": map[string]any{"bindings": []any{map[string]any{
			"role":    "roles/cloudkms.cryptoKeyEncrypterDecrypter",
			"members": []any{"user:test@example.com"},
		}}},
	})); err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}
	resp, err := p.TestIamPermissions(ctx, newNR(map[string]any{
		"name": job,
		"body": map[string]any{"permissions": []any{"cloudkms.cryptoKeys.get"}},
	}))
	if err != nil {
		t.Fatalf("TestIamPermissions: %v", err)
	}
	if perms, _ := resp.Data["permissions"].([]string); len(perms) != 1 {
		t.Fatalf("permissions = %v, want the echoed permission", resp.Data["permissions"])
	}

	// An unknown import job is NotFound, not a silent empty policy.
	missing := testKR + "/importJobs/nope"
	if _, err := p.GetIamPolicy(ctx, newNR(map[string]any{"name": missing})); err == nil || errStatus(err) != 404 {
		t.Fatalf("GetIamPolicy(missing): err = %v, want 404", err)
	}
}
