package kms

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"testing"

	"jaiscloud/internal/model"
)

// setCryptoKeyPolicy writes a cryptoKey IAM policy binding a single role.
func setCryptoKeyPolicy(t *testing.T, p *Provider, keyPath, role string) {
	t.Helper()
	if _, err := p.SetIamPolicy(context.Background(), newNR(map[string]any{
		"name": keyPath,
		"body": map[string]any{"policy": map[string]any{
			"bindings": []any{map[string]any{
				"role":    role,
				"members": []any{"user:caller@example.com"},
			}},
		}},
	})); err != nil {
		t.Fatalf("setIamPolicy(%s): %v", role, err)
	}
}

// TestCryptoKeyPolicyEnforced verifies the default-permissive cryptoKey
// resource-policy hook: an unset policy allows crypto use, a policy scoped to
// roles that omit the required permission denies with 403 PERMISSION_DENIED,
// and a policy granting the matching role allows.
func TestCryptoKeyPolicyEnforced(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "authz-kr"})); err != nil {
		t.Fatalf("keyring create: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/authz-kr", "cryptoKeyId": "authz-key",
		"body": map[string]any{"purpose": "ENCRYPT_DECRYPT"},
	})); err != nil {
		t.Fatalf("cryptokey create: %v", err)
	}
	keyPath := "locations/global/keyRings/authz-kr/cryptoKeys/authz-key"
	encrypt := func() error {
		_, err := p.CryptoKeyEncrypt(ctx, newNR(map[string]any{
			"name": keyPath, "body": map[string]any{"plaintext": "aGVsbG8="},
		}))
		return err
	}
	decrypt := func() error {
		_, err := p.CryptoKeyDecrypt(ctx, newNR(map[string]any{
			"name": keyPath, "body": map[string]any{"ciphertext": "AAAA"},
		}))
		return err
	}

	// No policy: allowed (default-permissive), even though the ciphertext is bogus
	// decrypt must fail on the payload, not on authorization.
	if err := encrypt(); err != nil {
		t.Fatalf("no-policy encrypt: %v", err)
	}
	if err := decrypt(); err == nil || errStatus(err) == 403 {
		t.Fatalf("no-policy decrypt should reach the payload, got %v", err)
	}

	// Policy scoped to a role without crypto use denies.
	setCryptoKeyPolicy(t, p, keyPath, "roles/cloudkms.viewer")
	err := encrypt()
	if err == nil {
		t.Fatal("viewer-only policy must deny encrypt")
	}
	var perr *model.ProviderError
	if !errors.As(err, &perr) || perr.Code != "PermissionDenied" || errStatus(err) != 403 {
		t.Fatalf("want PermissionDenied/403, got %#v", err)
	}
	if err := decrypt(); errStatus(err) != 403 {
		t.Fatalf("viewer-only policy must deny decrypt, got %v", err)
	}

	// A policy granting the encrypter role allows encrypt but not decrypt.
	setCryptoKeyPolicy(t, p, keyPath, "roles/cloudkms.cryptoKeyEncrypter")
	if err := encrypt(); err != nil {
		t.Fatalf("encrypter-role encrypt: %v", err)
	}
	if err := decrypt(); errStatus(err) != 403 {
		t.Fatalf("encrypter-role must deny decrypt, got %v", err)
	}

	// A combined encrypter/decrypter role allows both.
	setCryptoKeyPolicy(t, p, keyPath, "roles/cloudkms.cryptoKeyEncrypterDecrypter")
	if err := encrypt(); err != nil {
		t.Fatalf("encrypterDecrypter-role encrypt: %v", err)
	}
	if err := decrypt(); errStatus(err) == 403 {
		// Authorization passed; the invalid payload must surface as a payload
		// error (400), never a 403.
		t.Fatalf("encrypterDecrypter-role must authorize decrypt, got %v", err)
	}
}

// TestCryptoKeyPolicyDeniesSignForEncrypter confirms the permission mapping is
// per-operation, using an asymmetric key: an encrypter-scoped policy must not
// authorize AsymmetricSign.
func TestCryptoKeyPolicyDeniesSignForEncrypter(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "sign-kr"})); err != nil {
		t.Fatalf("keyring create: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/sign-kr", "cryptoKeyId": "sign-key",
		"body": map[string]any{"purpose": "ASYMMETRIC_SIGN"},
	})); err != nil {
		t.Fatalf("cryptokey create: %v", err)
	}
	keyPath := "locations/global/keyRings/sign-kr/cryptoKeys/sign-key"
	verPath := keyPath + "/cryptoKeyVersions/1"
	digest := sha256.Sum256([]byte("hello"))
	sign := func() error {
		_, err := p.CryptoKeyVersionAsymmetricSign(ctx, newNR(map[string]any{
			"name": verPath, "body": map[string]any{"digest": base64.StdEncoding.EncodeToString(digest[:])},
		}))
		return err
	}

	setCryptoKeyPolicy(t, p, keyPath, "roles/cloudkms.cryptoKeyEncrypter")
	if err := sign(); errStatus(err) != 403 {
		t.Fatalf("encrypter role must deny sign, got %v", err)
	}
	setCryptoKeyPolicy(t, p, keyPath, "roles/cloudkms.signerVerifier")
	if err := sign(); err != nil {
		t.Fatalf("signerVerifier role must allow sign, got %v", err)
	}
}
