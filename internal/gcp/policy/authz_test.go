package policy

import (
	"context"
	"errors"
	"testing"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func setBindings(t *testing.T, s store.ResourceStore, roles ...string) {
	t.Helper()
	bindings := make([]any, 0, len(roles))
	for _, r := range roles {
		bindings = append(bindings, map[string]any{
			"role":    r,
			"members": []any{"user:caller@example.com"},
		})
	}
	if _, err := Set(context.Background(), s, "proj", "gcp_cryptokey_policy", "us/ring/key",
		map[string]any{"bindings": bindings}); err != nil {
		t.Fatalf("Set bindings: %v", err)
	}
}

func TestAuthorizeKMS(t *testing.T) {
	const (
		encrypt = PermCryptoKeyUseToEncrypt
		decrypt = PermCryptoKeyUseToDecrypt
		sign    = PermCryptoKeyUseToSign
		verify  = PermCryptoKeyUseToVerify
		view    = PermCryptoKeyViewPublicKey
	)
	cases := []struct {
		name   string
		roles  []string
		perm   string
		denied bool
	}{
		{"no policy allows", nil, encrypt, false},
		{"empty bindings allows", []string{}, encrypt, false},
		{"encrypterDecrypter encrypt", []string{"roles/cloudkms.cryptoKeyEncrypterDecrypter"}, encrypt, false},
		{"encrypterDecrypter decrypt", []string{"roles/cloudkms.cryptoKeyEncrypterDecrypter"}, decrypt, false},
		{"encrypterDecrypter sign denied", []string{"roles/cloudkms.cryptoKeyEncrypterDecrypter"}, sign, true},
		{"encrypter encrypt", []string{"roles/cloudkms.cryptoKeyEncrypter"}, encrypt, false},
		{"encrypter decrypt denied", []string{"roles/cloudkms.cryptoKeyEncrypter"}, decrypt, true},
		{"signerVerifier sign", []string{"roles/cloudkms.signerVerifier"}, sign, false},
		{"signerVerifier verify", []string{"roles/cloudkms.signerVerifier"}, verify, false},
		{"signerVerifier view", []string{"roles/cloudkms.signerVerifier"}, view, false},
		{"signerVerifier encrypt denied", []string{"roles/cloudkms.signerVerifier"}, encrypt, true},
		{"verifier view", []string{"roles/cloudkms.verifier"}, view, false},
		{"viewer denies use", []string{"roles/cloudkms.viewer"}, encrypt, true},
		{"unknown role denies", []string{"roles/cloudkms.doesNotExist"}, encrypt, true},
		{"owner allows all", []string{"roles/owner"}, sign, false},
		{"admin allows all", []string{"roles/cloudkms.admin"}, view, false},
		{"one of several roles grants", []string{"roles/cloudkms.viewer", "roles/cloudkms.signer"}, sign, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := store.NewMemoryResourceStore()
			if len(tc.roles) > 0 {
				setBindings(t, s, tc.roles...)
			} else if tc.name == "empty bindings allows" {
				setBindings(t, s)
			}
			err := AuthorizeKMS(context.Background(), s, "proj", "gcp_cryptokey_policy",
				"us/ring/key", tc.perm, "projects/proj/locations/us/keyRings/ring/cryptoKeys/key")
			if tc.denied {
				if err == nil {
					t.Fatalf("want denied for %s, got nil", tc.perm)
				}
				var perr *model.ProviderError
				if !errors.As(err, &perr) || perr.Code != "PermissionDenied" || perr.HTTPStatus != 403 {
					t.Fatalf("want PermissionDenied/403, got %#v", err)
				}
			} else if err != nil {
				t.Fatalf("want allowed for %s, got %v", tc.perm, err)
			}
		})
	}
}

// TestAuthorizeKMSIgnoresCondition documents that binding conditions are treated
// as satisfied, matching the AWS KMS emulator evaluator which ignores Condition.
func TestAuthorizeKMSIgnoresCondition(t *testing.T) {
	ctx := context.Background()
	s := store.NewMemoryResourceStore()
	body := map[string]any{"bindings": []any{map[string]any{
		"role":    "roles/cloudkms.cryptoKeyEncrypter",
		"members": []any{"user:caller@example.com"},
		"condition": map[string]any{
			"title":      "always-false",
			"expression": "false",
		},
	}}}
	if _, err := Set(ctx, s, "proj", "gcp_cryptokey_policy", "us/ring/key", body); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := AuthorizeKMS(ctx, s, "proj", "gcp_cryptokey_policy", "us/ring/key",
		PermCryptoKeyUseToEncrypt, "res"); err != nil {
		t.Fatalf("condition should be ignored (allowed), got %v", err)
	}
}
