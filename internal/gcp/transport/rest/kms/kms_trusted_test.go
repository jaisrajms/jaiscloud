package kms

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"

	kmscore "jaiscloud/internal/gcp/service/kms"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// newTestProviderWithKeys returns a KMS provider over an explicit memory store so
// a test can read the key material the emulator stored.
func newTestProviderWithKeys() (*Provider, kmsstore.Store) {
	keys := kmsstore.NewMemoryStore()
	return New(kmscore.NewService(keys, store.NewMemoryResourceStore(), "proj")), keys
}

// restTrustedWrappingFixture creates the shared HSM wrapping key and a
// single-tenant raw target key whose first version was created with
// trustedWrappingEnabled=true (the REST cryptoKeys.create query parameter).
func restTrustedWrappingFixture(t *testing.T, p *Provider) (target, wrapVersion string) {
	t.Helper()
	ctx := context.Background()
	kr := "projects/proj/locations/global/keyRings/kr"
	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("KeyRingCreate: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": kr, "cryptoKeyId": "wrap",
		"body": map[string]any{
			"purpose":         "AES_WRAPPING",
			"versionTemplate": map[string]any{"algorithm": "AES_256_KWP", "protectionLevel": "HSM_SINGLE_TENANT"},
		},
	})); err != nil {
		t.Fatalf("CryptoKeyCreate(wrap): %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": kr, "cryptoKeyId": "raw", "trustedWrappingEnabled": "true",
		"body": map[string]any{
			"purpose":         "RAW_ENCRYPT_DECRYPT",
			"versionTemplate": map[string]any{"algorithm": "AES_256_GCM", "protectionLevel": "HSM_SINGLE_TENANT"},
		},
	})); err != nil {
		t.Fatalf("CryptoKeyCreate(raw, trustedWrappingEnabled): %v", err)
	}
	targetVersion := kr + "/cryptoKeys/raw/cryptoKeyVersions/1"
	version, err := p.CryptoKeyVersionGet(ctx, newNR(map[string]any{"name": targetVersion}))
	if err != nil {
		t.Fatalf("CryptoKeyVersionGet: %v", err)
	}
	if enabled, _ := version.Data["trustedWrappingEnabled"].(bool); !enabled {
		t.Fatalf("cryptoKeys.create trustedWrappingEnabled did not land on the first version: %v", version.Data)
	}
	return targetVersion, kr + "/cryptoKeys/wrap/cryptoKeyVersions/1"
}

// TestRESTTrustedWrappingExportShape pins the documented REST export shape
// (wrappedKey + wrappedKeyCrc32c) and the cryptoKeys.create
// trustedWrappingEnabled query parameter.
func TestRESTTrustedWrappingExportShape(t *testing.T) {
	ctx := context.Background()
	p, keys := newTestProviderWithKeys()
	target, wrapVersion := restTrustedWrappingFixture(t, p)

	resp, err := p.CryptoKeyVersionExportTrusted(ctx, newNR(map[string]any{
		"name": target, "wrappingKey": wrapVersion,
	}))
	if err != nil {
		t.Fatalf("CryptoKeyVersionExportTrusted: %v", err)
	}
	wrappedB64, _ := resp.Data["wrappedKey"].(string)
	wrapped, err := base64.StdEncoding.DecodeString(wrappedB64)
	if err != nil || len(wrapped) == 0 {
		t.Fatalf("wrappedKey = %q (%v), want non-empty base64", wrappedB64, err)
	}
	if got, want := resp.Data["wrappedKeyCrc32c"], crc32cString(wrapped); got != want {
		t.Fatalf("wrappedKeyCrc32c = %v, want %s", got, want)
	}

	kwk, err := keys.KeyMaterial(ctx, "proj", "global", "kr", "wrap", "1")
	if err != nil {
		t.Fatal(err)
	}
	got, err := kmsstore.UnwrapTrustedKey(kwk, wrapped)
	if err != nil {
		t.Fatalf("UnwrapTrustedKey: %v", err)
	}
	want, err := keys.KeyMaterial(ctx, "proj", "global", "kr", "raw", "1")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatal("exported wrappedKey does not unwrap to the version's material")
	}
}

// TestRESTTrustedImportByCryptoKeyName pins the documented importing_key form
// ("the name of a CryptoKeyVersion or a CryptoKey") over REST.
func TestRESTTrustedImportByCryptoKeyName(t *testing.T) {
	ctx := context.Background()
	p, keys := newTestProviderWithKeys()
	kr := "projects/proj/locations/global/keyRings/kr"
	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("KeyRingCreate: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": kr, "cryptoKeyId": "wrap",
		"body": map[string]any{
			"purpose":         "AES_WRAPPING",
			"versionTemplate": map[string]any{"algorithm": "AES_256_KWP", "protectionLevel": "HSM_SINGLE_TENANT"},
		},
	})); err != nil {
		t.Fatalf("CryptoKeyCreate(wrap): %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": kr, "cryptoKeyId": "raw",
		"body": map[string]any{
			"purpose":         "RAW_ENCRYPT_DECRYPT",
			"versionTemplate": map[string]any{"algorithm": "AES_256_GCM", "protectionLevel": "HSM_SINGLE_TENANT"},
		},
	})); err != nil {
		t.Fatalf("CryptoKeyCreate(raw): %v", err)
	}
	kwk, err := keys.KeyMaterial(ctx, "proj", "global", "kr", "wrap", "1")
	if err != nil {
		t.Fatal(err)
	}
	material := []byte("rest-trusted-import-material")
	wrapped, err := kmsstore.WrapTrustedKey(kwk, material)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := p.CryptoKeyVersionImportTrusted(ctx, newNR(map[string]any{
		"name": kr + "/cryptoKeys/raw",
		"body": map[string]any{
			"importingKey": kr + "/cryptoKeys/wrap",
			"algorithm":    "AES_256_GCM",
			"wrappedKey":   base64.StdEncoding.EncodeToString(wrapped),
		},
	}))
	if err != nil {
		t.Fatalf("CryptoKeyVersionImportTrusted: %v", err)
	}
	if enabled, _ := resp.Data["trustedWrappingEnabled"].(bool); !enabled {
		t.Fatalf("imported version trustedWrappingEnabled = %v, want true", resp.Data["trustedWrappingEnabled"])
	}
}

// TestRESTTrustedWrappingRejections pins the documented rejections through the
// REST adapter, so a client sees the real status code.
func TestRESTTrustedWrappingRejections(t *testing.T) {
	ctx := context.Background()
	kr := "projects/proj/locations/global/keyRings/kr"

	t.Run("trustedWrappingEnabled on a software key", func(t *testing.T) {
		p, _ := newTestProviderWithKeys()
		if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
			t.Fatal(err)
		}
		_, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
			"name": kr, "cryptoKeyId": "soft", "trustedWrappingEnabled": "true",
			"body": map[string]any{"purpose": "RAW_ENCRYPT_DECRYPT"},
		}))
		assertRESTCode(t, err, "InvalidArgument", 400)
	})

	t.Run("trustedWrappingEnabled on create version", func(t *testing.T) {
		p, _ := newTestProviderWithKeys()
		if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
			t.Fatal(err)
		}
		if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
			"name": kr, "cryptoKeyId": "soft", "body": map[string]any{"purpose": "RAW_ENCRYPT_DECRYPT"},
		})); err != nil {
			t.Fatal(err)
		}
		_, err := p.CryptoKeyVersionCreate(ctx, newNR(map[string]any{
			"name": kr + "/cryptoKeys/soft",
			"body": map[string]any{"trustedWrappingEnabled": true},
		}))
		assertRESTCode(t, err, "InvalidArgument", 400)
	})

	t.Run("external protection level", func(t *testing.T) {
		p, _ := newTestProviderWithKeys()
		if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
			t.Fatal(err)
		}
		_, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
			"name": kr, "cryptoKeyId": "ekm",
			"body": map[string]any{
				"purpose":         "ENCRYPT_DECRYPT",
				"versionTemplate": map[string]any{"protectionLevel": "EXTERNAL"},
			},
		}))
		assertRESTCode(t, err, "Unimplemented", 501)
	})

	t.Run("external options on a software version", func(t *testing.T) {
		p, _ := newTestProviderWithKeys()
		if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
			t.Fatal(err)
		}
		if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
			"name": kr, "cryptoKeyId": "soft", "body": map[string]any{"purpose": "RAW_ENCRYPT_DECRYPT"},
		})); err != nil {
			t.Fatal(err)
		}
		_, err := p.CryptoKeyVersionCreate(ctx, newNR(map[string]any{
			"name": kr + "/cryptoKeys/soft",
			"body": map[string]any{"externalProtectionLevelOptions": map[string]any{"externalKeyUri": "https://ekm.example/key"}},
		}))
		assertRESTCode(t, err, "InvalidArgument", 400)
	})
}

// assertRESTCode asserts the canonical code and HTTP status of a provider error.
func assertRESTCode(t *testing.T, err error, code string, status int) {
	t.Helper()
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("error is %T, want *model.ProviderError: %v", err, err)
	}
	if pe.Code != code || pe.HTTPStatus != status {
		t.Fatalf("error = %s (%d), want %s (%d)", pe.Code, pe.HTTPStatus, code, status)
	}
}
