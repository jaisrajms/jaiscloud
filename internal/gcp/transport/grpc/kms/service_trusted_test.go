package kms

import (
	"context"
	"fmt"
	"testing"

	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// TestKMSTrustedWrappingCreateFlag pins CreateCryptoKeyRequest.trusted_wrapping_enabled
// over gRPC: the flag lands on the implicit first version, and the Discovery
// restrictions reject it up front.
func TestKMSTrustedWrappingCreateFlag(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "twkr")
	kr := "projects/test/locations/global/keyRings/twkr"

	// Wrapping key: HSM single-tenant AES_WRAPPING / AES_256_KWP.
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent: kr, CryptoKeyId: "wrap",
		CryptoKey: &kmspb.CryptoKey{
			Purpose: kmspb.CryptoKey_AES_WRAPPING,
			VersionTemplate: &kmspb.CryptoKeyVersionTemplate{
				Algorithm:       kmspb.CryptoKeyVersion_AES_256_KWP,
				ProtectionLevel: kmspb.ProtectionLevel_HSM_SINGLE_TENANT,
			},
		},
	}); err != nil {
		t.Fatalf("CreateCryptoKey(wrap): %v", err)
	}
	// Target: HSM single-tenant raw AES key with trusted wrapping enabled.
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent: kr, CryptoKeyId: "raw", TrustedWrappingEnabled: true,
		CryptoKey: &kmspb.CryptoKey{
			Purpose: kmspb.CryptoKey_RAW_ENCRYPT_DECRYPT,
			VersionTemplate: &kmspb.CryptoKeyVersionTemplate{
				Algorithm:       kmspb.CryptoKeyVersion_AES_256_GCM,
				ProtectionLevel: kmspb.ProtectionLevel_HSM_SINGLE_TENANT,
			},
		},
	}); err != nil {
		t.Fatalf("CreateCryptoKey(raw): %v", err)
	}
	v, err := client.GetCryptoKeyVersion(ctx, &kmspb.GetCryptoKeyVersionRequest{
		Name: kr + "/cryptoKeys/raw/cryptoKeyVersions/1",
	})
	if err != nil {
		t.Fatalf("GetCryptoKeyVersion: %v", err)
	}
	if !v.GetTrustedWrappingEnabled() {
		t.Fatal("trusted_wrapping_enabled at create must land on the first version")
	}

	// Export through the gRPC surface returns the documented wrapped key shape.
	exported, err := client.ExportTrustedKeyWrappedCryptoKeyVersion(ctx, &kmspb.ExportTrustedKeyWrappedCryptoKeyVersionRequest{
		Name:        v.GetName(),
		WrappingKey: kr + "/cryptoKeys/wrap/cryptoKeyVersions/1",
	})
	if err != nil {
		t.Fatalf("ExportTrustedKeyWrappedCryptoKeyVersion: %v", err)
	}
	if len(exported.GetWrappedKey()) == 0 {
		t.Fatal("export returned an empty wrapped key")
	}
	if got := exported.GetWrappedKeyCrc32C().GetValue(); got != crc32cOf(exported.GetWrappedKey()) {
		t.Fatalf("wrapped_key_crc32c = %d, want %d", got, crc32cOf(exported.GetWrappedKey()))
	}

	// A trusted import addressed by CryptoKey name (documented importing_key
	// form) creates a trusted-wrapping version.
	imported, err := client.ImportTrustedKeyWrappedCryptoKeyVersion(ctx, &kmspb.ImportTrustedKeyWrappedCryptoKeyVersionRequest{
		Parent:       kr + "/cryptoKeys/raw",
		ImportingKey: kr + "/cryptoKeys/wrap",
		Algorithm:    kmspb.CryptoKeyVersion_AES_256_GCM,
		WrappedKey:   exported.GetWrappedKey(),
	})
	if err != nil {
		t.Fatalf("ImportTrustedKeyWrappedCryptoKeyVersion: %v", err)
	}
	if !imported.GetTrustedWrappingEnabled() {
		t.Fatal("trusted import by CryptoKey name must set trusted_wrapping_enabled")
	}
}

// TestKMSTrustedWrappingRejections pins the documented rejections over gRPC
// with their canonical status codes.
func TestKMSTrustedWrappingRejections(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "twkr")
	kr := "projects/test/locations/global/keyRings/twkr"

	createKey := func(id string, purpose kmspb.CryptoKey_CryptoKeyPurpose, algo kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm, level kmspb.ProtectionLevel) {
		t.Helper()
		if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
			Parent: kr, CryptoKeyId: id,
			CryptoKey: &kmspb.CryptoKey{
				Purpose:         purpose,
				VersionTemplate: &kmspb.CryptoKeyVersionTemplate{Algorithm: algo, ProtectionLevel: level},
			},
		}); err != nil {
			t.Fatalf("CreateCryptoKey(%s): %v", id, err)
		}
	}
	createKey("soft", kmspb.CryptoKey_RAW_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_AES_256_GCM, kmspb.ProtectionLevel_SOFTWARE)
	createKey("enc", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION, kmspb.ProtectionLevel_HSM_SINGLE_TENANT)
	createKey("wrap", kmspb.CryptoKey_AES_WRAPPING, kmspb.CryptoKeyVersion_AES_256_KWP, kmspb.ProtectionLevel_HSM_SINGLE_TENANT)

	cases := []struct {
		name string
		call func() error
		code codes.Code
	}{
		{
			name: "trustedWrappingEnabled on a software key",
			call: func() error {
				_, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
					Parent: kr, CryptoKeyId: "soft-trusted", TrustedWrappingEnabled: true,
					CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_RAW_ENCRYPT_DECRYPT},
				})
				return err
			},
			code: codes.InvalidArgument,
		},
		{
			name: "trustedWrappingEnabled on an ENCRYPT_DECRYPT key",
			call: func() error {
				_, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
					Parent: kr, CryptoKeyId: "enc-trusted", TrustedWrappingEnabled: true,
					CryptoKey: &kmspb.CryptoKey{
						Purpose:         kmspb.CryptoKey_ENCRYPT_DECRYPT,
						VersionTemplate: &kmspb.CryptoKeyVersionTemplate{ProtectionLevel: kmspb.ProtectionLevel_HSM_SINGLE_TENANT},
					},
				})
				return err
			},
			code: codes.InvalidArgument,
		},
		{
			name: "trusted_wrapping_enabled on create version",
			call: func() error {
				_, err := client.CreateCryptoKeyVersion(ctx, &kmspb.CreateCryptoKeyVersionRequest{
					Parent:           kr + "/cryptoKeys/soft",
					CryptoKeyVersion: &kmspb.CryptoKeyVersion{TrustedWrappingEnabled: true},
				})
				return err
			},
			code: codes.InvalidArgument,
		},
		{
			name: "external protection level at key create",
			call: func() error {
				_, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
					Parent: kr, CryptoKeyId: "ekm",
					CryptoKey: &kmspb.CryptoKey{
						Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT,
						VersionTemplate: &kmspb.CryptoKeyVersionTemplate{
							ProtectionLevel: kmspb.ProtectionLevel_EXTERNAL,
						},
					},
				})
				return err
			},
			code: codes.Unimplemented,
		},
		{
			name: "external options on a software version",
			call: func() error {
				_, err := client.CreateCryptoKeyVersion(ctx, &kmspb.CreateCryptoKeyVersionRequest{
					Parent: kr + "/cryptoKeys/soft",
					CryptoKeyVersion: &kmspb.CryptoKeyVersion{
						ExternalProtectionLevelOptions: &kmspb.ExternalProtectionLevelOptions{
							ExternalKeyUri: "https://ekm.example/key",
						},
					},
				})
				return err
			},
			code: codes.InvalidArgument,
		},
		{
			name: "wrapping key of the wrong purpose",
			call: func() error {
				_, err := client.ImportTrustedKeyWrappedCryptoKeyVersion(ctx, &kmspb.ImportTrustedKeyWrappedCryptoKeyVersionRequest{
					Parent:       kr + "/cryptoKeys/soft",
					ImportingKey: kr + "/cryptoKeys/enc/cryptoKeyVersions/1",
					Algorithm:    kmspb.CryptoKeyVersion_AES_256_GCM,
					WrappedKey:   []byte("not-wrapped"),
				})
				return err
			},
			code: codes.FailedPrecondition,
		},
		{
			name: "wrapping key not found",
			call: func() error {
				_, err := client.ImportTrustedKeyWrappedCryptoKeyVersion(ctx, &kmspb.ImportTrustedKeyWrappedCryptoKeyVersionRequest{
					Parent:       kr + "/cryptoKeys/soft",
					ImportingKey: kr + "/cryptoKeys/missing/cryptoKeyVersions/1",
					Algorithm:    kmspb.CryptoKeyVersion_AES_256_GCM,
					WrappedKey:   []byte("not-wrapped"),
				})
				return err
			},
			code: codes.NotFound,
		},
		{
			name: "wrapping key disabled",
			call: func() error {
				if _, err := client.UpdateCryptoKeyVersion(ctx, &kmspb.UpdateCryptoKeyVersionRequest{
					CryptoKeyVersion: &kmspb.CryptoKeyVersion{
						Name:  kr + "/cryptoKeys/wrap/cryptoKeyVersions/1",
						State: kmspb.CryptoKeyVersion_DISABLED,
					},
					UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}},
				}); err != nil {
					return fmt.Errorf("disable wrapping key: %w", err)
				}
				_, err := client.ImportTrustedKeyWrappedCryptoKeyVersion(ctx, &kmspb.ImportTrustedKeyWrappedCryptoKeyVersionRequest{
					Parent:       kr + "/cryptoKeys/soft",
					ImportingKey: kr + "/cryptoKeys/wrap/cryptoKeyVersions/1",
					Algorithm:    kmspb.CryptoKeyVersion_AES_256_GCM,
					WrappedKey:   []byte("not-wrapped"),
				})
				return err
			},
			code: codes.FailedPrecondition,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); status.Code(err) != tc.code {
				t.Fatalf("error = %v (code %v), want %v", err, status.Code(err), tc.code)
			}
		})
	}
}
