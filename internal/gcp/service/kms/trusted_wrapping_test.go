package kms

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"testing"

	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
)

// providerErr returns the canonical code and HTTP status of a core error, so a
// rejection test asserts the wire status a client would see.
func providerErr(t *testing.T, err error) (string, int) {
	t.Helper()
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("error is %T, want *model.ProviderError: %v", err, err)
	}
	return pe.Code, pe.HTTPStatus
}

// mustCreateKeyRing creates the shared key ring, failing the test on error.
func mustCreateKeyRing(t *testing.T, core *Service, kr string) {
	t.Helper()
	if _, err := core.CreateKeyRing(context.Background(), "proj", "global", kr); err != nil {
		t.Fatalf("CreateKeyRing(%s): %v", kr, err)
	}
}

// mustCreateKey creates a crypto key, failing the test on error.
func mustCreateKey(t *testing.T, core *Service, kr, id string, in CryptoKeyInput) {
	t.Helper()
	if _, err := core.CreateCryptoKey(context.Background(), "proj", "global", kr, id, in); err != nil {
		t.Fatalf("CreateCryptoKey(%s): %v", id, err)
	}
}

// trustedWrappingKey creates an HSM single-tenant AES_WRAPPING / AES_256_KWP key.
func trustedWrappingKey(t *testing.T, core *Service, kr, id string) {
	t.Helper()
	mustCreateKey(t, core, kr, id, CryptoKeyInput{
		Purpose: "AES_WRAPPING", Algorithm: "AES_256_KWP", ProtectionLevel: "HSM_SINGLE_TENANT",
	})
}

// singleTenantRawKey creates the documented trusted-wrapping target shape: an
// HSM_SINGLE_TENANT raw AES key (trusted wrapping is never valid for
// ENCRYPT_DECRYPT).
func singleTenantRawKey(t *testing.T, core *Service, kr, id string) {
	t.Helper()
	mustCreateKey(t, core, kr, id, CryptoKeyInput{
		Purpose: "RAW_ENCRYPT_DECRYPT", Algorithm: "AES_256_GCM", ProtectionLevel: "HSM_SINGLE_TENANT",
	})
}

// trustedImport runs a trusted import of material wrapped by the HSM wrapping
// key named by importingKey.
func trustedImport(t *testing.T, core *Service, keys kmsstore.Store, target, importingKey string, material []byte) kmsstore.Version {
	t.Helper()
	ctx := context.Background()
	kwk, err := wrappingMaterial(t, keys, importingKey)
	if err != nil {
		t.Fatalf("wrapping key material: %v", err)
	}
	wrapped, err := kmsstore.WrapTrustedKey(kwk, material)
	if err != nil {
		t.Fatalf("WrapTrustedKey: %v", err)
	}
	v, err := core.ImportTrustedKeyWrappedCryptoKeyVersion(ctx, "proj", "global", "kr", target, ImportVersionRequest{
		ImportingKey: importingKey,
		Algorithm:    "AES_256_GCM",
		WrappedKey:   wrapped,
	})
	if err != nil {
		t.Fatalf("ImportTrustedKeyWrappedCryptoKeyVersion: %v", err)
	}
	return v
}

// wrappingMaterial returns a wrapping key's material given its full version or
// crypto-key name (the same names the API accepts).
func wrappingMaterial(t *testing.T, keys kmsstore.Store, name string) ([]byte, error) {
	t.Helper()
	p, l, kr, key, version, ok := SplitKeyOrVersionName(name)
	if !ok {
		return nil, errors.New("bad wrapping key name")
	}
	if version == "" {
		version = "1"
	}
	return keys.KeyMaterial(context.Background(), p, l, kr, key, version)
}

// TestCoreTrustedImportByCryptoKeyName pins the documented importing_key shape:
// "This can be the name of a CryptoKeyVersion or a CryptoKey." A CryptoKey name
// resolves through its primary version, and the round trip still returns the
// documented wrapped-key material.
func TestCoreTrustedImportByCryptoKeyName(t *testing.T) {
	ctx := context.Background()
	core, keys := newCore()
	mustCreateKeyRing(t, core, "kr")
	trustedWrappingKey(t, core, "kr", "wrap")
	singleTenantRawKey(t, core, "kr", "raw")

	material := []byte("trusted-key-material-by-crypto-key")
	v := trustedImport(t, core, keys, "raw", CryptoKeyName("proj", "global", "kr", "wrap"), material)
	if !v.TrustedWrappingEnabled {
		t.Fatal("trusted import by CryptoKey name should set trustedWrappingEnabled")
	}
	got, err := keys.KeyMaterial(ctx, "proj", "global", "kr", "raw", v.Version)
	if err != nil {
		t.Fatalf("KeyMaterial: %v", err)
	}
	if !bytes.Equal(got, material) {
		t.Fatalf("imported material = %x, want %x", got, material)
	}

	kwk, err := keys.KeyMaterial(ctx, "proj", "global", "kr", "wrap", "1")
	if err != nil {
		t.Fatal(err)
	}
	exported, err := core.ExportTrustedKeyWrappedCryptoKeyVersion(ctx, "proj", "global", "kr", "raw", v.Version,
		VersionName("proj", "global", "kr", "wrap", "1"))
	if err != nil {
		t.Fatalf("ExportTrustedKeyWrappedCryptoKeyVersion: %v", err)
	}
	back, err := kmsstore.UnwrapTrustedKey(kwk, exported)
	if err != nil {
		t.Fatalf("UnwrapTrustedKey: %v", err)
	}
	if !bytes.Equal(back, material) {
		t.Fatal("exported material does not round-trip")
	}
}

// TestCoreTrustedWrappingKeyValidation covers every documented rejection of an
// importing/wrapping key: it must be an ENABLED, HSM-trusted AES_WRAPPING key
// with the AES_256_KWP algorithm at HSM_SINGLE_TENANT, inside the target key
// ring.
func TestCoreTrustedWrappingKeyValidation(t *testing.T) {
	cases := []struct {
		name   string
		setup  func(t *testing.T, core *Service) string // returns importing_key
		code   string
		status int
	}{
		{
			name: "not AES_WRAPPING",
			setup: func(t *testing.T, core *Service) string {
				mustCreateKey(t, core, "kr", "enc", CryptoKeyInput{
					Purpose: "ENCRYPT_DECRYPT", ProtectionLevel: "HSM_SINGLE_TENANT",
				})
				return VersionName("proj", "global", "kr", "enc", "1")
			},
			code: "FailedPrecondition", status: 400,
		},
		{
			name: "not AES_256_KWP",
			setup: func(t *testing.T, core *Service) string {
				mustCreateKey(t, core, "kr", "gcm", CryptoKeyInput{
					Purpose: "AES_WRAPPING", Algorithm: "AES_256_GCM", ProtectionLevel: "HSM_SINGLE_TENANT",
				})
				return VersionName("proj", "global", "kr", "gcm", "1")
			},
			code: "FailedPrecondition", status: 400,
		},
		{
			name: "not HSM single tenant",
			setup: func(t *testing.T, core *Service) string {
				mustCreateKey(t, core, "kr", "soft", CryptoKeyInput{
					Purpose: "AES_WRAPPING", Algorithm: "AES_256_KWP", ProtectionLevel: "SOFTWARE",
				})
				return VersionName("proj", "global", "kr", "soft", "1")
			},
			code: "FailedPrecondition", status: 400,
		},
		{
			name: "disabled version",
			setup: func(t *testing.T, core *Service) string {
				trustedWrappingKey(t, core, "kr", "off")
				if _, err := core.UpdateVersionState(context.Background(), "proj", "global", "kr", "off", "1", "DISABLED"); err != nil {
					t.Fatalf("disable wrapping key: %v", err)
				}
				return VersionName("proj", "global", "kr", "off", "1")
			},
			code: "FailedPrecondition", status: 400,
		},
		{
			name: "import-only key without a version",
			setup: func(t *testing.T, core *Service) string {
				mustCreateKey(t, core, "kr", "empty", CryptoKeyInput{
					Purpose: "AES_WRAPPING", Algorithm: "AES_256_KWP", ProtectionLevel: "HSM_SINGLE_TENANT",
					SkipInitialVersion: true,
				})
				return CryptoKeyName("proj", "global", "kr", "empty")
			},
			code: "FailedPrecondition", status: 400,
		},
		{
			name: "outside the target key ring",
			setup: func(t *testing.T, core *Service) string {
				mustCreateKeyRing(t, core, "other")
				trustedWrappingKey(t, core, "other", "wrap")
				return VersionName("proj", "global", "other", "wrap", "1")
			},
			code: "InvalidArgument", status: 400,
		},
		{
			name:  "missing key",
			setup: func(t *testing.T, core *Service) string { return VersionName("proj", "global", "kr", "nope", "1") },
			code:  "NotFound", status: 404,
		},
		{
			name: "malformed name",
			setup: func(t *testing.T, core *Service) string {
				return "projects/proj/locations/global/keyRings/kr/cryptoKeys"
			},
			code: "InvalidArgument", status: 400,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			core, _ := newCore()
			mustCreateKeyRing(t, core, "kr")
			singleTenantRawKey(t, core, "kr", "raw")
			importing := tc.setup(t, core)
			_, err := core.ImportTrustedKeyWrappedCryptoKeyVersion(ctx, "proj", "global", "kr", "raw", ImportVersionRequest{
				ImportingKey: importing,
				Algorithm:    "AES_256_GCM",
				WrappedKey:   []byte("not-even-wrapped"),
			})
			code, status := providerErr(t, err)
			if code != tc.code || status != tc.status {
				t.Fatalf("error = %s (%d), want %s (%d)", code, status, tc.code, tc.status)
			}
		})
	}
}

// TestCoreTrustedWrappingTargetValidation covers the target-side rule from
// CryptoKeyVersion.trusted_wrapping_enabled / ImportCryptoKeyVersionRequest:
// trusted wrapping is never valid for ENCRYPT_DECRYPT and only for
// HSM_SINGLE_TENANT keys — on both the plain and the trusted import entry
// points, and at CreateCryptoKeyVersion.
func TestCoreTrustedWrappingTargetValidation(t *testing.T) {
	cases := []struct {
		name string
		in   CryptoKeyInput
	}{
		{"software key", CryptoKeyInput{Purpose: "RAW_ENCRYPT_DECRYPT", Algorithm: "AES_256_GCM"}},
		{"encrypt decrypt key", CryptoKeyInput{Purpose: "ENCRYPT_DECRYPT", ProtectionLevel: "HSM_SINGLE_TENANT"}},
		{"HSM key", CryptoKeyInput{Purpose: "RAW_ENCRYPT_DECRYPT", Algorithm: "AES_256_GCM", ProtectionLevel: "HSM"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			core, keys := newCore()
			mustCreateKeyRing(t, core, "kr")
			trustedWrappingKey(t, core, "kr", "wrap")
			mustCreateKey(t, core, "kr", "target", tc.in)

			kwk, err := keys.KeyMaterial(ctx, "proj", "global", "kr", "wrap", "1")
			if err != nil {
				t.Fatal(err)
			}
			wrapped, err := kmsstore.WrapTrustedKey(kwk, []byte("material"))
			if err != nil {
				t.Fatal(err)
			}
			_, err = core.ImportTrustedKeyWrappedCryptoKeyVersion(ctx, "proj", "global", "kr", "target", ImportVersionRequest{
				ImportingKey: VersionName("proj", "global", "kr", "wrap", "1"),
				Algorithm:    "AES_256_GCM",
				WrappedKey:   wrapped,
			})
			code, status := providerErr(t, err)
			if code != "InvalidArgument" || status != 400 {
				t.Fatalf("trusted import error = %s (%d), want InvalidArgument (400)", code, status)
			}

			// CreateCryptoKeyVersion applies the same rule.
			_, err = core.CreateVersion(ctx, "proj", "global", "kr", "target", CreateVersionInput{TrustedWrappingEnabled: true})
			code, status = providerErr(t, err)
			if code != "InvalidArgument" || status != 400 {
				t.Fatalf("CreateVersion error = %s (%d), want InvalidArgument (400)", code, status)
			}
		})
	}

	// The plain ImportCryptoKeyVersion request carries the flag too; an import
	// job into a software key cannot enable trusted wrapping.
	t.Run("plain import request", func(t *testing.T) {
		ctx := context.Background()
		core, _ := newCore()
		mustCreateKeyRing(t, core, "kr")
		job, wrapped := rsaWrappedImport(t, core, "kr")
		mustCreateKey(t, core, "kr", "target", CryptoKeyInput{Purpose: "RAW_ENCRYPT_DECRYPT", Algorithm: "AES_256_GCM"})
		_, err := core.ImportCryptoKeyVersion(ctx, "proj", "global", "kr", "target", ImportVersionRequest{
			ImportJob:              job.Name,
			Algorithm:              "AES_256_GCM",
			WrappedKey:             wrapped,
			TrustedWrappingEnabled: true,
		})
		code, status := providerErr(t, err)
		if code != "InvalidArgument" || status != 400 {
			t.Fatalf("import error = %s (%d), want InvalidArgument (400)", code, status)
		}
	})
}

// TestCoreTrustedWrappingEnabledOnCreate pins cryptoKeys.create
// trustedWrappingEnabled: it stashes the flag on the implicit first version, and
// the same documented restrictions reject it up front.
func TestCoreTrustedWrappingEnabledOnCreate(t *testing.T) {
	ctx := context.Background()
	core, keys := newCore()
	mustCreateKeyRing(t, core, "kr")
	trustedWrappingKey(t, core, "kr", "wrap")
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "raw", CryptoKeyInput{
		Purpose: "RAW_ENCRYPT_DECRYPT", Algorithm: "AES_256_GCM", ProtectionLevel: "HSM_SINGLE_TENANT",
		TrustedWrappingEnabled: true,
	}); err != nil {
		t.Fatalf("CreateCryptoKey(trustedWrappingEnabled): %v", err)
	}
	v, err := core.GetVersion(ctx, "proj", "global", "kr", "raw", "1")
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if !v.TrustedWrappingEnabled {
		t.Fatal("trustedWrappingEnabled at create must land on the first version")
	}
	kwk, err := keys.KeyMaterial(ctx, "proj", "global", "kr", "wrap", "1")
	if err != nil {
		t.Fatal(err)
	}
	exported, err := core.ExportTrustedKeyWrappedCryptoKeyVersion(ctx, "proj", "global", "kr", "raw", "1",
		VersionName("proj", "global", "kr", "wrap", "1"))
	if err != nil {
		t.Fatalf("ExportTrustedKeyWrappedCryptoKeyVersion: %v", err)
	}
	got, err := keys.KeyMaterial(ctx, "proj", "global", "kr", "raw", "1")
	if err != nil {
		t.Fatal(err)
	}
	back, err := kmsstore.UnwrapTrustedKey(kwk, exported)
	if err != nil {
		t.Fatalf("UnwrapTrustedKey: %v", err)
	}
	if !bytes.Equal(back, got) {
		t.Fatal("the first version of a trustedWrappingEnabled key must be exportable")
	}

	for _, tc := range []struct {
		name string
		in   CryptoKeyInput
	}{
		{"software key", CryptoKeyInput{Purpose: "RAW_ENCRYPT_DECRYPT", Algorithm: "AES_256_GCM", TrustedWrappingEnabled: true}},
		{"encrypt decrypt key", CryptoKeyInput{Purpose: "ENCRYPT_DECRYPT", ProtectionLevel: "HSM_SINGLE_TENANT", TrustedWrappingEnabled: true}},
		{"HSM key", CryptoKeyInput{Purpose: "RAW_ENCRYPT_DECRYPT", Algorithm: "AES_256_GCM", ProtectionLevel: "HSM", TrustedWrappingEnabled: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			core, _ := newCore()
			mustCreateKeyRing(t, core, "kr")
			_, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "k", tc.in)
			code, status := providerErr(t, err)
			if code != "InvalidArgument" || status != 400 {
				t.Fatalf("error = %s (%d), want InvalidArgument (400)", code, status)
			}
		})
	}
}

// TestCoreExternalProtectionLevelRejected pins the EKM boundary: the emulator
// has no EKM plane, so an EXTERNAL / EXTERNAL_VPC key or version fails loud
// (Unimplemented, 501) instead of fabricating SOFTWARE key material. External
// options on a software key are a documented shape error.
func TestCoreExternalProtectionLevelRejected(t *testing.T) {
	ctx := context.Background()
	for _, level := range []string{"EXTERNAL", "EXTERNAL_VPC"} {
		t.Run(level, func(t *testing.T) {
			core, _ := newCore()
			mustCreateKeyRing(t, core, "kr")
			_, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "ext", CryptoKeyInput{
				Purpose: "ENCRYPT_DECRYPT", ProtectionLevel: level,
			})
			code, status := providerErr(t, err)
			if code != "Unimplemented" || status != 501 {
				t.Fatalf("CreateCryptoKey error = %s (%d), want Unimplemented (501)", code, status)
			}

			// An EXTERNAL target cannot be imported into either, even though the
			// key itself could not be created through the API.
			mustCreateKey(t, core, "kr", "raw", CryptoKeyInput{Purpose: "RAW_ENCRYPT_DECRYPT", Algorithm: "AES_256_GCM"})
			_, err = core.CreateVersion(ctx, "proj", "global", "kr", "raw", CreateVersionInput{ExternalProtectionLevelOptions: true})
			code, status = providerErr(t, err)
			if code != "InvalidArgument" || status != 400 {
				t.Fatalf("CreateVersion(external options) error = %s (%d), want InvalidArgument (400)", code, status)
			}
		})
	}

	// An import job's protection level can name EXTERNAL; importing through it
	// must not fabricate a software version.
	t.Run("import job protection level", func(t *testing.T) {
		core, _ := newCore()
		mustCreateKeyRing(t, core, "kr")
		job, wrapped := rsaWrappedImport(t, core, "kr", "EXTERNAL")
		mustCreateKey(t, core, "kr", "raw", CryptoKeyInput{Purpose: "RAW_ENCRYPT_DECRYPT", Algorithm: "AES_256_GCM"})
		_, err := core.ImportCryptoKeyVersion(ctx, "proj", "global", "kr", "raw", ImportVersionRequest{
			ImportJob: job.Name, Algorithm: "AES_256_GCM", WrappedKey: wrapped,
		})
		code, status := providerErr(t, err)
		if code != "Unimplemented" || status != 501 {
			t.Fatalf("import error = %s (%d), want Unimplemented (501)", code, status)
		}
	})
}

// TestCoreReimportRejected pins that naming an existing CryptoKeyVersion as an
// import target fails loud instead of silently creating a new version.
func TestCoreReimportRejected(t *testing.T) {
	ctx := context.Background()
	core, _ := newCore()
	mustCreateKeyRing(t, core, "kr")
	trustedWrappingKey(t, core, "kr", "wrap")
	singleTenantRawKey(t, core, "kr", "raw")
	_, err := core.ImportCryptoKeyVersion(ctx, "proj", "global", "kr", "raw", ImportVersionRequest{
		Algorithm: "AES_256_GCM", ImportJob: "projects/proj/locations/global/keyRings/kr/importJobs/job", CryptoKeyVersion: "1",
	})
	code, status := providerErr(t, err)
	if code != "Unimplemented" || status != 501 {
		t.Fatalf("ImportCryptoKeyVersion(reimport) error = %s (%d), want Unimplemented (501)", code, status)
	}
	_, err = core.ImportTrustedKeyWrappedCryptoKeyVersion(ctx, "proj", "global", "kr", "raw", ImportVersionRequest{
		Algorithm: "AES_256_GCM", ImportingKey: VersionName("proj", "global", "kr", "wrap", "1"),
		WrappedKey: []byte("x"), CryptoKeyVersion: "1",
	})
	code, status = providerErr(t, err)
	if code != "Unimplemented" || status != 501 {
		t.Fatalf("ImportTrustedKeyWrappedCryptoKeyVersion(reimport) error = %s (%d), want Unimplemented (501)", code, status)
	}
}

// TestCoreExportTrustedRejections covers the export-side checks: the version
// must be trusted-wrappable, and wrapping_key must be a CryptoKeyVersion name
// for an HSM-trusted AES_WRAPPING / AES_256_KWP version.
func TestCoreExportTrustedRejections(t *testing.T) {
	ctx := context.Background()
	core, keys := newCore()
	mustCreateKeyRing(t, core, "kr")
	trustedWrappingKey(t, core, "kr", "wrap")
	singleTenantRawKey(t, core, "kr", "raw")
	mustCreateKey(t, core, "kr", "plain", CryptoKeyInput{
		Purpose: "ENCRYPT_DECRYPT", ProtectionLevel: "HSM_SINGLE_TENANT",
	})
	mustCreateKey(t, core, "kr", "softwrap", CryptoKeyInput{
		Purpose: "AES_WRAPPING", Algorithm: "AES_256_KWP", ProtectionLevel: "SOFTWARE",
	})
	v := trustedImport(t, core, keys, "raw", VersionName("proj", "global", "kr", "wrap", "1"), []byte("material"))

	cases := []struct {
		name        string
		version     string
		wrappingKey string
		code        string
		status      int
	}{
		{
			name: "version without trusted wrapping", version: "1",
			wrappingKey: VersionName("proj", "global", "kr", "wrap", "1"),
			code:        "FailedPrecondition", status: 400,
		},
		{
			name: "wrapping key not AES_WRAPPING", version: v.Version,
			wrappingKey: VersionName("proj", "global", "kr", "plain", "1"),
			code:        "FailedPrecondition", status: 400,
		},
		{
			name: "wrapping key not single tenant", version: v.Version,
			wrappingKey: VersionName("proj", "global", "kr", "softwrap", "1"),
			code:        "FailedPrecondition", status: 400,
		},
		{
			name: "wrapping key named as a CryptoKey", version: v.Version,
			wrappingKey: CryptoKeyName("proj", "global", "kr", "wrap"),
			code:        "InvalidArgument", status: 400,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := core.ExportTrustedKeyWrappedCryptoKeyVersion(ctx, "proj", "global", "kr", "raw", tc.version, tc.wrappingKey)
			code, status := providerErr(t, err)
			if code != tc.code || status != tc.status {
				t.Fatalf("error = %s (%d), want %s (%d)", code, status, tc.code, tc.status)
			}
		})
	}
}

// rsaWrappedImport creates an import job and returns it with key material
// wrapped by its RSA-OAEP + AES-KWP public key, ready for
// ImportCryptoKeyVersion.
func rsaWrappedImport(t *testing.T, core *Service, kr string, protectionLevel ...string) (ImportJob, []byte) {
	t.Helper()
	ctx := context.Background()
	in := ImportJobInput{ImportMethod: "RSA_OAEP_3072_SHA256_AES_256"}
	if len(protectionLevel) > 0 {
		in.ProtectionLevel = protectionLevel[0]
	}
	job, err := core.CreateImportJob(ctx, "proj", "global", kr, "job", in)
	if err != nil {
		t.Fatalf("CreateImportJob: %v", err)
	}
	block, _ := pem.Decode([]byte(job.PublicKeyPEM))
	if block == nil {
		t.Fatal("import job public key is not PEM")
	}
	pubAny, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("parse import job public key: %v", err)
	}
	rpub, ok := pubAny.(*rsa.PublicKey)
	if !ok {
		t.Fatalf("import job public key is %T, want RSA", pubAny)
	}
	kwpKey := make([]byte, 32)
	if _, err := rand.Read(kwpKey); err != nil {
		t.Fatal(err)
	}
	enc, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, rpub, kwpKey, nil)
	if err != nil {
		t.Fatalf("RSA-OAEP wrap: %v", err)
	}
	wrapped, err := kmsstore.AESKeyWrapWithPadding(kwpKey, bytes.Repeat([]byte{0x24}, 32))
	if err != nil {
		t.Fatalf("AES-KWP wrap: %v", err)
	}
	return job, append(enc, wrapped...)
}
