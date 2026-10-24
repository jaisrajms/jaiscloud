package kms

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"testing"

	kmspb "cloud.google.com/go/kms/apiv1/kmspb"

	kmsstore "jaiscloud/internal/gcp/store/kms"
)

func TestKMSImportCryptoKeyVersionRPC(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()

	createKeyRing(t, client, "impkr")
	kr := "projects/test/locations/global/keyRings/impkr"
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: kr, CryptoKeyId: "sym"}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	job, err := client.CreateImportJob(ctx, &kmspb.CreateImportJobRequest{
		Parent: kr, ImportJobId: "job",
		ImportJob: &kmspb.ImportJob{ImportMethod: kmspb.ImportJob_RSA_OAEP_3072_SHA256_AES_256},
	})
	if err != nil {
		t.Fatalf("CreateImportJob: %v", err)
	}
	block, _ := pem.Decode([]byte(job.GetPublicKey().GetPem()))
	if block == nil {
		t.Fatal("import job public key is not PEM")
	}
	pubAny, _ := x509.ParsePKIXPublicKey(block.Bytes)
	rpub := pubAny.(*rsa.PublicKey)

	material := []byte("0123456789abcdef0123456789abcdef")
	kwpKey := make([]byte, 32)
	rand.Read(kwpKey)
	enc, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, rpub, kwpKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	wrappedKWP, err := kmsstore.AESKeyWrapWithPadding(kwpKey, material)
	if err != nil {
		t.Fatal(err)
	}
	v, err := client.ImportCryptoKeyVersion(ctx, &kmspb.ImportCryptoKeyVersionRequest{
		Parent:     kr + "/cryptoKeys/sym",
		ImportJob:  job.GetName(),
		Algorithm:  kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION,
		WrappedKey: append(enc, wrappedKWP...),
	})
	if err != nil {
		t.Fatalf("ImportCryptoKeyVersion: %v", err)
	}
	if v.GetState() != kmspb.CryptoKeyVersion_ENABLED || v.GetImportTime() == nil {
		t.Fatalf("imported version state=%v importTime=%v", v.GetState(), v.GetImportTime())
	}
}

func TestKMSDecapsulateRPC(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()

	createKeyRing(t, client, "kemkr")
	kr := "projects/test/locations/global/keyRings/kemkr"
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent: kr, CryptoKeyId: "kem",
		CryptoKey: &kmspb.CryptoKey{
			Purpose: kmspb.CryptoKey_KEY_ENCAPSULATION,
			VersionTemplate: &kmspb.CryptoKeyVersionTemplate{
				Algorithm: kmspb.CryptoKeyVersion_ML_KEM_768,
			},
		},
	}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	versionName := kr + "/cryptoKeys/kem/cryptoKeyVersions/1"
	pk, err := client.GetPublicKey(ctx, &kmspb.GetPublicKeyRequest{
		Name:            versionName,
		PublicKeyFormat: kmspb.PublicKey_NIST_PQC,
	})
	if err != nil {
		t.Fatalf("GetPublicKey: %v", err)
	}
	raw := pk.GetPublicKey().GetData()
	if len(raw) == 0 {
		t.Fatal("KEM public key should be raw bytes")
	}
	ss, ct, err := kmsstore.EncapsulateKEM("ML_KEM_768", raw)
	if err != nil {
		t.Fatalf("EncapsulateKEM: %v", err)
	}
	resp, err := client.Decapsulate(ctx, &kmspb.DecapsulateRequest{Name: versionName, Ciphertext: ct})
	if err != nil {
		t.Fatalf("Decapsulate: %v", err)
	}
	if string(resp.GetSharedSecret()) != string(ss) {
		t.Fatal("decapsulated shared secret mismatch")
	}
}

func TestKMSExportTrustedRoundTrip(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()

	createKeyRing(t, client, "twkr")
	kr := "projects/test/locations/global/keyRings/twkr"
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent: kr, CryptoKeyId: "wrap",
		CryptoKey: &kmspb.CryptoKey{
			Purpose:         kmspb.CryptoKey_AES_WRAPPING,
			VersionTemplate: &kmspb.CryptoKeyVersionTemplate{Algorithm: kmspb.CryptoKeyVersion_AES_256_KWP, ProtectionLevel: kmspb.ProtectionLevel_HSM_SINGLE_TENANT},
		},
	}); err != nil {
		t.Fatalf("CreateCryptoKey(wrap): %v", err)
	}
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: kr, CryptoKeyId: "sym"}); err != nil {
		t.Fatalf("CreateCryptoKey(sym): %v", err)
	}
	// Exporting a generated version must fail: trusted wrapping is only enabled
	// on versions created by a trusted import.
	if _, err := client.ExportTrustedKeyWrappedCryptoKeyVersion(ctx, &kmspb.ExportTrustedKeyWrappedCryptoKeyVersionRequest{
		Name:        kr + "/cryptoKeys/sym/cryptoKeyVersions/1",
		WrappingKey: kr + "/cryptoKeys/wrap/cryptoKeyVersions/1",
	}); err == nil {
		t.Fatal("export of a non-trusted version should fail")
	}
}
