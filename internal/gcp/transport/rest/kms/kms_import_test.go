package kms

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"strings"
	"testing"

	kmsstore "jaiscloud/internal/gcp/store/kms"
)

func TestRESTImportDecapsulate(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	kr := "projects/proj/locations/global/keyRings/kr"

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("KeyRingCreate: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{"name": kr, "cryptoKeyId": "sym", "body": map[string]any{}})); err != nil {
		t.Fatalf("CryptoKeyCreate: %v", err)
	}

	// Import job.
	resp, err := p.ImportJobCreate(ctx, newNR(map[string]any{
		"name": kr, "importJobId": "job",
		"body": map[string]any{"importMethod": "RSA_OAEP_3072_SHA256_AES_256"},
	}))
	if err != nil {
		t.Fatalf("ImportJobCreate: %v", err)
	}
	pubMap, _ := resp.Data["publicKey"].(map[string]any)
	pemStr, _ := pubMap["pem"].(string)
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		t.Fatal("import job public key is not PEM")
	}
	pubAny, _ := x509.ParsePKIXPublicKey(block.Bytes)
	rpub := pubAny.(*rsa.PublicKey)

	material := []byte("0123456789abcdef0123456789abcdef")
	kwpKey := make([]byte, 32)
	rand.Read(kwpKey)
	enc, _ := rsa.EncryptOAEP(sha256.New(), rand.Reader, rpub, kwpKey, nil)
	wrappedKWP, _ := kmsstore.AESKeyWrapWithPadding(kwpKey, material)
	wrapped := append(enc, wrappedKWP...)

	if _, err := p.CryptoKeyVersionImport(ctx, newNR(map[string]any{
		"name": kr + "/cryptoKeys/sym",
		"body": map[string]any{
			"importJob":  kr + "/importJobs/job",
			"algorithm":  "GOOGLE_SYMMETRIC_ENCRYPTION",
			"wrappedKey": base64.StdEncoding.EncodeToString(wrapped),
		},
	})); err != nil {
		t.Fatalf("CryptoKeyVersionImport: %v", err)
	}

	// Encrypt/decrypt with the imported key (set it primary first).
	if _, err := p.CryptoKeyUpdatePrimaryVersion(ctx, newNR(map[string]any{
		"name": kr + "/cryptoKeys/sym",
		"body": map[string]any{"cryptoKeyVersionId": "2"},
	})); err != nil {
		t.Fatalf("UpdatePrimaryVersion: %v", err)
	}
	ctResp, err := p.CryptoKeyEncrypt(ctx, newNR(map[string]any{
		"name": kr + "/cryptoKeys/sym",
		"body": map[string]any{"plaintext": base64.StdEncoding.EncodeToString([]byte("hello"))},
	}))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	ctStr, _ := ctResp.Data["ciphertext"].(string)
	ptResp, err := p.CryptoKeyDecrypt(ctx, newNR(map[string]any{
		"name": kr + "/cryptoKeys/sym",
		"body": map[string]any{"ciphertext": ctStr},
	}))
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	pt, _ := base64.StdEncoding.DecodeString(ptResp.Data["plaintext"].(string))
	if string(pt) != "hello" {
		t.Fatalf("decrypt = %q, want hello", pt)
	}

	// KEM decapsulate.
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": kr, "cryptoKeyId": "kem",
		"body": map[string]any{
			"purpose":         "KEY_ENCAPSULATION",
			"versionTemplate": map[string]any{"algorithm": "ML_KEM_768"},
		},
	})); err != nil {
		t.Fatalf("CryptoKeyCreate(kem): %v", err)
	}
	kemVersion := kr + "/cryptoKeys/kem/cryptoKeyVersions/1"
	pkResp, err := p.CryptoKeyVersionGetPublicKey(ctx, newNR(map[string]any{
		"name": kemVersion + "/publicKey", "publicKeyFormat": "NIST_PQC",
	}))
	if err != nil {
		t.Fatalf("GetPublicKey: %v", err)
	}
	pubMap, ok := pkResp.Data["publicKey"].(map[string]any)
	if !ok {
		t.Fatalf("GetPublicKey(publicKey) = %T, want Discovery ChecksummedData object", pkResp.Data["publicKey"])
	}
	rawB64, _ := pubMap["data"].(string)
	raw, _ := base64.StdEncoding.DecodeString(rawB64)
	if got, want := pubMap["crc32cChecksum"], crc32cString(raw); got != want {
		t.Fatalf("publicKey.crc32cChecksum = %v, want %s (CRC32C of the decoded data)", got, want)
	}
	ss, kemCT, err := kmsstore.EncapsulateKEM("ML_KEM_768", raw)
	if err != nil {
		t.Fatalf("EncapsulateKEM: %v", err)
	}
	decResp, err := p.CryptoKeyVersionDecapsulate(ctx, newNR(map[string]any{
		"name": kemVersion,
		"body": map[string]any{"ciphertext": base64.StdEncoding.EncodeToString(kemCT)},
	}))
	if err != nil {
		t.Fatalf("Decapsulate: %v", err)
	}
	ssGot, _ := base64.StdEncoding.DecodeString(decResp.Data["sharedSecret"].(string))
	if string(ssGot) != string(ss) {
		t.Fatal("decapsulated shared secret mismatch")
	}
}

// TestGETPublicKeyFormatRules pins the Discovery contract for
// GetPublicKeyRequest.public_key_format: it is required for PQC algorithms and
// the raw key is returned as ChecksummedData; PEM algorithms need no format.
func TestGETPublicKeyFormatRules(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	kr := "projects/proj/locations/global/keyRings/kr"
	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("KeyRingCreate: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": kr, "cryptoKeyId": "kem",
		"body": map[string]any{"purpose": "KEY_ENCAPSULATION", "versionTemplate": map[string]any{"algorithm": "ML_KEM_768"}},
	})); err != nil {
		t.Fatalf("CryptoKeyCreate: %v", err)
	}
	kemVersion := kr + "/cryptoKeys/kem/cryptoKeyVersions/1"
	if _, err := p.CryptoKeyVersionGetPublicKey(ctx, newNR(map[string]any{"name": kemVersion + "/publicKey"})); err == nil {
		t.Fatal("getPublicKey without public_key_format must fail for a PQC algorithm")
	}
	resp, err := p.CryptoKeyVersionGetPublicKey(ctx, newNR(map[string]any{
		"name": kemVersion + "/publicKey", "publicKeyFormat": "NIST_PQC",
	}))
	if err != nil {
		t.Fatalf("GetPublicKey(NIST_PQC): %v", err)
	}
	if resp.Data["publicKeyFormat"] != "NIST_PQC" {
		t.Fatalf("publicKeyFormat = %v, want NIST_PQC", resp.Data["publicKeyFormat"])
	}
	if _, ok := resp.Data["publicKey"].(map[string]any); !ok {
		t.Fatalf("publicKey = %T, want ChecksummedData object", resp.Data["publicKey"])
	}

	// An asymmetric PEM key needs no format.
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": kr, "cryptoKeyId": "sign",
		"body": map[string]any{"purpose": "ASYMMETRIC_SIGN", "versionTemplate": map[string]any{"algorithm": "EC_SIGN_P256_SHA256"}},
	})); err != nil {
		t.Fatalf("CryptoKeyCreate(sign): %v", err)
	}
	pemResp, err := p.CryptoKeyVersionGetPublicKey(ctx, newNR(map[string]any{
		"name": kr + "/cryptoKeys/sign/cryptoKeyVersions/1/publicKey",
	}))
	if err != nil {
		t.Fatalf("GetPublicKey(PEM): %v", err)
	}
	if pemResp.Data["pem"] == "" {
		t.Fatal("PEM public key is empty")
	}
}

// TestImportJobPQCPublicKeyShape pins WrappingPublicKey: a PQC import job
// returns its wrapping key as raw base64 `data` only — WrappingPublicKey has no
// checksum field, unlike PublicKey.publicKey (ChecksummedData).
func TestImportJobPQCPublicKeyShape(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	kr := "projects/proj/locations/global/keyRings/kr"
	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("KeyRingCreate: %v", err)
	}
	resp, err := p.ImportJobCreate(ctx, newNR(map[string]any{
		"name": kr, "importJobId": "pqc",
		"body": map[string]any{"importMethod": "HPKE_KEM_ML_KEM_768_HKDF_SHA256_AES_256_GCM"},
	}))
	if err != nil {
		t.Fatalf("ImportJobCreate: %v", err)
	}
	pub, ok := resp.Data["publicKey"].(map[string]any)
	if !ok {
		t.Fatalf("publicKey = %T, want WrappingPublicKey object", resp.Data["publicKey"])
	}
	if pub["data"] == nil || pub["data"] == "" {
		t.Fatal("PQC import job is missing publicKey.data")
	}
	if _, ok := pub["crc32cChecksum"]; ok {
		t.Fatal("WrappingPublicKey has no crc32cChecksum field; the response must not invent one")
	}
}

// TestCryptoOperationCRCFlags verifies the verified*Crc32c flags report whether
// the caller sent the matching checksum, and that a mismatched checksum is
// rejected (gRPC already behaves this way).
func TestCryptoOperationCRCFlags(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	kr := "projects/proj/locations/global/keyRings/kr"
	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("KeyRingCreate: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": kr, "cryptoKeyId": "mac",
		"body": map[string]any{"purpose": "MAC", "versionTemplate": map[string]any{"algorithm": "HMAC_SHA256"}},
	})); err != nil {
		t.Fatalf("CryptoKeyCreate: %v", err)
	}
	macVersion := kr + "/cryptoKeys/mac/cryptoKeyVersions/1"
	data := []byte("crc-flag-data")

	// No checksum in the request -> flag false.
	resp, err := p.CryptoKeyVersionMacSign(ctx, newNR(map[string]any{
		"name": macVersion, "body": map[string]any{"data": base64.StdEncoding.EncodeToString(data)},
	}))
	if err != nil {
		t.Fatalf("MacSign: %v", err)
	}
	if resp.Data["verifiedDataCrc32c"] != false {
		t.Fatalf("verifiedDataCrc32c = %v with no request checksum, want false", resp.Data["verifiedDataCrc32c"])
	}

	// Matching checksum -> flag true; mismatching checksum -> 400.
	for _, tc := range []struct {
		crc    string
		reject bool
	}{{crc32cString(data), false}, {"1", true}} {
		_, err := p.CryptoKeyVersionMacSign(ctx, newNR(map[string]any{
			"name": macVersion,
			"body": map[string]any{
				"data":       base64.StdEncoding.EncodeToString(data),
				"dataCrc32c": tc.crc,
			},
		}))
		if tc.reject && err == nil {
			t.Fatal("MacSign with a mismatched dataCrc32c must fail")
		}
		if !tc.reject && err != nil {
			t.Fatalf("MacSign with a matching dataCrc32c: %v", err)
		}
	}
}

// TestGETPublicKeyFormats covers the public_key_format contract on asymmetric
// keys: PEM is the default, DER returns the PKIX bytes as ChecksummedData, and
// a PQC-only format is rejected.
func TestGETPublicKeyFormats(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	kr := "projects/proj/locations/global/keyRings/kr"
	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("KeyRingCreate: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": kr, "cryptoKeyId": "sign",
		"body": map[string]any{"purpose": "ASYMMETRIC_SIGN", "versionTemplate": map[string]any{"algorithm": "EC_SIGN_P256_SHA256"}},
	})); err != nil {
		t.Fatalf("CryptoKeyCreate: %v", err)
	}
	version := kr + "/cryptoKeys/sign/cryptoKeyVersions/1"

	pemResp, err := p.CryptoKeyVersionGetPublicKey(ctx, newNR(map[string]any{"name": version + "/publicKey"}))
	if err != nil {
		t.Fatalf("GetPublicKey(default): %v", err)
	}
	if pemResp.Data["publicKeyFormat"] != "PEM" || pemResp.Data["pem"] == "" {
		t.Fatalf("default public key = %v, want PEM fields", pemResp.Data)
	}

	derResp, err := p.CryptoKeyVersionGetPublicKey(ctx, newNR(map[string]any{
		"name": version + "/publicKey", "publicKeyFormat": "DER",
	}))
	if err != nil {
		t.Fatalf("GetPublicKey(DER): %v", err)
	}
	if derResp.Data["publicKeyFormat"] != "DER" {
		t.Fatalf("publicKeyFormat = %v, want DER", derResp.Data["publicKeyFormat"])
	}
	der, ok := derResp.Data["publicKey"].(map[string]any)
	if !ok {
		t.Fatalf("DER publicKey = %T, want ChecksummedData object", derResp.Data["publicKey"])
	}
	derBytes, _ := base64.StdEncoding.DecodeString(der["data"].(string))
	if _, err := x509.ParsePKIXPublicKey(derBytes); err != nil {
		t.Fatalf("DER public key does not parse as PKIX: %v", err)
	}

	if _, err := p.CryptoKeyVersionGetPublicKey(ctx, newNR(map[string]any{
		"name": version + "/publicKey", "publicKeyFormat": "NIST_PQC",
	})); err == nil {
		t.Fatal("a PQC-only public_key_format must be rejected for an EC key")
	}
}

// TestRESTDeleteReturnsOperation pins the Discovery response type of
// cryptoKeys.delete / cryptoKeyVersions.delete: a completed
// google.longrunning.Operation (the gRPC adapter already returns one).
func TestRESTDeleteReturnsOperation(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	kr := "projects/proj/locations/global/keyRings/kr"
	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("KeyRingCreate: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": kr, "cryptoKeyId": "imp",
		"body": map[string]any{"purpose": "ENCRYPT_DECRYPT", "importOnly": true},
	})); err != nil {
		t.Fatalf("CryptoKeyCreate: %v", err)
	}
	resp, err := p.CryptoKeyDelete(ctx, newNR(map[string]any{"name": kr + "/cryptoKeys/imp"}))
	if err != nil {
		t.Fatalf("CryptoKeyDelete: %v", err)
	}
	if resp.Data["done"] != true {
		t.Fatalf("delete response = %v, want a completed Operation", resp.Data)
	}
	if name, _ := resp.Data["name"].(string); !strings.HasSuffix(name, "/operations/delete") {
		t.Fatalf("operation name = %q, want a .../operations/delete name", name)
	}
}
