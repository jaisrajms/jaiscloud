package grpcconformance

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// kmsExtraChecks covers the Cloud KMS RPCs beyond the create/get/list/delete
// control-plane probes in checks_kms.go: the cryptographic operations and the
// crypto-key-version lifecycle.
//
// Every probe creates (idempotently) the fixtures it needs, keyed by
// cfg.ResourceName, so the sequence is self-contained and safe against a
// long-lived emulator. The keyring itself is created by the existing
// CreateKeyRing probe, which runs first.
func kmsExtraChecks() []Check {
	return []Check{
		// ── symmetric encryption ────────────────────────────────────────────
		{Service: "kms", RPC: "Encrypt", Method: "Encrypt", KeyField: "ciphertext non-empty, name=version", Run: checkKMSEncrypt},
		{Service: "kms", RPC: "Decrypt", Method: "Decrypt", KeyField: "plaintext round-trips", Run: checkKMSDecrypt},
		{Service: "kms", RPC: "GenerateRandomBytes", Method: "GenerateRandomBytes", KeyField: "length_bytes of non-zero data", Run: checkKMSGenerateRandomBytes},

		// ── crypto-key-version lifecycle ────────────────────────────────────
		{Service: "kms", RPC: "CreateCryptoKeyVersion", Method: "CreateCryptoKeyVersion", KeyField: "version name/state=ENABLED", Run: checkKMSCreateCryptoKeyVersion},
		{Service: "kms", RPC: "GetCryptoKeyVersion", Method: "GetCryptoKeyVersion", KeyField: "version name/state", Run: checkKMSGetCryptoKeyVersion},
		{Service: "kms", RPC: "ListCryptoKeyVersions", Method: "ListCryptoKeyVersions", KeyField: "cryptoKeyVersions[].name contains v1", Run: checkKMSListCryptoKeyVersions},
		{Service: "kms", RPC: "UpdateCryptoKeyVersion", Method: "UpdateCryptoKeyVersion", KeyField: "state ENABLED<->DISABLED", Run: checkKMSUpdateCryptoKeyVersion},
		{Service: "kms", RPC: "RestoreCryptoKeyVersion", Method: "RestoreCryptoKeyVersion", KeyField: "restored state=DISABLED", Run: checkKMSRestoreCryptoKeyVersion},

		// ── asymmetric ──────────────────────────────────────────────────────
		{Service: "kms", RPC: "GetPublicKey", Method: "GetPublicKey", KeyField: "PEM/algorithm", Run: checkKMSGetPublicKey},
		{Service: "kms", RPC: "AsymmetricSign", Method: "AsymmetricSign", KeyField: "signature verifies with public key", Run: checkKMSAsymmetricSign},
		{Service: "kms", RPC: "AsymmetricDecrypt", Method: "AsymmetricDecrypt", KeyField: "plaintext round-trips", Run: checkKMSAsymmetricDecrypt},

		// ── MAC ─────────────────────────────────────────────────────────────
		{Service: "kms", RPC: "MacSign", Method: "MacSign", KeyField: "mac non-empty", Run: checkKMSMacSign},
		{Service: "kms", RPC: "MacVerify", Method: "MacVerify", KeyField: "success=true; wrong data=false", Run: checkKMSMacVerify},

		// ── crypto key metadata ─────────────────────────────────────────────
		{Service: "kms", RPC: "ListCryptoKeys", Method: "ListCryptoKeys", KeyField: "cryptoKeys[].name contains sym key", Run: checkKMSListCryptoKeys},
		{Service: "kms", RPC: "UpdateCryptoKey", Method: "UpdateCryptoKey", KeyField: "masked labels applied", Run: checkKMSUpdateCryptoKey},
		{Service: "kms", RPC: "UpdateCryptoKeyPrimaryVersion", Method: "UpdateCryptoKeyPrimaryVersion", KeyField: "primary version updated", Run: checkKMSUpdateCryptoKeyPrimaryVersion},

		// ── raw (portable AES-GCM) ──────────────────────────────────────────
		{Service: "kms", RPC: "RawEncrypt", Method: "RawEncrypt", KeyField: "ciphertext/iv/tag_length", Run: checkKMSRawEncrypt},
		{Service: "kms", RPC: "RawDecrypt", Method: "RawDecrypt", KeyField: "plaintext round-trips with IV+AAD", Run: checkKMSRawDecrypt},

		// ── delete + retired resources ──────────────────────────────────────
		{Service: "kms", RPC: "DeleteCryptoKeyVersion", Method: "DeleteCryptoKeyVersion", KeyField: "version absent after delete", Run: checkKMSDeleteCryptoKeyVersion},
		{Service: "kms", RPC: "DeleteCryptoKey", Method: "DeleteCryptoKey", KeyField: "key absent after delete", Run: checkKMSDeleteCryptoKey},
		{Service: "kms", RPC: "ListRetiredResources", Method: "ListRetiredResources", KeyField: "retiredResources[] contains deleted key", Run: checkKMSListRetiredResources},
		{Service: "kms", RPC: "GetRetiredResource", Method: "GetRetiredResource", KeyField: "originalResource/deleteTime", Run: checkKMSGetRetiredResource},

		// ── import jobs ─────────────────────────────────────────────────────
		{Service: "kms", RPC: "CreateImportJob", Method: "CreateImportJob", KeyField: "name/importMethod/state=ACTIVE/publicKey", Run: checkKMSCreateImportJob},
		{Service: "kms", RPC: "GetImportJob", Method: "GetImportJob", KeyField: "importMethod/state", Run: checkKMSGetImportJob},
		{Service: "kms", RPC: "ListImportJobs", Method: "ListImportJobs", KeyField: "importJobs[] contains job", Run: checkKMSListImportJobs},
	}
}

// ─── fixture helpers ──────────────────────────────────────────────────────────

// kmsExtraKeyID is the run-unique crypto-key id for a fixture kind.
func kmsExtraKeyID(cfg Config, kind string) string { return cfg.ResourceName("gcpc-" + kind) }

// kmsExtraKeyName is the full crypto-key resource name for a fixture kind.
func kmsExtraKeyName(cfg Config, kind string) string {
	return kmsRingName(cfg) + "/cryptoKeys/" + kmsExtraKeyID(cfg, kind)
}

// kmsExtraVersionName addresses a version of a fixture key.
func kmsExtraVersionName(cfg Config, kind, version string) string {
	return kmsExtraKeyName(cfg, kind) + "/cryptoKeyVersions/" + version
}

// kmsEnsureKey returns the fixture key name, creating the key if it does not
// exist yet. The keyring itself is created by the existing CreateKeyRing probe.
func kmsEnsureKey(ctx context.Context, client *kms.KeyManagementClient, cfg Config, kind string, purpose kmspb.CryptoKey_CryptoKeyPurpose, algo kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm) (string, error) {
	name := kmsExtraKeyName(cfg, kind)
	_, err := client.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: name})
	if err == nil {
		return name, nil
	}
	if status.Code(err) != codes.NotFound {
		return "", fmt.Errorf("GetCryptoKey(%s): %w", kind, err)
	}
	ck := &kmspb.CryptoKey{Purpose: purpose}
	if algo != kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED {
		ck.VersionTemplate = &kmspb.CryptoKeyVersionTemplate{
			Algorithm:       algo,
			ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
		}
	}
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent:      kmsRingName(cfg),
		CryptoKeyId: kmsExtraKeyID(cfg, kind),
		CryptoKey:   ck,
	}); err != nil {
		return "", fmt.Errorf("CreateCryptoKey(%s): %w", kind, err)
	}
	return name, nil
}

// kmsRSAPublicKey fetches a version's public key and parses it as an RSA key.
func kmsRSAPublicKey(ctx context.Context, client *kms.KeyManagementClient, name string) (*rsa.PublicKey, error) {
	pk, err := client.GetPublicKey(ctx, &kmspb.GetPublicKeyRequest{Name: name})
	if err != nil {
		return nil, fmt.Errorf("GetPublicKey: %w", err)
	}
	block, _ := pem.Decode([]byte(pk.GetPem()))
	if block == nil {
		return nil, fmt.Errorf("GetPublicKey returned a non-PEM public key")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse public key: %w", err)
	}
	rk, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("public key is %T, want *rsa.PublicKey", pub)
	}
	return rk, nil
}

// ─── symmetric ────────────────────────────────────────────────────────────────

func checkKMSEncrypt(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	keyName, err := kmsEnsureKey(ctx, client, cfg, "sym", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED)
	if err != nil {
		return err
	}
	resp, err := client.Encrypt(ctx, &kmspb.EncryptRequest{
		Name:      keyName,
		Plaintext: []byte("gcpc-kms-encrypt"),
	})
	if err != nil {
		return fmt.Errorf("Encrypt: %w", err)
	}
	if !strings.HasPrefix(resp.GetName(), keyName+"/cryptoKeyVersions/") {
		return fmt.Errorf("Encrypt name = %q, want a %s/cryptoKeyVersions/ prefix", resp.GetName(), keyName)
	}
	if len(resp.GetCiphertext()) == 0 {
		return fmt.Errorf("Encrypt returned an empty ciphertext")
	}
	return nil
}

func checkKMSDecrypt(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	keyName, err := kmsEnsureKey(ctx, client, cfg, "sym", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED)
	if err != nil {
		return err
	}
	plaintext := []byte("gcpc-kms-decrypt")
	enc, err := client.Encrypt(ctx, &kmspb.EncryptRequest{Name: keyName, Plaintext: plaintext})
	if err != nil {
		return fmt.Errorf("Encrypt: %w", err)
	}
	dec, err := client.Decrypt(ctx, &kmspb.DecryptRequest{Name: keyName, Ciphertext: enc.GetCiphertext()})
	if err != nil {
		return fmt.Errorf("Decrypt: %w", err)
	}
	if !bytes.Equal(dec.GetPlaintext(), plaintext) {
		return fmt.Errorf("Decrypt plaintext = %q, want %q", dec.GetPlaintext(), plaintext)
	}
	return nil
}

func checkKMSGenerateRandomBytes(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	const n = 32
	resp, err := client.GenerateRandomBytes(ctx, &kmspb.GenerateRandomBytesRequest{
		Location:    kmsParent(cfg),
		LengthBytes: n,
	})
	if err != nil {
		return fmt.Errorf("GenerateRandomBytes: %w", err)
	}
	if len(resp.GetData()) != n {
		return fmt.Errorf("GenerateRandomBytes returned %d bytes, want %d", len(resp.GetData()), n)
	}
	nonZero := false
	for _, b := range resp.GetData() {
		if b != 0 {
			nonZero = true
			break
		}
	}
	if !nonZero {
		return fmt.Errorf("GenerateRandomBytes returned all-zero data")
	}
	return nil
}

// ─── crypto-key-version lifecycle ─────────────────────────────────────────────

func checkKMSCreateCryptoKeyVersion(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	keyName, err := kmsEnsureKey(ctx, client, cfg, "ver", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED)
	if err != nil {
		return err
	}
	ver, err := client.CreateCryptoKeyVersion(ctx, &kmspb.CreateCryptoKeyVersionRequest{Parent: keyName})
	if err != nil {
		return fmt.Errorf("CreateCryptoKeyVersion: %w", err)
	}
	if !strings.HasPrefix(ver.GetName(), keyName+"/cryptoKeyVersions/") {
		return fmt.Errorf("CreateCryptoKeyVersion name = %q, want a %s/cryptoKeyVersions/ prefix", ver.GetName(), keyName)
	}
	if ver.GetState() != kmspb.CryptoKeyVersion_ENABLED {
		return fmt.Errorf("CreateCryptoKeyVersion state = %v, want ENABLED", ver.GetState())
	}
	return nil
}

func checkKMSGetCryptoKeyVersion(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if _, err := kmsEnsureKey(ctx, client, cfg, "ver", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED); err != nil {
		return err
	}
	name := kmsExtraVersionName(cfg, "ver", "1")
	ver, err := client.GetCryptoKeyVersion(ctx, &kmspb.GetCryptoKeyVersionRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetCryptoKeyVersion: %w", err)
	}
	if ver.GetName() != name {
		return fmt.Errorf("GetCryptoKeyVersion name = %q, want %q", ver.GetName(), name)
	}
	if ver.GetState() != kmspb.CryptoKeyVersion_ENABLED {
		return fmt.Errorf("GetCryptoKeyVersion state = %v, want ENABLED", ver.GetState())
	}
	return nil
}

func checkKMSListCryptoKeyVersions(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	keyName, err := kmsEnsureKey(ctx, client, cfg, "ver", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED)
	if err != nil {
		return err
	}
	want := kmsExtraVersionName(cfg, "ver", "1")
	it := client.ListCryptoKeyVersions(ctx, &kmspb.ListCryptoKeyVersionsRequest{Parent: keyName})
	for {
		v, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListCryptoKeyVersions: %w", err)
		}
		if v.GetName() == want {
			return nil
		}
	}
	return fmt.Errorf("ListCryptoKeyVersions did not include %q", want)
}

func checkKMSUpdateCryptoKeyVersion(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	keyName, err := kmsEnsureKey(ctx, client, cfg, "ver", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED)
	if err != nil {
		return err
	}
	created, err := client.CreateCryptoKeyVersion(ctx, &kmspb.CreateCryptoKeyVersionRequest{Parent: keyName})
	if err != nil {
		return fmt.Errorf("CreateCryptoKeyVersion: %w", err)
	}
	disabled, err := client.UpdateCryptoKeyVersion(ctx, &kmspb.UpdateCryptoKeyVersionRequest{
		CryptoKeyVersion: &kmspb.CryptoKeyVersion{Name: created.GetName(), State: kmspb.CryptoKeyVersion_DISABLED},
		UpdateMask:       &fieldmaskpb.FieldMask{Paths: []string{"state"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateCryptoKeyVersion(DISABLED): %w", err)
	}
	if disabled.GetState() != kmspb.CryptoKeyVersion_DISABLED {
		return fmt.Errorf("state after disable = %v, want DISABLED", disabled.GetState())
	}
	enabled, err := client.UpdateCryptoKeyVersion(ctx, &kmspb.UpdateCryptoKeyVersionRequest{
		CryptoKeyVersion: &kmspb.CryptoKeyVersion{Name: created.GetName(), State: kmspb.CryptoKeyVersion_ENABLED},
		UpdateMask:       &fieldmaskpb.FieldMask{Paths: []string{"state"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateCryptoKeyVersion(ENABLED): %w", err)
	}
	if enabled.GetState() != kmspb.CryptoKeyVersion_ENABLED {
		return fmt.Errorf("state after enable = %v, want ENABLED", enabled.GetState())
	}
	return nil
}

func checkKMSRestoreCryptoKeyVersion(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if _, err := kmsEnsureKey(ctx, client, cfg, "restore", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED); err != nil {
		return err
	}
	name := kmsExtraVersionName(cfg, "restore", "1")
	if _, err := client.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: name}); err != nil {
		return fmt.Errorf("DestroyCryptoKeyVersion: %w", err)
	}
	restored, err := client.RestoreCryptoKeyVersion(ctx, &kmspb.RestoreCryptoKeyVersionRequest{Name: name})
	if err != nil {
		return fmt.Errorf("RestoreCryptoKeyVersion: %w", err)
	}
	if restored.GetState() != kmspb.CryptoKeyVersion_DISABLED {
		return fmt.Errorf("RestoreCryptoKeyVersion state = %v, want DISABLED", restored.GetState())
	}
	return nil
}

// ─── asymmetric ───────────────────────────────────────────────────────────────

func kmsEnsureSignKey(ctx context.Context, client *kms.KeyManagementClient, cfg Config) (string, error) {
	return kmsEnsureKey(ctx, client, cfg, "sign", kmspb.CryptoKey_ASYMMETRIC_SIGN, kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_2048_SHA256)
}

func kmsEnsureDecryptKey(ctx context.Context, client *kms.KeyManagementClient, cfg Config) (string, error) {
	return kmsEnsureKey(ctx, client, cfg, "dec", kmspb.CryptoKey_ASYMMETRIC_DECRYPT, kmspb.CryptoKeyVersion_RSA_DECRYPT_OAEP_2048_SHA256)
}

func checkKMSGetPublicKey(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if _, err := kmsEnsureSignKey(ctx, client, cfg); err != nil {
		return err
	}
	name := kmsExtraVersionName(cfg, "sign", "1")
	pk, err := client.GetPublicKey(ctx, &kmspb.GetPublicKeyRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetPublicKey: %w", err)
	}
	if !strings.Contains(pk.GetPem(), "BEGIN PUBLIC KEY") {
		return fmt.Errorf("GetPublicKey pem does not look like a PEM public key")
	}
	if pk.GetAlgorithm() != kmspb.CryptoKeyVersion_RSA_SIGN_PKCS1_2048_SHA256 {
		return fmt.Errorf("GetPublicKey algorithm = %v, want RSA_SIGN_PKCS1_2048_SHA256", pk.GetAlgorithm())
	}
	if pk.GetName() != name {
		return fmt.Errorf("GetPublicKey name = %q, want %q", pk.GetName(), name)
	}
	return nil
}

func checkKMSAsymmetricSign(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if _, err := kmsEnsureSignKey(ctx, client, cfg); err != nil {
		return err
	}
	versionName := kmsExtraVersionName(cfg, "sign", "1")
	digest := sha256.Sum256([]byte("gcpc-kms-sign"))
	resp, err := client.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{
		Name:   versionName,
		Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: digest[:]}},
	})
	if err != nil {
		return fmt.Errorf("AsymmetricSign: %w", err)
	}
	if len(resp.GetSignature()) == 0 {
		return fmt.Errorf("AsymmetricSign returned an empty signature")
	}
	// Verify the signature with the published public key: proves the signature
	// is a real RSA-PKCS1v15 signature over the submitted digest.
	pub, err := kmsRSAPublicKey(ctx, client, versionName)
	if err != nil {
		return err
	}
	if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], resp.GetSignature()); err != nil {
		return fmt.Errorf("signature did not verify with GetPublicKey output: %w", err)
	}
	return nil
}

func checkKMSAsymmetricDecrypt(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if _, err := kmsEnsureDecryptKey(ctx, client, cfg); err != nil {
		return err
	}
	versionName := kmsExtraVersionName(cfg, "dec", "1")
	pub, err := kmsRSAPublicKey(ctx, client, versionName)
	if err != nil {
		return err
	}
	plaintext := []byte("gcpc-kms-asym-decrypt")
	ciphertext, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, plaintext, nil)
	if err != nil {
		return fmt.Errorf("RSA-OAEP encrypt with public key: %w", err)
	}
	resp, err := client.AsymmetricDecrypt(ctx, &kmspb.AsymmetricDecryptRequest{Name: versionName, Ciphertext: ciphertext})
	if err != nil {
		return fmt.Errorf("AsymmetricDecrypt: %w", err)
	}
	if !bytes.Equal(resp.GetPlaintext(), plaintext) {
		return fmt.Errorf("AsymmetricDecrypt plaintext = %q, want %q", resp.GetPlaintext(), plaintext)
	}
	return nil
}

// ─── MAC ──────────────────────────────────────────────────────────────────────

func kmsEnsureMACKey(ctx context.Context, client *kms.KeyManagementClient, cfg Config) (string, error) {
	return kmsEnsureKey(ctx, client, cfg, "mac", kmspb.CryptoKey_MAC, kmspb.CryptoKeyVersion_HMAC_SHA256)
}

func checkKMSMacSign(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if _, err := kmsEnsureMACKey(ctx, client, cfg); err != nil {
		return err
	}
	resp, err := client.MacSign(ctx, &kmspb.MacSignRequest{
		Name: kmsExtraVersionName(cfg, "mac", "1"),
		Data: []byte("gcpc-kms-mac"),
	})
	if err != nil {
		return fmt.Errorf("MacSign: %w", err)
	}
	if len(resp.GetMac()) == 0 {
		return fmt.Errorf("MacSign returned an empty mac")
	}
	return nil
}

func checkKMSMacVerify(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if _, err := kmsEnsureMACKey(ctx, client, cfg); err != nil {
		return err
	}
	versionName := kmsExtraVersionName(cfg, "mac", "1")
	data := []byte("gcpc-kms-mac-verify")
	signed, err := client.MacSign(ctx, &kmspb.MacSignRequest{Name: versionName, Data: data})
	if err != nil {
		return fmt.Errorf("MacSign: %w", err)
	}
	ok, err := client.MacVerify(ctx, &kmspb.MacVerifyRequest{Name: versionName, Data: data, Mac: signed.GetMac()})
	if err != nil {
		return fmt.Errorf("MacVerify: %w", err)
	}
	if !ok.GetSuccess() {
		return fmt.Errorf("MacVerify success = false for a valid mac")
	}
	bad, err := client.MacVerify(ctx, &kmspb.MacVerifyRequest{Name: versionName, Data: []byte("tampered"), Mac: signed.GetMac()})
	if err != nil {
		return fmt.Errorf("MacVerify(tampered): %w", err)
	}
	if bad.GetSuccess() {
		return fmt.Errorf("MacVerify success = true for a tampered message")
	}
	return nil
}

// ─── crypto key metadata ──────────────────────────────────────────────────────

func checkKMSListCryptoKeys(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	keyName, err := kmsEnsureKey(ctx, client, cfg, "sym", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED)
	if err != nil {
		return err
	}
	it := client.ListCryptoKeys(ctx, &kmspb.ListCryptoKeysRequest{Parent: kmsRingName(cfg)})
	for {
		ck, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListCryptoKeys: %w", err)
		}
		if ck.GetName() == keyName {
			return nil
		}
	}
	return fmt.Errorf("ListCryptoKeys did not include %q", keyName)
}

func checkKMSUpdateCryptoKey(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	keyName, err := kmsEnsureKey(ctx, client, cfg, "sym", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED)
	if err != nil {
		return err
	}
	updated, err := client.UpdateCryptoKey(ctx, &kmspb.UpdateCryptoKeyRequest{
		CryptoKey:  &kmspb.CryptoKey{Name: keyName, Labels: map[string]string{"conformance": "updated"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		return fmt.Errorf("UpdateCryptoKey: %w", err)
	}
	if got := updated.GetLabels()["conformance"]; got != "updated" {
		return fmt.Errorf("updated labels[conformance] = %q, want updated", got)
	}
	return nil
}

func checkKMSUpdateCryptoKeyPrimaryVersion(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	keyName, err := kmsEnsureKey(ctx, client, cfg, "sym", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED)
	if err != nil {
		return err
	}
	created, err := client.CreateCryptoKeyVersion(ctx, &kmspb.CreateCryptoKeyVersionRequest{Parent: keyName})
	if err != nil {
		return fmt.Errorf("CreateCryptoKeyVersion: %w", err)
	}
	versionID := created.GetName()[strings.LastIndex(created.GetName(), "/")+1:]
	updated, err := client.UpdateCryptoKeyPrimaryVersion(ctx, &kmspb.UpdateCryptoKeyPrimaryVersionRequest{
		Name:               keyName,
		CryptoKeyVersionId: versionID,
	})
	if err != nil {
		return fmt.Errorf("UpdateCryptoKeyPrimaryVersion: %w", err)
	}
	if updated.GetPrimary().GetName() != created.GetName() {
		return fmt.Errorf("primary name = %q, want %q", updated.GetPrimary().GetName(), created.GetName())
	}
	return nil
}

// ─── raw ──────────────────────────────────────────────────────────────────────

func kmsEnsureRawKey(ctx context.Context, client *kms.KeyManagementClient, cfg Config) (string, error) {
	return kmsEnsureKey(ctx, client, cfg, "raw", kmspb.CryptoKey_RAW_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_AES_256_GCM)
}

func checkKMSRawEncrypt(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if _, err := kmsEnsureRawKey(ctx, client, cfg); err != nil {
		return err
	}
	resp, err := client.RawEncrypt(ctx, &kmspb.RawEncryptRequest{
		Name:                        kmsExtraVersionName(cfg, "raw", "1"),
		Plaintext:                   []byte("gcpc-kms-raw"),
		AdditionalAuthenticatedData: []byte("gcpc-aad"),
	})
	if err != nil {
		return fmt.Errorf("RawEncrypt: %w", err)
	}
	if len(resp.GetCiphertext()) == 0 {
		return fmt.Errorf("RawEncrypt returned an empty ciphertext")
	}
	if len(resp.GetInitializationVector()) != 12 {
		return fmt.Errorf("RawEncrypt initialization_vector length = %d, want 12", len(resp.GetInitializationVector()))
	}
	if resp.GetTagLength() != 16 {
		return fmt.Errorf("RawEncrypt tag_length = %d, want 16", resp.GetTagLength())
	}
	return nil
}

func checkKMSRawDecrypt(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if _, err := kmsEnsureRawKey(ctx, client, cfg); err != nil {
		return err
	}
	versionName := kmsExtraVersionName(cfg, "raw", "1")
	plaintext := []byte("gcpc-kms-raw-decrypt")
	aad := []byte("gcpc-aad")
	enc, err := client.RawEncrypt(ctx, &kmspb.RawEncryptRequest{
		Name:                        versionName,
		Plaintext:                   plaintext,
		AdditionalAuthenticatedData: aad,
	})
	if err != nil {
		return fmt.Errorf("RawEncrypt: %w", err)
	}
	dec, err := client.RawDecrypt(ctx, &kmspb.RawDecryptRequest{
		Name:                        versionName,
		Ciphertext:                  enc.GetCiphertext(),
		InitializationVector:        enc.GetInitializationVector(),
		AdditionalAuthenticatedData: aad,
		TagLength:                   enc.GetTagLength(),
	})
	if err != nil {
		return fmt.Errorf("RawDecrypt: %w", err)
	}
	if !bytes.Equal(dec.GetPlaintext(), plaintext) {
		return fmt.Errorf("RawDecrypt plaintext = %q, want %q", dec.GetPlaintext(), plaintext)
	}
	return nil
}

// ─── delete + retired resources ───────────────────────────────────────────────

// kmsRetiredResourceName is the deterministic RetiredResource name the
// emulator assigns when a CryptoKey is deleted (key id suffix).
func kmsRetiredResourceName(cfg Config, kind string) string {
	return "projects/" + cfg.Project + "/locations/global/retiredResources/" + kmsExtraKeyID(cfg, kind)
}

// kmsDestroyNow destroys a version and drives the emulator clock past its
// destroy window so the lazy promotion moves it to DESTROYED (a version can
// only be deleted once DESTROYED). Cloud KMS does this transition
// automatically; the emulator has no scheduler, so the probe advances the
// emulator clock over its management endpoint and restores real time before
// returning. Real time is restored with defer even on error so later probes are
// unaffected.
func kmsDestroyNow(ctx context.Context, cfg Config, client *kms.KeyManagementClient, name string) error {
	defer kmsSetRealClock(cfg)
	scheduled, err := client.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DestroyCryptoKeyVersion: %w", err)
	}
	if scheduled.GetState() == kmspb.CryptoKeyVersion_DESTROYED {
		return nil // already promoted by an earlier probe
	}
	dt := scheduled.GetDestroyTime()
	if dt == nil {
		return fmt.Errorf("DestroyCryptoKeyVersion returned %v with no destroy_time", scheduled.GetState())
	}
	if err := kmsSetFixedClock(cfg, dt.AsTime().Add(time.Second)); err != nil {
		return err
	}
	if _, err := client.GetCryptoKeyVersion(ctx, &kmspb.GetCryptoKeyVersionRequest{Name: name}); err != nil {
		return fmt.Errorf("GetCryptoKeyVersion after destroy window: %w", err)
	}
	return nil
}

// kmsSetFixedClock freezes the emulator clock at t via /_jaiscloud/clock.
func kmsSetFixedClock(cfg Config, t time.Time) error {
	return kmsPostClock(cfg, fmt.Sprintf(`{"mode":"fixed","time":%q}`, t.UTC().Format(time.RFC3339Nano)))
}

// kmsSetRealClock restores the emulator clock to wall time (best-effort).
func kmsSetRealClock(cfg Config) {
	_ = kmsPostClock(cfg, `{"mode":"real"}`)
}

func kmsPostClock(cfg Config, body string) error {
	url := strings.TrimRight(cfg.RESTEndpoint, "/") + "/_jaiscloud/clock"
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("set clock: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("set clock: HTTP %d", resp.StatusCode)
	}
	return nil
}

func checkKMSDeleteCryptoKeyVersion(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	if _, err := kmsEnsureKey(ctx, client, cfg, "delver", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED); err != nil {
		return err
	}
	name := kmsExtraVersionName(cfg, "delver", "1")
	if err := kmsDestroyNow(ctx, cfg, client, name); err != nil {
		return err
	}
	op, err := client.DeleteCryptoKeyVersion(ctx, &kmspb.DeleteCryptoKeyVersionRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DeleteCryptoKeyVersion: %w", err)
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("DeleteCryptoKeyVersion wait: %w", err)
	}
	if _, err := client.GetCryptoKeyVersion(ctx, &kmspb.GetCryptoKeyVersionRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetCryptoKeyVersion after delete = %v, want NotFound", err)
	}
	return nil
}

func checkKMSDeleteCryptoKey(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	keyName, err := kmsEnsureKey(ctx, client, cfg, "delkey", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED)
	if err != nil {
		return err
	}
	name := kmsExtraVersionName(cfg, "delkey", "1")
	if err := kmsDestroyNow(ctx, cfg, client, name); err != nil {
		return err
	}
	delVer, err := client.DeleteCryptoKeyVersion(ctx, &kmspb.DeleteCryptoKeyVersionRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DeleteCryptoKeyVersion: %w", err)
	}
	if err := delVer.Wait(ctx); err != nil {
		return fmt.Errorf("DeleteCryptoKeyVersion wait: %w", err)
	}
	op, err := client.DeleteCryptoKey(ctx, &kmspb.DeleteCryptoKeyRequest{Name: keyName})
	if err != nil {
		return fmt.Errorf("DeleteCryptoKey: %w", err)
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("DeleteCryptoKey wait: %w", err)
	}
	if _, err := client.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: keyName}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetCryptoKey after delete = %v, want NotFound", err)
	}
	return nil
}

func checkKMSListRetiredResources(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	want := kmsRetiredResourceName(cfg, "delkey")
	it := client.ListRetiredResources(ctx, &kmspb.ListRetiredResourcesRequest{Parent: kmsParent(cfg)})
	for {
		rr, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListRetiredResources: %w", err)
		}
		if rr.GetName() == want {
			return nil
		}
	}
	return fmt.Errorf("ListRetiredResources did not include %q", want)
}

func checkKMSGetRetiredResource(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	want := kmsRetiredResourceName(cfg, "delkey")
	rr, err := client.GetRetiredResource(ctx, &kmspb.GetRetiredResourceRequest{Name: want})
	if err != nil {
		return fmt.Errorf("GetRetiredResource: %w", err)
	}
	if rr.GetName() != want {
		return fmt.Errorf("RetiredResource name = %q, want %q", rr.GetName(), want)
	}
	if rr.GetOriginalResource() != kmsExtraKeyName(cfg, "delkey") {
		return fmt.Errorf("original_resource = %q, want %q", rr.GetOriginalResource(), kmsExtraKeyName(cfg, "delkey"))
	}
	if rr.GetDeleteTime() == nil {
		return fmt.Errorf("RetiredResource has no delete_time")
	}
	return nil
}

// ─── import jobs ──────────────────────────────────────────────────────────────

func kmsImportJobName(cfg Config) string {
	return kmsRingName(cfg) + "/importJobs/" + cfg.ResourceName("gcpc-import")
}

func checkKMSCreateImportJob(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	job, err := client.CreateImportJob(ctx, &kmspb.CreateImportJobRequest{
		Parent:      kmsRingName(cfg),
		ImportJobId: cfg.ResourceName("gcpc-import"),
		ImportJob: &kmspb.ImportJob{
			ImportMethod:    kmspb.ImportJob_RSA_OAEP_3072_SHA256,
			ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
		},
	})
	if err != nil {
		return fmt.Errorf("CreateImportJob: %w", err)
	}
	if job.GetName() != kmsImportJobName(cfg) {
		return fmt.Errorf("CreateImportJob name = %q, want %q", job.GetName(), kmsImportJobName(cfg))
	}
	if job.GetImportMethod() != kmspb.ImportJob_RSA_OAEP_3072_SHA256 {
		return fmt.Errorf("import_method = %v, want RSA_OAEP_3072_SHA256", job.GetImportMethod())
	}
	if job.GetState() != kmspb.ImportJob_ACTIVE {
		return fmt.Errorf("state = %v, want ACTIVE", job.GetState())
	}
	if !strings.Contains(job.GetPublicKey().GetPem(), "BEGIN PUBLIC KEY") {
		return fmt.Errorf("import job public_key is not a PEM public key")
	}
	if job.GetExpireTime() == nil {
		return fmt.Errorf("import job has no expire_time")
	}
	return nil
}

func checkKMSGetImportJob(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	job, err := client.GetImportJob(ctx, &kmspb.GetImportJobRequest{Name: kmsImportJobName(cfg)})
	if err != nil {
		return fmt.Errorf("GetImportJob: %w", err)
	}
	if job.GetImportMethod() != kmspb.ImportJob_RSA_OAEP_3072_SHA256 {
		return fmt.Errorf("import_method = %v, want RSA_OAEP_3072_SHA256", job.GetImportMethod())
	}
	if job.GetState() != kmspb.ImportJob_ACTIVE {
		return fmt.Errorf("state = %v, want ACTIVE", job.GetState())
	}
	return nil
}

func checkKMSListImportJobs(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	want := kmsImportJobName(cfg)
	it := client.ListImportJobs(ctx, &kmspb.ListImportJobsRequest{Parent: kmsRingName(cfg)})
	for {
		job, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListImportJobs: %w", err)
		}
		if job.GetName() == want {
			return nil
		}
	}
	return fmt.Errorf("ListImportJobs did not include %q", want)
}
