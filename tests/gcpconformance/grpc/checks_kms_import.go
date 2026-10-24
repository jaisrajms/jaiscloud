package grpcconformance

import (
	"context"
	"crypto/aes"
	"crypto/mlkem"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"fmt"

	"cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// checkKMSImportCryptoKeyVersion imports a symmetric key wrapped with an import
// job's RSA-OAEP + AES-KWP wrapping key and checks the new version is ENABLED
// with an import_time.
func checkKMSImportCryptoKeyVersion(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	keyName, err := kmsEnsureKey(ctx, client, cfg, "import", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED)
	if err != nil {
		return err
	}
	jobName := kmsImportJobName(cfg)
	if _, err := client.CreateImportJob(ctx, &kmspb.CreateImportJobRequest{
		Parent:      kmsRingName(cfg),
		ImportJobId: cfg.ResourceName("gcpc-import"),
		ImportJob:   &kmspb.ImportJob{ImportMethod: kmspb.ImportJob_RSA_OAEP_3072_SHA256_AES_256},
	}); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("CreateImportJob: %w", err)
	}
	job, err := client.GetImportJob(ctx, &kmspb.GetImportJobRequest{Name: jobName})
	if err != nil {
		return fmt.Errorf("GetImportJob: %w", err)
	}
	rpub, err := kmsJobRSAPublicKey(job)
	if err != nil {
		return err
	}

	material := []byte("0123456789abcdef0123456789abcdef")
	kwpKey := make([]byte, 32)
	if _, err := rand.Read(kwpKey); err != nil {
		return err
	}
	enc, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, rpub, kwpKey, nil)
	if err != nil {
		return fmt.Errorf("RSA-OAEP wrap: %w", err)
	}
	wrappedKWP, err := kwpWrap(kwpKey, material)
	if err != nil {
		return fmt.Errorf("AES-KWP wrap: %w", err)
	}
	v, err := client.ImportCryptoKeyVersion(ctx, &kmspb.ImportCryptoKeyVersionRequest{
		Parent:     keyName,
		ImportJob:  jobName,
		Algorithm:  kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION,
		WrappedKey: append(enc, wrappedKWP...),
	})
	if err != nil {
		return fmt.Errorf("ImportCryptoKeyVersion: %w", err)
	}
	if v.GetState() != kmspb.CryptoKeyVersion_ENABLED {
		return fmt.Errorf("imported version state = %v, want ENABLED", v.GetState())
	}
	if v.GetImportTime() == nil {
		return fmt.Errorf("imported version has no import_time")
	}
	return nil
}

// checkKMSDecapsulate generates a KEM key, encapsulates with its public key and
// confirms Decapsulate recovers the same shared secret.
func checkKMSDecapsulate(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	keyName, err := kmsEnsureKey(ctx, client, cfg, "kem", kmspb.CryptoKey_KEY_ENCAPSULATION, kmspb.CryptoKeyVersion_ML_KEM_768)
	if err != nil {
		return err
	}
	versionName := keyName + "/cryptoKeyVersions/1"
	pk, err := client.GetPublicKey(ctx, &kmspb.GetPublicKeyRequest{
		Name:            versionName,
		PublicKeyFormat: kmspb.PublicKey_NIST_PQC,
	})
	if err != nil {
		return fmt.Errorf("GetPublicKey: %w", err)
	}
	raw := pk.GetPublicKey().GetData()
	if len(raw) == 0 {
		return fmt.Errorf("KEM public key is empty")
	}
	ek, err := mlkem.NewEncapsulationKey768(raw)
	if err != nil {
		return fmt.Errorf("parse encapsulation key: %w", err)
	}
	sharedSecret, ciphertext := ek.Encapsulate()
	resp, err := client.Decapsulate(ctx, &kmspb.DecapsulateRequest{Name: versionName, Ciphertext: ciphertext})
	if err != nil {
		return fmt.Errorf("Decapsulate: %w", err)
	}
	if string(resp.GetSharedSecret()) != string(sharedSecret) {
		return fmt.Errorf("Decapsulate shared secret mismatch")
	}
	return nil
}

// checkKMSExportImportTrustedKeyWrapped exercises HSM trusted wrapping in both
// directions: a trusted-wrapping target version is exported wrapped by an HSM
// AES-256-KWP key, then re-imported through the same importing key.
func checkKMSExportImportTrustedKeyWrapped(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	wrapKey, err := kmsEnsureHSMKey(ctx, client, cfg, "wrap", kmspb.CryptoKey_AES_WRAPPING, kmspb.CryptoKeyVersion_AES_256_KWP)
	if err != nil {
		return err
	}
	targetKey, err := kmsEnsureKey(ctx, client, cfg, "trusted", kmspb.CryptoKey_ENCRYPT_DECRYPT, kmspb.CryptoKeyVersion_CRYPTO_KEY_VERSION_ALGORITHM_UNSPECIFIED)
	if err != nil {
		return err
	}
	jobName := kmsImportJobName(cfg)
	job, err := client.GetImportJob(ctx, &kmspb.GetImportJobRequest{Name: jobName})
	if err != nil {
		return fmt.Errorf("GetImportJob: %w", err)
	}
	rpub, err := kmsJobRSAPublicKey(job)
	if err != nil {
		return err
	}

	material := []byte("trusted-wrapped-key-material-1234")
	kwpKey := make([]byte, 32)
	if _, err := rand.Read(kwpKey); err != nil {
		return err
	}
	enc, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, rpub, kwpKey, nil)
	if err != nil {
		return err
	}
	wrappedKWP, err := kwpWrap(kwpKey, material)
	if err != nil {
		return err
	}
	trustedVersion, err := client.ImportCryptoKeyVersion(ctx, &kmspb.ImportCryptoKeyVersionRequest{
		Parent:                 targetKey,
		ImportJob:              jobName,
		Algorithm:              kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION,
		WrappedKey:             append(enc, wrappedKWP...),
		TrustedWrappingEnabled: true,
	})
	if err != nil {
		return fmt.Errorf("trusted ImportCryptoKeyVersion: %w", err)
	}
	if !trustedVersion.GetTrustedWrappingEnabled() {
		return fmt.Errorf("imported version does not have trusted_wrapping_enabled")
	}
	exported, err := client.ExportTrustedKeyWrappedCryptoKeyVersion(ctx, &kmspb.ExportTrustedKeyWrappedCryptoKeyVersionRequest{
		Name:        trustedVersion.GetName(),
		WrappingKey: wrapKey + "/cryptoKeyVersions/1",
	})
	if err != nil {
		return fmt.Errorf("ExportTrustedKeyWrappedCryptoKeyVersion: %w", err)
	}
	if len(exported.GetWrappedKey()) == 0 {
		return fmt.Errorf("ExportTrustedKeyWrappedCryptoKeyVersion returned an empty key")
	}
	reimported, err := client.ImportTrustedKeyWrappedCryptoKeyVersion(ctx, &kmspb.ImportTrustedKeyWrappedCryptoKeyVersionRequest{
		Parent:       targetKey,
		ImportingKey: wrapKey + "/cryptoKeyVersions/1",
		Algorithm:    kmspb.CryptoKeyVersion_GOOGLE_SYMMETRIC_ENCRYPTION,
		WrappedKey:   exported.GetWrappedKey(),
	})
	if err != nil {
		return fmt.Errorf("ImportTrustedKeyWrappedCryptoKeyVersion: %w", err)
	}
	if !reimported.GetTrustedWrappingEnabled() {
		return fmt.Errorf("re-imported version does not have trusted_wrapping_enabled")
	}
	return nil
}

// kmsJobRSAPublicKey parses an import job's PEM wrapping public key.
func kmsJobRSAPublicKey(job *kmspb.ImportJob) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(job.GetPublicKey().GetPem()))
	if block == nil {
		return nil, fmt.Errorf("import job public key is not PEM")
	}
	pubAny, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse wrapping public key: %w", err)
	}
	rpub, ok := pubAny.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("wrapping public key is %T, want *rsa.PublicKey", pubAny)
	}
	return rpub, nil
}

// kmsEnsureHSMKey returns an HSM-key name, creating the key (idempotently) with
// the given protection level if it does not exist yet.
func kmsEnsureHSMKey(ctx context.Context, client *kms.KeyManagementClient, cfg Config, kind string, purpose kmspb.CryptoKey_CryptoKeyPurpose, algo kmspb.CryptoKeyVersion_CryptoKeyVersionAlgorithm) (string, error) {
	name := kmsExtraKeyName(cfg, kind)
	if _, err := client.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: name}); err == nil {
		return name, nil
	} else if status.Code(err) != codes.NotFound {
		return "", fmt.Errorf("GetCryptoKey(%s): %w", kind, err)
	}
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent:      kmsRingName(cfg),
		CryptoKeyId: kmsExtraKeyID(cfg, kind),
		CryptoKey: &kmspb.CryptoKey{
			Purpose: purpose,
			VersionTemplate: &kmspb.CryptoKeyVersionTemplate{
				Algorithm:       algo,
				ProtectionLevel: kmspb.ProtectionLevel_HSM_SINGLE_TENANT,
			},
		},
	}); err != nil {
		return "", fmt.Errorf("CreateCryptoKey(%s): %w", kind, err)
	}
	return name, nil
}

// kwpWrap implements AES Key Wrap with Padding (RFC 5649) so the conformance
// probe can produce the RSA-OAEP + AES-KWP wrapped key the import job expects
// without importing the emulator's internal packages.
func kwpWrap(kek, plaintext []byte) ([]byte, error) {
	const ivPrefix uint32 = 0xA65959A6
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	n := (len(plaintext) + 7) / 8
	a := make([]byte, 8)
	binary.BigEndian.PutUint32(a[:4], ivPrefix)
	binary.BigEndian.PutUint32(a[4:], uint32(len(plaintext)))
	padded := make([]byte, n*8)
	copy(padded, plaintext)
	r := make([][]byte, n)
	for i := 0; i < n; i++ {
		r[i] = padded[i*8 : i*8+8]
	}
	var buf [16]byte
	for j := 0; j < 6; j++ {
		for i := 0; i < n; i++ {
			copy(buf[:8], a)
			copy(buf[8:], r[i])
			block.Encrypt(buf[:], buf[:])
			copy(a, buf[:8])
			var t [8]byte
			binary.BigEndian.PutUint64(t[:], uint64(n*j+i+1))
			for k := 0; k < 8; k++ {
				a[k] ^= t[k]
			}
			copy(r[i], buf[8:])
		}
	}
	out := make([]byte, 8*(n+1))
	copy(out, a)
	for i := 0; i < n; i++ {
		copy(out[8+i*8:], r[i])
	}
	return out, nil
}
