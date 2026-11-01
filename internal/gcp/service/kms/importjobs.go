package kms

import (
	"context"
	"crypto"
	"crypto/x509"
	"encoding/json"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/policy"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/store"
)

// DefaultAlgorithmForPurpose maps a KMS purpose to its default algorithm.
func DefaultAlgorithmForPurpose(purpose string) string {
	switch purpose {
	case "ASYMMETRIC_SIGN":
		return "RSA_SIGN_PKCS1_2048_SHA256"
	case "ASYMMETRIC_DECRYPT":
		return "RSA_DECRYPT_OAEP_2048_SHA256"
	case "MAC":
		return "HMAC_SHA256"
	case "RAW_ENCRYPT_DECRYPT":
		return "AES_256_GCM"
	case "KEY_ENCAPSULATION":
		return "KEM_XWING"
	case "AES_WRAPPING":
		return "AES_256_KWP"
	default:
		return "GOOGLE_SYMMETRIC_ENCRYPTION"
	}
}

// RetiredResource records a deleted crypto key whose name cannot be reused.
type RetiredResource struct {
	Name             string
	OriginalResource string
	ResourceType     string
	DeleteTime       time.Time
}

func retiredResourceID(location, keyID string) string { return location + "/" + keyID }

// GetRetiredResource returns a retired resource by location-qualified id.
func (s *Service) GetRetiredResource(ctx context.Context, project, location, id string) (RetiredResource, error) {
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtRetiredResource, retiredResourceID(location, id))
	if err != nil {
		return RetiredResource{}, notFound("retired resource not found")
	}
	var rr RetiredResource
	if err := json.Unmarshal(e.Data, &rr); err != nil {
		return RetiredResource{}, err
	}
	return rr, nil
}

// ListRetiredResources lists a location's retired resources.
func (s *Service) ListRetiredResources(ctx context.Context, project, location string) ([]RetiredResource, error) {
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, rtRetiredResource, location+"/")
	if err != nil {
		return nil, err
	}
	out := make([]RetiredResource, 0, len(entries))
	for _, e := range entries {
		var rr RetiredResource
		if err := json.Unmarshal(e.Data, &rr); err != nil {
			continue
		}
		out = append(out, rr)
	}
	return out, nil
}

// ImportJob is the domain view of a Cloud KMS import job. PublicKeyPEM is set
// for RSA import methods; PublicKeyRaw for HPKE-KEM methods.
type ImportJob struct {
	Name            string
	ImportMethod    string
	ProtectionLevel string
	CreateTime      time.Time
	GenerateTime    time.Time
	ExpireTime      time.Time
	State           string
	PublicKeyPEM    string
	PublicKeyRaw    []byte
	// WrappedPrivateKey is the job's private wrapping key, DEK-wrapped at rest.
	WrappedPrivateKey []byte
}

// ImportJobInput carries the caller-supplied fields of a CreateImportJob.
type ImportJobInput struct {
	ImportMethod    string
	ProtectionLevel string
}

func importJobID(location, kr, id string) string { return location + "/" + kr + "/" + id }

// CreateImportJob generates the wrapping key for an import job and stores it.
func (s *Service) CreateImportJob(ctx context.Context, project, location, kr, id string, in ImportJobInput) (ImportJob, error) {
	if in.ImportMethod == "" {
		return ImportJob{}, invalidArgument("import_method is required")
	}
	priv, pub, rawPublic, err := kmsstore.GenerateImportJobWrappingKey(in.ImportMethod)
	if err != nil {
		return ImportJob{}, invalidArgument("unsupported import method: " + in.ImportMethod)
	}
	dek, err := s.keys.ServerDEK(ctx)
	if err != nil {
		return ImportJob{}, err
	}
	wrappedPriv, err := kmsstore.EncryptData(dek, priv, []byte(id))
	if err != nil {
		return ImportJob{}, internal("wrapping key protection failed")
	}
	now := clock.Now()
	ij := ImportJob{
		Name:              ImportJobName(project, location, kr, id),
		ImportMethod:      in.ImportMethod,
		ProtectionLevel:   in.ProtectionLevel,
		CreateTime:        now,
		GenerateTime:      now,
		ExpireTime:        now.Add(72 * time.Hour),
		State:             "ACTIVE",
		WrappedPrivateKey: wrappedPriv,
	}
	if rawPublic {
		ij.PublicKeyRaw = pub
	} else {
		pemStr, err := kmsstore.PublicKeyPEM(pub)
		if err != nil {
			return ImportJob{}, internal("public key encode failed")
		}
		ij.PublicKeyPEM = pemStr
	}
	data, _ := json.Marshal(ij)
	if err := s.resources.Upsert(ctx, project, store.GlobalRegion, store.ResourceEntry{
		Type: rtImportJob, ID: importJobID(location, kr, id), Data: data,
	}); err != nil {
		return ImportJob{}, err
	}
	return ij, nil
}

// GetImportJob returns an import job, marking it EXPIRED once its window passed.
func (s *Service) GetImportJob(ctx context.Context, project, location, kr, id string) (ImportJob, error) {
	e, err := s.resources.Get(ctx, project, store.GlobalRegion, rtImportJob, importJobID(location, kr, id))
	if err != nil {
		return ImportJob{}, notFound("import job not found")
	}
	var ij ImportJob
	if err := json.Unmarshal(e.Data, &ij); err != nil {
		return ImportJob{}, err
	}
	if ij.State == "ACTIVE" && !ij.ExpireTime.IsZero() && clock.Now().After(ij.ExpireTime) {
		ij.State = "EXPIRED"
	}
	return ij, nil
}

// ListImportJobs lists a key ring's import jobs.
func (s *Service) ListImportJobs(ctx context.Context, project, location, kr string) ([]ImportJob, error) {
	entries, err := s.resources.List(ctx, project, store.GlobalRegion, rtImportJob, location+"/"+kr+"/")
	if err != nil {
		return nil, err
	}
	now := clock.Now()
	out := make([]ImportJob, 0, len(entries))
	for _, e := range entries {
		var ij ImportJob
		if err := json.Unmarshal(e.Data, &ij); err != nil {
			continue
		}
		if ij.State == "ACTIVE" && !ij.ExpireTime.IsZero() && now.After(ij.ExpireTime) {
			ij.State = "EXPIRED"
		}
		out = append(out, ij)
	}
	return out, nil
}

// ImportVersionRequest carries the fields of ImportCryptoKeyVersion (and the
// trusted-wrapping variant).
type ImportVersionRequest struct {
	ImportJob              string // full import-job name (RSA/HPKE import)
	ImportingKey           string // full version name (trusted wrapping)
	Algorithm              string
	WrappedKey             []byte
	CryptoKeyVersion       string // optional existing target
	TrustedWrappingEnabled bool
}

// ImportCryptoKeyVersion unwraps caller-supplied key material with an import
// job's private wrapping key and creates a new crypto-key version.
func (s *Service) ImportCryptoKeyVersion(ctx context.Context, project, location, kr, key string, in ImportVersionRequest) (kmsstore.Version, error) {
	if in.Algorithm == "" {
		return kmsstore.Version{}, invalidArgument("algorithm is required")
	}
	if in.CryptoKeyVersion != "" {
		return kmsstore.Version{}, reimportUnsupported()
	}
	jp, jloc, jkr, jid, ok := SplitImportJobName(in.ImportJob)
	if !ok || jp != project || jloc != location || jkr != kr {
		return kmsstore.Version{}, invalidArgument("import_job must name a job in the target key ring")
	}
	ij, err := s.GetImportJob(ctx, project, location, kr, jid)
	if err != nil {
		return kmsstore.Version{}, err
	}
	if ij.State != "ACTIVE" {
		return kmsstore.Version{}, failedPrecondition("import job is " + ij.State)
	}
	dek, err := s.keys.ServerDEK(ctx)
	if err != nil {
		return kmsstore.Version{}, err
	}
	priv, err := kmsstore.DecryptData(dek, ij.WrappedPrivateKey, []byte(jid))
	if err != nil {
		return kmsstore.Version{}, internal("wrapping key unavailable")
	}
	material, err := kmsstore.UnwrapImportedKeyMaterial(ij.ImportMethod, priv, in.WrappedKey)
	if err != nil {
		return kmsstore.Version{}, invalidArgument("unwrapping imported key material failed")
	}
	return s.createImportedVersion(ctx, project, location, kr, key, in, material, ij.ProtectionLevel)
}

// ImportTrustedKeyWrappedCryptoKeyVersion unwraps key material protected with
// AES-256-KWP by an HSM importing key and creates a trusted-wrapping version.
// The importing key may be named as a CryptoKeyVersion or as a CryptoKey (its
// primary version), and must be an HSM_SINGLE_TENANT AES_WRAPPING key with the
// AES_256_KWP algorithm.
func (s *Service) ImportTrustedKeyWrappedCryptoKeyVersion(ctx context.Context, project, location, kr, key string, in ImportVersionRequest) (kmsstore.Version, error) {
	if in.Algorithm == "" {
		return kmsstore.Version{}, invalidArgument("algorithm is required")
	}
	if in.CryptoKeyVersion != "" {
		return kmsstore.Version{}, reimportUnsupported()
	}
	kwk, err := s.resolveTrustedWrappingKey(ctx, project, location, kr, in.ImportingKey, "importing_key", true)
	if err != nil {
		return kmsstore.Version{}, err
	}
	material, err := kmsstore.AESKeyUnwrapWithPadding(kwk, in.WrappedKey)
	if err != nil {
		return kmsstore.Version{}, invalidArgument("unwrapping trusted key material failed")
	}
	in.TrustedWrappingEnabled = true
	return s.createImportedVersion(ctx, project, location, kr, key, in, material, "")
}

// ExportTrustedKeyWrappedCryptoKeyVersion wraps a trusted-wrapping version's
// key material with AES-256-KWP using an HSM wrapping key.
func (s *Service) ExportTrustedKeyWrappedCryptoKeyVersion(ctx context.Context, project, location, kr, key, version, wrappingKey string) ([]byte, error) {
	v, err := s.GetVersion(ctx, project, location, kr, key, version)
	if err != nil {
		return nil, err
	}
	if v.State != "ENABLED" {
		return nil, versionNotEnabledErr(version, v.State)
	}
	if !v.TrustedWrappingEnabled {
		return nil, failedPrecondition("CryptoKeyVersion does not have trusted wrapping enabled")
	}
	kwk, err := s.resolveTrustedWrappingKey(ctx, project, location, kr, wrappingKey, "wrapping_key", false)
	if err != nil {
		return nil, err
	}
	material, err := s.versionMaterial(ctx, project, location, kr, key, version, v)
	if err != nil {
		return nil, err
	}
	return kmsstore.WrapTrustedKey(kwk, material)
}

// Decapsulate recovers a shared secret from a KEM ciphertext.
func (s *Service) Decapsulate(ctx context.Context, project, location, kr, key, version string, ciphertext []byte) ([]byte, error) {
	if err := s.authorizeCryptoKey(ctx, project, location, kr, key, policy.PermCryptoKeyUseToDecrypt); err != nil {
		return nil, err
	}
	v, err := s.GetVersion(ctx, project, location, kr, key, version)
	if err != nil {
		return nil, err
	}
	if v.State != "ENABLED" {
		return nil, versionNotEnabledErr(version, v.State)
	}
	if !kmsstore.IsKEMAlgorithm(v.Algorithm) {
		return nil, failedPrecondition("key is not a KEM key")
	}
	priv, err := s.keys.PrivateKey(ctx, project, location, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	ss, err := kmsstore.DecapsulateKEM(v.Algorithm, priv, ciphertext)
	if err != nil {
		return nil, invalidArgument("decapsulation failed")
	}
	return ss, nil
}

// createImportedVersion splits unwrapped material into the store's components
// and persists a new version.
func (s *Service) createImportedVersion(ctx context.Context, project, location, kr, key string, in ImportVersionRequest, material []byte, protectionLevel string) (kmsstore.Version, error) {
	ck, err := s.keys.GetCryptoKey(ctx, project, location, kr, key)
	if err != nil {
		return kmsstore.Version{}, keyErr(err)
	}
	if protectionLevel == "" {
		protectionLevel = ck.ProtectionLevel
	}
	if isExternalProtectionLevel(protectionLevel) {
		return kmsstore.Version{}, externalVersionUnsupported()
	}
	if in.TrustedWrappingEnabled {
		if err := validateTrustedWrappingTarget(ck.Purpose, ck.ProtectionLevel); err != nil {
			return kmsstore.Version{}, err
		}
	}
	keyMat, privDER, pubDER, err := splitImportedMaterial(in.Algorithm, material)
	if err != nil {
		return kmsstore.Version{}, err
	}
	now := clock.Now()
	v := kmsstore.Version{
		State:                  "ENABLED",
		Algorithm:              in.Algorithm,
		CreateTime:             now,
		ImportTime:             now,
		ProtectionLevel:        protectionLevel,
		TrustedWrappingEnabled: in.TrustedWrappingEnabled,
		HsmTrusted:             ck.Purpose == "AES_WRAPPING" && protectionLevel == "HSM_SINGLE_TENANT",
	}
	version, err := s.keys.CreateImportedVersion(ctx, project, location, kr, key, v, keyMat, privDER, pubDER)
	if err != nil {
		return kmsstore.Version{}, versionErr(err)
	}
	v.Version = version
	v.KeyID = key
	return v, nil
}

// splitImportedMaterial validates and splits unwrapped import material into the
// store's symmetric / asymmetric components.
func splitImportedMaterial(algorithm string, material []byte) (keyMat, privDER, pubDER []byte, err error) {
	switch {
	case strings.HasPrefix(algorithm, "RSA_SIGN"), strings.HasPrefix(algorithm, "RSA_DECRYPT"),
		strings.HasPrefix(algorithm, "EC_SIGN"):
		key, perr := x509.ParsePKCS8PrivateKey(material)
		if perr != nil {
			return nil, nil, nil, invalidArgument("asymmetric key material must be PKCS#8 DER")
		}
		signer, ok := key.(crypto.Signer)
		if !ok {
			return nil, nil, nil, invalidArgument("unsupported private key type")
		}
		pubDER, perr = x509.MarshalPKIXPublicKey(signer.Public())
		if perr != nil {
			return nil, nil, nil, invalidArgument("public key encode failed")
		}
		return nil, material, pubDER, nil
	default:
		// Symmetric / HMAC / KEM material is stored as-is.
		return material, nil, nil, nil
	}
}

// versionMaterial returns the raw material of a version (symmetric key material
// or asymmetric/KEM private key).
func (s *Service) versionMaterial(ctx context.Context, project, location, kr, key, version string, v kmsstore.Version) ([]byte, error) {
	if !strings.HasPrefix(v.Algorithm, "RSA_") && !strings.HasPrefix(v.Algorithm, "EC_") && !kmsstore.IsKEMAlgorithm(v.Algorithm) {
		return s.keys.KeyMaterial(ctx, project, location, kr, key, version)
	}
	return s.keys.PrivateKey(ctx, project, location, kr, key, version)
}
