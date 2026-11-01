package kms

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/policy"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/store"
)

// CryptoKeyInput carries the caller-supplied fields of a CreateCryptoKey.
type CryptoKeyInput struct {
	Purpose          string
	Algorithm        string
	Labels           map[string]string
	RotationPeriod   time.Duration
	NextRotationTime *time.Time
	ProtectionLevel  string
	ImportOnly       bool
	// SkipInitialVersion is CreateCryptoKeyRequest.skip_initial_version_creation:
	// the key is created without the implicit first version.
	SkipInitialVersion bool
	// TrustedWrappingEnabled is the cryptoKeys.create trustedWrappingEnabled
	// query parameter / CreateCryptoKeyRequest.trusted_wrapping_enabled: it
	// enables trusted wrapping on the implicit first version (Discovery:
	// HSM_SINGLE_TENANT keys only, never for ENCRYPT_DECRYPT).
	TrustedWrappingEnabled bool
}

// CryptoKeyUpdate carries a masked UpdateCryptoKey. UpdateMask is the request's
// required FieldMask path set: a nil mask means the field was omitted (real GCP
// rejects that — UpdateCryptoKeyRequest.update_mask is REQUIRED), while a
// non-nil empty mask is the explicit no-op real GCP accepts.
type CryptoKeyUpdate struct {
	UpdateMask       []string
	Labels           map[string]string
	RotationPeriod   *time.Duration
	NextRotationTime *time.Time
}

// Rotation-schedule bounds documented on CryptoKey.rotation_period: "Must be at
// least 24 hours and at most 876,000 hours."
const (
	minRotationPeriod = 24 * time.Hour
	maxRotationPeriod = 876000 * time.Hour
)

// normalizeCryptoKeyMaskPath maps a FieldMask path onto the canonical snake_case
// field name the core switches on. Real GCP accepts both the REST camelCase
// spelling and the proto snake_case one.
func normalizeCryptoKeyMaskPath(path string) string {
	switch path {
	case "rotationPeriod", "rotation_period":
		return "rotation_period"
	case "nextRotationTime", "next_rotation_time":
		return "next_rotation_time"
	default:
		return path
	}
}

// validateRotationSchedule enforces the rotation invariants the Discovery
// documents on the resulting CryptoKey: only ENCRYPT_DECRYPT keys rotate and a
// set rotation_period requires next_rotation_time. It deliberately does not
// check that next_rotation_time is in the future, so a key whose rotation has
// come due can still be updated.
func validateRotationSchedule(purpose string, rp time.Duration, nrt time.Time) error {
	if rp == 0 && nrt.IsZero() {
		return nil
	}
	if purpose != "" && purpose != "ENCRYPT_DECRYPT" {
		return invalidArgument("automatic rotation is not available for keys with purpose " + purpose)
	}
	if rp != 0 && nrt.IsZero() {
		return invalidArgument("next_rotation_time is required if rotation_schedule is set")
	}
	return nil
}

// validateRotationPeriodBounds rejects a rotation_period outside the documented
// [24h, 876000h] range. Callers pass an explicitly set period (create omits a
// zero period, which means "no schedule"; an update's zeroed period is a real
// value and is rejected here).
func validateRotationPeriodBounds(rp time.Duration) error {
	if rp < minRotationPeriod {
		return invalidArgument("rotation_period must be at least 24h")
	}
	if rp > maxRotationPeriod {
		return invalidArgument("rotation_period must be at most 876000h")
	}
	return nil
}

// validateNextRotationTime rejects a next_rotation_time that is not in the
// future, the one schedule rule that depends on the current clock.
func validateNextRotationTime(nrt time.Time) error {
	if !nrt.IsZero() && !nrt.After(clock.Now()) {
		return invalidArgument("next_rotation_time must be in the future")
	}
	return nil
}

// EncryptResult is the outcome of an Encrypt/RawEncrypt call.
type EncryptResult struct {
	Version     string
	Ciphertext  []byte // versioned blob (Encrypt)
	UsedPrimary bool
}

// DecryptResult is the outcome of a Decrypt call.
type DecryptResult struct {
	Plaintext   []byte
	UsedPrimary bool
}

// RawEncryptResult is the outcome of a RawEncrypt call.
type RawEncryptResult struct {
	Version    string
	Ciphertext []byte
	IV         []byte
}

// SignResult is the outcome of an AsymmetricSign call.
type SignResult struct {
	Version   string
	Signature []byte
}

// MacResult is the outcome of a MacSign call.
type MacResult struct {
	Version string
	Mac     []byte
}

// PublicKeyResult is the outcome of a GetPublicKey call. For asymmetric keys
// PEM is set; for KEM keys Raw is set with the PQC public-key format.
type PublicKeyResult struct {
	Version   string
	Algorithm string
	PEM       string
	Raw       []byte
	Format    string // "PEM", "DER", "NIST_PQC" or "XWING_RAW_BYTES"
}

// ─── KeyRings ─────────────────────────────────────────────────────────────────

// CreateKeyRing creates a key ring, returning AlreadyExists on a duplicate.
func (s *Service) CreateKeyRing(ctx context.Context, project, location, id string) (kmsstore.KeyRing, error) {
	kr := kmsstore.KeyRing{Location: location, ID: id, CreateTime: clock.Now()}
	if err := s.keys.CreateKeyRing(ctx, project, location, id, kr); err != nil {
		if errorsIsAlreadyExists(err) {
			return kmsstore.KeyRing{}, alreadyExists("key ring already exists")
		}
		return kmsstore.KeyRing{}, err
	}
	return kr, nil
}

// GetKeyRing returns a key ring.
func (s *Service) GetKeyRing(ctx context.Context, project, location, id string) (kmsstore.KeyRing, error) {
	kr, err := s.keys.GetKeyRing(ctx, project, location, id)
	if err != nil {
		return kmsstore.KeyRing{}, keyRingErr(err)
	}
	return kr, nil
}

// ListKeyRings lists a location's key rings.
func (s *Service) ListKeyRings(ctx context.Context, project, location string) ([]kmsstore.KeyRing, error) {
	return s.keys.ListKeyRings(ctx, project, location)
}

// ─── CryptoKeys ───────────────────────────────────────────────────────────────

// CreateCryptoKey creates a crypto key and its implicit primary version.
func (s *Service) CreateCryptoKey(ctx context.Context, project, location, kr, id string, in CryptoKeyInput) (kmsstore.CryptoKey, error) {
	purpose := in.Purpose
	if purpose == "" {
		purpose = "ENCRYPT_DECRYPT"
	}
	algorithm := in.Algorithm
	if algorithm == "" {
		algorithm = DefaultAlgorithmForPurpose(purpose)
	}
	protectionLevel := in.ProtectionLevel
	if protectionLevel == "" {
		protectionLevel = "SOFTWARE"
	}
	// The emulator has no EKM plane, so an EXTERNAL / EXTERNAL_VPC key could
	// never have a usable version: fail loud instead of fabricating SOFTWARE
	// material (see externalVersionUnsupported).
	if isExternalProtectionLevel(protectionLevel) {
		return kmsstore.CryptoKey{}, externalVersionUnsupported()
	}
	if in.TrustedWrappingEnabled {
		if err := validateTrustedWrappingTarget(purpose, protectionLevel); err != nil {
			return kmsstore.CryptoKey{}, err
		}
	}
	// A deleted crypto key leaves a RetiredResource behind; its name cannot be
	// reused (Cloud KMS RetiredResource semantics).
	if _, err := s.resources.Get(ctx, project, store.GlobalRegion, rtRetiredResource, retiredResourceID(location, id)); err == nil {
		return kmsstore.CryptoKey{}, alreadyExists("crypto key name is retired and cannot be reused")
	}
	var nrt time.Time
	if in.NextRotationTime != nil {
		nrt = *in.NextRotationTime
	}
	if in.RotationPeriod != 0 {
		if err := validateRotationPeriodBounds(in.RotationPeriod); err != nil {
			return kmsstore.CryptoKey{}, err
		}
	}
	if err := validateRotationSchedule(purpose, in.RotationPeriod, nrt); err != nil {
		return kmsstore.CryptoKey{}, err
	}
	if err := validateNextRotationTime(nrt); err != nil {
		return kmsstore.CryptoKey{}, err
	}
	now := clock.Now()
	primary, withInitialVersion := "1", true
	if in.ImportOnly || in.SkipInitialVersion {
		// Cloud KMS creates no version for an import-only key or when the
		// caller skips it; the first ImportCryptoKeyVersion becomes version 1.
		primary, withInitialVersion = "", false
	}
	ck := kmsstore.CryptoKey{
		Location: location, KeyRingID: kr, ID: id, Purpose: purpose, CreateTime: now,
		PrimaryVersion: primary, Algorithm: algorithm, Labels: in.Labels,
		RotationPeriod: in.RotationPeriod, NextRotationTime: nrt,
		ProtectionLevel: in.ProtectionLevel, ImportOnly: in.ImportOnly,
	}
	// trustedWrappingEnabled applies to the CryptoKeyVersions created by this
	// request (the implicit first version). A create that skips that version has
	// no version to stamp, so the flag only validates here.
	initial := kmsstore.Version{
		ProtectionLevel:        in.ProtectionLevel,
		TrustedWrappingEnabled: in.TrustedWrappingEnabled && withInitialVersion,
	}
	if err := s.keys.CreateCryptoKeyWithVersion(ctx, project, location, kr, id, ck, initial, withInitialVersion); err != nil {
		if errorsIsAlreadyExists(err) {
			return kmsstore.CryptoKey{}, alreadyExists("crypto key already exists")
		}
		return kmsstore.CryptoKey{}, err
	}
	return ck, nil
}

// GetCryptoKey returns a crypto key, first applying any due rotation or elapsed
// destruction window.
func (s *Service) GetCryptoKey(ctx context.Context, project, location, kr, id string) (kmsstore.CryptoKey, error) {
	kmsstore.RotateIfDue(ctx, s.keys, project, location, kr, id, clock.Now())
	s.PromoteDestroyed(ctx, project, location, kr, id)
	k, err := s.keys.GetCryptoKey(ctx, project, location, kr, id)
	if err != nil {
		return kmsstore.CryptoKey{}, keyErr(err)
	}
	return k, nil
}

// ListCryptoKeys lists a key ring's crypto keys, applying due rotations.
func (s *Service) ListCryptoKeys(ctx context.Context, project, location, kr string) ([]kmsstore.CryptoKey, error) {
	keys, err := s.keys.ListCryptoKeys(ctx, project, location, kr)
	if err != nil {
		return nil, err
	}
	for _, k := range keys {
		kmsstore.RotateIfDue(ctx, s.keys, project, location, kr, k.ID, clock.Now())
		s.PromoteDestroyed(ctx, project, location, kr, k.ID)
	}
	return s.keys.ListCryptoKeys(ctx, project, location, kr)
}

// UpdateCryptoKey applies a masked update to a crypto key's mutable fields.
// Real GCP requires the FieldMask (UpdateCryptoKeyRequest.update_mask is
// REQUIRED) and validates every path against the resource's writable field set
// (labels, rotation_period, next_rotation_time); an omitted mask is
// INVALID_ARGUMENT and any other path is too (AIP-134/AIP-161, the AUD2-7
// precedent). Only the masked fields are validated and merged; the rotation
// schedule invariants (purpose, next_rotation_time required with a period) are
// then enforced on the merged result.
func (s *Service) UpdateCryptoKey(ctx context.Context, project, location, kr, id string, in CryptoKeyUpdate) (kmsstore.CryptoKey, error) {
	if in.UpdateMask == nil {
		return kmsstore.CryptoKey{}, invalidArgument("update_mask is required")
	}
	paths := make([]string, 0, len(in.UpdateMask))
	selected := make(map[string]bool, len(in.UpdateMask))
	for _, raw := range in.UpdateMask {
		path := normalizeCryptoKeyMaskPath(raw)
		switch path {
		case "labels", "rotation_period", "next_rotation_time":
			if !selected[path] {
				selected[path] = true
				paths = append(paths, path)
			}
		default:
			return kmsstore.CryptoKey{}, invalidArgument("unsupported update_mask path: " + raw)
		}
	}
	// Validate only the fields the mask actually selects: AIP-134 field-mask
	// semantics ignore (and do not validate) an unmasked body field, so a stale
	// or invalid rotation value alongside a labels patch is not an error. This
	// also keeps a due rotation on an untouched field from rejecting the update.
	if selected["rotation_period"] && in.RotationPeriod != nil {
		if err := validateRotationPeriodBounds(*in.RotationPeriod); err != nil {
			return kmsstore.CryptoKey{}, err
		}
	}
	if selected["next_rotation_time"] && in.NextRotationTime != nil {
		if err := validateNextRotationTime(*in.NextRotationTime); err != nil {
			return kmsstore.CryptoKey{}, err
		}
	}
	k, err := s.keys.UpdateCryptoKeyAtomic(ctx, project, location, kr, id, func(stored kmsstore.CryptoKey) (kmsstore.CryptoKey, error) {
		for _, path := range paths {
			switch path {
			case "labels":
				stored.Labels = in.Labels
			case "rotation_period":
				if in.RotationPeriod == nil {
					stored.RotationPeriod = 0
					continue
				}
				stored.RotationPeriod = *in.RotationPeriod
			case "next_rotation_time":
				if in.NextRotationTime == nil {
					stored.NextRotationTime = time.Time{}
					continue
				}
				stored.NextRotationTime = *in.NextRotationTime
			}
		}
		if err := validateRotationSchedule(stored.Purpose, stored.RotationPeriod, stored.NextRotationTime); err != nil {
			return stored, err
		}
		return stored, nil
	})
	if err != nil {
		return kmsstore.CryptoKey{}, keyErr(err)
	}
	return k, nil
}

// SetPrimaryVersion makes a version the crypto key's primary.
func (s *Service) SetPrimaryVersion(ctx context.Context, project, location, kr, id, version string) (kmsstore.CryptoKey, error) {
	if err := s.keys.UpdatePrimaryVersion(ctx, project, location, kr, id, version); err != nil {
		return kmsstore.CryptoKey{}, versionErr(err)
	}
	ck, err := s.keys.GetCryptoKey(ctx, project, location, kr, id)
	if err != nil {
		return kmsstore.CryptoKey{}, keyErr(err)
	}
	return ck, nil
}

// DeleteCryptoKey removes a crypto key whose versions are all gone and records
// a RetiredResource so the name cannot be reused.
func (s *Service) DeleteCryptoKey(ctx context.Context, project, location, kr, id string) (RetiredResource, error) {
	if _, err := s.keys.GetCryptoKey(ctx, project, location, kr, id); err != nil {
		return RetiredResource{}, keyErr(err)
	}
	versions, err := s.keys.ListVersions(ctx, project, location, kr, id)
	if err != nil {
		return RetiredResource{}, versionErr(err)
	}
	if len(versions) > 0 {
		return RetiredResource{}, failedPrecondition("all CryptoKeyVersions must be deleted before the CryptoKey")
	}
	if err := s.keys.DeleteCryptoKey(ctx, project, location, kr, id); err != nil {
		return RetiredResource{}, keyErr(err)
	}
	rr := RetiredResource{
		Name:             RetiredResourceName(project, location, id),
		OriginalResource: CryptoKeyName(project, location, kr, id),
		ResourceType:     "CRYPTO_KEY",
		DeleteTime:       clock.Now(),
	}
	data, _ := json.Marshal(rr)
	if err := s.resources.Upsert(ctx, project, store.GlobalRegion, store.ResourceEntry{
		Type: rtRetiredResource, ID: retiredResourceID(location, id), Data: data,
	}); err != nil {
		return RetiredResource{}, err
	}
	return rr, nil
}

// ─── Versions ─────────────────────────────────────────────────────────────────

// CreateVersionInput carries the caller-supplied fields of a
// CreateCryptoKeyVersion request. ExternalProtectionLevelOptions reports
// whether the request carried Discovery's ExternalProtectionLevelOptions
// (external key URI / EKM connection key path), which the emulator cannot
// represent.
type CreateVersionInput struct {
	TrustedWrappingEnabled         bool
	ExternalProtectionLevelOptions bool
}

// CreateVersion creates a generated version of a crypto key.
func (s *Service) CreateVersion(ctx context.Context, project, location, kr, id string, in CreateVersionInput) (kmsstore.Version, error) {
	ck, err := s.keys.GetCryptoKey(ctx, project, location, kr, id)
	if err != nil {
		return kmsstore.Version{}, keyErr(err)
	}
	if ck.ImportOnly {
		return kmsstore.Version{}, failedPrecondition("CryptoKey is import-only; versions must be created by import")
	}
	// Discovery: ExternalProtectionLevelOptions is "specific to the EXTERNAL and
	// EXTERNAL_VPC protection levels"; on any other key it is a shape error.
	if isExternalProtectionLevel(ck.ProtectionLevel) {
		return kmsstore.Version{}, externalVersionUnsupported()
	}
	if in.ExternalProtectionLevelOptions {
		return kmsstore.Version{}, invalidArgument("external_protection_level_options is only valid for EXTERNAL or EXTERNAL_VPC CryptoKeyVersions")
	}
	if in.TrustedWrappingEnabled {
		if err := validateTrustedWrappingTarget(ck.Purpose, ck.ProtectionLevel); err != nil {
			return kmsstore.Version{}, err
		}
	}
	now := clock.Now()
	version, err := s.keys.CreateVersion(ctx, project, location, kr, id, kmsstore.Version{
		CreateTime: now, Algorithm: ck.Algorithm, ProtectionLevel: ck.ProtectionLevel,
		TrustedWrappingEnabled: in.TrustedWrappingEnabled,
		HsmTrusted:             ck.Purpose == "AES_WRAPPING" && ck.ProtectionLevel == "HSM_SINGLE_TENANT",
	})
	if err != nil {
		return kmsstore.Version{}, versionErr(err)
	}
	return kmsstore.Version{Version: version, State: "ENABLED", Algorithm: ck.Algorithm, CreateTime: now, ProtectionLevel: ck.ProtectionLevel, TrustedWrappingEnabled: in.TrustedWrappingEnabled}, nil
}

// GetVersion returns a version, applying elapsed destruction windows.
func (s *Service) GetVersion(ctx context.Context, project, location, kr, id, version string) (kmsstore.Version, error) {
	s.PromoteDestroyed(ctx, project, location, kr, id)
	v, err := s.keys.GetVersion(ctx, project, location, kr, id, version)
	if err != nil {
		return kmsstore.Version{}, versionErr(err)
	}
	return v, nil
}

// ListVersions lists a crypto key's versions, applying elapsed destruction
// windows.
func (s *Service) ListVersions(ctx context.Context, project, location, kr, id string) ([]kmsstore.Version, error) {
	s.PromoteDestroyed(ctx, project, location, kr, id)
	versions, err := s.keys.ListVersions(ctx, project, location, kr, id)
	if err != nil {
		return nil, versionErr(err)
	}
	return versions, nil
}

// UpdateVersionState moves a version between ENABLED and DISABLED.
func (s *Service) UpdateVersionState(ctx context.Context, project, location, kr, id, version, state string) (kmsstore.Version, error) {
	if state != "ENABLED" && state != "DISABLED" {
		return kmsstore.Version{}, invalidArgument("cryptoKeyVersion.state must be ENABLED or DISABLED")
	}
	s.PromoteDestroyed(ctx, project, location, kr, id)
	cur, err := s.keys.GetVersion(ctx, project, location, kr, id, version)
	if err != nil {
		return kmsstore.Version{}, versionErr(err)
	}
	if cur.State == "DESTROY_SCHEDULED" || cur.State == "DESTROYED" {
		return kmsstore.Version{}, failedPrecondition("CryptoKeyVersion is " + cur.State + "; use RestoreCryptoKeyVersion")
	}
	if err := s.keys.UpdateVersionState(ctx, project, location, kr, id, version, state); err != nil {
		return kmsstore.Version{}, versionErr(err)
	}
	return s.keys.GetVersion(ctx, project, location, kr, id, version)
}

// DestroyVersion schedules a version for destruction.
func (s *Service) DestroyVersion(ctx context.Context, project, location, kr, id, version string) (kmsstore.Version, error) {
	s.PromoteDestroyed(ctx, project, location, kr, id)
	v, err := s.keys.DestroyVersion(ctx, project, location, kr, id, version, clock.Now().Add(kmsstore.DefaultDestroyScheduledDuration))
	if err != nil {
		return kmsstore.Version{}, versionErr(err)
	}
	return v, nil
}

// RestoreVersion reverses a scheduled destruction.
func (s *Service) RestoreVersion(ctx context.Context, project, location, kr, id, version string) (kmsstore.Version, error) {
	s.PromoteDestroyed(ctx, project, location, kr, id)
	v, err := s.keys.RestoreVersion(ctx, project, location, kr, id, version)
	if err != nil {
		return kmsstore.Version{}, versionErr(err)
	}
	return v, nil
}

// DeleteVersion permanently removes a DESTROYED (or IMPORT_FAILED /
// GENERATION_FAILED) version.
func (s *Service) DeleteVersion(ctx context.Context, project, location, kr, id, version string) error {
	s.PromoteDestroyed(ctx, project, location, kr, id)
	v, err := s.keys.GetVersion(ctx, project, location, kr, id, version)
	if err != nil {
		return versionErr(err)
	}
	switch v.State {
	case "DESTROYED", "IMPORT_FAILED", "GENERATION_FAILED":
	default:
		return failedPrecondition("CryptoKeyVersion must be DESTROYED before deletion")
	}
	return versionErr(s.keys.DeleteVersion(ctx, project, location, kr, id, version))
}

// ─── Crypto operations ────────────────────────────────────────────────────────

// Encrypt encrypts plaintext with a crypto key's primary (or named) version.
func (s *Service) Encrypt(ctx context.Context, project, location, kr, key, version string, plaintext, aad []byte) (EncryptResult, error) {
	if err := s.authorizeCryptoKey(ctx, project, location, kr, key, policy.PermCryptoKeyUseToEncrypt); err != nil {
		return EncryptResult{}, err
	}
	kmsstore.RotateIfDue(ctx, s.keys, project, location, kr, key, clock.Now())
	ck, err := s.keys.GetCryptoKey(ctx, project, location, kr, key)
	if err != nil {
		return EncryptResult{}, keyErr(err)
	}
	if version == "" {
		version = ck.PrimaryVersion
	}
	if err := s.requireVersionEnabled(ctx, project, location, kr, key, version); err != nil {
		return EncryptResult{}, err
	}
	keyMat, err := s.keys.KeyMaterial(ctx, project, location, kr, key, version)
	if err != nil {
		return EncryptResult{}, versionErr(err)
	}
	ct, err := kmsstore.EncryptData(keyMat, plaintext, aad)
	if err != nil {
		return EncryptResult{}, internal("encryption failed")
	}
	return EncryptResult{Version: version, Ciphertext: kmsstore.EncodeVersionedCiphertext(version, ct), UsedPrimary: version == ck.PrimaryVersion}, nil
}

// Decrypt decrypts a versioned ciphertext blob.
func (s *Service) Decrypt(ctx context.Context, project, location, kr, key string, ciphertext, aad []byte) (DecryptResult, error) {
	if err := s.authorizeCryptoKey(ctx, project, location, kr, key, policy.PermCryptoKeyUseToDecrypt); err != nil {
		return DecryptResult{}, err
	}
	ck, err := s.keys.GetCryptoKey(ctx, project, location, kr, key)
	if err != nil {
		return DecryptResult{}, keyErr(err)
	}
	version, ct, err := kmsstore.DecodeVersionedCiphertext(ciphertext)
	if err != nil {
		return DecryptResult{}, invalidArgument("invalid ciphertext")
	}
	if err := s.requireVersionEnabled(ctx, project, location, kr, key, version); err != nil {
		return DecryptResult{}, err
	}
	keyMat, err := s.keys.KeyMaterial(ctx, project, location, kr, key, version)
	if err != nil {
		return DecryptResult{}, versionErr(err)
	}
	pt, err := kmsstore.DecryptData(keyMat, ct, aad)
	if err != nil {
		return DecryptResult{}, invalidArgument("decryption failed")
	}
	return DecryptResult{Plaintext: pt, UsedPrimary: version == ck.PrimaryVersion}, nil
}

// RawEncrypt encrypts plaintext with the portable AES-GCM primitive (RAW
// purpose); a caller-supplied IV is honored, otherwise a random 12-byte nonce
// is generated.
func (s *Service) RawEncrypt(ctx context.Context, project, location, kr, key, version string, plaintext, aad, iv []byte) (RawEncryptResult, error) {
	v, err := s.GetVersion(ctx, project, location, kr, key, version)
	if err != nil {
		return RawEncryptResult{}, err
	}
	if v.State != "ENABLED" {
		return RawEncryptResult{}, versionNotEnabledErr(version, v.State)
	}
	if !strings.Contains(v.Algorithm, "GCM") {
		return RawEncryptResult{}, failedPrecondition("key is not for raw AES-GCM encryption")
	}
	keyMat, err := s.keys.KeyMaterial(ctx, project, location, kr, key, version)
	if err != nil {
		return RawEncryptResult{}, versionErr(err)
	}
	if len(iv) == 0 {
		iv = make([]byte, 12)
		if _, err := rand.Read(iv); err != nil {
			return RawEncryptResult{}, internal("random generation failed")
		}
	}
	ct, err := kmsstore.RawEncryptGCM(keyMat, plaintext, aad, iv)
	if err != nil {
		return RawEncryptResult{}, invalidArgument("encryption failed")
	}
	return RawEncryptResult{Version: version, Ciphertext: ct, IV: iv}, nil
}

// RawDecrypt is the inverse of RawEncrypt.
func (s *Service) RawDecrypt(ctx context.Context, project, location, kr, key, version string, ciphertext, aad, iv []byte, tagLength int) ([]byte, error) {
	v, err := s.GetVersion(ctx, project, location, kr, key, version)
	if err != nil {
		return nil, err
	}
	if v.State != "ENABLED" {
		return nil, versionNotEnabledErr(version, v.State)
	}
	if !strings.Contains(v.Algorithm, "GCM") {
		return nil, failedPrecondition("key is not for raw AES-GCM decryption")
	}
	if tagLength != 0 && tagLength != 16 {
		return nil, invalidArgument("unsupported tag_length")
	}
	keyMat, err := s.keys.KeyMaterial(ctx, project, location, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	pt, err := kmsstore.RawDecryptGCM(keyMat, ciphertext, aad, iv)
	if err != nil {
		return nil, invalidArgument("decryption failed")
	}
	return pt, nil
}

// AsymmetricSign signs a pre-computed digest with an asymmetric version.
func (s *Service) AsymmetricSign(ctx context.Context, project, location, kr, key, version string, digest []byte) (SignResult, error) {
	if err := s.authorizeCryptoKey(ctx, project, location, kr, key, policy.PermCryptoKeyUseToSign); err != nil {
		return SignResult{}, err
	}
	v, err := s.GetVersion(ctx, project, location, kr, key, version)
	if err != nil {
		return SignResult{}, err
	}
	if v.State != "ENABLED" {
		return SignResult{}, versionNotEnabledErr(version, v.State)
	}
	priv, err := s.keys.PrivateKey(ctx, project, location, kr, key, version)
	if err != nil {
		return SignResult{}, versionErr(err)
	}
	var sig []byte
	switch {
	case strings.HasPrefix(v.Algorithm, "RSA_SIGN"):
		sig, err = kmsstore.RSASign(priv, digest, v.Algorithm)
	case strings.HasPrefix(v.Algorithm, "EC_SIGN"):
		sig, err = kmsstore.ECSign(priv, digest)
	default:
		return SignResult{}, failedPrecondition("key is not for asymmetric signing")
	}
	if err != nil {
		return SignResult{}, invalidArgument("signing failed")
	}
	return SignResult{Version: version, Signature: sig}, nil
}

// AsymmetricDecrypt decrypts ciphertext with an RSA-OAEP version.
func (s *Service) AsymmetricDecrypt(ctx context.Context, project, location, kr, key, version string, ciphertext []byte) ([]byte, error) {
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
	if !strings.HasPrefix(v.Algorithm, "RSA_DECRYPT") {
		return nil, failedPrecondition("key is not for asymmetric decryption")
	}
	priv, err := s.keys.PrivateKey(ctx, project, location, kr, key, version)
	if err != nil {
		return nil, versionErr(err)
	}
	pt, err := kmsstore.RSADecryptOAEP(priv, ciphertext, v.Algorithm)
	if err != nil {
		return nil, invalidArgument("decryption failed")
	}
	return pt, nil
}

// MacSign computes a MAC over data with an HMAC version.
func (s *Service) MacSign(ctx context.Context, project, location, kr, key, version string, data []byte) (MacResult, error) {
	if err := s.authorizeCryptoKey(ctx, project, location, kr, key, policy.PermCryptoKeyUseToSign); err != nil {
		return MacResult{}, err
	}
	v, err := s.GetVersion(ctx, project, location, kr, key, version)
	if err != nil {
		return MacResult{}, err
	}
	if v.State != "ENABLED" {
		return MacResult{}, versionNotEnabledErr(version, v.State)
	}
	if !strings.HasPrefix(v.Algorithm, "HMAC_") {
		return MacResult{}, failedPrecondition("key is not for MAC")
	}
	mat, err := s.keys.KeyMaterial(ctx, project, location, kr, key, version)
	if err != nil {
		return MacResult{}, versionErr(err)
	}
	mac, err := kmsstore.HMACSign(mat, data, v.Algorithm)
	if err != nil {
		return MacResult{}, invalidArgument("mac sign failed")
	}
	return MacResult{Version: version, Mac: mac}, nil
}

// MacVerify reports whether mac is a valid HMAC over data.
func (s *Service) MacVerify(ctx context.Context, project, location, kr, key, version string, data, mac []byte) (bool, error) {
	if err := s.authorizeCryptoKey(ctx, project, location, kr, key, policy.PermCryptoKeyUseToVerify); err != nil {
		return false, err
	}
	v, err := s.GetVersion(ctx, project, location, kr, key, version)
	if err != nil {
		return false, err
	}
	if v.State != "ENABLED" {
		return false, versionNotEnabledErr(version, v.State)
	}
	if !strings.HasPrefix(v.Algorithm, "HMAC_") {
		return false, failedPrecondition("key is not for MAC")
	}
	mat, err := s.keys.KeyMaterial(ctx, project, location, kr, key, version)
	if err != nil {
		return false, versionErr(err)
	}
	return kmsstore.HMACVerify(mat, data, mac, v.Algorithm), nil
}

// GetPublicKey returns a version's public key in the requested format. format
// is the caller's GetPublicKeyRequest.public_key_format: PEM (the default) or
// DER for asymmetric keys, and the algorithm's PQC format for KEM keys, where
// Cloud KMS requires it and returns INVALID_ARGUMENT when it is omitted or
// names another format.
func (s *Service) GetPublicKey(ctx context.Context, project, location, kr, key, version, format string) (PublicKeyResult, error) {
	if err := s.authorizeCryptoKey(ctx, project, location, kr, key, policy.PermCryptoKeyViewPublicKey); err != nil {
		return PublicKeyResult{}, err
	}
	v, err := s.GetVersion(ctx, project, location, kr, key, version)
	if err != nil {
		return PublicKeyResult{}, err
	}
	if v.State != "ENABLED" {
		return PublicKeyResult{}, versionNotEnabledErr(version, v.State)
	}
	wantFormat := publicKeyFormatForAlgorithm(v.Algorithm)
	if format == "" || format == "PUBLIC_KEY_FORMAT_UNSPECIFIED" {
		if wantFormat != "PEM" {
			return PublicKeyResult{}, invalidArgument("public_key_format is required for PQC algorithms")
		}
		format = "PEM"
	}
	if format != wantFormat && (wantFormat != "PEM" || (format != "PEM" && format != "DER")) {
		return PublicKeyResult{}, invalidArgument("public_key_format " + format + " is not supported for algorithm " + v.Algorithm)
	}
	pub, err := s.keys.PublicKey(ctx, project, location, kr, key, version)
	if err != nil {
		return PublicKeyResult{}, versionErr(err)
	}
	res := PublicKeyResult{Version: version, Algorithm: v.Algorithm, Format: format}
	switch format {
	case "DER":
		res.Raw = pub
	case "NIST_PQC", "XWING_RAW_BYTES":
		res.Raw = pub
	default:
		pemStr, err := kmsstore.PublicKeyPEM(pub)
		if err != nil {
			return PublicKeyResult{}, internal("public key encode failed")
		}
		res.PEM = pemStr
	}
	return res, nil
}

// publicKeyFormatForAlgorithm returns the format Cloud KMS uses for an
// algorithm's public key: the raw PQC format for KEM algorithms, PEM otherwise.
func publicKeyFormatForAlgorithm(algorithm string) string {
	switch algorithm {
	case "ML_KEM_768", "ML_KEM_1024":
		return "NIST_PQC"
	case "KEM_XWING":
		return "XWING_RAW_BYTES"
	default:
		return "PEM"
	}
}

// errorsIsAlreadyExists reports whether err is the store's AlreadyExists.
func errorsIsAlreadyExists(err error) bool {
	return errors.Is(err, kmsstore.ErrAlreadyExists)
}
