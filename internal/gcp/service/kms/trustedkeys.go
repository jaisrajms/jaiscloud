package kms

import (
	"context"

	"jaiscloud/internal/model"
)

// This file holds the documented rules around HSM single-tenant trusted
// wrapping: which keys and versions may carry trusted_wrapping_enabled, which
// keys may serve as a trusted wrapping key, and the EKM-backed protection
// levels the emulator cannot represent.
//
// Discovery (cloudkms v1, rev 20260827) is the authority:
//
//   - CryptoKeyVersion.trusted_wrapping_enabled: "This field can be set for all
//     key purposes except ENCRYPT_DECRYPT, and is only valid for keys with
//     protection level HSM_SINGLE_TENANT."
//   - cryptoKeys.create trustedWrappingEnabled: the same two restrictions.
//   - cryptoKeyVersions.exportTrustedKeyWrappedCryptoKeyVersion: "The
//     CryptoKeyVersion of the [wrapping_key] must have the AES_WRAPPING purpose.
//     The [wrapping_key] must have the AES_256_KWP algorithm." plus
//     CryptoKeyVersion.hsm_trusted.
//   - ImportTrustedKeyWrappedCryptoKeyVersionRequest.importing_key: "the CKV of
//     the trusted key used to import. This can be the name of a
//     CryptoKeyVersion or a CryptoKey."

// isExternalProtectionLevel reports whether a protection level is backed by an
// external key manager (EXTERNAL) or an EKM-over-VPC backend (EXTERNAL_VPC).
func isExternalProtectionLevel(level string) bool {
	return level == "EXTERNAL" || level == "EXTERNAL_VPC"
}

// externalVersionUnsupported is the fail-loud answer for every EXTERNAL /
// EXTERNAL_VPC key or version. Cloud EKM key material is "created and stored in
// your EKM ... it is never shared with Google"
// (https://cloud.google.com/kms/docs/ekm), and this emulator has no
// ekmConnections/singleTenantHsmInstances control plane (REST 501, gRPC
// EkmService not registered). It therefore cannot hold or serve external key
// material, so it refuses instead of fabricating SOFTWARE material for a key
// the API declares as EKM-backed.
func externalVersionUnsupported() error {
	return model.NewProviderError("Unimplemented",
		"EXTERNAL and EXTERNAL_VPC CryptoKeyVersions require an EKM connection, which jaiscloud does not implement", 501)
}

// validateTrustedWrappingTarget enforces the Discovery rule shared by
// cryptoKeys.create's trustedWrappingEnabled, CreateCryptoKeyVersion and
// ImportCryptoKeyVersion: trusted wrapping is only valid for keys whose version
// template is HSM_SINGLE_TENANT and whose purpose is not ENCRYPT_DECRYPT.
func validateTrustedWrappingTarget(purpose, protectionLevel string) error {
	if purpose == "ENCRYPT_DECRYPT" {
		return invalidArgument("trusted wrapping is not supported for CryptoKeys with purpose ENCRYPT_DECRYPT")
	}
	if protectionLevel != "HSM_SINGLE_TENANT" {
		return invalidArgument("trusted wrapping is only supported for keys with protection level HSM_SINGLE_TENANT")
	}
	return nil
}

// reimportUnsupported is the fail-loud answer for a request that names an
// existing CryptoKeyVersion as an import target. Reimporting key material into
// a DESTROYED/IMPORT_FAILED version is a separate operation the emulator does
// not model; creating a new version instead would silently ignore the request.
func reimportUnsupported() error {
	return model.NewProviderError("Unimplemented",
		"reimporting into an existing CryptoKeyVersion is not supported by jaiscloud", 501)
}

// resolveTrustedWrappingKey resolves a trusted import's importing_key / a
// trusted export's wrapping_key to the ENABLED, HSM-trusted AES-256-KWP key
// material used to unwrap or wrap key material. name may be a CryptoKey name
// (import only: the docs accept "a CryptoKeyVersion or a CryptoKey", resolved
// through the key's primary version) or a CryptoKeyVersion name. field is the
// request field name used in error messages.
func (s *Service) resolveTrustedWrappingKey(ctx context.Context, project, location, kr, name, field string, allowKeyName bool) ([]byte, error) {
	p, l, kkr, kkey, ver, ok := SplitKeyOrVersionName(name)
	if !allowKeyName && ver == "" {
		ok = false
	}
	if !ok || p != project || l != location || kkr != kr {
		if allowKeyName {
			return nil, invalidArgument(field + " must name a CryptoKey or CryptoKeyVersion in the target key ring")
		}
		return nil, invalidArgument(field + " must name a CryptoKeyVersion in the target key ring")
	}
	ck, err := s.GetCryptoKey(ctx, project, location, kr, kkey)
	if err != nil {
		return nil, err
	}
	if ck.Purpose != "AES_WRAPPING" {
		return nil, failedPrecondition(field + " must be a CryptoKey with purpose AES_WRAPPING")
	}
	if ver == "" {
		ver = ck.PrimaryVersion
		if ver == "" {
			return nil, failedPrecondition(field + " has no primary CryptoKeyVersion")
		}
	}
	if err := s.requireVersionEnabled(ctx, project, location, kr, kkey, ver); err != nil {
		return nil, err
	}
	v, err := s.keys.GetVersion(ctx, project, location, kr, kkey, ver)
	if err != nil {
		return nil, versionErr(err)
	}
	if v.Algorithm != "AES_256_KWP" {
		return nil, failedPrecondition(field + " must have algorithm AES_256_KWP")
	}
	if v.ProtectionLevel != "HSM_SINGLE_TENANT" {
		return nil, failedPrecondition(field + " must have protection level HSM_SINGLE_TENANT")
	}
	if !v.HsmTrusted {
		return nil, failedPrecondition(field + " is not HSM-trusted")
	}
	mat, err := s.keys.KeyMaterial(ctx, project, location, kr, kkey, ver)
	if err != nil {
		return nil, versionErr(err)
	}
	return mat, nil
}
