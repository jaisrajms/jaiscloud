package kms

import (
	"encoding/base64"

	"jaiscloud/internal/gcp/policy"
	kmscore "jaiscloud/internal/gcp/service/kms"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/model"
)

// cryptoKeyMap renders a CryptoKey as its Cloud KMS JSON object. primary is the
// crypto key's primary version.
func cryptoKeyMap(nr *model.NormalizedRequest, k kmsstore.CryptoKey, primaryVersion kmsstore.Version) map[string]any {
	// Discovery: "Keys with purpose ENCRYPT_DECRYPT may have a primary. For other
	// keys, this field will be omitted." An import-only key has no version yet.
	if k.Purpose != "ENCRYPT_DECRYPT" || k.PrimaryVersion == "" {
		return cryptoKeyBaseMap(nr, k)
	}
	primaryState := primaryVersion.State
	if primaryState == "" {
		primaryState = "ENABLED"
	}
	primaryCreateTime := primaryVersion.CreateTime
	if primaryCreateTime.IsZero() {
		primaryCreateTime = k.CreateTime
	}
	primary := map[string]any{
		"name":            kmscore.VersionName(nr.AccountID, k.Location, k.KeyRingID, k.ID, k.PrimaryVersion),
		"state":           primaryState,
		"algorithm":       k.Algorithm,
		"protectionLevel": protectionLevel(k.ProtectionLevel),
	}
	if !primaryCreateTime.IsZero() {
		ts := timestamp.Canonical(primaryCreateTime)
		primary["createTime"] = ts
		primary["generateTime"] = ts
	}
	if primaryVersion.State == "DESTROY_SCHEDULED" && !primaryVersion.DestroyTime.IsZero() {
		primary["destroyTime"] = timestamp.Canonical(primaryVersion.DestroyTime)
	}
	if primaryVersion.State == "DESTROYED" && !primaryVersion.DestroyEventTime.IsZero() {
		primary["destroyEventTime"] = timestamp.Canonical(primaryVersion.DestroyEventTime)
	}
	out := cryptoKeyBaseMap(nr, k)
	out["primary"] = primary
	return out
}

// cryptoKeyBaseMap renders the CryptoKey fields that do not depend on a primary
// version.
func cryptoKeyBaseMap(nr *model.NormalizedRequest, k kmsstore.CryptoKey) map[string]any {
	out := map[string]any{
		"name":       kmscore.CryptoKeyName(nr.AccountID, k.Location, k.KeyRingID, k.ID),
		"purpose":    k.Purpose,
		"createTime": timestamp.Canonical(k.CreateTime),
		"versionTemplate": map[string]any{
			"algorithm":       k.Algorithm,
			"protectionLevel": protectionLevel(k.ProtectionLevel),
		},
	}
	if k.ImportOnly {
		out["importOnly"] = true
	}
	if len(k.Labels) > 0 {
		out["labels"] = k.Labels
	}
	if k.RotationPeriod > 0 {
		out["rotationPeriod"] = durationString(k.RotationPeriod)
	}
	if !k.NextRotationTime.IsZero() {
		out["nextRotationTime"] = timestamp.Canonical(k.NextRotationTime)
	}
	return out
}

// versionMap renders a CryptoKeyVersion as its Cloud KMS JSON object.
func versionMap(nr *model.NormalizedRequest, loc, kr, key string, v kmsstore.Version) map[string]any {
	out := map[string]any{
		"name":            kmscore.VersionName(nr.AccountID, loc, kr, key, v.Version),
		"state":           v.State,
		"algorithm":       v.Algorithm,
		"protectionLevel": protectionLevel(v.ProtectionLevel),
		"createTime":      timestamp.Canonical(v.CreateTime),
		"generateTime":    timestamp.Canonical(v.CreateTime),
	}
	if !v.ImportTime.IsZero() {
		out["importTime"] = timestamp.Canonical(v.ImportTime)
	}
	if v.TrustedWrappingEnabled {
		out["trustedWrappingEnabled"] = true
	}
	if v.HsmTrusted {
		out["hsmTrusted"] = true
	}
	if v.State == "DESTROY_SCHEDULED" && !v.DestroyTime.IsZero() {
		out["destroyTime"] = timestamp.Canonical(v.DestroyTime)
	}
	if v.State == "DESTROYED" && !v.DestroyEventTime.IsZero() {
		out["destroyEventTime"] = timestamp.Canonical(v.DestroyEventTime)
	}
	return out
}

// importJobMap renders an ImportJob as its Cloud KMS JSON object.
func importJobMap(ij kmscore.ImportJob) map[string]any {
	out := map[string]any{
		"name":            ij.Name,
		"importMethod":    ij.ImportMethod,
		"protectionLevel": protectionLevel(ij.ProtectionLevel),
		"state":           ij.State,
		"createTime":      timestamp.Canonical(ij.CreateTime),
		"generateTime":    timestamp.Canonical(ij.GenerateTime),
		"expireTime":      timestamp.Canonical(ij.ExpireTime),
	}
	// The wrapping public key is documented "Only returned if state is ACTIVE";
	// the key material stays stored (and is only usable to import while ACTIVE,
	// which ImportCryptoKeyVersion enforces), so an EXPIRED job renders none.
	// publicKeyFormat carries no such condition — it echoes the format the
	// caller asked for — so it is rendered for every state, which also keeps the
	// publicKeyFormat list filter in agreement with the body (DC42).
	switch {
	case len(ij.PublicKeyRaw) > 0:
		out["publicKeyFormat"] = publicKeyFormatForMethod(ij.ImportMethod)
		if ij.State == "ACTIVE" {
			out["publicKey"] = map[string]any{"data": base64.StdEncoding.EncodeToString(ij.PublicKeyRaw)}
		}
	case ij.PublicKeyPEM != "":
		out["publicKeyFormat"] = "PEM"
		if ij.State == "ACTIVE" {
			out["publicKey"] = map[string]any{"pem": ij.PublicKeyPEM}
		}
	}
	return out
}

func publicKeyFormatForMethod(method string) string {
	if method == "HPKE_KEM_XWING_HKDF_SHA256_AES_256_GCM" {
		return "XWING_RAW_BYTES"
	}
	return "NIST_PQC"
}

// protectionLevel maps an empty protection level to SOFTWARE.
func protectionLevel(p string) string {
	if p == "" {
		return "SOFTWARE"
	}
	return p
}

// iamPolicyMap renders a Policy the way Cloud KMS's REST API does: the etag is
// always present, while version and bindings are omitted when empty/default.
func iamPolicyMap(p policy.Policy) map[string]any {
	out := map[string]any{"etag": p.Etag}
	if p.Version > 1 {
		out["version"] = p.Version
	}
	if len(p.Bindings) > 0 {
		out["bindings"] = p.Bindings
	}
	return out
}
