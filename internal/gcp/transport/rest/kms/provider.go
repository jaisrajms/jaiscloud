// Package kms is the REST transport adapter for Cloud KMS. It maps the
// Discovery-derived Cloud KMS JSON API onto the transport-neutral core
// (internal/gcp/service/kms).
package kms

import (
	"context"
	"encoding/base64"
	"hash/crc32"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/gcp/paging"
	kmscore "jaiscloud/internal/gcp/service/kms"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider handles Cloud KMS key rings, crypto keys and crypto-key versions.
type Provider struct {
	core *kmscore.Service
}

// New returns a REST KMS provider over the shared core.
func New(core *kmscore.Service) *Provider { return &Provider{core: core} }

// Routes maps the emulator registry dispatch actions to handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"KMS.KeyRingCreate":                     p.KeyRingCreate,
		"KMS.KeyRingList":                       p.KeyRingList,
		"KMS.KeyRingGet":                        p.KeyRingGet,
		"KMS.CryptoKeyCreate":                   p.CryptoKeyCreate,
		"KMS.CryptoKeyList":                     p.CryptoKeyList,
		"KMS.CryptoKeyGet":                      p.CryptoKeyGet,
		"KMS.CryptoKeyUpdate":                   p.CryptoKeyUpdate,
		"KMS.CryptoKeyEncrypt":                  p.CryptoKeyEncrypt,
		"KMS.CryptoKeyDecrypt":                  p.CryptoKeyDecrypt,
		"KMS.CryptoKeyVersionCreate":            p.CryptoKeyVersionCreate,
		"KMS.CryptoKeyVersionList":              p.CryptoKeyVersionList,
		"KMS.CryptoKeyVersionGet":               p.CryptoKeyVersionGet,
		"KMS.CryptoKeyVersionUpdate":            p.CryptoKeyVersionUpdate,
		"KMS.CryptoKeyVersionDestroy":           p.CryptoKeyVersionDestroy,
		"KMS.CryptoKeyVersionRestore":           p.CryptoKeyVersionRestore,
		"KMS.CryptoKeyVersionDelete":            p.CryptoKeyVersionDelete,
		"KMS.CryptoKeyDelete":                   p.CryptoKeyDelete,
		"KMS.CryptoKeyUpdatePrimaryVersion":     p.CryptoKeyUpdatePrimaryVersion,
		"KMS.CryptoKeyVersionAsymmetricSign":    p.CryptoKeyVersionAsymmetricSign,
		"KMS.CryptoKeyVersionAsymmetricDecrypt": p.CryptoKeyVersionAsymmetricDecrypt,
		"KMS.CryptoKeyVersionMacSign":           p.CryptoKeyVersionMacSign,
		"KMS.CryptoKeyVersionMacVerify":         p.CryptoKeyVersionMacVerify,
		"KMS.CryptoKeyVersionRawEncrypt":        p.CryptoKeyVersionRawEncrypt,
		"KMS.CryptoKeyVersionRawDecrypt":        p.CryptoKeyVersionRawDecrypt,
		"KMS.CryptoKeyVersionGetPublicKey":      p.CryptoKeyVersionGetPublicKey,
		"KMS.CryptoKeyVersionImport":            p.CryptoKeyVersionImport,
		"KMS.CryptoKeyVersionImportTrusted":     p.CryptoKeyVersionImportTrusted,
		"KMS.CryptoKeyVersionExportTrusted":     p.CryptoKeyVersionExportTrusted,
		"KMS.CryptoKeyVersionDecapsulate":       p.CryptoKeyVersionDecapsulate,
		"KMS.ImportJobCreate":                   p.ImportJobCreate,
		"KMS.ImportJobGet":                      p.ImportJobGet,
		"KMS.ImportJobList":                     p.ImportJobList,
		"KMS.GenerateRandomBytes":               p.GenerateRandomBytes,
		"KMS.KeyRingGetIamPolicy":               p.GetIamPolicy,
		"KMS.KeyRingSetIamPolicy":               p.SetIamPolicy,
		"KMS.KeyRingTestIamPermissions":         p.TestIamPermissions,
		"KMS.CryptoKeyGetIamPolicy":             p.GetIamPolicy,
		"KMS.CryptoKeySetIamPolicy":             p.SetIamPolicy,
		"KMS.CryptoKeyTestIamPermissions":       p.TestIamPermissions,
		"KMS.ImportJobGetIamPolicy":             p.GetIamPolicy,
		"KMS.ImportJobSetIamPolicy":             p.SetIamPolicy,
		"KMS.ImportJobTestIamPermissions":       p.TestIamPermissions,
	}
}

// resourceName returns the "name" path param, or a 400 when absent.
func resourceName(nr *model.NormalizedRequest) (string, error) {
	n, ok := nr.Params["name"].(string)
	if !ok || n == "" {
		return "", model.NewProviderError("InvalidRequest", "missing resource name", 400)
	}
	return n, nil
}

// ─── KeyRings ─────────────────────────────────────────────────────────────────

func (p *Provider) KeyRingCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	loc, _ := nr.Params["location"].(string)
	kr, _ := nr.Params["keyRingId"].(string)
	if kr == "" {
		if body, ok := nr.Params["body"].(map[string]any); ok {
			if n, _ := body["name"].(string); n != "" {
				_, kr = parseKeyRing(n)
			}
		}
	}
	if loc == "" || kr == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing location or keyRingId", 400)
	}
	meta, err := p.core.CreateKeyRing(ctx, nr.AccountID, loc, kr)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"name":       kmscore.KeyRingName(nr.AccountID, loc, meta.ID),
		"createTime": timestamp.Canonical(meta.CreateTime),
	}), nil
}

func (p *Provider) KeyRingList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	loc, _ := nr.Params["location"].(string)
	query, err := listQueryFromParams(nr.Params)
	if err != nil {
		return nil, err
	}
	page, next, total, err := p.core.PageKeyRings(ctx, nr.AccountID, loc, query)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, kr := range page {
		items = append(items, map[string]any{
			"name":       kmscore.KeyRingName(nr.AccountID, kr.Location, kr.ID),
			"createTime": timestamp.Canonical(kr.CreateTime),
		})
	}
	resp := map[string]any{"keyRings": items, "totalSize": total}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) KeyRingGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr := parseKeyRing(name)
	meta, err := p.core.GetKeyRing(ctx, nr.AccountID, loc, kr)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"name":       kmscore.KeyRingName(nr.AccountID, loc, meta.ID),
		"createTime": timestamp.Canonical(meta.CreateTime),
	}), nil
}

// ─── CryptoKeys ───────────────────────────────────────────────────────────────

func (p *Provider) CryptoKeyCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr := parseKeyRing(name)
	if loc == "" || kr == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing location/keyRing", 400)
	}
	key, _ := nr.Params["cryptoKeyId"].(string)
	if key == "" {
		if body, ok := nr.Params["body"].(map[string]any); ok {
			if n, _ := body["name"].(string); n != "" {
				_, _, key = parseCryptoKey(n)
			}
		}
	}
	if key == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing cryptoKeyId", 400)
	}
	in := kmscore.CryptoKeyInput{}
	if body, ok := nr.Params["body"].(map[string]any); ok {
		if purp, _ := body["purpose"].(string); purp != "" {
			in.Purpose = purp
		}
		if vt, ok := body["versionTemplate"].(map[string]any); ok {
			if alg, _ := vt["algorithm"].(string); alg != "" {
				in.Algorithm = alg
			}
			if pl, _ := vt["protectionLevel"].(string); pl != "" {
				in.ProtectionLevel = pl
			}
		}
		in.Labels = parseLabels(body["labels"])
		period, set, err := parseRotationPeriod(body["rotationPeriod"])
		if err != nil {
			return nil, err
		}
		if set {
			in.RotationPeriod = period
		}
		nrt, set, err := parseTimestamp(body["nextRotationTime"])
		if err != nil {
			return nil, err
		}
		if set {
			in.NextRotationTime = &nrt
		}
		if io, _ := body["importOnly"].(bool); io {
			in.ImportOnly = true
		}
	}
	if strField(nr.Params, "skipInitialVersionCreation") == "true" {
		in.SkipInitialVersion = true
	}
	if strField(nr.Params, "trustedWrappingEnabled") == "true" {
		in.TrustedWrappingEnabled = true
	}
	ck, err := p.core.CreateCryptoKey(ctx, nr.AccountID, loc, kr, key, in)
	if err != nil {
		return nil, err
	}
	return provider.OK(cryptoKeyMap(nr, ck, kmsstore.Version{State: "ENABLED", Algorithm: ck.Algorithm, CreateTime: ck.CreateTime, ProtectionLevel: ck.ProtectionLevel})), nil
}

func (p *Provider) CryptoKeyList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr := parseKeyRing(strings.TrimSuffix(name, "/cryptoKeys"))
	if loc == "" || kr == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing location/keyRing", 400)
	}
	query, err := listQueryFromParams(nr.Params)
	if err != nil {
		return nil, err
	}
	page, next, total, err := p.core.PageCryptoKeys(ctx, nr.AccountID, loc, kr, query)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, k := range page {
		items = append(items, cryptoKeyMap(nr, k, p.core.PrimaryVersion(ctx, nr.AccountID, k)))
	}
	resp := map[string]any{"cryptoKeys": items, "totalSize": total}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) CryptoKeyGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKey(name)
	k, err := p.core.GetCryptoKey(ctx, nr.AccountID, loc, kr, key)
	if err != nil {
		return nil, err
	}
	return provider.OK(cryptoKeyMap(nr, k, p.core.PrimaryVersion(ctx, nr.AccountID, k))), nil
}

// CryptoKeyUpdate serves cryptoKeys.patch (the Discovery's PATCH v1/{+name} with
// an updateMask query parameter) — the REST update the official clients send,
// mirroring gRPC UpdateCryptoKey. The shared core requires the mask, validates
// each path against the resource's writable field set (labels, rotation_period,
// next_rotation_time) and applies the rotation invariants.
func (p *Provider) CryptoKeyUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKey(name)
	in, err := cryptoKeyUpdateFromPatch(nr)
	if err != nil {
		return nil, err
	}
	ck, err := p.core.UpdateCryptoKey(ctx, nr.AccountID, loc, kr, key, in)
	if err != nil {
		return nil, err
	}
	return provider.OK(cryptoKeyMap(nr, ck, p.core.PrimaryVersion(ctx, nr.AccountID, ck))), nil
}

// cryptoKeyUpdateFromPatch builds the core update from the PATCH body and
// updateMask query parameter. The Discovery marks updateMask Required, so an
// absent parameter leaves UpdateMask nil (the core rejects it) while an
// explicitly empty one is the no-op real GCP accepts. Paths are passed through
// verbatim; the core normalizes the REST camelCase / proto snake_case spellings.
func cryptoKeyUpdateFromPatch(nr *model.NormalizedRequest) (kmscore.CryptoKeyUpdate, error) {
	var in kmscore.CryptoKeyUpdate
	if raw, ok := nr.Params["updateMask"]; ok {
		in.UpdateMask = []string{}
		mask, _ := raw.(string)
		for _, path := range strings.Split(mask, ",") {
			path = strings.TrimSpace(path)
			if path != "" {
				in.UpdateMask = append(in.UpdateMask, path)
			}
		}
	}
	body, _ := nr.Params["body"].(map[string]any)
	in.Labels = parseLabels(body["labels"])
	period, set, err := parseRotationPeriod(body["rotationPeriod"])
	if err != nil {
		return kmscore.CryptoKeyUpdate{}, err
	}
	if set {
		in.RotationPeriod = &period
	}
	nrt, set, err := parseTimestamp(body["nextRotationTime"])
	if err != nil {
		return kmscore.CryptoKeyUpdate{}, err
	}
	if set {
		in.NextRotationTime = &nrt
	}
	return in, nil
}

func (p *Provider) CryptoKeyEncrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKey(name)
	body, _ := nr.Params["body"].(map[string]any)
	ptStr, _ := body["plaintext"].(string)
	pt, err := base64.StdEncoding.DecodeString(ptStr)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "plaintext must be base64", 400)
	}
	aad := decodeAAD(body["additionalAuthenticatedData"])
	if err := verifyCRCField(body, "plaintextCrc32c", pt); err != nil {
		return nil, err
	}
	if err := verifyCRCField(body, "additionalAuthenticatedDataCrc32c", aad); err != nil {
		return nil, err
	}
	res, err := p.core.Encrypt(ctx, nr.AccountID, loc, kr, key, "", pt, aad)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"name":                    kmscore.VersionName(nr.AccountID, loc, kr, key, res.Version),
		"ciphertext":              base64.StdEncoding.EncodeToString(res.Ciphertext),
		"ciphertextCrc32c":        crc32cString(res.Ciphertext),
		"protectionLevel":         "SOFTWARE",
		"verifiedPlaintextCrc32c": strField(body, "plaintextCrc32c") != "",
		"verifiedAdditionalAuthenticatedDataCrc32c": strField(body, "additionalAuthenticatedDataCrc32c") != "",
	}), nil
}

func (p *Provider) CryptoKeyDecrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKey(name)
	body, _ := nr.Params["body"].(map[string]any)
	ctStr, _ := body["ciphertext"].(string)
	blob, err := base64.StdEncoding.DecodeString(ctStr)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "ciphertext must be base64", 400)
	}
	aad := decodeAAD(body["additionalAuthenticatedData"])
	res, err := p.core.Decrypt(ctx, nr.AccountID, loc, kr, key, blob, aad)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"plaintext":       base64.StdEncoding.EncodeToString(res.Plaintext),
		"plaintextCrc32c": crc32cString(res.Plaintext),
		"protectionLevel": "SOFTWARE",
		"usedPrimary":     res.UsedPrimary,
	}), nil
}

// ─── CryptoKeyVersions ────────────────────────────────────────────────────────

func (p *Provider) CryptoKeyVersionRawEncrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	body, _ := nr.Params["body"].(map[string]any)
	pt := decodeBase64Field(body, "plaintext")
	if err := verifyCRCField(body, "plaintextCrc32c", pt); err != nil {
		return nil, err
	}
	aad := decodeBase64Field(body, "additionalAuthenticatedData")
	if err := verifyCRCField(body, "additionalAuthenticatedDataCrc32c", aad); err != nil {
		return nil, err
	}
	iv := decodeBase64Field(body, "initializationVector")
	if err := verifyCRCField(body, "initializationVectorCrc32c", iv); err != nil {
		return nil, err
	}
	res, err := p.core.RawEncrypt(ctx, nr.AccountID, loc, kr, key, version, pt, aad, iv)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"name":                       kmscore.VersionName(nr.AccountID, loc, kr, key, version),
		"ciphertext":                 base64.StdEncoding.EncodeToString(res.Ciphertext),
		"initializationVector":       base64.StdEncoding.EncodeToString(res.IV),
		"tagLength":                  16,
		"ciphertextCrc32c":           crc32cString(res.Ciphertext),
		"initializationVectorCrc32c": crc32cString(res.IV),
		"protectionLevel":            "SOFTWARE",
		"verifiedPlaintextCrc32c":    strField(body, "plaintextCrc32c") != "",
		"verifiedAdditionalAuthenticatedDataCrc32c": strField(body, "additionalAuthenticatedDataCrc32c") != "",
		"verifiedInitializationVectorCrc32c":        strField(body, "initializationVectorCrc32c") != "",
	}), nil
}

func (p *Provider) CryptoKeyVersionRawDecrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	body, _ := nr.Params["body"].(map[string]any)
	ct := decodeBase64Field(body, "ciphertext")
	if err := verifyCRCField(body, "ciphertextCrc32c", ct); err != nil {
		return nil, err
	}
	aad := decodeBase64Field(body, "additionalAuthenticatedData")
	if err := verifyCRCField(body, "additionalAuthenticatedDataCrc32c", aad); err != nil {
		return nil, err
	}
	iv := decodeBase64Field(body, "initializationVector")
	if err := verifyCRCField(body, "initializationVectorCrc32c", iv); err != nil {
		return nil, err
	}
	pt, err := p.core.RawDecrypt(ctx, nr.AccountID, loc, kr, key, version, ct, aad, iv, intField(body, "tagLength"))
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"plaintext":                base64.StdEncoding.EncodeToString(pt),
		"plaintextCrc32c":          crc32cString(pt),
		"protectionLevel":          "SOFTWARE",
		"verifiedCiphertextCrc32c": strField(body, "ciphertextCrc32c") != "",
		"verifiedAdditionalAuthenticatedDataCrc32c": strField(body, "additionalAuthenticatedDataCrc32c") != "",
		"verifiedInitializationVectorCrc32c":        strField(body, "initializationVectorCrc32c") != "",
	}), nil
}

// GenerateRandomBytes implements locations.generateRandomBytes.
func (p *Provider) GenerateRandomBytes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	data, err := p.core.GenerateRandomBytes(ctx, name, intField(body, "lengthBytes"), strField(body, "protectionLevel"))
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"data":       base64.StdEncoding.EncodeToString(data),
		"dataCrc32c": crc32cString(data),
	}), nil
}

func (p *Provider) CryptoKeyVersionCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKeyFromParent(name)
	if loc == "" || kr == "" || key == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing cryptoKey parent", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	in := kmscore.CreateVersionInput{
		TrustedWrappingEnabled: boolField(body, "trustedWrappingEnabled"),
	}
	// Discovery: ExternalProtectionLevelOptions is a group of fields for
	// EXTERNAL / EXTERNAL_VPC versions; an empty object is the same as omitted.
	if ext, ok := body["externalProtectionLevelOptions"].(map[string]any); ok && len(ext) > 0 {
		in.ExternalProtectionLevelOptions = true
	}
	v, err := p.core.CreateVersion(ctx, nr.AccountID, loc, kr, key, in)
	if err != nil {
		return nil, err
	}
	return provider.OK(versionMap(nr, loc, kr, key, v)), nil
}

func (p *Provider) CryptoKeyVersionList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKeyFromParent(name)
	query, err := listQueryFromParams(nr.Params)
	if err != nil {
		return nil, err
	}
	page, next, total, err := p.core.PageVersions(ctx, nr.AccountID, loc, kr, key, query)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, v := range page {
		items = append(items, versionMap(nr, loc, kr, key, v))
	}
	resp := map[string]any{"cryptoKeyVersions": items, "totalSize": total}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) CryptoKeyVersionGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	v, err := p.core.GetVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, err
	}
	return provider.OK(versionMap(nr, loc, kr, key, v)), nil
}

func (p *Provider) CryptoKeyVersionDestroy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	v, err := p.core.DestroyVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, err
	}
	return provider.OK(versionMap(nr, loc, kr, key, v)), nil
}

func (p *Provider) CryptoKeyVersionRestore(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	v, err := p.core.RestoreVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, err
	}
	return provider.OK(versionMap(nr, loc, kr, key, v)), nil
}

func (p *Provider) CryptoKeyVersionDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	if err := p.core.DeleteVersion(ctx, nr.AccountID, loc, kr, key, version); err != nil {
		return nil, err
	}
	return provider.OK(doneOperation(kmscore.VersionName(nr.AccountID, loc, kr, key, version))), nil
}

// doneOperation renders the google.longrunning.Operation Cloud KMS returns for
// a delete: completed inline with an empty response (the gRPC adapter returns
// the same shape).
func doneOperation(resourceName string) map[string]any {
	return map[string]any{
		"name":     resourceName + "/operations/delete",
		"done":     true,
		"response": map[string]any{},
	}
}

func (p *Provider) CryptoKeyDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKey(name)
	if _, err := p.core.DeleteCryptoKey(ctx, nr.AccountID, loc, kr, key); err != nil {
		return nil, err
	}
	return provider.OK(doneOperation(kmscore.CryptoKeyName(nr.AccountID, loc, kr, key))), nil
}

func (p *Provider) CryptoKeyVersionUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	state, err := versionStateFromPatch(nr)
	if err != nil {
		return nil, err
	}
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	v, err := p.core.UpdateVersionState(ctx, nr.AccountID, loc, kr, key, version, state)
	if err != nil {
		return nil, err
	}
	return provider.OK(versionMap(nr, loc, kr, key, v)), nil
}

func versionStateFromPatch(nr *model.NormalizedRequest) (string, error) {
	if mask, _ := nr.Params["updateMask"].(string); mask != "" && mask != "state" {
		return "", model.NewProviderError("InvalidArgument",
			"unsupported update_mask path "+mask+"; cryptoKeyVersions.patch only supports state", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	state, _ := body["state"].(string)
	if state == "" {
		return "", model.NewProviderError("InvalidArgument", "cryptoKeyVersion.state is required", 400)
	}
	switch state {
	case "ENABLED", "DISABLED":
		return state, nil
	default:
		return "", model.NewProviderError("InvalidArgument",
			"cryptoKeyVersion.state must be ENABLED or DISABLED, got "+state, 400)
	}
}

func (p *Provider) CryptoKeyUpdatePrimaryVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKey(name)
	body, _ := nr.Params["body"].(map[string]any)
	versionID, _ := body["cryptoKeyVersionId"].(string)
	if versionID == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing cryptoKeyVersionId", 400)
	}
	ck, err := p.core.SetPrimaryVersion(ctx, nr.AccountID, loc, kr, key, versionID)
	if err != nil {
		return nil, err
	}
	return provider.OK(cryptoKeyMap(nr, ck, p.core.PrimaryVersion(ctx, nr.AccountID, ck))), nil
}

// ─── Crypto operations ────────────────────────────────────────────────────────

func (p *Provider) CryptoKeyVersionAsymmetricSign(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	body, _ := nr.Params["body"].(map[string]any)
	digestStr := ""
	if d, ok := body["digest"].(map[string]any); ok {
		for _, k := range []string{"sha256", "sha384", "sha512"} {
			if s, _ := d[k].(string); s != "" {
				digestStr = s
				break
			}
		}
	} else if s, _ := body["digest"].(string); s != "" {
		digestStr = s
	}
	digest, err := base64.StdEncoding.DecodeString(digestStr)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "digest must be base64", 400)
	}
	if err := verifyCRCField(body, "digestCrc32c", digest); err != nil {
		return nil, err
	}
	res, err := p.core.AsymmetricSign(ctx, nr.AccountID, loc, kr, key, version, digest)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"name":                 kmscore.VersionName(nr.AccountID, loc, kr, key, version),
		"signature":            base64.StdEncoding.EncodeToString(res.Signature),
		"signatureCrc32c":      crc32cString(res.Signature),
		"verifiedDigestCrc32c": strField(body, "digestCrc32c") != "",
		"protectionLevel":      "SOFTWARE",
	}), nil
}

func (p *Provider) CryptoKeyVersionAsymmetricDecrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	body, _ := nr.Params["body"].(map[string]any)
	ctStr, _ := body["ciphertext"].(string)
	if ctStr == "" {
		return nil, model.NewProviderError("InvalidRequest", "ciphertext is required", 400)
	}
	ct, err := base64.StdEncoding.DecodeString(ctStr)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "ciphertext must be base64", 400)
	}
	if err := verifyCRCField(body, "ciphertextCrc32c", ct); err != nil {
		return nil, err
	}
	pt, err := p.core.AsymmetricDecrypt(ctx, nr.AccountID, loc, kr, key, version, ct)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"plaintext":                base64.StdEncoding.EncodeToString(pt),
		"plaintextCrc32c":          crc32cString(pt),
		"verifiedCiphertextCrc32c": strField(body, "ciphertextCrc32c") != "",
		"protectionLevel":          "SOFTWARE",
	}), nil
}

func (p *Provider) CryptoKeyVersionMacSign(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	body, _ := nr.Params["body"].(map[string]any)
	dataStr, _ := body["data"].(string)
	if dataStr == "" {
		return nil, model.NewProviderError("InvalidRequest", "data is required", 400)
	}
	data, err := base64.StdEncoding.DecodeString(dataStr)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "data must be base64", 400)
	}
	if err := verifyCRCField(body, "dataCrc32c", data); err != nil {
		return nil, err
	}
	res, err := p.core.MacSign(ctx, nr.AccountID, loc, kr, key, version, data)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"name":               kmscore.VersionName(nr.AccountID, loc, kr, key, version),
		"mac":                base64.StdEncoding.EncodeToString(res.Mac),
		"macCrc32c":          crc32cString(res.Mac),
		"verifiedDataCrc32c": strField(body, "dataCrc32c") != "",
		"protectionLevel":    "SOFTWARE",
	}), nil
}

func (p *Provider) CryptoKeyVersionMacVerify(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	body, _ := nr.Params["body"].(map[string]any)
	dataStr, _ := body["data"].(string)
	if dataStr == "" {
		return nil, model.NewProviderError("InvalidRequest", "data is required", 400)
	}
	data, err := base64.StdEncoding.DecodeString(dataStr)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "data must be base64", 400)
	}
	macStr, _ := body["mac"].(string)
	if macStr == "" {
		return nil, model.NewProviderError("InvalidRequest", "mac is required", 400)
	}
	mac, err := base64.StdEncoding.DecodeString(macStr)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "mac must be base64", 400)
	}
	if err := verifyCRCField(body, "dataCrc32c", data); err != nil {
		return nil, err
	}
	if err := verifyCRCField(body, "macCrc32c", mac); err != nil {
		return nil, err
	}
	success, err := p.core.MacVerify(ctx, nr.AccountID, loc, kr, key, version, data, mac)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"success":            success,
		"verifiedDataCrc32c": strField(body, "dataCrc32c") != "",
		"verifiedMacCrc32c":  strField(body, "macCrc32c") != "",
		"protectionLevel":    "SOFTWARE",
	}), nil
}

func (p *Provider) CryptoKeyVersionGetPublicKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	name = strings.TrimSuffix(name, "/publicKey")
	loc, kr, key, version := parseVersion(name)
	res, err := p.core.GetPublicKey(ctx, nr.AccountID, loc, kr, key, version, strField(nr.Params, "publicKeyFormat"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{
		"algorithm":       res.Algorithm,
		"name":            kmscore.PublicKeyName(nr.AccountID, loc, kr, key, version),
		"protectionLevel": "SOFTWARE",
		"publicKeyFormat": res.Format,
	}
	switch res.Format {
	case "NIST_PQC", "XWING_RAW_BYTES", "DER":
		out["publicKey"] = checksummedData(res.Raw)
	default:
		out["pem"] = res.PEM
		out["pemCrc32c"] = crc32cString([]byte(res.PEM))
	}
	return provider.OK(out), nil
}

// checksummedData renders Discovery's ChecksummedData for a raw byte public
// key: the bytes base64-encoded plus their CRC32C, the same shape the gRPC
// adapter returns (PublicKey.publicKey is a ChecksummedData, not a bare string).
func checksummedData(b []byte) map[string]any {
	return map[string]any{
		"data":           base64.StdEncoding.EncodeToString(b),
		"crc32cChecksum": crc32cString(b),
	}
}

// ─── Import / trusted wrapping / KEM ──────────────────────────────────────────

func (p *Provider) CryptoKeyVersionImport(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseImportParent(name)
	if key == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing cryptoKey parent", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	wrapped := decodeBase64Field(body, "wrappedKey")
	if len(wrapped) == 0 {
		wrapped = decodeBase64Field(body, "rsaAesWrappedKey")
	}
	if len(wrapped) == 0 {
		return nil, model.NewProviderError("InvalidRequest", "wrappedKey is required", 400)
	}
	alg, _ := body["algorithm"].(string)
	job, _ := body["importJob"].(string)
	v, err := p.core.ImportCryptoKeyVersion(ctx, nr.AccountID, loc, kr, key, kmscore.ImportVersionRequest{
		ImportJob:              job,
		Algorithm:              alg,
		WrappedKey:             wrapped,
		CryptoKeyVersion:       strField(body, "cryptoKeyVersion"),
		TrustedWrappingEnabled: boolField(body, "trustedWrappingEnabled"),
	})
	if err != nil {
		return nil, err
	}
	return provider.OK(versionMap(nr, loc, kr, key, v)), nil
}

func (p *Provider) CryptoKeyVersionImportTrusted(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseImportParent(name)
	if key == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing cryptoKey parent", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	wrapped := decodeBase64Field(body, "wrappedKey")
	if len(wrapped) == 0 {
		return nil, model.NewProviderError("InvalidRequest", "wrappedKey is required", 400)
	}
	v, err := p.core.ImportTrustedKeyWrappedCryptoKeyVersion(ctx, nr.AccountID, loc, kr, key, kmscore.ImportVersionRequest{
		ImportingKey:     strField(body, "importingKey"),
		Algorithm:        strField(body, "algorithm"),
		WrappedKey:       wrapped,
		CryptoKeyVersion: strField(body, "cryptoKeyVersion"),
	})
	if err != nil {
		return nil, err
	}
	return provider.OK(versionMap(nr, loc, kr, key, v)), nil
}

func (p *Provider) CryptoKeyVersionExportTrusted(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	wrappingKey, _ := nr.Params["wrappingKey"].(string)
	wrapped, err := p.core.ExportTrustedKeyWrappedCryptoKeyVersion(ctx, nr.AccountID, loc, kr, key, version, wrappingKey)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"wrappedKey":       base64.StdEncoding.EncodeToString(wrapped),
		"wrappedKeyCrc32c": crc32cString(wrapped),
	}), nil
}

func (p *Provider) CryptoKeyVersionDecapsulate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	body, _ := nr.Params["body"].(map[string]any)
	ct := decodeBase64Field(body, "ciphertext")
	if len(ct) == 0 {
		return nil, model.NewProviderError("InvalidRequest", "ciphertext is required", 400)
	}
	if err := verifyCRCField(body, "ciphertextCrc32c", ct); err != nil {
		return nil, err
	}
	ss, err := p.core.Decapsulate(ctx, nr.AccountID, loc, kr, key, version, ct)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"name":                     kmscore.VersionName(nr.AccountID, loc, kr, key, version),
		"sharedSecret":             base64.StdEncoding.EncodeToString(ss),
		"sharedSecretCrc32c":       crc32cString(ss),
		"verifiedCiphertextCrc32c": strField(body, "ciphertextCrc32c") != "",
		"protectionLevel":          "SOFTWARE",
	}), nil
}

// ─── ImportJobs ───────────────────────────────────────────────────────────────

func (p *Provider) ImportJobCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr := parseKeyRing(name)
	if kr == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing keyRing parent", 400)
	}
	id, _ := nr.Params["importJobId"].(string)
	body, _ := nr.Params["body"].(map[string]any)
	if id == "" {
		if n, _ := body["name"].(string); n != "" {
			_, _, _, id, _ = kmscore.SplitImportJobName(n)
		}
	}
	if id == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing importJobId", 400)
	}
	in := kmscore.ImportJobInput{}
	if body != nil {
		in.ImportMethod = strField(body, "importMethod")
		in.ProtectionLevel = strField(body, "protectionLevel")
	}
	ij, err := p.core.CreateImportJob(ctx, nr.AccountID, loc, kr, id, in)
	if err != nil {
		return nil, err
	}
	return provider.OK(importJobMap(ij)), nil
}

func (p *Provider) ImportJobGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, id := parseImportJob(name)
	if id == "" {
		return nil, model.NewProviderError("InvalidRequest", "invalid resource name", 400)
	}
	ij, err := p.core.GetImportJob(ctx, nr.AccountID, loc, kr, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(importJobMap(ij)), nil
}

func (p *Provider) ImportJobList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr := parseKeyRing(strings.TrimSuffix(name, "/importJobs"))
	query, err := listQueryFromParams(nr.Params)
	if err != nil {
		return nil, err
	}
	page, next, total, err := p.core.PageImportJobs(ctx, nr.AccountID, loc, kr, query)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, ij := range page {
		items = append(items, importJobMap(ij))
	}
	resp := map[string]any{"importJobs": items, "totalSize": total}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

// ─── IAM ──────────────────────────────────────────────────────────────────────

func (p *Provider) GetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	pol, err := p.core.GetIamPolicy(ctx, nr.AccountID, name)
	if err != nil {
		return nil, err
	}
	return provider.OK(iamPolicyMap(pol)), nil
}

func (p *Provider) SetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	pol, err := p.core.SetIamPolicy(ctx, nr.AccountID, name, body)
	if err != nil {
		return nil, err
	}
	return provider.OK(iamPolicyMap(pol)), nil
}

func (p *Provider) TestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	perms, err := p.core.TestIamPermissions(ctx, nr.AccountID, name, permissionsField(body))
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"permissions": perms}), nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func crc32cString(b []byte) string {
	return strconv.FormatUint(uint64(crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli))), 10)
}

// verifyCRCField enforces a request's optional CRC32C integrity field: an
// absent/empty field is ignored, a present one must parse as the int64 CRC of
// data or the request is rejected (mirrors the gRPC transport's verifyCRC).
func verifyCRCField(body map[string]any, field string, data []byte) error {
	s := strField(body, field)
	if s == "" {
		return nil
	}
	want, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return model.NewProviderError("InvalidRequest", field+" must be an int64 checksum", 400)
	}
	got := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	if uint64(want) != uint64(got) {
		return model.NewProviderError("InvalidRequest", field+" mismatch", 400)
	}
	return nil
}

// intField reads an integer body field, tolerating the JSON number forms
// encoding/json produces for an int32/int64.
func intField(body map[string]any, field string) int {
	switch v := body[field].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	case string:
		n, _ := strconv.Atoi(v)
		return n
	}
	return 0
}

func decodeAAD(v any) []byte {
	return decodeBase64Field(map[string]any{"v": v}, "v")
}

func decodeBase64Field(body map[string]any, field string) []byte {
	s, _ := body[field].(string)
	if s == "" {
		return nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil
	}
	return b
}

func strField(body map[string]any, field string) string {
	s, _ := body[field].(string)
	return s
}

func boolField(body map[string]any, field string) bool {
	b, _ := body[field].(bool)
	return b
}

func permissionsField(body map[string]any) []string {
	items, _ := body["permissions"].([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func parseLabels(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok || len(m) == 0 {
		return nil
	}
	labels := make(map[string]string, len(m))
	for k, val := range m {
		if s, ok := val.(string); ok {
			labels[k] = s
		}
	}
	return labels
}

func parseRotationPeriod(v any) (time.Duration, bool, error) {
	s, _ := v.(string)
	if s == "" {
		return 0, false, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, false, model.NewProviderError("InvalidRequest", "rotationPeriod must be a duration string (e.g. \"86400s\")", 400)
	}
	if d <= 0 {
		return 0, false, model.NewProviderError("InvalidRequest", "rotationPeriod must be positive", 400)
	}
	return d, true, nil
}

// parseTimestamp parses a google-datetime/RFC 3339 body field (nextRotationTime).
func parseTimestamp(v any) (time.Time, bool, error) {
	s, _ := v.(string)
	if s == "" {
		return time.Time{}, false, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, false, model.NewProviderError("InvalidRequest", "nextRotationTime must be an RFC 3339 timestamp (e.g. \"2026-01-01T00:00:00Z\")", 400)
	}
	return t, true, nil
}

func durationString(d time.Duration) string {
	return strconv.FormatInt(int64(d/time.Second), 10) + "s"
}

// listQueryFromParams reads the documented filter/orderBy/pageSize/pageToken
// query parameters into the transport-neutral list query. pageSize is parsed as
// an int32 by the shared resolver: a non-integer value is INVALID_ARGUMENT, as
// real GCP's query binding rejects it, rather than being folded into zero and
// silently treated as absent.
func listQueryFromParams(params map[string]any) (kmscore.ListQuery, error) {
	filter, _ := params["filter"].(string)
	orderBy, _ := params["orderBy"].(string)
	pageSize, err := paging.ParseSize(params["pageSize"])
	if err != nil {
		return kmscore.ListQuery{}, err
	}
	pageToken, _ := params["pageToken"].(string)
	return kmscore.ListQuery{Filter: filter, OrderBy: orderBy, PageSize: pageSize, PageToken: pageToken}, nil
}

// parseKeyRing parses a name's location/keyRing pair, tolerating a full
// "projects/{p}/..." prefix.
func parseKeyRing(name string) (loc, kr string) {
	if _, l, r, ok := kmscore.SplitKeyRingName(name); ok {
		return l, r
	}
	name = strings.TrimPrefix(name, "/")
	name = strings.TrimPrefix(name, "locations/")
	parts := strings.Split(name, "/")
	if len(parts) >= 3 && parts[1] == "keyRings" {
		return parts[0], parts[2]
	}
	return "", ""
}

func parseCryptoKey(name string) (loc, kr, key string) {
	if _, l, r, k, ok := kmscore.SplitCryptoKeyName(name); ok {
		return l, r, k
	}
	name = strings.TrimPrefix(name, "/")
	name = strings.TrimPrefix(name, "locations/")
	parts := strings.Split(name, "/")
	if len(parts) >= 5 && parts[1] == "keyRings" && parts[3] == "cryptoKeys" {
		return parts[0], parts[2], parts[4]
	}
	return "", "", ""
}

func parseVersion(name string) (loc, kr, key, version string) {
	if _, l, r, k, v, ok := kmscore.SplitVersionName(name); ok {
		return l, r, k, v
	}
	name = strings.TrimPrefix(name, "/")
	name = strings.TrimPrefix(name, "locations/")
	parts := strings.Split(name, "/")
	if len(parts) >= 7 && parts[1] == "keyRings" && parts[3] == "cryptoKeys" && parts[5] == "cryptoKeyVersions" {
		return parts[0], parts[2], parts[4], parts[6]
	}
	return "", "", "", ""
}

func parseCryptoKeyFromParent(name string) (loc, kr, key string) {
	return parseCryptoKey(strings.TrimSuffix(name, "/cryptoKeyVersions"))
}

// parseImportJob splits an import-job name that may or may not carry a
// projects/{p} prefix into location/keyRing/job. The JSON codec strips the
// project segment, so the project-relative form
// (locations/{l}/keyRings/{kr}/importJobs/{id}) is what a client's GET produces.
func parseImportJob(name string) (loc, kr, id string) {
	if _, l, r, j, ok := kmscore.SplitImportJobName(name); ok {
		return l, r, j
	}
	name = strings.TrimPrefix(strings.TrimPrefix(name, "/"), "locations/")
	parts := strings.Split(name, "/")
	if len(parts) >= 5 && parts[1] == "keyRings" && parts[3] == "importJobs" {
		return parts[0], parts[2], parts[4]
	}
	return "", "", ""
}

// parseImportParent parses a cryptoKey parent from a ":import"-style call, whose
// parent may be ".../cryptoKeys/{key}/cryptoKeyVersions".
func parseImportParent(name string) (loc, kr, key string) {
	name = strings.TrimSuffix(name, ":import")
	name = strings.TrimSuffix(name, ":importTrustedKeyWrappedCryptoKeyVersion")
	name = strings.TrimSuffix(name, "/cryptoKeyVersions")
	return parseCryptoKey(name)
}
