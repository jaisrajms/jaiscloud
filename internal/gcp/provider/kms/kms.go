// Package kms implements the Google Cloud KMS provider.
package kms

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// Policy resource types for KMS IAM (key rings and crypto keys).
const (
	rtKeyRingPolicy   = "gcp_keyring_policy"
	rtCryptoKeyPolicy = "gcp_cryptokey_policy"
)

// Provider handles Cloud KMS key rings, crypto keys, and crypto-key versions.
type Provider struct {
	keys      kmsstore.Store
	resources store.ResourceStore // IAM policies (control-plane)
}

func New(keys kmsstore.Store, resources store.ResourceStore) *Provider {
	return &Provider{keys: keys, resources: resources}
}

// authorizeCryptoKey enforces the cryptoKey's IAM policy for one crypto-use
// permission. It is default-permissive: a key with no policy (or no bindings)
// allows every operation, so existing callers are unaffected; only a policy
// scoped to roles that omit permission denies. Mirrors the AWS KMS emulator's
// resource-policy hook (internal/aws/key/provider.go checkKeyPolicy).
func (p *Provider) authorizeCryptoKey(ctx context.Context, nr *model.NormalizedRequest, loc, kr, key, permission string) error {
	return policy.AuthorizeKMS(ctx, p.resources, nr.AccountID, rtCryptoKeyPolicy,
		loc+"/"+kr+"/"+key, permission, cryptoKeyName(nr, loc, kr, key))
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"KMS.KeyRingCreate":                     p.KeyRingCreate,
		"KMS.KeyRingList":                       p.KeyRingList,
		"KMS.KeyRingGet":                        p.KeyRingGet,
		"KMS.CryptoKeyCreate":                   p.CryptoKeyCreate,
		"KMS.CryptoKeyList":                     p.CryptoKeyList,
		"KMS.CryptoKeyGet":                      p.CryptoKeyGet,
		"KMS.CryptoKeyEncrypt":                  p.CryptoKeyEncrypt,
		"KMS.CryptoKeyDecrypt":                  p.CryptoKeyDecrypt,
		"KMS.CryptoKeyVersionCreate":            p.CryptoKeyVersionCreate,
		"KMS.CryptoKeyVersionList":              p.CryptoKeyVersionList,
		"KMS.CryptoKeyVersionGet":               p.CryptoKeyVersionGet,
		"KMS.CryptoKeyVersionUpdate":            p.CryptoKeyVersionUpdate,
		"KMS.CryptoKeyVersionDestroy":           p.CryptoKeyVersionDestroy,
		"KMS.CryptoKeyUpdatePrimaryVersion":     p.CryptoKeyUpdatePrimaryVersion,
		"KMS.CryptoKeyVersionAsymmetricSign":    p.CryptoKeyVersionAsymmetricSign,
		"KMS.CryptoKeyVersionAsymmetricDecrypt": p.CryptoKeyVersionAsymmetricDecrypt,
		"KMS.CryptoKeyVersionMacSign":           p.CryptoKeyVersionMacSign,
		"KMS.CryptoKeyVersionMacVerify":         p.CryptoKeyVersionMacVerify,
		"KMS.CryptoKeyVersionGetPublicKey":      p.CryptoKeyVersionGetPublicKey,
		"KMS.KeyRingGetIamPolicy":               p.GetIamPolicy,
		"KMS.KeyRingSetIamPolicy":               p.SetIamPolicy,
		"KMS.KeyRingTestIamPermissions":         p.TestIamPermissions,
		"KMS.CryptoKeyGetIamPolicy":             p.GetIamPolicy,
		"KMS.CryptoKeySetIamPolicy":             p.SetIamPolicy,
		"KMS.CryptoKeyTestIamPermissions":       p.TestIamPermissions,
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

// parseKeyRing splits "locations/{loc}/keyRings/{kr}" into loc and keyring ID.
func parseKeyRing(name string) (loc, kr string) {
	name = strings.TrimPrefix(name, "locations/")
	parts := strings.Split(name, "/") // [loc, keyRings, kr]
	if len(parts) >= 3 {
		return parts[0], parts[2]
	}
	return "", ""
}

// parseCryptoKey splits "locations/{loc}/keyRings/{kr}/cryptoKeys/{key}" into
// loc, keyring ID, and key ID.
func parseCryptoKey(name string) (loc, kr, key string) {
	name = strings.TrimPrefix(name, "locations/")
	parts := strings.Split(name, "/") // [loc, keyRings, kr, cryptoKeys, key]
	if len(parts) >= 5 {
		return parts[0], parts[2], parts[4]
	}
	return "", "", ""
}

// parseVersion splits "locations/{loc}/keyRings/{kr}/cryptoKeys/{key}/cryptoKeyVersions/{v}".
func parseVersion(name string) (loc, kr, key, version string) {
	name = strings.TrimPrefix(name, "locations/")
	parts := strings.Split(name, "/") // [loc, keyRings, kr, cryptoKeys, key, cryptoKeyVersions, v]
	if len(parts) >= 7 {
		return parts[0], parts[2], parts[4], parts[6]
	}
	return "", "", "", ""
}

// parseCryptoKeyFromParent parses a version-collection parent name
// (".../cryptoKeys/{key}/cryptoKeyVersions") into its crypto key.
func parseCryptoKeyFromParent(name string) (loc, kr, key string) {
	return parseCryptoKey(strings.TrimSuffix(name, "/cryptoKeyVersions"))
}

func keyRingName(nr *model.NormalizedRequest, loc, kr string) string {
	return nr.ResourceID("kms-keyring", loc+"/"+kr)
}

func cryptoKeyName(nr *model.NormalizedRequest, loc, kr, key string) string {
	return nr.ResourceID("kms-cryptokey", loc+"/"+kr+"/"+key)
}

func cryptoKeyVersionName(nr *model.NormalizedRequest, loc, kr, key, version string) string {
	return nr.ResourceID("kms-cryptokey-version", loc+"/"+kr+"/"+key+"/"+version)
}

// cryptoKeyMap renders a CryptoKey as its GCP response object. primary is the
// crypto key's primary version (see primaryVersion); its create time feeds the
// embedded primary's createTime/generateTime (GCP reports both as the version's
// generation time) and its destruction timestamps are carried through when the
// primary is DESTROY_SCHEDULED/DESTROYED.
func cryptoKeyMap(nr *model.NormalizedRequest, k kmsstore.CryptoKey, primaryVersion kmsstore.Version) map[string]any {
	primaryState := primaryVersion.State
	if primaryState == "" {
		primaryState = "ENABLED"
	}
	primaryCreateTime := primaryVersion.CreateTime
	if primaryCreateTime.IsZero() {
		primaryCreateTime = k.CreateTime
	}
	primary := map[string]any{
		"name":      cryptoKeyVersionName(nr, k.Location, k.KeyRingID, k.ID, k.PrimaryVersion),
		"state":     primaryState,
		"algorithm": k.Algorithm,
	}
	if !primaryCreateTime.IsZero() {
		ts := primaryCreateTime.UTC().Format(time.RFC3339Nano)
		primary["createTime"] = ts
		primary["generateTime"] = ts
	}
	if primaryVersion.State == "DESTROY_SCHEDULED" && !primaryVersion.DestroyTime.IsZero() {
		primary["destroyTime"] = primaryVersion.DestroyTime.UTC().Format(time.RFC3339Nano)
	}
	if primaryVersion.State == "DESTROYED" && !primaryVersion.DestroyEventTime.IsZero() {
		primary["destroyEventTime"] = primaryVersion.DestroyEventTime.UTC().Format(time.RFC3339Nano)
	}
	out := map[string]any{
		"name":       cryptoKeyName(nr, k.Location, k.KeyRingID, k.ID),
		"purpose":    k.Purpose,
		"createTime": k.CreateTime.UTC().Format(time.RFC3339Nano),
		"primary":    primary,
		"versionTemplate": map[string]any{
			"algorithm":       k.Algorithm,
			"protectionLevel": "SOFTWARE",
		},
	}
	if len(k.Labels) > 0 {
		out["labels"] = k.Labels
	}
	if k.RotationPeriod > 0 {
		out["rotationPeriod"] = durationString(k.RotationPeriod)
	}
	if !k.NextRotationTime.IsZero() {
		out["nextRotationTime"] = k.NextRotationTime.UTC().Format(time.RFC3339Nano)
	}
	return out
}

// durationString formats a duration the way GCP's JSON mapping encodes a
// google.protobuf.Duration: a decimal seconds value with an "s" suffix
// (e.g. 86400s).
func durationString(d time.Duration) string {
	return strconv.FormatInt(int64(d/time.Second), 10) + "s"
}

// parseRotationPeriod parses a GCP JSON duration string (e.g. "86400s").
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

// parseLabels converts a decoded JSON labels object into a string map.
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

// versionMap renders a CryptoKeyVersion as its GCP response object. destroyTime
// is emitted only while DESTROY_SCHEDULED and destroyEventTime only once
// DESTROYED (both output-only in Cloud KMS).
func versionMap(nr *model.NormalizedRequest, loc, kr, key string, v kmsstore.Version) map[string]any {
	out := map[string]any{
		"name":       cryptoKeyVersionName(nr, loc, kr, key, v.Version),
		"state":      v.State,
		"algorithm":  v.Algorithm,
		"createTime": v.CreateTime.UTC().Format(time.RFC3339Nano),
	}
	if v.State == "DESTROY_SCHEDULED" && !v.DestroyTime.IsZero() {
		out["destroyTime"] = v.DestroyTime.UTC().Format(time.RFC3339Nano)
	}
	if v.State == "DESTROYED" && !v.DestroyEventTime.IsZero() {
		out["destroyEventTime"] = v.DestroyEventTime.UTC().Format(time.RFC3339Nano)
	}
	return out
}

// versionPageKey renders a version number zero-padded to a fixed width so
// paging.Page sorts and cursors numerically (1, 2, ..., 12) instead of
// lexicographically (1, 10, 11, 12, 2, ...). Real KMS orders crypto key
// versions numerically by version number.
func versionPageKey(v kmsstore.Version) string {
	n, err := strconv.ParseInt(v.Version, 10, 64)
	if err != nil {
		return v.Version
	}
	return fmt.Sprintf("%020d", n)
}

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
	now := clock.Now()
	if err := p.keys.CreateKeyRing(ctx, nr.AccountID, loc, kr, kmsstore.KeyRing{
		Location: loc, ID: kr, CreateTime: now,
	}); err != nil {
		if errors.Is(err, kmsstore.ErrAlreadyExists) {
			return nil, model.NewProviderError("AlreadyExists", "key ring already exists", 409)
		}
		return nil, err
	}
	name := keyRingName(nr, loc, kr)
	return provider.OK(map[string]any{"name": name, "createTime": now.UTC().Format(time.RFC3339Nano)}), nil
}

func (p *Provider) KeyRingList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	loc, _ := nr.Params["location"].(string)
	krs, err := p.keys.ListKeyRings(ctx, nr.AccountID, loc)
	if err != nil {
		return nil, err
	}
	page, next := paging.Page(krs, func(kr kmsstore.KeyRing) string { return kr.ID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, kr := range page {
		items = append(items, map[string]any{
			"name":       keyRingName(nr, kr.Location, kr.ID),
			"createTime": kr.CreateTime.UTC().Format(time.RFC3339Nano),
		})
	}
	resp := map[string]any{"keyRings": items, "totalSize": len(krs)}
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
	krMeta, err := p.keys.GetKeyRing(ctx, nr.AccountID, loc, kr)
	if err != nil {
		if errors.Is(err, kmsstore.ErrNoSuchKeyRing) {
			return nil, model.NewProviderError("NotFound", "key ring not found", 404)
		}
		return nil, err
	}
	return provider.OK(map[string]any{
		"name":       keyRingName(nr, loc, krMeta.ID),
		"createTime": krMeta.CreateTime.UTC().Format(time.RFC3339Nano),
	}), nil
}

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
	purpose := "ENCRYPT_DECRYPT"
	algorithm := ""
	var labels map[string]string
	var rotationPeriod time.Duration
	if body, ok := nr.Params["body"].(map[string]any); ok {
		if purp, _ := body["purpose"].(string); purp != "" {
			purpose = purp
		}
		if vt, ok := body["versionTemplate"].(map[string]any); ok {
			if alg, _ := vt["algorithm"].(string); alg != "" {
				algorithm = alg
			}
		}
		labels = parseLabels(body["labels"])
		period, set, err := parseRotationPeriod(body["rotationPeriod"])
		if err != nil {
			return nil, err
		}
		if set {
			rotationPeriod = period
		}
	}
	if algorithm == "" {
		algorithm = defaultAlgorithmForPurpose(purpose)
	}
	now := clock.Now()
	ck := kmsstore.CryptoKey{Location: loc, KeyRingID: kr, ID: key, Purpose: purpose, CreateTime: now, PrimaryVersion: "1", Algorithm: algorithm, Labels: labels, RotationPeriod: rotationPeriod}
	if rotationPeriod > 0 {
		ck.NextRotationTime = now.Add(rotationPeriod)
	}
	if err := p.keys.CreateCryptoKey(ctx, nr.AccountID, loc, kr, key, ck); err != nil {
		if errors.Is(err, kmsstore.ErrAlreadyExists) {
			return nil, model.NewProviderError("AlreadyExists", "crypto key already exists", 409)
		}
		return nil, err
	}
	return provider.OK(cryptoKeyMap(nr, ck, kmsstore.Version{State: "ENABLED", Algorithm: ck.Algorithm, CreateTime: ck.CreateTime})), nil
}

func (p *Provider) CryptoKeyList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	// The collection request targets the key-ring parent
	// ("locations/{loc}/keyRings/{kr}/cryptoKeys"), which parseCryptoKey (five
	// segments) cannot split. Parse the key ring itself instead.
	loc, kr := parseKeyRing(strings.TrimSuffix(name, "/cryptoKeys"))
	if loc == "" || kr == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing location/keyRing", 400)
	}
	keys, err := p.keys.ListCryptoKeys(ctx, nr.AccountID, loc, kr)
	if err != nil {
		return nil, err
	}
	// Lazily execute any due rotation schedules before paging, then re-list so
	// the page reflects the rotated primaries.
	for _, k := range keys {
		kmsstore.RotateIfDue(ctx, p.keys, nr.AccountID, loc, kr, k.ID, clock.Now())
		p.promoteDestroyed(ctx, nr.AccountID, loc, kr, k.ID)
	}
	if keys, err = p.keys.ListCryptoKeys(ctx, nr.AccountID, loc, kr); err != nil {
		return nil, err
	}
	page, next := paging.Page(keys, func(k kmsstore.CryptoKey) string { return k.ID }, nr.Params)
	items := make([]any, 0, len(page))
	for _, k := range page {
		items = append(items, cryptoKeyMap(nr, k, p.primaryVersion(ctx, nr.AccountID, k)))
	}
	resp := map[string]any{"cryptoKeys": items, "totalSize": len(keys)}
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
	kmsstore.RotateIfDue(ctx, p.keys, nr.AccountID, loc, kr, key, clock.Now())
	p.promoteDestroyed(ctx, nr.AccountID, loc, kr, key)
	k, err := p.keys.GetCryptoKey(ctx, nr.AccountID, loc, kr, key)
	if err != nil {
		return nil, p.keyErr(err)
	}
	return provider.OK(cryptoKeyMap(nr, k, p.primaryVersion(ctx, nr.AccountID, k))), nil
}

func (p *Provider) CryptoKeyEncrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKey(name)
	if err := p.authorizeCryptoKey(ctx, nr, loc, kr, key, policy.PermCryptoKeyUseToEncrypt); err != nil {
		return nil, err
	}
	kmsstore.RotateIfDue(ctx, p.keys, nr.AccountID, loc, kr, key, clock.Now())
	ck, err := p.keys.GetCryptoKey(ctx, nr.AccountID, loc, kr, key)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if err := p.requireVersionEnabled(ctx, nr.AccountID, loc, kr, key, ck.PrimaryVersion); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	pt, err := base64.StdEncoding.DecodeString(body["plaintext"].(string))
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "plaintext must be base64", 400)
	}
	aad := decodeAAD(body["additionalAuthenticatedData"])
	keyMat, err := p.keys.KeyMaterial(ctx, nr.AccountID, loc, kr, key, ck.PrimaryVersion)
	if err != nil {
		return nil, p.versionErr(err)
	}
	ct, err := kmsstore.EncryptData(keyMat, pt, aad)
	if err != nil {
		return nil, model.NewProviderError("Internal", "encryption failed", 500)
	}
	blob := kmsstore.EncodeVersionedCiphertext(ck.PrimaryVersion, ct)
	return provider.OK(map[string]any{
		"name":                    cryptoKeyVersionName(nr, loc, kr, key, ck.PrimaryVersion),
		"ciphertext":              base64.StdEncoding.EncodeToString(blob),
		"ciphertextCrc32c":        crc32cString(blob),
		"protectionLevel":         "SOFTWARE",
		"verifiedPlaintextCrc32c": true,
		"verifiedAdditionalAuthenticatedDataCrc32c": len(aad) > 0,
	}), nil
}

func (p *Provider) CryptoKeyDecrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKey(name)
	if err := p.authorizeCryptoKey(ctx, nr, loc, kr, key, policy.PermCryptoKeyUseToDecrypt); err != nil {
		return nil, err
	}
	ck, err := p.keys.GetCryptoKey(ctx, nr.AccountID, loc, kr, key)
	if err != nil {
		return nil, p.keyErr(err)
	}
	if err := p.requireVersionEnabled(ctx, nr.AccountID, loc, kr, key, ck.PrimaryVersion); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	blob, err := base64.StdEncoding.DecodeString(body["ciphertext"].(string))
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "ciphertext must be base64", 400)
	}
	version, ct, err := kmsstore.DecodeVersionedCiphertext(blob)
	if err != nil {
		return nil, model.NewProviderError("InvalidCiphertext", "invalid ciphertext", 400)
	}
	aad := decodeAAD(body["additionalAuthenticatedData"])
	keyMat, err := p.keys.KeyMaterial(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	pt, err := kmsstore.DecryptData(keyMat, ct, aad)
	if err != nil {
		return nil, model.NewProviderError("InvalidCiphertext", "decryption failed", 400)
	}
	return provider.OK(map[string]any{
		"plaintext":       base64.StdEncoding.EncodeToString(pt),
		"plaintextCrc32c": crc32cString(pt),
		"protectionLevel": "SOFTWARE",
		"usedPrimary":     version == ck.PrimaryVersion,
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
	if err := p.requireCryptoKey(ctx, nr.AccountID, loc, kr, key); err != nil {
		return nil, err
	}
	now := clock.Now()
	ck, err := p.keys.GetCryptoKey(ctx, nr.AccountID, loc, kr, key)
	if err != nil {
		return nil, p.keyErr(err)
	}
	version, err := p.keys.CreateVersion(ctx, nr.AccountID, loc, kr, key, kmsstore.Version{CreateTime: now, Algorithm: ck.Algorithm})
	if err != nil {
		return nil, p.versionErr(err)
	}
	return provider.OK(versionMap(nr, loc, kr, key, kmsstore.Version{Version: version, State: "ENABLED", Algorithm: ck.Algorithm, CreateTime: now})), nil
}

func (p *Provider) CryptoKeyVersionList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key := parseCryptoKeyFromParent(name)
	p.promoteDestroyed(ctx, nr.AccountID, loc, kr, key)
	versions, err := p.keys.ListVersions(ctx, nr.AccountID, loc, kr, key)
	if err != nil {
		return nil, p.versionErr(err)
	}
	page, next := paging.Page(versions, versionPageKey, nr.Params)
	items := make([]any, 0, len(page))
	for _, v := range page {
		items = append(items, versionMap(nr, loc, kr, key, v))
	}
	resp := map[string]any{"cryptoKeyVersions": items, "totalSize": len(versions)}
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
	p.promoteDestroyed(ctx, nr.AccountID, loc, kr, key)
	v, err := p.keys.GetVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	return provider.OK(versionMap(nr, loc, kr, key, v)), nil
}

// CryptoKeyVersionDestroy schedules a version for destruction: it moves to
// DESTROY_SCHEDULED with a destroy_time one destroy_scheduled_duration (30 days
// by default) in the future. A version already DESTROY_SCHEDULED or DESTROYED is
// returned unchanged (idempotent, as in Cloud KMS).
func (p *Provider) CryptoKeyVersionDestroy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	// Apply any elapsed destroy window first, so a version already past its
	// destroy_time is reported (and kept) DESTROYED rather than stale
	// DESTROY_SCHEDULED.
	p.promoteDestroyed(ctx, nr.AccountID, loc, kr, key)
	v, err := p.keys.DestroyVersion(ctx, nr.AccountID, loc, kr, key, version, clock.Now().Add(kmsstore.DefaultDestroyScheduledDuration))
	if err != nil {
		return nil, p.versionErr(err)
	}
	return provider.OK(versionMap(nr, loc, kr, key, v)), nil
}

// CryptoKeyVersionUpdate implements cryptoKeyVersions.patch. Real KMS exposes
// no :disable/:enable custom methods; a version's state is changed here, and
// `state` is the only mutable field (ENABLED or DISABLED). The update_mask,
// when supplied, may name only `state`.
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
	// Apply any elapsed destroy window first, then reject a scheduled or
	// destroyed version: patch only moves between ENABLED and DISABLED (a
	// DESTROY_SCHEDULED version is restored, not patched).
	p.promoteDestroyed(ctx, nr.AccountID, loc, kr, key)
	cur, err := p.keys.GetVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	if cur.State == "DESTROY_SCHEDULED" || cur.State == "DESTROYED" {
		return nil, model.NewProviderError("FailedPrecondition",
			"CryptoKeyVersion is "+cur.State+"; use restore", 400)
	}
	if err := p.keys.UpdateVersionState(ctx, nr.AccountID, loc, kr, key, version, state); err != nil {
		return nil, p.versionErr(err)
	}
	v, _ := p.keys.GetVersion(ctx, nr.AccountID, loc, kr, key, version)
	return provider.OK(versionMap(nr, loc, kr, key, v)), nil
}

// versionStateFromPatch extracts and validates the `state` field of a
// cryptoKeyVersions.patch request body, enforcing that updateMask (if present)
// names only `state` and that the state is one KMS allows updating to.
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
	if err := p.keys.UpdatePrimaryVersion(ctx, nr.AccountID, loc, kr, key, versionID); err != nil {
		return nil, p.versionErr(err)
	}
	p.promoteDestroyed(ctx, nr.AccountID, loc, kr, key)
	ck, _ := p.keys.GetCryptoKey(ctx, nr.AccountID, loc, kr, key)
	return provider.OK(cryptoKeyMap(nr, ck, p.primaryVersion(ctx, nr.AccountID, ck))), nil
}

// defaultAlgorithmForPurpose maps a GCP KMS purpose to its default algorithm.
func defaultAlgorithmForPurpose(purpose string) string {
	switch purpose {
	case "ASYMMETRIC_SIGN":
		return "RSA_SIGN_PKCS1_2048_SHA256"
	case "ASYMMETRIC_DECRYPT":
		return "RSA_DECRYPT_OAEP_2048_SHA256"
	case "MAC":
		return "HMAC_SHA256"
	default:
		return "GOOGLE_SYMMETRIC_ENCRYPTION"
	}
}

func (p *Provider) CryptoKeyVersionAsymmetricSign(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	if err := p.authorizeCryptoKey(ctx, nr, loc, kr, key, policy.PermCryptoKeyUseToSign); err != nil {
		return nil, err
	}
	p.promoteDestroyed(ctx, nr.AccountID, loc, kr, key)
	v, err := p.keys.GetVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, versionNotEnabledErr(version, v.State)
	}
	body, _ := nr.Params["body"].(map[string]any)
	// GCP's AsymmetricSignRequest carries the digest as an object with one of
	// sha256/sha384/sha512; accept a bare base64 string too for backward
	// compatibility with earlier emulator callers.
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
	priv, err := p.keys.PrivateKey(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	var sig []byte
	switch {
	case strings.HasPrefix(v.Algorithm, "RSA_SIGN"):
		sig, err = kmsstore.RSASign(priv, digest, v.Algorithm)
	case strings.HasPrefix(v.Algorithm, "EC_SIGN"):
		sig, err = kmsstore.ECSign(priv, digest)
	default:
		return nil, model.NewProviderError("FailedPrecondition", "key is not for asymmetric signing", 400)
	}
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", "signing failed", 400)
	}
	return provider.OK(map[string]any{
		"name":                 cryptoKeyVersionName(nr, loc, kr, key, version),
		"signature":            base64.StdEncoding.EncodeToString(sig),
		"signatureCrc32c":      crc32cString(sig),
		"verifiedDigestCrc32c": true,
		"protectionLevel":      "SOFTWARE",
	}), nil
}

func (p *Provider) CryptoKeyVersionAsymmetricDecrypt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	if err := p.authorizeCryptoKey(ctx, nr, loc, kr, key, policy.PermCryptoKeyUseToDecrypt); err != nil {
		return nil, err
	}
	p.promoteDestroyed(ctx, nr.AccountID, loc, kr, key)
	v, err := p.keys.GetVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, versionNotEnabledErr(version, v.State)
	}
	body, _ := nr.Params["body"].(map[string]any)
	ct, err := base64.StdEncoding.DecodeString(body["ciphertext"].(string))
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "ciphertext must be base64", 400)
	}
	priv, err := p.keys.PrivateKey(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	if !strings.HasPrefix(v.Algorithm, "RSA_DECRYPT") {
		return nil, model.NewProviderError("FailedPrecondition", "key is not for asymmetric decryption", 400)
	}
	pt, err := kmsstore.RSADecryptOAEP(priv, ct, v.Algorithm)
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", "decryption failed", 400)
	}
	return provider.OK(map[string]any{
		"plaintext":                base64.StdEncoding.EncodeToString(pt),
		"plaintextCrc32c":          crc32cString(pt),
		"verifiedCiphertextCrc32c": true,
		"protectionLevel":          "SOFTWARE",
	}), nil
}

func (p *Provider) CryptoKeyVersionMacSign(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	if err := p.authorizeCryptoKey(ctx, nr, loc, kr, key, policy.PermCryptoKeyUseToSign); err != nil {
		return nil, err
	}
	p.promoteDestroyed(ctx, nr.AccountID, loc, kr, key)
	v, err := p.keys.GetVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, versionNotEnabledErr(version, v.State)
	}
	if !strings.HasPrefix(v.Algorithm, "HMAC_") {
		return nil, model.NewProviderError("FailedPrecondition", "key is not for MAC", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	data, err := base64.StdEncoding.DecodeString(body["data"].(string))
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "data must be base64", 400)
	}
	mat, err := p.keys.KeyMaterial(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	mac, err := kmsstore.HMACSign(mat, data, v.Algorithm)
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", "mac sign failed", 400)
	}
	return provider.OK(map[string]any{
		"name":               cryptoKeyVersionName(nr, loc, kr, key, version),
		"mac":                base64.StdEncoding.EncodeToString(mac),
		"macCrc32c":          crc32cString(mac),
		"verifiedDataCrc32c": true,
		"protectionLevel":    "SOFTWARE",
	}), nil
}

func (p *Provider) CryptoKeyVersionMacVerify(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	loc, kr, key, version := parseVersion(name)
	if err := p.authorizeCryptoKey(ctx, nr, loc, kr, key, policy.PermCryptoKeyUseToVerify); err != nil {
		return nil, err
	}
	p.promoteDestroyed(ctx, nr.AccountID, loc, kr, key)
	v, err := p.keys.GetVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, versionNotEnabledErr(version, v.State)
	}
	if !strings.HasPrefix(v.Algorithm, "HMAC_") {
		return nil, model.NewProviderError("FailedPrecondition", "key is not for MAC", 400)
	}
	body, _ := nr.Params["body"].(map[string]any)
	data, err := base64.StdEncoding.DecodeString(body["data"].(string))
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "data must be base64", 400)
	}
	mac, err := base64.StdEncoding.DecodeString(body["mac"].(string))
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "mac must be base64", 400)
	}
	mat, err := p.keys.KeyMaterial(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	return provider.OK(map[string]any{
		"success":            kmsstore.HMACVerify(mat, data, mac, v.Algorithm),
		"verifiedDataCrc32c": true,
		"verifiedMacCrc32c":  true,
		"protectionLevel":    "SOFTWARE",
	}), nil
}

func (p *Provider) CryptoKeyVersionGetPublicKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	// name may be ".../cryptoKeyVersions/{v}/publicKey" — strip the suffix.
	name = strings.TrimSuffix(name, "/publicKey")
	loc, kr, key, version := parseVersion(name)
	if err := p.authorizeCryptoKey(ctx, nr, loc, kr, key, policy.PermCryptoKeyViewPublicKey); err != nil {
		return nil, err
	}
	p.promoteDestroyed(ctx, nr.AccountID, loc, kr, key)
	v, err := p.keys.GetVersion(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	if v.State != "ENABLED" {
		return nil, versionNotEnabledErr(version, v.State)
	}
	pub, err := p.keys.PublicKey(ctx, nr.AccountID, loc, kr, key, version)
	if err != nil {
		return nil, p.versionErr(err)
	}
	pemStr, err := kmsstore.PublicKeyPEM(pub)
	if err != nil {
		return nil, model.NewProviderError("Internal", "public key encode failed", 500)
	}
	return provider.OK(map[string]any{
		"pem":             pemStr,
		"algorithm":       v.Algorithm,
		"pemCrc32c":       crc32cString([]byte(pemStr)),
		"name":            cryptoKeyVersionName(nr, loc, kr, key, version) + "/publicKey",
		"protectionLevel": "SOFTWARE",
	}), nil
}

// decodeAAD decodes the base64 additionalAuthenticatedData field (or empty).
func decodeAAD(v any) []byte {
	s, _ := v.(string)
	if s == "" {
		return nil
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil
	}
	return b
}

// ─── IAM (google.iam.v1.IAMPolicy over keyrings/keys) ─────────────────────────

// parseIamResource splits a relative KMS resource name into its policy resource
// type and location-qualified id. Real KMS scopes IAM to key rings and crypto
// keys (versions have no IAM), so a cryptoKeyVersions name is rejected. It must
// be tested before cryptoKeys because parseCryptoKey also matches a version's
// path segments.
func parseIamResource(name string) (policyType, id string, ok bool) {
	if _, _, _, version := parseVersion(name); version != "" {
		return "", "", false
	}
	if loc, kr, key := parseCryptoKey(name); loc != "" {
		return rtCryptoKeyPolicy, loc + "/" + kr + "/" + key, true
	}
	if loc, kr := parseKeyRing(name); loc != "" {
		return rtKeyRingPolicy, loc + "/" + kr, true
	}
	return "", "", false
}

// requireIamResource verifies the KMS resource backing an IAM request exists.
func (p *Provider) requireIamResource(ctx context.Context, accountID, name string) error {
	if _, _, _, version := parseVersion(name); version != "" {
		return model.NewProviderError("InvalidArgument", "invalid resource name", 400)
	}
	if loc, kr, key := parseCryptoKey(name); loc != "" {
		if _, err := p.keys.GetCryptoKey(ctx, accountID, loc, kr, key); err != nil {
			return p.keyErr(err)
		}
		return nil
	}
	if loc, kr := parseKeyRing(name); loc != "" {
		if _, err := p.keys.GetKeyRing(ctx, accountID, loc, kr); err != nil {
			if errors.Is(err, kmsstore.ErrNoSuchKeyRing) {
				return model.NewProviderError("NotFound", "key ring not found", 404)
			}
			return err
		}
		return nil
	}
	return model.NewProviderError("InvalidArgument", "invalid resource name", 400)
}

// iamPolicyMap renders a Policy the way Cloud KMS's REST API does: the etag is
// always present, while version and bindings are omitted when empty/default —
// an empty key-ring policy is just {"etag": "..."}.
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

// GetIamPolicy implements getIamPolicy for key rings, crypto keys and versions.
func (p *Provider) GetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	policyType, id, ok := parseIamResource(name)
	if !ok {
		return nil, model.NewProviderError("InvalidArgument", "invalid resource name", 400)
	}
	if err := p.requireIamResource(ctx, nr.AccountID, name); err != nil {
		return nil, err
	}
	return provider.OK(iamPolicyMap(policy.Load(ctx, p.resources, nr.AccountID, policyType, id))), nil
}

// SetIamPolicy implements setIamPolicy for key rings, crypto keys and versions.
func (p *Provider) SetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	policyType, id, ok := parseIamResource(name)
	if !ok {
		return nil, model.NewProviderError("InvalidArgument", "invalid resource name", 400)
	}
	if err := p.requireIamResource(ctx, nr.AccountID, name); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	pol, err := policy.Set(ctx, p.resources, nr.AccountID, policyType, id, body)
	if err != nil {
		return nil, err
	}
	return provider.OK(iamPolicyMap(pol)), nil
}

// TestIamPermissions implements testIamPermissions for key rings, crypto keys
// and versions.
func (p *Provider) TestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	if _, _, ok := parseIamResource(name); !ok {
		return nil, model.NewProviderError("InvalidArgument", "invalid resource name", 400)
	}
	if err := p.requireIamResource(ctx, nr.AccountID, name); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	return provider.OK(map[string]any{"permissions": policy.TestPermissions(policy.Permissions(body))}), nil
}

// promoteDestroyed lazily applies any elapsed destruction windows for a crypto
// key before its versions are observed. Cloud KMS transitions DESTROY_SCHEDULED
// to DESTROYED automatically; without a scheduler the emulator does it on
// access (mirrors RotateIfDue).
func (p *Provider) promoteDestroyed(ctx context.Context, accountID, loc, kr, key string) {
	kmsstore.PromoteDestroyedIfDue(ctx, p.keys, accountID, loc, kr, key, clock.Now())
}

// requireVersionEnabled rejects use of a crypto-key version that is not
// ENABLED (DISABLED/DESTROY_SCHEDULED/DESTROYED) with FailedPrecondition.
func (p *Provider) requireVersionEnabled(ctx context.Context, accountID, loc, kr, key, version string) error {
	p.promoteDestroyed(ctx, accountID, loc, kr, key)
	v, err := p.keys.GetVersion(ctx, accountID, loc, kr, key, version)
	if err != nil {
		return p.versionErr(err)
	}
	if v.State != "ENABLED" {
		return versionNotEnabledErr(version, v.State)
	}
	return nil
}

// primaryVersion returns a crypto key's primary version, used to render the
// embedded CryptoKey.primary. A missing/unknown primary falls back to a
// synthetic ENABLED version stamped with the key's own create time.
func (p *Provider) primaryVersion(ctx context.Context, accountID string, k kmsstore.CryptoKey) kmsstore.Version {
	fallback := kmsstore.Version{Version: k.PrimaryVersion, State: "ENABLED", Algorithm: k.Algorithm, CreateTime: k.CreateTime}
	if k.PrimaryVersion == "" {
		return fallback
	}
	v, err := p.keys.GetVersion(ctx, accountID, k.Location, k.KeyRingID, k.ID, k.PrimaryVersion)
	if err != nil || v.State == "" {
		return fallback
	}
	if v.CreateTime.IsZero() {
		v.CreateTime = k.CreateTime
	}
	return v
}

func versionNotEnabledErr(version, state string) error {
	return &model.ProviderError{
		Code: "FailedPrecondition", HTTPStatus: 400, Status: "FAILED_PRECONDITION",
		Message: "CryptoKeyVersion " + version + " is " + state,
	}
}

// keyErr maps crypto-key store errors to GCP provider errors.
func (p *Provider) keyErr(err error) error {
	if errors.Is(err, kmsstore.ErrNoSuchCryptoKey) {
		return model.NewProviderError("NotFound", "crypto key not found", 404)
	}
	return err
}

// versionErr maps version store errors to GCP provider errors.
func (p *Provider) versionErr(err error) error {
	switch {
	case errors.Is(err, kmsstore.ErrNoSuchCryptoKey):
		return model.NewProviderError("NotFound", "crypto key not found", 404)
	case errors.Is(err, kmsstore.ErrNoSuchVersion):
		return model.NewProviderError("NotFound", "crypto key version not found", 404)
	case errors.Is(err, kmsstore.ErrNotDestroyable):
		return model.NewProviderError("FailedPrecondition", "CryptoKeyVersion must be ENABLED or DISABLED to destroy", 400)
	case errors.Is(err, kmsstore.ErrNotRestorable):
		return model.NewProviderError("FailedPrecondition", "CryptoKeyVersion must be DESTROY_SCHEDULED to restore", 400)
	}
	return err
}

// requireCryptoKey verifies the crypto key exists, returning 404 otherwise.
func (p *Provider) requireCryptoKey(ctx context.Context, accountID, loc, kr, key string) error {
	if _, err := p.keys.GetCryptoKey(ctx, accountID, loc, kr, key); err != nil {
		return p.keyErr(err)
	}
	return nil
}

// crc32cString returns the CRC32C-Castagnoli checksum of b as a decimal string
// (google.protobuf.Int64Value encoding).
func crc32cString(b []byte) string {
	return strconv.FormatUint(uint64(crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli))), 10)
}
