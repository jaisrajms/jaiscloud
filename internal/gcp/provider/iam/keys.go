package iam

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/serviceaccount"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// rtServiceAccountKey is the generic ResourceStore type for service-account
// keys (RSA key pairs backing signBlob/signJwt). The key store is shared with
// the IAM Credentials (iamcredentials) core via internal/gcp/serviceaccount.
const rtServiceAccountKey = serviceaccount.ResourceType

// serviceAccountKeyMeta is the stored representation of a service-account key.
// It aliases the shared type so keys and signing are not duplicated between the
// iam and iamcredentials surfaces.
type serviceAccountKeyMeta = serviceaccount.Key

// parseKeyParent extracts the SA email from a ".../serviceAccounts/{email}/keys"
// collection name.
func parseKeyParent(name string) string {
	name = strings.TrimPrefix(name, "serviceAccounts/")
	return strings.TrimSuffix(name, "/keys")
}

// parseKeyName extracts (email, keyID) from ".../serviceAccounts/{email}/keys/{keyId}".
func parseKeyName(name string) (email, keyID string) {
	name = strings.TrimPrefix(name, "serviceAccounts/")
	if i := strings.LastIndex(name, "/keys/"); i >= 0 {
		return name[:i], name[i+len("/keys/"):]
	}
	return "", ""
}

// nonExpiringKeyTime is the validBeforeTime the API reports for a key that does
// not expire: user-managed keys never do, and the emulator's Google-managed
// key pair is not rotated either.
const nonExpiringKeyTime = "9999-12-31T23:59:59Z"

// keyBaseMap renders the ServiceAccountKey fields every method returns.
//
// The key id is not a top-level field of the Discovery ServiceAccountKey schema
// — it is the trailing segment of `name`, and that is the only place real GCP
// exposes it. The emulator must not invent a separate `keyId` field to match a
// sibling emulator's internal model.
//
// account is the resolved store account, which differs from the request's
// project on a "projects/-" wildcard request: the key name is always rendered
// under the account's real project.
func keyBaseMap(nr *model.NormalizedRequest, account string, m serviceAccountKeyMeta) map[string]any {
	out := map[string]any{
		"name":            saNameFormat(nr, account)("service-account", m.Email) + "/keys/" + m.KeyID,
		"keyAlgorithm":    m.Algorithm,
		"keyOrigin":       m.Origin(),
		"keyType":         m.Type(),
		"validAfterTime":  m.ValidAfter,
		"validBeforeTime": nonExpiringKeyTime,
		"disabled":        m.Disabled,
	}
	if m.DisableTime != "" {
		out["disableTime"] = m.DisableTime
	}
	return out
}

// keyListMap renders a ListServiceAccountKeysResponse item. Discovery scopes
// privateKeyType to keys.create and publicKeyData to keys.get, so list carries
// neither.
func keyListMap(nr *model.NormalizedRequest, account string, m serviceAccountKeyMeta) map[string]any {
	return keyBaseMap(nr, account, m)
}

// keyGetMap renders a keys.get resource: publicKeyData is populated there (and
// only there) from the stored private key's public half.
func keyGetMap(nr *model.NormalizedRequest, account string, m serviceAccountKeyMeta) map[string]any {
	out := keyBaseMap(nr, account, m)
	if pub, ok := m.PublicKeyData(); ok {
		out["publicKeyData"] = pub
	}
	return out
}

// keyCreateMap renders a keys.create resource: privateKeyType and
// privateKeyData are returned only by create (never by get or list).
func keyCreateMap(nr *model.NormalizedRequest, account string, m serviceAccountKeyMeta, privateKeyData string) map[string]any {
	out := keyBaseMap(nr, account, m)
	out["privateKeyType"] = "TYPE_GOOGLE_CREDENTIALS_FILE"
	out["privateKeyData"] = privateKeyData
	return out
}

// keyTypeFilter reads the documented repeated `keyTypes` query parameter
// (projects.serviceAccounts.keys.list). The shared query decode
// (adapter.queryToParams) keeps only the first value, so the repeated form the
// official clients send — gcloud's --managed-by and the Go client's
// urlParams_.SetMulti — is read from the raw request; Params is the fallback for
// callers without one (the console UI). Comma-joined values are accepted because
// Google's REST transcoding accepts both spellings.
//
// The Discovery document states that duplicate key types are not allowed and
// that KEY_TYPE_UNSPECIFIED "will immediately result in an error", so both (and
// any unknown value) are INVALID_ARGUMENT rather than being silently ignored.
func keyTypeFilter(nr *model.NormalizedRequest) ([]string, error) {
	var raw []string
	if nr.Raw != nil {
		raw = nr.Raw.URL.Query()["keyTypes"]
	}
	if len(raw) == 0 {
		if v, ok := nr.Params["keyTypes"].(string); ok && v != "" {
			raw = []string{v}
		}
	}
	var types []string
	seen := map[string]bool{}
	for _, v := range raw {
		for _, part := range strings.Split(v, ",") {
			t := strings.TrimSpace(part)
			if t == "" {
				continue
			}
			if t == "KEY_TYPE_UNSPECIFIED" {
				return nil, model.NewProviderError("InvalidArgument",
					"keyTypes must not be KEY_TYPE_UNSPECIFIED", 400)
			}
			if t != "USER_MANAGED" && t != "SYSTEM_MANAGED" {
				return nil, model.NewProviderError("InvalidArgument",
					"invalid keyTypes value "+strconv.Quote(t)+": must be USER_MANAGED or SYSTEM_MANAGED", 400)
			}
			if seen[t] {
				return nil, model.NewProviderError("InvalidArgument",
					"duplicate keyTypes value "+strconv.Quote(t)+": duplicate key types are not allowed", 400)
			}
			seen[t] = true
			types = append(types, t)
		}
	}
	return types, nil
}

// matchesKeyType reports whether a key's type passes the keyTypes filter.
func matchesKeyType(keyType string, filter []string) bool {
	if len(filter) == 0 {
		return true
	}
	for _, t := range filter {
		if t == keyType {
			return true
		}
	}
	return false
}

// createKey generates an RSA-2048 key pair and persists it.
func (p *Provider) createKey(ctx context.Context, account, email string) (*serviceAccountKeyMeta, error) {
	return serviceaccount.CreateKey(ctx, p.resources, account, email)
}

// ensureKey returns an existing key for the SA, or creates a system-managed one.
func (p *Provider) ensureKey(ctx context.Context, account, email string) (*serviceAccountKeyMeta, error) {
	return serviceaccount.EnsureKey(ctx, p.resources, account, email)
}

// loadKey loads a key by ID, verifying it belongs to the given SA.
func (p *Provider) loadKey(ctx context.Context, account, email, keyID string) (*serviceAccountKeyMeta, error) {
	k, err := serviceaccount.LoadKey(ctx, p.resources, account, email, keyID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "key not found", 404)
		}
		return nil, err
	}
	return k, nil
}

// requireWildcardServiceAccount re-checks the parent account on a "projects/-"
// request: a key addressed under an account the wildcard cannot resolve answers
// the documented 403 rather than the per-key 404. The concrete project path
// keeps its per-resource 404 ("key not found"), which the conformance
// transcripts pin, so the check is deliberately wildcard-only.
func (p *Provider) requireWildcardServiceAccount(ctx context.Context, nr *model.NormalizedRequest, ref saRef) error {
	if nr.AccountID != "-" {
		return nil
	}
	return p.requireServiceAccount(ctx, nr, ref)
}

func (p *Provider) ServiceAccountKeyCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	ref, err := p.resolveSA(ctx, nr, parseKeyParent(name))
	if err != nil {
		return nil, err
	}
	sa, err := p.loadServiceAccountMeta(ctx, nr, ref)
	if err != nil {
		return nil, err
	}
	m, err := p.createKey(ctx, ref.Account, ref.Email)
	if err != nil {
		return nil, err
	}
	privDER, _ := m.PrivDER()
	privPEM, _ := kmsstore.PrivateKeyPEM(privDER)
	// client_id is the account's oauth2ClientId: the official client libraries
	// read it out of the credentials file to build the OAuth2 client, so a
	// placeholder value would make the file unusable against real Google
	// endpoints.
	creds, _ := json.Marshal(map[string]any{
		"type":            "service_account",
		"project_id":      ref.Account,
		"private_key_id":  m.KeyID,
		"private_key":     privPEM,
		"client_email":    ref.Email,
		"client_id":       sa.Oauth2ClientID,
		"auth_uri":        "https://accounts.google.com/o/oauth2/auth",
		"token_uri":       "https://oauth2.googleapis.com/token",
		"universe_domain": "googleapis.com",
	})
	resp := keyCreateMap(nr, ref.Account, *m, base64.StdEncoding.EncodeToString(creds))
	return provider.OK(resp), nil
}

func (p *Provider) ServiceAccountKeyList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	// Request validation precedes the resource lookup, as it does on real GCP: a
	// bad keyTypes value is 400 even when the service account is also missing.
	keyTypes, err := keyTypeFilter(nr)
	if err != nil {
		return nil, err
	}
	ref, err := p.resolveSA(ctx, nr, parseKeyParent(name))
	if err != nil {
		return nil, err
	}
	if err := p.requireServiceAccount(ctx, nr, ref); err != nil {
		return nil, err
	}
	entries, err := p.resources.List(ctx, ref.Account, store.GlobalRegion, rtServiceAccountKey, "")
	if err != nil {
		return nil, err
	}
	items := make([]any, 0)
	for _, e := range entries {
		var m serviceAccountKeyMeta
		if json.Unmarshal(e.Data, &m) == nil && m.Email == ref.Email && matchesKeyType(m.Type(), keyTypes) {
			items = append(items, keyListMap(nr, ref.Account, m))
		}
	}
	return provider.OK(map[string]any{"keys": items}), nil
}

func (p *Provider) ServiceAccountKeyGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email, keyID := parseKeyName(name)
	ref, err := p.resolveSA(ctx, nr, email)
	if err != nil {
		return nil, err
	}
	if err := p.requireWildcardServiceAccount(ctx, nr, ref); err != nil {
		return nil, err
	}
	m, err := p.loadKey(ctx, ref.Account, ref.Email, keyID)
	if err != nil {
		return nil, err
	}
	return provider.OK(keyGetMap(nr, ref.Account, *m)), nil
}

func (p *Provider) ServiceAccountKeyDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email, keyID := parseKeyName(name)
	ref, err := p.resolveSA(ctx, nr, email)
	if err != nil {
		return nil, err
	}
	if err := p.requireWildcardServiceAccount(ctx, nr, ref); err != nil {
		return nil, err
	}
	if _, err := p.loadKey(ctx, ref.Account, ref.Email, keyID); err != nil {
		return nil, err
	}
	if err := p.resources.Delete(ctx, ref.Account, store.GlobalRegion, rtServiceAccountKey, keyID); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

// setKeyDisabled flips a service-account key's disabled flag
// (projects.serviceAccounts.keys.disable/.enable) and persists it. Both methods
// declare an Empty response, so the state (and disableTime) is readable only
// through keys.get.
func (p *Provider) setKeyDisabled(ctx context.Context, nr *model.NormalizedRequest, disabled bool) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email, keyID := parseKeyName(name)
	ref, err := p.resolveSA(ctx, nr, email)
	if err != nil {
		return nil, err
	}
	if err := p.requireWildcardServiceAccount(ctx, nr, ref); err != nil {
		return nil, err
	}
	m, err := p.loadKey(ctx, ref.Account, ref.Email, keyID)
	if err != nil {
		return nil, err
	}
	m.Disabled = disabled
	if disabled {
		m.DisableTime = clock.Now().UTC().Format(time.RFC3339Nano)
	} else {
		m.DisableTime = ""
	}
	data, _ := json.Marshal(m)
	if err := p.resources.Update(ctx, ref.Account, store.GlobalRegion, store.ResourceEntry{
		Type: rtServiceAccountKey, ID: keyID, Data: data,
	}); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// ServiceAccountKeyDisable implements projects.serviceAccounts.keys.disable.
func (p *Provider) ServiceAccountKeyDisable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setKeyDisabled(ctx, nr, true)
}

// ServiceAccountKeyEnable implements projects.serviceAccounts.keys.enable.
func (p *Provider) ServiceAccountKeyEnable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setKeyDisabled(ctx, nr, false)
}

// signTarget resolves the service account a signBlob/signJwt request signs for.
// Both identifier forms are documented, but signing resolves the account more
// leniently than the management surface: through "projects/-" it signs lazily
// for an account that need not exist (the IAM Credentials surface does the
// same), while a concrete project path requires the account. A unique-id
// identifier names no project of its own, so it is resolved against the
// addressed account; the wildcard cannot resolve one (DC29) and answers the
// documented miss instead of signing for a phantom account.
func (p *Provider) signTarget(ctx context.Context, nr *model.NormalizedRequest, name string) (saRef, error) {
	identifier := emailFromName(name)
	if !strings.Contains(identifier, "@") {
		return p.resolveSA(ctx, nr, identifier)
	}
	account := serviceaccount.AccountForSA(nr.AccountID, identifier)
	if account == "" || account == "-" {
		// The identifier embeds no project and the path project is the
		// wildcard, so there is nothing to scope the signature to.
		return saRef{}, saLookupError(nr, "serviceAccounts/"+identifier, "service account not found")
	}
	ref := saRef{Account: account, Email: identifier}
	if nr.AccountID != "-" {
		if err := p.requireServiceAccount(ctx, nr, ref); err != nil {
			return saRef{}, err
		}
	}
	return ref, nil
}

// ServiceAccountSignBlob serves iam.projects.serviceAccounts.signBlob. The
// IAM and IAM Credentials APIs share this path on the emulator's single origin
// but use different response field names: IAM returns `signature`, IAM
// Credentials returns `signedBlob`. Real GCP never returns both, so the request
// Host discriminates which surface was called and exactly one field is emitted.
// The request accepts both bytesToSign (IAM) and payload (IAM Credentials).
func (p *Provider) ServiceAccountSignBlob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	ref, err := p.signTarget(ctx, nr, name)
	if err != nil {
		return nil, err
	}
	m, err := p.ensureKey(ctx, ref.Account, ref.Email)
	if err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	// IAM carries the payload under bytesToSign; IAM Credentials uses payload.
	blob, _ := body["bytesToSign"].(string)
	if blob == "" {
		blob, _ = body["payload"].(string)
	}
	raw, err := base64.StdEncoding.DecodeString(blob)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "payload must be base64", 400)
	}
	if err := serviceaccount.ValidateBlobPayload(raw); err != nil {
		return nil, err
	}
	priv, _ := m.PrivDER()
	sig, err := serviceaccount.SignBlob(priv, raw)
	if err != nil {
		return nil, model.NewProviderError("Internal", "sign failed", 500)
	}
	encoded := base64.StdEncoding.EncodeToString(sig)
	resp := map[string]any{"keyId": m.KeyID}
	if isCredentialsHost(nr) {
		resp["signedBlob"] = encoded
	} else {
		resp["signature"] = encoded
	}
	return provider.OK(resp), nil
}

// isCredentialsHost reports whether the request was addressed to the IAM
// Credentials API (iamcredentials.googleapis.com) rather than IAM. The two
// surfaces share a path on one origin, so the Host is the only discriminator;
// an unqualified or absent host is treated as IAM, the surface that owns the
// path.
func isCredentialsHost(nr *model.NormalizedRequest) bool {
	if nr == nil || nr.Raw == nil {
		return false
	}
	return strings.Contains(strings.ToLower(nr.Raw.Host), "iamcredentials.")
}

func (p *Provider) ServiceAccountSignJwt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	ref, err := p.signTarget(ctx, nr, name)
	if err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	payloadStr, _ := body["payload"].(string)
	if err := serviceaccount.ValidateJWTPayload(payloadStr, clock.Now().UTC()); err != nil {
		return nil, err
	}
	m, err := p.ensureKey(ctx, ref.Account, ref.Email)
	if err != nil {
		return nil, err
	}
	priv, _ := m.PrivDER()
	signed, err := serviceaccount.SignJWT(priv, m.KeyID, payloadStr)
	if err != nil {
		return nil, model.NewProviderError("Internal", "sign failed", 500)
	}
	return provider.OK(map[string]any{
		"keyId":     m.KeyID,
		"signedJwt": signed,
	}), nil
}
