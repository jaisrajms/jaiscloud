package iam

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
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

// keyOrigin / keyType classify a key created through the API: a user-provided,
// user-managed key (as opposed to Google-managed/system-managed).
const (
	keyOrigin = serviceaccount.KeyOrigin
	keyType   = serviceaccount.KeyType
)

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

// keyToMap renders a service-account key as its GCP response object (no
// privateKeyData — that is returned only on create).
//
// keyId is not a field of the Discovery ServiceAccountKey schema (the id is
// implicit in `name`), but the Java gax client and the floci-gcp reference
// surface the key id as a top-level string, so callers can address a key
// without parsing `name`. Emitting it keeps the SDK compatibility suite green;
// the value is stable for the key's lifetime.
func keyToMap(nr *model.NormalizedRequest, m serviceAccountKeyMeta) map[string]any {
	out := map[string]any{
		"name":           nr.ResourceID("service-account", m.Email) + "/keys/" + m.KeyID,
		"keyId":          m.KeyID,
		"privateKeyType": "TYPE_GOOGLE_CREDENTIALS_FILE",
		"keyAlgorithm":   m.Algorithm,
		"keyOrigin":      keyOrigin,
		"keyType":        keyType,
		"validAfterTime": m.ValidAfter,
		"disabled":       m.Disabled,
	}
	if m.DisableTime != "" {
		out["disableTime"] = m.DisableTime
	}
	if pub, ok := m.PublicKeyData(); ok {
		out["publicKeyData"] = pub
	}
	return out
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

func (p *Provider) ServiceAccountKeyCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email := parseKeyParent(name)
	if err := p.requireServiceAccount(ctx, nr.AccountID, email); err != nil {
		return nil, err
	}
	m, err := p.createKey(ctx, nr.AccountID, email)
	if err != nil {
		return nil, err
	}
	privDER, _ := m.PrivDER()
	privPEM, _ := kmsstore.PrivateKeyPEM(privDER)
	creds, _ := json.Marshal(map[string]any{
		"type":            "service_account",
		"project_id":      nr.AccountID,
		"private_key_id":  m.KeyID,
		"private_key":     privPEM,
		"client_email":    email,
		"client_id":       "0",
		"auth_uri":        "https://accounts.google.com/o/oauth2/auth",
		"token_uri":       "https://oauth2.googleapis.com/token",
		"universe_domain": "googleapis.com",
	})
	resp := keyToMap(nr, *m)
	resp["privateKeyData"] = base64.StdEncoding.EncodeToString(creds)
	return provider.OK(resp), nil
}

func (p *Provider) ServiceAccountKeyList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email := parseKeyParent(name)
	if err := p.requireServiceAccount(ctx, nr.AccountID, email); err != nil {
		return nil, err
	}
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtServiceAccountKey, "")
	if err != nil {
		return nil, err
	}
	items := make([]any, 0)
	for _, e := range entries {
		var m serviceAccountKeyMeta
		if json.Unmarshal(e.Data, &m) == nil && m.Email == email {
			items = append(items, keyToMap(nr, m))
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
	m, err := p.loadKey(ctx, nr.AccountID, email, keyID)
	if err != nil {
		return nil, err
	}
	return provider.OK(keyToMap(nr, *m)), nil
}

func (p *Provider) ServiceAccountKeyDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email, keyID := parseKeyName(name)
	if _, err := p.loadKey(ctx, nr.AccountID, email, keyID); err != nil {
		return nil, err
	}
	if err := p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtServiceAccountKey, keyID); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

// setKeyDisabled flips a service-account key's disabled flag
// (projects.serviceAccounts.keys.disable/.enable) and persists it.
func (p *Provider) setKeyDisabled(ctx context.Context, nr *model.NormalizedRequest, disabled bool) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email, keyID := parseKeyName(name)
	m, err := p.loadKey(ctx, nr.AccountID, email, keyID)
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
	if err := p.resources.Update(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{
		Type: rtServiceAccountKey, ID: keyID, Data: data,
	}); err != nil {
		return nil, err
	}
	return provider.OK(keyToMap(nr, *m)), nil
}

// ServiceAccountKeyDisable implements projects.serviceAccounts.keys.disable.
func (p *Provider) ServiceAccountKeyDisable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setKeyDisabled(ctx, nr, true)
}

// ServiceAccountKeyEnable implements projects.serviceAccounts.keys.enable.
func (p *Provider) ServiceAccountKeyEnable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setKeyDisabled(ctx, nr, false)
}

// ServiceAccountSignBlob serves iam.projects.serviceAccounts.signBlob. The
// shared path also carries iamcredentials signBlob calls (the two services are
// indistinguishable on one origin), so the response emits both the IAM
// spelling (signature) and the IAM Credentials spelling (signedBlob), and the
// request accepts both bytesToSign and payload.
func (p *Provider) ServiceAccountSignBlob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email := emailFromName(name)
	// Impersonation addresses the account through "projects/-"; the project in
	// the email is authoritative then. The IAM Credentials surface signs
	// lazily (the account need not exist); the IAM management API on a
	// concrete project path requires it.
	account := serviceaccount.AccountForSA(nr.AccountID, email)
	if nr.AccountID != "-" {
		if err := p.requireServiceAccount(ctx, account, email); err != nil {
			return nil, err
		}
	}
	m, err := p.ensureKey(ctx, account, email)
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
	return provider.OK(map[string]any{
		"keyId":      m.KeyID,
		"signature":  encoded,
		"signedBlob": encoded,
	}), nil
}

func (p *Provider) ServiceAccountSignJwt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email := emailFromName(name)
	account := serviceaccount.AccountForSA(nr.AccountID, email)
	if nr.AccountID != "-" {
		if err := p.requireServiceAccount(ctx, account, email); err != nil {
			return nil, err
		}
	}
	body, _ := nr.Params["body"].(map[string]any)
	payloadStr, _ := body["payload"].(string)
	if err := serviceaccount.ValidateJWTPayload(payloadStr, clock.Now().UTC()); err != nil {
		return nil, err
	}
	m, err := p.ensureKey(ctx, account, email)
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
