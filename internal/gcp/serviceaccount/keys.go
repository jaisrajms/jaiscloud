// Package serviceaccount owns the emulator's service-account key material: the
// RSA key pairs that back IAM signBlob/signJwt and the IAM Credentials
// (iamcredentials) surface. Both the iam provider and the iamcredentials core
// share this package so a key created through either surface is the same key.
package serviceaccount

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"jaiscloud/internal/clock"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/store"
)

// ResourceType is the generic ResourceStore type for service-account keys.
const ResourceType = "gcp_service_account_key"

// KeyOrigin / KeyType classify a key: a key created through
// projects.serviceAccounts.keys.create is user-provided and user-managed, while
// the key pair Google maintains for signing is system-managed and
// Google-provided (see the IAM ServiceAccountKey keyOrigin/keyType enums).
const (
	KeyOrigin = "USER_PROVIDED"
	KeyType   = "USER_MANAGED"

	KeyOriginGoogleProvided = "GOOGLE_PROVIDED"
	KeyTypeSystemManaged    = "SYSTEM_MANAGED"
)

// Key is the stored representation of a service-account key.
type Key struct {
	Email      string `json:"email"`
	KeyID      string `json:"keyId"`
	Algorithm  string `json:"algorithm"`
	PrivateDER string `json:"privateDer"` // base64 PKCS8 private key DER
	ValidAfter string `json:"validAfterTime"`
	// KeyOrigin/KeyType are the ServiceAccountKey enums. They are empty on
	// records written before the type was modelled; Origin()/Type() then report
	// the user-managed pair those records were created as.
	KeyOrigin string `json:"keyOrigin,omitempty"`
	KeyType   string `json:"keyType,omitempty"`
	// Disabled marks a key turned off through
	// projects.serviceAccounts.keys.disable; DisableTime is when it was
	// disabled (empty while enabled). A disabled key still exists and is
	// returned by list/get.
	Disabled    bool   `json:"disabled,omitempty"`
	DisableTime string `json:"disableTime,omitempty"`
}

// Origin returns the key's keyOrigin, defaulting a record that predates the
// field to USER_PROVIDED.
func (k *Key) Origin() string {
	if k.KeyOrigin == "" {
		return KeyOrigin
	}
	return k.KeyOrigin
}

// Type returns the key's keyType, defaulting a record that predates the field
// to USER_MANAGED.
func (k *Key) Type() string {
	if k.KeyType == "" {
		return KeyType
	}
	return k.KeyType
}

// PrivDER decodes the stored private key DER.
func (k *Key) PrivDER() ([]byte, error) {
	return base64.StdEncoding.DecodeString(k.PrivateDER)
}

// PublicKeyData derives the base64 PKIX public-key PEM from the stored PKCS8
// private key DER. validBeforeTime stays omitted for non-expiring keys.
func (k *Key) PublicKeyData() (string, bool) {
	privDER, err := k.PrivDER()
	if err != nil {
		return "", false
	}
	priv, err := x509.ParsePKCS8PrivateKey(privDER)
	if err != nil {
		return "", false
	}
	signer, ok := priv.(crypto.Signer)
	if !ok {
		return "", false
	}
	pubDER, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil {
		return "", false
	}
	pemStr, err := kmsstore.PublicKeyPEM(pubDER)
	if err != nil {
		return "", false
	}
	return base64.StdEncoding.EncodeToString([]byte(pemStr)), true
}

// GenerateKeyID returns a fresh random key id (hex).
func GenerateKeyID() string {
	b := make([]byte, 16)
	_, _ = io.ReadFull(rand.Reader, b)
	return hex.EncodeToString(b)
}

// CreateKey generates a user-managed RSA-2048 key pair for email and persists
// it under account (the projects.serviceAccounts.keys.create shape).
func CreateKey(ctx context.Context, resources store.ResourceStore, account, email string) (*Key, error) {
	return createKey(ctx, resources, account, email, KeyType, KeyOrigin)
}

// createKey generates an RSA-2048 key pair classified by keyType/keyOrigin and
// persists it under account.
func createKey(ctx context.Context, resources store.ResourceStore, account, email, keyType, keyOrigin string) (*Key, error) {
	priv, _, err := kmsstore.GenerateRSAKeyPair(2048)
	if err != nil {
		return nil, err
	}
	k := Key{
		Email:      email,
		KeyID:      GenerateKeyID(),
		Algorithm:  "KEY_ALG_RSA_2048",
		PrivateDER: base64.StdEncoding.EncodeToString(priv),
		ValidAfter: timestamp.Canonical(clock.Now()),
		KeyType:    keyType,
		KeyOrigin:  keyOrigin,
	}
	data, _ := json.Marshal(k)
	if err := resources.Create(ctx, account, store.GlobalRegion, store.ResourceEntry{Type: ResourceType, ID: k.KeyID, Data: data}); err != nil {
		return nil, err
	}
	return &k, nil
}

// EnsureKey returns the service account's system-managed (Google-provided) key,
// creating it on first use. Real GCP maintains that key pair per service
// account and signs with it; the emulator materialises it lazily so creating a
// service account does not pay for an RSA keygen, and a key created through
// keys.create is never reused for signing. It deliberately does not require the
// service account to exist: impersonation calls address accounts by email and
// the emulator signs for them lazily (a documented divergence from real GCP,
// which requires the account to exist).
func EnsureKey(ctx context.Context, resources store.ResourceStore, account, email string) (*Key, error) {
	entries, err := resources.List(ctx, account, store.GlobalRegion, ResourceType, "")
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		var k Key
		if json.Unmarshal(e.Data, &k) == nil && k.Email == email && k.Type() == KeyTypeSystemManaged {
			return &k, nil
		}
	}
	return createKey(ctx, resources, account, email, KeyTypeSystemManaged, KeyOriginGoogleProvided)
}

// LoadKey loads a key by ID, verifying it belongs to the given service account.
func LoadKey(ctx context.Context, resources store.ResourceStore, account, email, keyID string) (*Key, error) {
	e, err := resources.Get(ctx, account, store.GlobalRegion, ResourceType, keyID)
	if err != nil {
		return nil, err
	}
	var k Key
	if json.Unmarshal(e.Data, &k) != nil || k.Email != email {
		return nil, store.ErrNotFound
	}
	return &k, nil
}

// SignBlob signs the SHA-256 digest of raw with the PKCS#1 v1.5 RSA private
// key, returning the raw signature bytes.
func SignBlob(privDER, raw []byte) ([]byte, error) {
	digest := sha256.Sum256(raw)
	return kmsstore.RSASign(privDER, digest[:], "RSA_SIGN_PKCS1_2048_SHA256")
}

// jsonJWTHEader is the fixed RS256 header (kid is the signing key id).
func jsonJWTHEader(keyID string) string {
	return fmt.Sprintf(`{"alg":"RS256","typ":"JWT","kid":"%s"}`, keyID)
}

// SignJWT signs the supplied raw JWT payload (a serialized JSON claims set) and
// returns the compact JWS. The caller is responsible for validating the claims.
func SignJWT(privDER []byte, keyID, payload string) (string, error) {
	header := base64.RawURLEncoding.EncodeToString([]byte(jsonJWTHEader(keyID)))
	body := base64.RawURLEncoding.EncodeToString([]byte(payload))
	return signParts(privDER, header, body)
}

// SignClaims marshals claims to JSON and returns a compact RS256 JWS.
func SignClaims(privDER []byte, keyID string, claims any) (string, error) {
	raw, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	header := base64.RawURLEncoding.EncodeToString([]byte(jsonJWTHEader(keyID)))
	body := base64.RawURLEncoding.EncodeToString(raw)
	return signParts(privDER, header, body)
}

func signParts(privDER []byte, header, body string) (string, error) {
	signingInput := header + "." + body
	digest := sha256.Sum256([]byte(signingInput))
	sig, err := kmsstore.RSASign(privDER, digest[:], "RSA_SIGN_PKCS1_2048_SHA256")
	if err != nil {
		return "", err
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// AccountForSA resolves the store account for an impersonation request. GCP
// addresses impersonation through the "projects/-" wildcard, which the emulator
// cannot scope by; the project embedded in the service-account email is
// authoritative, so fall back to it.
func AccountForSA(account, email string) string {
	if account != "" && account != "-" {
		return account
	}
	if p := ProjectFromEmail(email); p != "" {
		return p
	}
	return account
}

// ProjectFromEmail extracts the project id from a standard service-account
// email ("{id}@{project}.iam.gserviceaccount.com"), or "" if it does not match.
func ProjectFromEmail(email string) string {
	const suffix = ".iam.gserviceaccount.com"
	i := len(email) - len(suffix)
	if i <= 0 || email[i:] != suffix {
		return ""
	}
	at := len(email)
	for j := i - 1; j >= 0; j-- {
		if email[j] == '@' {
			at = j
			break
		}
	}
	if at <= 0 || at >= i {
		return ""
	}
	return email[at+1 : i]
}
