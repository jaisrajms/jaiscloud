// Package iamcredentials implements the transport-neutral core of the IAM
// Service Account Credentials API (iamcredentials.googleapis.com v1): it mints
// short-lived access/ID tokens for a service account and signs blobs/JWTs with
// the account's emulator key. The REST and gRPC transports are thin adapters
// over this core, so both produce identical credentials.
//
// Credentials are emulator stubs, not real Google tokens: the access token is
// an opaque RS256 JWT signed by the account's emulator key, so
// internal/gcp/identity can recover the caller identity from it and
// impersonation flows keep working. Real GCP returns an opaque token; the
// emulator's is likewise opaque to clients — it carries no recognizable prefix.
package iamcredentials

import (
	"context"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/serviceaccount"
	"jaiscloud/internal/store"
)

const (
	// defaultLifetime is the token lifetime when the request omits one.
	defaultLifetime = time.Hour
	// idTokenLifetime is the fixed lifetime of a generated ID token.
	idTokenLifetime = time.Hour

	// idTokenIssuer is the issuer claim of a Google ID token.
	idTokenIssuer = "https://accounts.google.com"
)

// Service is the IAM Credentials core.
type Service struct {
	resources store.ResourceStore
}

// New returns an IAM Credentials core over the given resource store.
func New(resources store.ResourceStore) *Service {
	return &Service{resources: resources}
}

// accessTokenClaims is the JWT payload of an emulator access token. The
// identity fields (email/sub/project_id) let internal/gcp/identity recover the
// caller from the bearer token.
type accessTokenClaims struct {
	Email     string   `json:"email"`
	Subject   string   `json:"sub"`
	ProjectID string   `json:"project_id"`
	Scope     []string `json:"scope,omitempty"`
	IssuedAt  int64    `json:"iat"`
	ExpiresAt int64    `json:"exp"`
}

// idTokenClaims is the JWT payload of a generated OpenID Connect ID token.
type idTokenClaims struct {
	Issuer        string `json:"iss"`
	Audience      string `json:"aud"`
	Subject       string `json:"sub"`
	IssuedAt      int64  `json:"iat"`
	ExpiresAt     int64  `json:"exp"`
	Email         string `json:"email,omitempty"`
	EmailVerified bool   `json:"email_verified,omitempty"`
}

// key resolves the signing key for (account, email). account may be the
// "projects/-" wildcard; the project embedded in the email is used then.
func (s *Service) key(ctx context.Context, account, email string) (*serviceaccount.Key, error) {
	if email == "" {
		return nil, serviceaccount.Invalid("service account is required")
	}
	return serviceaccount.EnsureKey(ctx, s.resources, serviceaccount.AccountForSA(account, email), email)
}

// GenerateAccessToken mints an OAuth2 access token for email. scopes must be
// non-empty and lifetime defaults to (and is capped at) the documented bounds.
func (s *Service) GenerateAccessToken(ctx context.Context, account, email string, scopes []string, lifetime time.Duration) (string, time.Time, error) {
	if len(scopes) == 0 {
		return "", time.Time{}, serviceaccount.Invalid("at least one scope is required")
	}
	if lifetime == 0 {
		lifetime = defaultLifetime
	}
	if lifetime < 0 || lifetime > serviceaccount.MaxJWTLifetime {
		return "", time.Time{}, serviceaccount.Invalid("lifetime must be between 0s and 12h")
	}
	k, err := s.key(ctx, account, email)
	if err != nil {
		return "", time.Time{}, err
	}
	privDER, err := k.PrivDER()
	if err != nil {
		return "", time.Time{}, err
	}
	now := clock.Now().UTC().Truncate(time.Second)
	expires := now.Add(lifetime)
	jwt, err := serviceaccount.SignClaims(privDER, k.KeyID, accessTokenClaims{
		Email:     email,
		Subject:   email,
		ProjectID: projectFor(account, email),
		Scope:     scopes,
		IssuedAt:  now.Unix(),
		ExpiresAt: expires.Unix(),
	})
	if err != nil {
		return "", time.Time{}, err
	}
	return jwt, expires, nil
}

// GenerateIDToken mints an OpenID Connect ID token for email. audience is
// required; includeEmail adds the email/email_verified claims.
func (s *Service) GenerateIDToken(ctx context.Context, account, email, audience string, includeEmail bool) (string, error) {
	if strings.TrimSpace(audience) == "" {
		return "", serviceaccount.Invalid("audience is required")
	}
	k, err := s.key(ctx, account, email)
	if err != nil {
		return "", err
	}
	privDER, err := k.PrivDER()
	if err != nil {
		return "", err
	}
	now := clock.Now().UTC()
	claims := idTokenClaims{
		Issuer:    idTokenIssuer,
		Audience:  audience,
		Subject:   email,
		IssuedAt:  now.Unix(),
		ExpiresAt: now.Add(idTokenLifetime).Unix(),
	}
	if includeEmail {
		claims.Email = email
		claims.EmailVerified = true
	}
	return serviceaccount.SignClaims(privDER, k.KeyID, claims)
}

// SignBlob signs the SHA-256 digest of payload with the account's emulator key.
func (s *Service) SignBlob(ctx context.Context, account, email string, payload []byte) (string, []byte, error) {
	if err := serviceaccount.ValidateBlobPayload(payload); err != nil {
		return "", nil, err
	}
	k, err := s.key(ctx, account, email)
	if err != nil {
		return "", nil, err
	}
	privDER, err := k.PrivDER()
	if err != nil {
		return "", nil, err
	}
	sig, err := serviceaccount.SignBlob(privDER, payload)
	if err != nil {
		return "", nil, err
	}
	return k.KeyID, sig, nil
}

// SignJWT signs a caller-supplied JWT claims set (a serialized JSON object).
// Any exp claim must be an integer within the next 12 hours.
func (s *Service) SignJWT(ctx context.Context, account, email, payload string) (string, string, error) {
	if err := serviceaccount.ValidateJWTPayload(payload, clock.Now().UTC()); err != nil {
		return "", "", err
	}
	k, err := s.key(ctx, account, email)
	if err != nil {
		return "", "", err
	}
	privDER, err := k.PrivDER()
	if err != nil {
		return "", "", err
	}
	signed, err := serviceaccount.SignJWT(privDER, k.KeyID, payload)
	if err != nil {
		return "", "", err
	}
	return k.KeyID, signed, nil
}

// allowedLocations is the static location list the emulator reports for the
// iamcredentials getAllowedLocations discovery methods (serviceAccounts and
// workloadIdentityPools). Real GCP derives the list from the organization's
// resource-locations constraint; the emulator has no org-policy plane, so it
// reports a single, always-valid location. This is a permissive answer, never a
// wrong one for a valid request.
var allowedLocations = []string{"global"}

// AllowedLocations returns the locations a caller may use for the IAM
// Credentials surface.
func (s *Service) AllowedLocations(context.Context) ([]string, error) {
	return append([]string(nil), allowedLocations...), nil
}

// projectFor returns the project id to embed in a minted token: the explicit
// account when it is a real project, else the one in the service-account email.
func projectFor(account, email string) string {
	if account != "" && account != "-" {
		return account
	}
	return serviceaccount.ProjectFromEmail(email)
}
