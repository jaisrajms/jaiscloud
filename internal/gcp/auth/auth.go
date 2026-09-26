// Package auth implements the emulator's OAuth2 token endpoint — the flow
// google-auth-library service-account credentials use to mint an access token.
//
// Real GCP serves it at https://oauth2.googleapis.com/token; the emulator
// serves the same RFC 6749 token flow at the root path /token (and /v1/token)
// so a client can point ServiceAccountCredentials.setTokenServerUri at the
// emulator endpoint.
//
// This is intentionally not a GCP Discovery service: the endpoint is defined by
// RFC 6749, returns the RFC error shape ({"error": ...,"error_description":
// ...}) rather than the GCP {"error":{"code",...}} envelope, and is mounted as a
// gateway extra route instead of being routed through the provider registry.
//
// The access token minted here is a JaisCloud JWT that internal/gcp/identity
// decodes to recover the service-account email and project, mirroring the GCE
// metadata-server token (internal/gcp/adapter/metadata.go). Real GCP access
// tokens are opaque; the emulator embeds identity as a convenience, and no
// signature is verified on the way back in (local emulator only).
//
// Extending the endpoint: grant types are dispatched through a registry
// (RegisterGrantType) rather than being hardcoded in the handler, so a later
// phase (the STS token-exchange grant) extends the existing /v1/token route by
// registering a handler instead of mounting a competing route — which would
// silently replace the jwt-bearer handler for that path in chi.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"jaiscloud/internal/clock"

	"github.com/go-chi/chi/v5"
)

// OAuth2 token-endpoint grant types the emulator understands.
const (
	// GrantTypeJWTBearer is the service-account assertion grant.
	// https://datatracker.ietf.org/doc/html/rfc7523
	GrantTypeJWTBearer = "urn:ietf:params:oauth:grant-type:jwt-bearer"
	// GrantTypeRefreshToken is the refresh-token grant. The emulator has no
	// real refresh tokens, but accepts a non-empty one and mints a fresh token
	// so user-credential-shaped clients still work.
	GrantTypeRefreshToken = "refresh_token"
)

// accessTokenTTLSeconds is the lifetime advertised for a minted access token.
// Real GCP access tokens last an hour.
const accessTokenTTLSeconds = 3600

// Config configures the emulator token service.
type Config struct {
	// ProjectID is the project claim minted when the assertion carries none.
	ProjectID string
	// ServiceAccount is the service-account email used when the assertion
	// carries no subject.
	ServiceAccount string
}

// GrantHandler handles one OAuth2 grant type. form is the parsed request form;
// the handler owns writing the response (success or RFC 6749 error).
type GrantHandler func(w http.ResponseWriter, r *http.Request, form url.Values)

// Service implements the OAuth2 token endpoint.
//
// The grant registry is written only by RegisterGrantType (setup time, before
// the server starts) and read by ServeToken; callers must not mutate it once
// the service is serving.
type Service struct {
	cfg    Config
	grants map[string]GrantHandler
}

// NewService returns a token service with the jwt-bearer and refresh-token
// grants installed.
func NewService(cfg Config) *Service {
	s := &Service{cfg: cfg, grants: make(map[string]GrantHandler, 2)}
	s.RegisterGrantType(GrantTypeJWTBearer, s.serveJWTBearer)
	s.RegisterGrantType(GrantTypeRefreshToken, s.serveRefreshToken)
	return s
}

// RegisterGrantType installs (or replaces) the handler for a grant_type. Call
// it during setup, before the service starts serving; it is not synchronized.
func (s *Service) RegisterGrantType(grantType string, h GrantHandler) {
	s.grants[grantType] = h
}

// RegisterRoutes mounts the token endpoint on r at both /token (the OAuth2
// canonical path) and /v1/token (what the google-auth-library and gRPC
// transcoding clients sometimes emit). Because grant types are dispatched from
// one Service, later phases extend this endpoint via RegisterGrantType rather
// than registering /v1/token again.
func (s *Service) RegisterRoutes(r chi.Router) {
	r.Post("/token", s.ServeToken)
	r.Post("/v1/token", s.ServeToken)
}

// tokenResponse is the RFC 6749 §5.1 successful token response.
type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

// ServeToken handles POST /token and POST /v1/token. The request body is the
// RFC 6749 application/x-www-form-urlencoded grant.
func (s *Service) ServeToken(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeOAuthError(w, "invalid_request", "malformed form body")
		return
	}

	grantType := strings.TrimSpace(r.PostFormValue("grant_type"))
	if grantType == "" {
		// RFC 6749 §5.2: a missing grant_type is a malformed request, not an
		// unknown grant.
		writeOAuthError(w, "invalid_request", "grant_type is required")
		return
	}
	handler, ok := s.grants[grantType]
	if !ok {
		writeOAuthError(w, "unsupported_grant_type", "Unsupported grant type")
		return
	}
	handler(w, r, r.PostForm)
}

// serveJWTBearer implements the RFC 7523 service-account assertion grant.
func (s *Service) serveJWTBearer(w http.ResponseWriter, r *http.Request, form url.Values) {
	claims, err := parseAssertion(strings.TrimSpace(form.Get("assertion")))
	if err != nil {
		writeOAuthError(w, "invalid_grant", err.Error())
		return
	}
	// Prefer the asserted subject, then the issuer; fall back to config.
	sa := claims.Subject
	if sa == "" {
		sa = claims.Issuer
	}
	if sa == "" {
		sa = s.cfg.ServiceAccount
	}
	project := claims.ProjectID
	if project == "" {
		project = s.cfg.ProjectID
	}
	s.writeToken(w, sa, project)
}

// serveRefreshToken implements the refresh-token grant. There is no real
// refresh-token store; any non-empty value mints a fresh access token.
func (s *Service) serveRefreshToken(w http.ResponseWriter, r *http.Request, form url.Values) {
	if strings.TrimSpace(form.Get("refresh_token")) == "" {
		writeOAuthError(w, "invalid_request", "refresh_token is required")
		return
	}
	s.writeToken(w, s.cfg.ServiceAccount, s.cfg.ProjectID)
}

// writeToken mints an access token and writes the RFC 6749 success response.
func (s *Service) writeToken(w http.ResponseWriter, sa, project string) {
	writeTokenHeaders(w)
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(tokenResponse{
		AccessToken: MintAccessToken(sa, project),
		TokenType:   "Bearer",
		ExpiresIn:   accessTokenTTLSeconds,
	})
}

// assertionClaims is the subset of an RSA-signed JWT assertion (RFC 7523) that
// the emulator reads.
type assertionClaims struct {
	Issuer    string          `json:"iss"`
	Subject   string          `json:"sub"`
	ProjectID string          `json:"project_id"`
	ExpiresAt json.RawMessage `json:"exp"`
}

// parseAssertion validates the structural shape of the service-account JWT
// assertion and returns its claims.
//
// The signature is deliberately NOT verified: the client signs the assertion
// with a private key the emulator has never seen (the compatibility suite
// generates a fresh key pair per run) and hardcodes the audience to
// https://oauth2.googleapis.com/token regardless of the configured token
// server, so neither is checkable. Matching the reference emulator and
// JaisCloud's treatment of bearer tokens, only the structural shape and the
// expiry are enforced: a well-formed, unexpired JWT identifying a
// subject/issuer.
func parseAssertion(assertion string) (*assertionClaims, error) {
	if assertion == "" {
		return nil, errors.New("assertion is required")
	}
	parts := strings.Split(assertion, ".")
	if len(parts) != 3 {
		return nil, errors.New("assertion is not a well-formed JWT")
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, errors.New("assertion payload is not valid base64url")
	}
	var c assertionClaims
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, errors.New("assertion payload is not valid JSON")
	}
	if c.Issuer == "" && c.Subject == "" {
		return nil, errors.New("assertion is missing iss/sub")
	}
	if len(c.ExpiresAt) > 0 {
		var exp int64
		if err := json.Unmarshal(c.ExpiresAt, &exp); err != nil {
			return nil, errors.New("assertion exp is not a number")
		}
		if clock.RealNow().Unix() > exp {
			return nil, errors.New("assertion has expired")
		}
	}
	return &c, nil
}

// writeTokenHeaders sets the headers RFC 6749 §5.1 requires on both success and
// error responses.
func writeTokenHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
}

// writeOAuthError writes the RFC 6749 §5.2 error response.
func writeOAuthError(w http.ResponseWriter, code, description string) {
	writeTokenHeaders(w)
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error":             code,
		"error_description": description,
	})
}

// tokenClaims is the JWT payload MintAccessToken signs. A struct (not a map)
// keeps json.Marshal's field order stable.
type tokenClaims struct {
	Email     string `json:"email"`
	Subject   string `json:"sub"`
	ProjectID string `json:"project_id"`
	ExpiresAt int64  `json:"exp"`
}

// MintAccessToken builds the emulator's HS256 access token. It carries the
// service-account email and project so internal/gcp/identity can recover the
// caller's identity from the Authorization header. The signature uses a fixed
// development secret and is never verified — this mirrors the GCE
// metadata-server token and keeps the emulator self-contained.
//
// Exported so the STS token-exchange surface (a later phase) can mint an
// ordinary access token through the same code path.
func MintAccessToken(sa, project string) string {
	const secret = "jaiscloud-dev"
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload, _ := json.Marshal(tokenClaims{
		Email:     sa,
		Subject:   sa,
		ProjectID: project,
		ExpiresAt: clock.RealNow().Unix() + accessTokenTTLSeconds,
	})
	signingInput := hdr + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + sig
}
