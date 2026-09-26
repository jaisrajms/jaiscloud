// Package identity extracts per-request GCP identity (project ID, service-account
// email, bearer token) from OAuth2 bearer credentials and URL paths. It has no
// dependencies on other JaisCloud packages and is safe to import from the
// gateway layer.
//
// GCP has no SigV4; identity comes from two independent sources:
//
//  1. The OAuth2 bearer token in the Authorization header. Its JWT payload
//     (decoded, never signature-verified — this is a local emulator) carries
//     the service-account email/sub and, occasionally, a project claim.
//  2. The project ID embedded in the URL path (e.g. /v1/projects/{project}/...).
//
// The two are merged: project from the path wins, then the token, then the
// config default. The service-account email comes from the token only.
package identity

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
)

// Defaults used when a request carries no recognisable credential.
const (
	DefaultProjectID      = "jaiscloud-project"
	DefaultServiceAccount = "jaiscloud@example.iam.gserviceaccount.com"
)

// projectPathRE matches the project segment of GCP resource-name URLs.
// Handles /v1/projects/{project}/... and /v2/projects/{project}/.... The
// optional beta suffix covers versioned beta APIs such as Cloud SQL's
// /sql/v1beta4/projects/{project}/... surface. The project capture excludes
// ':' so a project-segment custom method (Cloud Resource Manager's
// /v1/projects/{project}:getIamPolicy) yields the project id rather than
// "{project}:getIamPolicy".
var projectPathRE = regexp.MustCompile(`/v[0-9]+(?:beta[0-9]*)?/projects/([^/:]+)`)

// Source describes how the identity was derived.
type Source uint8

const (
	SourceDefault       Source = iota // no credential or path project; identity is the config default
	SourceBearer                      // Authorization: Bearer <token>
	SourcePath                        // project parsed from the URL path
	SourceBearerAndPath               // both token and path present
)

// Parsed is the per-request identity derived from the HTTP request.
type Parsed struct {
	ProjectID      string // always non-empty; falls back to DefaultProjectID
	ServiceAccount string // service-account email; falls back to DefaultServiceAccount
	AccessKey      string // raw bearer token string; empty for anonymous requests
	Source         Source
}

// FromRequest parses the bearer token + path project from the HTTP request and
// returns the resolved identity. It never errors — any unparseable or missing
// credential falls back to the defaults.
func FromRequest(r *http.Request) Parsed {
	var (
		project  string
		sa       string
		token    string
		hasToken bool
		hasPath  bool
	)

	if p := ProjectFromPath(r.URL.Path); p != "" {
		project = p
		hasPath = true
	}

	// GCS and several other GCP APIs carry the project as a ?project= query
	// parameter rather than in the URL path (e.g. POST /storage/v1/b?project=p).
	// Treat it as an explicit source so it is not overridden by the config
	// default; the path still wins when both are present.
	if project == "" {
		if qp := r.URL.Query().Get("project"); qp != "" {
			project = qp
			hasPath = true
		}
	}

	if t := BearerToken(r.Header.Get("Authorization")); t != "" {
		token = t
		hasToken = true
		if claims := decodeClaims(t); claims != nil {
			if v := claims.Email; v != "" {
				sa = v
			} else if v := claims.Subject; v != "" {
				sa = v
			}
			if project == "" && claims.ProjectID != "" {
				project = claims.ProjectID
			}
		}
	}

	if project == "" {
		project = DefaultProjectID
	}
	if sa == "" {
		sa = DefaultServiceAccount
	}

	src := SourceDefault
	switch {
	case hasToken && hasPath:
		src = SourceBearerAndPath
	case hasToken:
		src = SourceBearer
	case hasPath:
		src = SourcePath
	}

	return Parsed{
		ProjectID:      project,
		ServiceAccount: sa,
		AccessKey:      token,
		Source:         src,
	}
}

// BearerToken extracts the token from an "Authorization: Bearer <token>" header.
// The scheme is matched case-insensitively (RFC 7235); returns "" for any other
// scheme or malformed input.
func BearerToken(auth string) string {
	const prefix = "Bearer "
	if len(auth) < len(prefix) || !strings.EqualFold(auth[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(auth[len(prefix):])
}

// ProjectFromToken returns the project ID embedded in a JWT bearer token's
// project_id claim, or "" for opaque tokens (e.g. the emulator's literal
// "owner" token) and malformed JWTs. The signature is never verified.
func ProjectFromToken(token string) string {
	if c := decodeClaims(token); c != nil {
		return c.ProjectID
	}
	return ""
}

// ServiceAccountFromToken returns the service-account email embedded in a JWT
// bearer token's email/sub claims, or "" for opaque tokens and malformed JWTs.
// The signature is never verified. It is the identity source the STS
// token-exchange surface uses to mint a downscoped token for the same caller.
func ServiceAccountFromToken(token string) string {
	c := decodeClaims(token)
	if c == nil {
		return ""
	}
	if c.Email != "" {
		return c.Email
	}
	return c.Subject
}

// ProjectFromPath extracts the project ID from a GCP resource-name URL path.
// Returns "" if the path does not embed a project segment.
func ProjectFromPath(path string) string {
	m := projectPathRE.FindStringSubmatch(path)
	if len(m) < 2 {
		return ""
	}
	return m[1]
}

// claims is the subset of a GCP OAuth2 access-token JWT payload that identity
// resolution cares about. Signature is never verified.
type claims struct {
	Email     string `json:"email"`
	Subject   string `json:"sub"`
	ProjectID string `json:"project_id"`
}

// decodeClaims decodes an unverified JWT payload (base64url JSON) into claims.
// Returns nil for opaque tokens or malformed JWTs — never errors.
func decodeClaims(token string) *claims {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil
	}
	var c claims
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil
	}
	return &c
}
