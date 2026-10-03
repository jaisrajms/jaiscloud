package auth

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/downscope"
	"jaiscloud/internal/gcp/identity"

	"github.com/go-chi/chi/v5"
)

func newRouter(svc *Service) *chi.Mux {
	r := chi.NewRouter()
	svc.RegisterRoutes(r)
	return r
}

// assertion builds a JWT-shaped assertion with the given claims. The signature
// is a placeholder: the emulator never verifies it.
func assertion(t *testing.T, claims map[string]any) string {
	t.Helper()
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	body := base64.RawURLEncoding.EncodeToString(payload)
	return hdr + "." + body + ".c2ln"
}

func postForm(t *testing.T, r *chi.Mux, path string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func decodeTokenResponse(t *testing.T, rec *httptest.ResponseRecorder) tokenResponse {
	t.Helper()
	var resp tokenResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode token response %q: %v", rec.Body.String(), err)
	}
	return resp
}

func TestServeTokenJWTBearer(t *testing.T) {
	svc := NewService(Config{ProjectID: "cfg-project", ServiceAccount: "default@example.com"})
	assert := assertion(t, map[string]any{
		"iss": "storage-test@test-project.iam.gserviceaccount.com",
		"sub": "storage-test@test-project.iam.gserviceaccount.com",
		"exp": clock.RealNow().Add(time.Hour).Unix(),
	})

	rec := postForm(t, newRouter(svc), "/token", url.Values{
		"grant_type": {GrantTypeJWTBearer},
		"assertion":  {assert},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Errorf("expected JSON content type, got %q", ct)
	}
	resp := decodeTokenResponse(t, rec)
	if resp.AccessToken == "" {
		t.Fatal("expected a non-empty access_token")
	}
	if resp.TokenType != "Bearer" {
		t.Errorf("expected Bearer token type, got %q", resp.TokenType)
	}
	if resp.ExpiresIn != accessTokenTTLSeconds {
		t.Errorf("expected expires_in %d, got %d", accessTokenTTLSeconds, resp.ExpiresIn)
	}

	// The minted token must resolve back to the asserted identity.
	req := httptest.NewRequest(http.MethodGet, "/storage/v1/b?project=test-project", nil)
	req.Header.Set("Authorization", "Bearer "+resp.AccessToken)
	ident := identity.FromRequest(req)
	if ident.ServiceAccount != "storage-test@test-project.iam.gserviceaccount.com" {
		t.Errorf("service account = %q, want asserted subject", ident.ServiceAccount)
	}
	if ident.ProjectID != "test-project" {
		t.Errorf("project = %q, want test-project", ident.ProjectID)
	}
}

func TestServeTokenV1Path(t *testing.T) {
	svc := NewService(Config{ProjectID: "proj", ServiceAccount: "sa@example.com"})
	assert := assertion(t, map[string]any{"iss": "sa@example.com", "exp": clock.RealNow().Add(time.Hour).Unix()})
	for _, path := range []string{"/token", "/v1/token"} {
		rec := postForm(t, newRouter(svc), path, url.Values{
			"grant_type": {GrantTypeJWTBearer},
			"assertion":  {assert},
		})
		if rec.Code != http.StatusOK {
			t.Errorf("%s: expected 200, got %d: %s", path, rec.Code, rec.Body.String())
		}
	}
}

func TestServeTokenFallsBackToConfigClaims(t *testing.T) {
	svc := NewService(Config{ProjectID: "cfg-proj", ServiceAccount: "cfg-sa@example.com"})
	// Assertion with only iss (no sub, no exp, no project_id).
	assert := assertion(t, map[string]any{"iss": "asserted@example.com"})
	rec := postForm(t, newRouter(svc), "/token", url.Values{
		"grant_type": {GrantTypeJWTBearer},
		"assertion":  {assert},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	resp := decodeTokenResponse(t, rec)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+resp.AccessToken)
	ident := identity.FromRequest(req)
	if ident.ServiceAccount != "asserted@example.com" {
		t.Errorf("service account = %q, want asserted@example.com", ident.ServiceAccount)
	}
	if ident.ProjectID != "cfg-proj" {
		t.Errorf("project = %q, want cfg-proj fallback", ident.ProjectID)
	}
}

func TestServeTokenErrors(t *testing.T) {
	svc := NewService(Config{ProjectID: "proj", ServiceAccount: "sa@example.com"})
	r := newRouter(svc)

	cases := []struct {
		name     string
		form     url.Values
		wantCode string
	}{
		{
			name:     "unsupported grant type",
			form:     url.Values{"grant_type": {"token-exchange"}},
			wantCode: "unsupported_grant_type",
		},
		{
			name:     "missing assertion",
			form:     url.Values{"grant_type": {GrantTypeJWTBearer}},
			wantCode: "invalid_grant",
		},
		{
			name:     "malformed assertion",
			form:     url.Values{"grant_type": {GrantTypeJWTBearer}, "assertion": {"not-a-jwt"}},
			wantCode: "invalid_grant",
		},
		{
			name: "expired assertion",
			form: url.Values{
				"grant_type": {GrantTypeJWTBearer},
				"assertion":  {assertion(t, map[string]any{"iss": "sa@example.com", "exp": clock.RealNow().Add(-time.Hour).Unix()})},
			},
			wantCode: "invalid_grant",
		},
		{
			name:     "refresh token missing",
			form:     url.Values{"grant_type": {GrantTypeRefreshToken}},
			wantCode: "invalid_request",
		},
		{
			name:     "refresh token whitespace only",
			form:     url.Values{"grant_type": {GrantTypeRefreshToken}, "refresh_token": {"   "}},
			wantCode: "invalid_request",
		},
		{
			name:     "missing grant type",
			form:     url.Values{},
			wantCode: "invalid_request",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := postForm(t, r, "/token", tc.form)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
			}
			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode error body %q: %v", rec.Body.String(), err)
			}
			if body["error"] != tc.wantCode {
				t.Errorf("error = %q, want %q (body %s)", body["error"], tc.wantCode, rec.Body.String())
			}
			if body["error_description"] == "" {
				t.Errorf("expected an error_description in %s", rec.Body.String())
			}
		})
	}
}

func TestServeTokenSubjectPrecedence(t *testing.T) {
	svc := NewService(Config{ProjectID: "proj", ServiceAccount: "cfg@example.com"})
	assert := assertion(t, map[string]any{
		"iss": "issuer@example.com",
		"sub": "subject@example.com",
		"exp": clock.RealNow().Add(time.Hour).Unix(),
	})
	rec := postForm(t, newRouter(svc), "/token", url.Values{
		"grant_type": {GrantTypeJWTBearer},
		"assertion":  {assert},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	resp := decodeTokenResponse(t, rec)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+resp.AccessToken)
	if got := identity.FromRequest(req).ServiceAccount; got != "subject@example.com" {
		t.Errorf("service account = %q, want the subject (subject@example.com)", got)
	}
}

// TestRegisterGrantType guards the extension point the STS phase relies on:
// adding a grant type to the existing Service must not require re-registering
// (and thus replacing) the /v1/token route.
func TestRegisterGrantType(t *testing.T) {
	svc := NewService(Config{ProjectID: "proj", ServiceAccount: "sa@example.com"})
	called := false
	svc.RegisterGrantType("urn:example:custom", func(w http.ResponseWriter, _ *http.Request, _ url.Values) {
		called = true
		writeTokenHeaders(w)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"access_token":"custom"}`))
	})
	rec := postForm(t, newRouter(svc), "/v1/token", url.Values{"grant_type": {"urn:example:custom"}})
	if rec.Code != http.StatusOK || !called {
		t.Fatalf("custom grant: code=%d called=%v body=%s", rec.Code, called, rec.Body.String())
	}
	// The built-in grants remain reachable through the same route.
	assert := assertion(t, map[string]any{"iss": "sa@example.com", "exp": clock.RealNow().Add(time.Hour).Unix()})
	rec = postForm(t, newRouter(svc), "/v1/token", url.Values{
		"grant_type": {GrantTypeJWTBearer},
		"assertion":  {assert},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("jwt-bearer after custom registration: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestServeTokenRefreshGrant(t *testing.T) {
	svc := NewService(Config{ProjectID: "proj", ServiceAccount: "sa@example.com"})
	rec := postForm(t, newRouter(svc), "/token", url.Values{
		"grant_type":    {GrantTypeRefreshToken},
		"refresh_token": {"some-refresh-token"},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if decodeTokenResponse(t, rec).AccessToken == "" {
		t.Fatal("expected a non-empty access_token")
	}
}

// TestServeTokenAssertionShapes exercises the assertion validator's rejection
// branches with raw (non-assertion(t,...)) payloads.
func TestServeTokenAssertionShapes(t *testing.T) {
	svc := NewService(Config{ProjectID: "proj", ServiceAccount: "sa@example.com"})
	r := newRouter(svc)

	cases := []struct {
		name      string
		assertion string
	}{
		{"empty", ""},
		{"not three parts", "a.b"},
		{"payload not base64url", "a.!!!.c"},
		{"payload not JSON", "a." + base64.RawURLEncoding.EncodeToString([]byte("not json")) + ".c"},
		{"missing iss and sub", "a." + base64.RawURLEncoding.EncodeToString([]byte(`{"aud":"x"}`)) + ".c"},
		{"exp not a number", "a." + base64.RawURLEncoding.EncodeToString([]byte(`{"iss":"sa@example.com","exp":"soon"}`)) + ".c"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := postForm(t, r, "/token", url.Values{
				"grant_type": {GrantTypeJWTBearer},
				"assertion":  {tc.assertion},
			})
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
			}
			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode error body %q: %v", rec.Body.String(), err)
			}
			if body["error"] != "invalid_grant" {
				t.Errorf("error = %q, want invalid_grant (body %s)", body["error"], rec.Body.String())
			}
		})
	}
}

// TestServeTokenEmptyConfig covers the final config fallbacks when the
// assertion supplies no identity and the service has no defaults.
func TestServeTokenEmptyConfig(t *testing.T) {
	svc := NewService(Config{})
	assert := assertion(t, map[string]any{"sub": "subject-only@example.com"})
	rec := postForm(t, newRouter(svc), "/token", url.Values{
		"grant_type": {GrantTypeJWTBearer},
		"assertion":  {assert},
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	resp := decodeTokenResponse(t, rec)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+resp.AccessToken)
	ident := identity.FromRequest(req)
	if ident.ServiceAccount != "subject-only@example.com" {
		t.Errorf("service account = %q, want subject-only@example.com", ident.ServiceAccount)
	}
	// No project in the assertion or config: identity falls back to its default.
	if ident.ProjectID != identity.DefaultProjectID {
		t.Errorf("project = %q, want default %q", ident.ProjectID, identity.DefaultProjectID)
	}
}

const testOptions = `{"accessBoundary":{"accessBoundaryRules":[{"availableResource":"//storage.googleapis.com/projects/_/buckets/bkt","availablePermissions":["inRole:roles/storage.objectViewer"]}]}}`

func exchangeForm(subjectToken string) url.Values {
	return url.Values{
		"grant_type":           {GrantTypeTokenExchange},
		"subject_token_type":   {TokenTypeAccessToken},
		"requested_token_type": {TokenTypeAccessToken},
		"subject_token":        {subjectToken},
		"options":              {testOptions},
	}
}

func decodeSTSResponse(t *testing.T, rec *httptest.ResponseRecorder) stsTokenResponse {
	t.Helper()
	var resp stsTokenResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode STS response %q: %v", rec.Body.String(), err)
	}
	return resp
}

func TestServeTokenExchange(t *testing.T) {
	svc := NewService(Config{ProjectID: "cfg-proj", ServiceAccount: "cfg-sa@example.com"})
	// A source access token minted by the emulator carries the caller identity.
	source := MintAccessToken("src@example.com", "src-proj")

	for _, path := range []string{"/token", "/v1/token"} {
		rec := postForm(t, newRouter(svc), path, exchangeForm(source))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: expected 200, got %d: %s", path, rec.Code, rec.Body.String())
		}
		resp := decodeSTSResponse(t, rec)
		if !strings.HasPrefix(resp.AccessToken, downscope.TokenPrefix) {
			t.Errorf("%s: access_token %q does not carry the downscoped prefix", path, resp.AccessToken)
		}
		if resp.IssuedTokenType != TokenTypeAccessToken {
			t.Errorf("issued_token_type = %q", resp.IssuedTokenType)
		}
		if resp.TokenType != "Bearer" {
			t.Errorf("token_type = %q", resp.TokenType)
		}
		if resp.ExpiresIn != accessTokenTTLSeconds {
			t.Errorf("expires_in = %d, want %d", resp.ExpiresIn, accessTokenTTLSeconds)
		}

		// The downscoped token still resolves to the source identity.
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+resp.AccessToken)
		ident := identity.FromRequest(req)
		if ident.ServiceAccount != "src@example.com" {
			t.Errorf("service account = %q, want src@example.com", ident.ServiceAccount)
		}
		if ident.ProjectID != "src-proj" {
			t.Errorf("project = %q, want src-proj", ident.ProjectID)
		}

		// ... and the GCS boundary is enforced.
		if err := downscope.Allowed("Bearer "+resp.AccessToken, downscope.ReadObject, "bkt", "o"); err != nil {
			t.Errorf("boundary should allow bkt/o: %v", err)
		}
		if err := downscope.Allowed("Bearer "+resp.AccessToken, downscope.ReadObject, "other", "o"); !errors.Is(err, downscope.ErrDenied) {
			t.Errorf("boundary should deny other bucket, got %v", err)
		}
	}
}

func TestServeTokenExchangeOpaqueSourceFallsBackToConfig(t *testing.T) {
	svc := NewService(Config{ProjectID: "cfg-proj", ServiceAccount: "cfg-sa@example.com"})
	rec := postForm(t, newRouter(svc), "/v1/token", exchangeForm("source-token"))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	resp := decodeSTSResponse(t, rec)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+resp.AccessToken)
	ident := identity.FromRequest(req)
	if ident.ServiceAccount != "cfg-sa@example.com" {
		t.Errorf("service account = %q, want cfg-sa@example.com", ident.ServiceAccount)
	}
	if ident.ProjectID != "cfg-proj" {
		t.Errorf("project = %q, want cfg-proj", ident.ProjectID)
	}
}

func TestServeTokenExchangeErrors(t *testing.T) {
	svc := NewService(Config{ProjectID: "proj", ServiceAccount: "sa@example.com"})
	r := newRouter(svc)

	valid := exchangeForm("source-token")
	cases := []struct {
		name     string
		mutate   func(url.Values)
		wantCode string
	}{
		{"missing subject_token_type", func(f url.Values) { f.Del("subject_token_type") }, "invalid_request"},
		{"wrong subject_token_type", func(f url.Values) { f.Set("subject_token_type", "urn:ietf:params:oauth:token-type:jwt") }, "invalid_request"},
		{"missing requested_token_type", func(f url.Values) { f.Del("requested_token_type") }, "invalid_request"},
		{"wrong requested_token_type", func(f url.Values) { f.Set("requested_token_type", "urn:ietf:params:oauth:token-type:jwt") }, "invalid_request"},
		{"missing subject_token", func(f url.Values) { f.Set("subject_token", "  ") }, "invalid_request"},
		{"missing options", func(f url.Values) { f.Del("options") }, "invalid_request"},
		{"invalid options json", func(f url.Values) { f.Set("options", "{not json") }, "invalid_grant"},
		{"no rules", func(f url.Values) { f.Set("options", `{"accessBoundary":{"accessBoundaryRules":[]}}`) }, "invalid_grant"},
		{"unsupported permission", func(f url.Values) {
			f.Set("options", `{"accessBoundary":{"accessBoundaryRules":[{"availableResource":"//storage.googleapis.com/projects/_/buckets/bkt","availablePermissions":["inRole:roles/storage.admin"]}]}}`)
		}, "invalid_grant"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			form := url.Values{}
			for k, v := range valid {
				form[k] = v
			}
			tc.mutate(form)
			rec := postForm(t, r, "/v1/token", form)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
			}
			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode error body %q: %v", rec.Body.String(), err)
			}
			if body["error"] != tc.wantCode {
				t.Errorf("error = %q, want %q (body %s)", body["error"], tc.wantCode, rec.Body.String())
			}
		})
	}

	// The jwt-bearer grant must remain reachable on the same /v1/token route
	// after the token-exchange grant is installed.
	assert := assertion(t, map[string]any{"iss": "sa@example.com", "exp": clock.RealNow().Add(time.Hour).Unix()})
	rec := postForm(t, r, "/v1/token", url.Values{"grant_type": {GrantTypeJWTBearer}, "assertion": {assert}})
	if rec.Code != http.StatusOK {
		t.Fatalf("jwt-bearer on /v1/token: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestMintDownscopedTokenIsWellFormed(t *testing.T) {
	rules := []downscope.Rule{{Bucket: "bkt", ObjectPrefix: "allowed/", Permissions: []string{downscope.PermissionObjectViewer}}}
	token := MintDownscopedToken("sa@example.com", "proj", rules)
	if !strings.HasPrefix(token, downscope.TokenPrefix) {
		t.Fatalf("token %q lacks the downscoped prefix", token)
	}
	rest := strings.TrimPrefix(token, downscope.TokenPrefix)
	parts := strings.Split(rest, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 JWT parts, got %d", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var claims struct {
		Email          string           `json:"email"`
		ProjectID      string           `json:"project_id"`
		AccessBoundary []downscope.Rule `json:"access_boundary"`
	}
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatalf("unmarshal claims: %v", err)
	}
	if claims.Email != "sa@example.com" || claims.ProjectID != "proj" {
		t.Errorf("unexpected identity claims: %+v", claims)
	}
	if len(claims.AccessBoundary) != 1 || claims.AccessBoundary[0].Bucket != "bkt" {
		t.Errorf("unexpected access boundary: %+v", claims.AccessBoundary)
	}
}

func TestMintAccessTokenIsWellFormed(t *testing.T) {
	token := MintAccessToken("sa@example.com", "proj")
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3 JWT parts, got %d", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatalf("unmarshal claims: %v", err)
	}
	if claims["email"] != "sa@example.com" || claims["sub"] != "sa@example.com" {
		t.Errorf("unexpected email/sub claims: %v", claims)
	}
	if claims["project_id"] != "proj" {
		t.Errorf("expected project_id proj, got %v", claims["project_id"])
	}
}
