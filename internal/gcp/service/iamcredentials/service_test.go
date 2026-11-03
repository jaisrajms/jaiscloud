package iamcredentials

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/gcp/serviceaccount"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

const (
	testEmail   = "compat@test-project.iam.gserviceaccount.com"
	testProject = "test-project"
	cloudScope  = "https://www.googleapis.com/auth/cloud-platform"
)

func newService() *Service { return New(store.NewMemoryResourceStore()) }

// seedAccount registers email as an existing service account in project — the
// record projects.serviceAccounts.create writes. Every signing method resolves
// the account through this registry (J67), so a test that signs must create one.
func seedAccount(t *testing.T, s *Service, project, email string) {
	t.Helper()
	data, _ := json.Marshal(serviceaccount.Account{
		Email:    email,
		UniqueID: fmt.Sprintf("%021d", crc32.ChecksumIEEE([]byte(email))),
	})
	if err := s.resources.Create(context.Background(), project, store.GlobalRegion, store.ResourceEntry{
		Type: serviceaccount.AccountResourceType, ID: email, Data: data,
	}); err != nil {
		t.Fatalf("seed service account %s: %v", email, err)
	}
}

// newServiceWithAccount returns a core whose testEmail exists in testProject.
func newServiceWithAccount(t *testing.T) *Service {
	t.Helper()
	s := newService()
	seedAccount(t, s, testProject, testEmail)
	return s
}

// jwtClaims decodes the (unverified) payload of a compact JWS.
func jwtClaims(t *testing.T, token string) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token is not a 3-part JWT: %q", token)
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatalf("unmarshal claims: %v", err)
	}
	return claims
}

func TestGenerateAccessToken(t *testing.T) {
	ctx := context.Background()
	s := newServiceWithAccount(t)
	before := time.Now()
	token, expires, err := s.GenerateAccessToken(ctx, "-", testEmail, []string{cloudScope}, time.Hour)
	if err != nil {
		t.Fatalf("GenerateAccessToken: %v", err)
	}
	if strings.Count(token, ".") != 2 {
		t.Fatalf("access token %q is not an opaque 3-part JWT", token)
	}
	if expires.Before(before.Add(59*time.Minute)) || expires.After(before.Add(61*time.Minute)) {
		t.Fatalf("expireTime %v not ~1h from now", expires)
	}
	claims := jwtClaims(t, token)
	if claims["email"] != testEmail || claims["sub"] != testEmail {
		t.Errorf("identity claims = %v", claims)
	}
	if claims["project_id"] != testProject {
		t.Errorf("project_id = %v, want %s", claims["project_id"], testProject)
	}
	exp, _ := claims["exp"].(float64)
	iat, _ := claims["iat"].(float64)
	if exp-iat != 3600 {
		t.Errorf("exp-iat = %v, want 3600", exp-iat)
	}
	scope, _ := claims["scope"].([]any)
	if len(scope) != 1 || scope[0] != cloudScope {
		t.Errorf("scope = %v", claims["scope"])
	}
}

func TestGenerateAccessTokenDefaultLifetime(t *testing.T) {
	s := newServiceWithAccount(t)
	token, _, err := s.GenerateAccessToken(context.Background(), "-", testEmail, []string{cloudScope}, 0)
	if err != nil {
		t.Fatalf("GenerateAccessToken: %v", err)
	}
	claims := jwtClaims(t, token)
	exp, _ := claims["exp"].(float64)
	iat, _ := claims["iat"].(float64)
	if exp-iat != 3600 {
		t.Errorf("default lifetime exp-iat = %v, want 3600", exp-iat)
	}
}

func TestGenerateAccessTokenValidation(t *testing.T) {
	s := newServiceWithAccount(t)
	ctx := context.Background()
	if _, _, err := s.GenerateAccessToken(ctx, "-", testEmail, nil, time.Hour); err == nil {
		t.Error("expected an error for empty scopes")
	} else if pe, ok := err.(*model.ProviderError); !ok || pe.HTTPStatus != 400 {
		t.Errorf("empty scopes error = %v, want 400 ProviderError", err)
	}
	if _, _, err := s.GenerateAccessToken(ctx, "-", testEmail, []string{cloudScope}, 13*time.Hour); err == nil {
		t.Error("expected an error for a lifetime over 12h")
	}
	if _, _, err := s.GenerateAccessToken(ctx, "-", testEmail, []string{cloudScope}, -time.Second); err == nil {
		t.Error("expected an error for a negative lifetime")
	}
}

func TestGenerateAccessTokenReusesKey(t *testing.T) {
	ctx := context.Background()
	s := newServiceWithAccount(t)
	if _, _, err := s.GenerateAccessToken(ctx, "-", testEmail, []string{cloudScope}, time.Hour); err != nil {
		t.Fatalf("first mint: %v", err)
	}
	// A wildcard request stores the key under the project from the email.
	if _, err := serviceaccount.EnsureKey(ctx, s.resources, testProject, testEmail); err != nil {
		t.Fatalf("key not stored under %s: %v", testProject, err)
	}
}

func TestGenerateIDToken(t *testing.T) {
	s := newServiceWithAccount(t)
	ctx := context.Background()
	token, err := s.GenerateIDToken(ctx, "-", testEmail, "https://example.com", true)
	if err != nil {
		t.Fatalf("GenerateIDToken: %v", err)
	}
	claims := jwtClaims(t, token)
	if claims["iss"] != "https://accounts.google.com" {
		t.Errorf("iss = %v", claims["iss"])
	}
	if claims["aud"] != "https://example.com" {
		t.Errorf("aud = %v", claims["aud"])
	}
	if claims["email"] != testEmail || claims["email_verified"] != true {
		t.Errorf("email claims = %v", claims)
	}

	noEmail, err := s.GenerateIDToken(ctx, "-", testEmail, "https://example.com", false)
	if err != nil {
		t.Fatalf("GenerateIDToken: %v", err)
	}
	if _, ok := jwtClaims(t, noEmail)["email"]; ok {
		t.Error("email claim present without includeEmail")
	}

	if _, err := s.GenerateIDToken(ctx, "-", testEmail, "", false); err == nil {
		t.Error("expected an error for a missing audience")
	}
}

func TestSignBlob(t *testing.T) {
	s := newServiceWithAccount(t)
	ctx := context.Background()
	keyID, sig, err := s.SignBlob(ctx, "-", testEmail, []byte("hello"))
	if err != nil {
		t.Fatalf("SignBlob: %v", err)
	}
	if keyID == "" || len(sig) == 0 {
		t.Fatalf("empty signature: keyID=%q len=%d", keyID, len(sig))
	}
	k, err := serviceaccount.EnsureKey(ctx, s.resources, testProject, testEmail)
	if err != nil {
		t.Fatalf("load key: %v", err)
	}
	if k.KeyID != keyID {
		t.Errorf("keyID = %q, want %q", keyID, k.KeyID)
	}
	privDER, _ := k.PrivDER()
	priv, err := x509.ParsePKCS8PrivateKey(privDER)
	if err != nil {
		t.Fatalf("parse key: %v", err)
	}
	digest := sha256.Sum256([]byte("hello"))
	if err := rsa.VerifyPKCS1v15(priv.(*rsa.PrivateKey).Public().(*rsa.PublicKey), crypto.SHA256, digest[:], sig); err != nil {
		t.Fatalf("signature does not verify: %v", err)
	}
	if _, _, err := s.SignBlob(ctx, "-", testEmail, nil); err == nil {
		t.Error("expected an error for an empty payload")
	}
}

func TestSignJWT(t *testing.T) {
	s := newServiceWithAccount(t)
	ctx := context.Background()
	keyID, signed, err := s.SignJWT(ctx, "-", testEmail, `{"iss":"test"}`)
	if err != nil {
		t.Fatalf("SignJWT: %v", err)
	}
	if keyID == "" || strings.Count(signed, ".") != 2 {
		t.Fatalf("bad signed JWT: keyID=%q jwt=%q", keyID, signed)
	}
	if jwtClaims(t, signed)["iss"] != "test" {
		t.Errorf("payload not preserved: %v", signed)
	}

	if _, _, err := s.SignJWT(ctx, "-", testEmail, "not json"); err == nil {
		t.Error("expected an error for a non-JSON payload")
	}
	past := time.Now().Add(-time.Hour).Unix()
	if _, _, err := s.SignJWT(ctx, "-", testEmail, `{"exp":`+itoa(past)+`}`); err == nil {
		t.Error("expected an error for an exp in the past")
	}
	future := time.Now().Add(13 * time.Hour).Unix()
	if _, _, err := s.SignJWT(ctx, "-", testEmail, `{"exp":`+itoa(future)+`}`); err == nil {
		t.Error("expected an error for an exp more than 12h out")
	}
	ok := time.Now().Add(time.Hour).Unix()
	if _, _, err := s.SignJWT(ctx, "-", testEmail, `{"exp":`+itoa(ok)+`}`); err != nil {
		t.Errorf("valid exp rejected: %v", err)
	}
}

// TestMissingServiceAccountIsNotFound covers J67: every IAM Credentials method
// requires the account to exist. The wildcard "projects/-" used to resolve only
// the project from the email and materialise a signing key, so a service account
// that was never created could still mint tokens and signatures.
func TestMissingServiceAccountIsNotFound(t *testing.T) {
	ctx := context.Background()
	s := newService()
	missing := "ghost@test-project.iam.gserviceaccount.com"

	calls := map[string]func() error{
		"GenerateAccessToken": func() error {
			_, _, err := s.GenerateAccessToken(ctx, "-", missing, []string{cloudScope}, time.Hour)
			return err
		},
		"GenerateIdToken": func() error {
			_, err := s.GenerateIDToken(ctx, "-", missing, "https://example.com", false)
			return err
		},
		"SignBlob": func() error {
			_, _, err := s.SignBlob(ctx, "-", missing, []byte("hello"))
			return err
		},
		"SignJwt": func() error {
			_, _, err := s.SignJWT(ctx, "-", missing, `{"iss":"test"}`)
			return err
		},
	}
	for name, call := range calls {
		pe, ok := call().(*model.ProviderError)
		if !ok {
			t.Fatalf("%s: error is not a *model.ProviderError", name)
		}
		if pe.HTTPStatus != 404 || pe.Code != "NotFound" {
			t.Errorf("%s = %v (status %d), want 404 NotFound", name, pe, pe.HTTPStatus)
		}
	}
	// A refused request must not leave a key behind for the phantom account.
	entries, err := s.resources.List(ctx, testProject, store.GlobalRegion, serviceaccount.ResourceType, "")
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("a refused request materialised %d key(s)", len(entries))
	}
}

func itoa(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}
