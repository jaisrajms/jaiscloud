package iamcredentials

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
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
	s := newService()
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
	s := newService()
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
	s := newService()
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
	s := newService()
	if _, _, err := s.GenerateAccessToken(ctx, "-", testEmail, []string{cloudScope}, time.Hour); err != nil {
		t.Fatalf("first mint: %v", err)
	}
	// A wildcard request stores the key under the project from the email.
	if _, err := serviceaccount.EnsureKey(ctx, s.resources, testProject, testEmail); err != nil {
		t.Fatalf("key not stored under %s: %v", testProject, err)
	}
}

func TestGenerateIDToken(t *testing.T) {
	s := newService()
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
	s := newService()
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
	s := newService()
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

func itoa(v int64) string {
	b, _ := json.Marshal(v)
	return string(b)
}
