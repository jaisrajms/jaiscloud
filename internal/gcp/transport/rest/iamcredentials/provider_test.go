package iamcredentials

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	core "jaiscloud/internal/gcp/service/iamcredentials"
	"jaiscloud/internal/gcp/serviceaccount"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

const (
	testEmail      = "compat@test-project.iam.gserviceaccount.com"
	delegateEmail  = "delegate@test-project.iam.gserviceaccount.com"
	delegate2Email = "delegate2@test-project.iam.gserviceaccount.com"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "-", Params: params}
}

// newProvider returns a REST provider whose service accounts exist in the IAM
// registry, as they would after projects.serviceAccounts.create.
func newProvider(t *testing.T) *Provider {
	t.Helper()
	res := store.NewMemoryResourceStore()
	ctx := context.Background()
	for _, email := range []string{testEmail, delegateEmail, delegate2Email} {
		if err := res.Create(ctx, "test-project", store.GlobalRegion, store.ResourceEntry{
			Type: serviceaccount.AccountResourceType,
			ID:   email,
			Data: []byte(`{"email":"` + email + `"}`),
		}); err != nil {
			t.Fatalf("seed service account %s: %v", email, err)
		}
	}
	return NewProvider(core.New(res), "test-project")
}

func TestGenerateAccessTokenRoute(t *testing.T) {
	p := newProvider(t)
	nr := newNR(map[string]any{
		"name": "serviceAccounts/" + testEmail,
		"body": map[string]any{
			"scope":    []any{"https://www.googleapis.com/auth/cloud-platform"},
			"lifetime": "3600s",
		},
	})
	resp, err := p.GenerateAccessToken(context.Background(), nr)
	if err != nil {
		t.Fatalf("GenerateAccessToken: %v", err)
	}
	token, _ := resp.Data["accessToken"].(string)
	if strings.Count(token, ".") != 2 {
		t.Errorf("accessToken = %q, want an opaque 3-part JWT", token)
	}
	exp, _ := resp.Data["expireTime"].(string)
	parsed, err := time.Parse(time.RFC3339, exp)
	if err != nil {
		t.Fatalf("expireTime %q is not RFC3339: %v", exp, err)
	}
	if !parsed.After(time.Now()) {
		t.Errorf("expireTime %v is not in the future", parsed)
	}
}

// TestGetAllowedLocationsRoute covers J65: the iamcredentials Discovery
// getAllowedLocations methods return a non-empty locations list for both the
// serviceAccounts and workloadIdentityPools paths.
func TestGetAllowedLocationsRoute(t *testing.T) {
	p := newProvider(t)
	for _, name := range []string{
		"serviceAccounts/" + testEmail,
		"projects/test-project/locations/us-central1/workloadIdentityPools/pool",
	} {
		resp, err := p.GetAllowedLocations(context.Background(), newNR(map[string]any{"name": name}))
		if err != nil {
			t.Fatalf("GetAllowedLocations(%q): %v", name, err)
		}
		locs, _ := resp.Data["locations"].([]string)
		if len(locs) == 0 {
			t.Fatalf("GetAllowedLocations(%q) returned no locations", name)
		}
	}
}

func TestGenerateAccessTokenBadLifetime(t *testing.T) {
	for _, lifetime := range []any{"not-a-duration", "0s", float64(0), float64(-5)} {
		p := newProvider(t)
		nr := newNR(map[string]any{
			"name": "serviceAccounts/" + testEmail,
			"body": map[string]any{"scope": []any{"a"}, "lifetime": lifetime},
		})
		_, err := p.GenerateAccessToken(context.Background(), nr)
		pe, ok := err.(*model.ProviderError)
		if !ok || pe.HTTPStatus != 400 {
			t.Fatalf("lifetime %v: error = %v, want 400 ProviderError", lifetime, err)
		}
	}
}

func TestGenerateIdTokenRoute(t *testing.T) {
	p := newProvider(t)
	nr := newNR(map[string]any{
		"name": "serviceAccounts/" + testEmail,
		"body": map[string]any{"audience": "https://example.com", "includeEmail": true},
	})
	resp, err := p.GenerateIdToken(context.Background(), nr)
	if err != nil {
		t.Fatalf("GenerateIdToken: %v", err)
	}
	if token, _ := resp.Data["token"].(string); strings.Count(token, ".") != 2 {
		t.Errorf("token is not a JWT: %q", token)
	}
}

// TestGenerateIdTokenOrganizationNumberRoute covers J68's org claim on REST: the
// field is read from the request body and lands in the documented `google`
// claim block.
func TestGenerateIdTokenOrganizationNumberRoute(t *testing.T) {
	p := newProvider(t)
	body := func(included bool) map[string]any {
		return map[string]any{"audience": "https://example.com", "organizationNumberIncluded": included}
	}

	resp, err := p.GenerateIdToken(context.Background(), newNR(map[string]any{
		"name": "serviceAccounts/" + testEmail, "body": body(true),
	}))
	if err != nil {
		t.Fatalf("GenerateIdToken: %v", err)
	}
	token, _ := resp.Data["token"].(string)
	claims := decodeClaims(t, token)
	google, ok := claims["google"].(map[string]any)
	if !ok {
		t.Fatalf("google claim = %v, want an object", claims["google"])
	}
	if v, present := google["organization_number"]; !present || v != nil {
		t.Errorf("google.organization_number = %v (present=%v), want an explicit null", v, present)
	}

	resp, err = p.GenerateIdToken(context.Background(), newNR(map[string]any{
		"name": "serviceAccounts/" + testEmail, "body": body(false),
	}))
	if err != nil {
		t.Fatalf("GenerateIdToken: %v", err)
	}
	if _, ok := decodeClaims(t, resp.Data["token"].(string))["google"]; ok {
		t.Error("google claim present without organizationNumberIncluded")
	}
}

// TestMissingServiceAccountIsNotFound covers J67 on REST: both verbs answer 404
// for a service account that does not exist.
func TestMissingServiceAccountIsNotFound(t *testing.T) {
	p := newProvider(t)
	missing := "serviceAccounts/ghost@test-project.iam.gserviceaccount.com"
	calls := map[string]func() error{
		"GenerateAccessToken": func() error {
			_, err := p.GenerateAccessToken(context.Background(), newNR(map[string]any{
				"name": missing, "body": map[string]any{"scope": []any{"a"}},
			}))
			return err
		},
		"GenerateIdToken": func() error {
			_, err := p.GenerateIdToken(context.Background(), newNR(map[string]any{
				"name": missing, "body": map[string]any{"audience": "https://example.com"},
			}))
			return err
		},
	}
	for name, call := range calls {
		pe, ok := call().(*model.ProviderError)
		if !ok || pe.HTTPStatus != 404 {
			t.Errorf("%s = %v, want 404 ProviderError", name, call())
		}
	}
}

// TestDelegationChainRoute covers J68 on REST: the body's delegates[] is
// validated, and a valid chain mints the target's credential.
func TestDelegationChainRoute(t *testing.T) {
	p := newProvider(t)
	valid := []any{
		"projects/-/serviceAccounts/" + delegateEmail,
		"projects/-/serviceAccounts/" + delegate2Email,
	}
	resp, err := p.GenerateAccessToken(context.Background(), newNR(map[string]any{
		"name": "serviceAccounts/" + testEmail,
		"body": map[string]any{"scope": []any{"a"}, "delegates": valid},
	}))
	if err != nil {
		t.Fatalf("delegated GenerateAccessToken: %v", err)
	}
	token, _ := resp.Data["accessToken"].(string)
	if claims := decodeClaims(t, token); claims["email"] != testEmail {
		t.Errorf("delegated token email = %v, want %s", claims["email"], testEmail)
	}

	bad := []any{"projects/test-project/serviceAccounts/" + delegateEmail}
	for name, call := range map[string]func() error{
		"GenerateAccessToken": func() error {
			_, err := p.GenerateAccessToken(context.Background(), newNR(map[string]any{
				"name": "serviceAccounts/" + testEmail,
				"body": map[string]any{"scope": []any{"a"}, "delegates": bad},
			}))
			return err
		},
		"GenerateIdToken": func() error {
			_, err := p.GenerateIdToken(context.Background(), newNR(map[string]any{
				"name": "serviceAccounts/" + testEmail,
				"body": map[string]any{"audience": "https://example.com", "delegates": bad},
			}))
			return err
		},
	} {
		pe, ok := call().(*model.ProviderError)
		if !ok || pe.HTTPStatus != 400 {
			t.Errorf("%s with a malformed delegate = %v, want 400 InvalidArgument", name, call())
		}
	}
}

// decodeClaims decodes a compact JWS payload without verifying it.
func decodeClaims(t *testing.T, token string) map[string]any {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("not a compact JWS: %q", token)
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode claims: %v", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatalf("unmarshal claims: %v", err)
	}
	return claims
}
