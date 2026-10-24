package iamcredentials

import (
	"context"
	"strings"
	"testing"
	"time"

	core "jaiscloud/internal/gcp/service/iamcredentials"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

const testEmail = "compat@test-project.iam.gserviceaccount.com"

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "-", Params: params}
}

func newProvider() *Provider {
	return NewProvider(core.New(store.NewMemoryResourceStore()), "test-project")
}

func TestGenerateAccessTokenRoute(t *testing.T) {
	p := newProvider()
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
	p := newProvider()
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
		p := newProvider()
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
	p := newProvider()
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
