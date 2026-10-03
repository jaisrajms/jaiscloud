// Package iamcredentials adapts the IAM Credentials core to the
// iamcredentials.googleapis.com v1 REST surface. Only the verbs unique to IAM
// Credentials (generateAccessToken, generateIdToken) are routed here;
// signBlob/signJwt share their path with iam.googleapis.com and stay on the
// iam provider (see internal/gcp/adapter/router.go).
package iamcredentials

import (
	"context"
	"strings"
	"time"

	"jaiscloud/internal/gcp/service/iamcredentials"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider is the REST adapter over the IAM Credentials core.
type Provider struct {
	core        *iamcredentials.Service
	defaultProj string
}

// NewProvider returns a REST provider over the shared core.
func NewProvider(c *iamcredentials.Service, defaultProj string) *Provider {
	return &Provider{core: c, defaultProj: defaultProj}
}

// Routes maps the IAMCredentials registry actions to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"IAMCredentials.GenerateAccessToken": p.GenerateAccessToken,
		"IAMCredentials.GenerateIdToken":     p.GenerateIdToken,
		"IAMCredentials.GetAllowedLocations": p.GetAllowedLocations,
	}
}

// account resolves the owning project: the request account (project) scope,
// else the configured default. The "projects/-" wildcard is resolved by the
// core from the service-account email.
func (p *Provider) account(nr *model.NormalizedRequest) string {
	if nr.AccountID != "" {
		return nr.AccountID
	}
	return p.defaultProj
}

// serviceAccountEmail extracts the email from a "serviceAccounts/{email}" name.
func serviceAccountEmail(nr *model.NormalizedRequest) string {
	name, _ := nr.Params["name"].(string)
	name = strings.TrimPrefix(name, "serviceAccounts/")
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
}

func bodyMap(nr *model.NormalizedRequest) map[string]any {
	m, _ := nr.Params["body"].(map[string]any)
	return m
}

func stringParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

func (p *Provider) GenerateAccessToken(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyMap(nr)
	scopes := stringSlice(body["scope"])
	lifetime, err := parseLifetime(body["lifetime"])
	if err != nil {
		return nil, err
	}
	token, expires, err := p.core.GenerateAccessToken(ctx, p.account(nr), serviceAccountEmail(nr), scopes, lifetime)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{
		"accessToken": token,
		"expireTime":  expires.Truncate(time.Second).Format(time.RFC3339),
	}), nil
}

func (p *Provider) GenerateIdToken(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyMap(nr)
	audience, _ := body["audience"].(string)
	if audience == "" {
		audience = stringParam(nr, "audience")
	}
	includeEmail, _ := body["includeEmail"].(bool)
	token, err := p.core.GenerateIDToken(ctx, p.account(nr), serviceAccountEmail(nr), audience, includeEmail)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"token": token}), nil
}

// GetAllowedLocations serves the Discovery getAllowedLocations methods for
// projects.serviceAccounts and projects.locations.workloadIdentityPools.
func (p *Provider) GetAllowedLocations(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	locations, err := p.core.AllowedLocations(ctx)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"locations": locations}), nil
}

// stringSlice coerces a JSON array (or a single string) into a []string.
func stringSlice(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, e := range t {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case string:
		if t == "" {
			return nil
		}
		return []string{t}
	}
	return nil
}

// parseLifetime accepts a google-duration string ("3600s") or a numeric number
// of seconds, and returns 0 when absent.
func parseLifetime(v any) (time.Duration, error) {
	switch t := v.(type) {
	case nil:
		return 0, nil
	case string:
		if strings.TrimSpace(t) == "" {
			return 0, nil
		}
		d, err := time.ParseDuration(t)
		if err != nil {
			return 0, model.NewProviderError("InvalidArgument", "lifetime must be a duration such as \"3600s\"", 400)
		}
		if d <= 0 {
			return 0, model.NewProviderError("InvalidArgument", "lifetime must be positive", 400)
		}
		return d, nil
	case float64:
		if t <= 0 {
			return 0, model.NewProviderError("InvalidArgument", "lifetime must be positive", 400)
		}
		return time.Duration(t) * time.Second, nil
	}
	return 0, model.NewProviderError("InvalidArgument", "invalid lifetime", 400)
}
