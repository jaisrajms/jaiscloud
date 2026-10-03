package resourcemanager

import (
	"context"

	"jaiscloud/internal/gcp/policy"
	core "jaiscloud/internal/gcp/service/resourcemanager"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider handles the Cloud Resource Manager v1 REST surface. It is a thin
// adapter: every handler resolves the NormalizedRequest params into the core's
// typed API, calls the shared core Service, and encodes the result as
// Discovery-shaped v1 JSON. No business logic lives here.
type Provider struct {
	core        *core.Service
	defaultProj string
}

// NewProvider returns a Resource Manager REST provider over the shared core.
func NewProvider(c *core.Service, defaultProj string) *Provider {
	return &Provider{core: c, defaultProj: defaultProj}
}

// Routes maps "ResourceManager.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"ResourceManager.ProjectGet":                p.GetProject,
		"ResourceManager.ProjectGetIamPolicy":       p.GetIamPolicy,
		"ResourceManager.ProjectSetIamPolicy":       p.SetIamPolicy,
		"ResourceManager.ProjectTestIamPermissions": p.TestIamPermissions,
	}
}

// project resolves the owning project: the path project, else the request's
// account (project) scope, else the configured default.
func (p *Provider) project(nr *model.NormalizedRequest) string {
	if s := strParam(nr, "project"); s != "" {
		return s
	}
	if nr.AccountID != "" {
		return nr.AccountID
	}
	return p.defaultProj
}

// GetProject returns the v1 Project shape (projectId, projectNumber, name =
// displayName, lifecycleState = state).
func (p *Provider) GetProject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	proj, err := p.core.GetProject(ctx, p.project(nr))
	if err != nil {
		return nil, err
	}
	out := map[string]any{
		"projectId":      proj.ProjectID,
		"projectNumber":  proj.ProjectNumber,
		"name":           proj.DisplayName,
		"lifecycleState": proj.State,
	}
	if len(proj.Labels) > 0 {
		out["labels"] = proj.Labels
	}
	return provider.OK(out), nil
}

// GetIamPolicy returns the stored project policy (or an empty default policy).
func (p *Provider) GetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	pol, err := p.core.GetIamPolicy(ctx, p.project(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

// SetIamPolicy stores the project policy. The v1 request wraps the policy in a
// "policy" field (SetIamPolicyRequest); a bare policy body is also accepted.
func (p *Provider) SetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	if nested, ok := body["policy"].(map[string]any); ok {
		body = nested
	}
	bindings, _ := body["bindings"].([]any)
	etag, _ := body["etag"].(string)
	pol, err := p.core.SetIamPolicy(ctx, p.project(nr), core.PolicyInput{
		Bindings: bindings,
		Etag:     etag,
		Version:  intOf(body["version"]),
	})
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

// TestIamPermissions echoes the requested permissions.
func (p *Provider) TestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	perms, err := p.core.TestIamPermissions(ctx, p.project(nr), policy.Permissions(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"permissions": perms}), nil
}
