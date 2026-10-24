package resourcemanager

import (
	"context"

	"jaiscloud/internal/gcp/paging"
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
		"ResourceManager.ProjectList":               p.ListProjects,
		"ResourceManager.ProjectCreate":             p.CreateProject,
		"ResourceManager.ProjectUpdate":             p.UpdateProject,
		"ResourceManager.ProjectDelete":             p.DeleteProject,
		"ResourceManager.ProjectUndelete":           p.UndeleteProject,
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
	return provider.OK(projectToV1JSON(proj)), nil
}

// ListProjects returns a page of the v1 Project shape. The v1 contract keeps
// DELETE_REQUESTED projects visible to list until deletion completes (which the
// emulator never does), so showDeleted is always set; the request filter is
// evaluated by the core.
func (p *Provider) ListProjects(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	pageSize, err := paging.ParseSize(nr.Params["pageSize"])
	if err != nil {
		return nil, err
	}
	page, next, err := p.core.ListProjects(ctx, pageSize, strParam(nr, "pageToken"), true, strParam(nr, "filter"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, proj := range page {
		items = append(items, projectToV1JSON(proj))
	}
	resp := map[string]any{"projects": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

// CreateProject registers a project and returns the google.longrunning
// Operation the v1 API specifies (the created Project is the operation
// response; in the opt-in async mode it is in flight and polled through the
// shared /v1/operations/{id} route).
func (p *Provider) CreateProject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	proj, op, err := p.core.CreateProject(ctx, core.CreateProjectInput{
		ProjectID:   mapStr(body, "projectId"),
		DisplayName: mapStr(body, "name"),
		Parent:      parentString(body["parent"]),
		Labels:      labelMap(body["labels"]),
	})
	if err != nil {
		return nil, err
	}
	var response map[string]any
	if op.Done {
		response = projectToV1JSON(proj)
	}
	return provider.OK(operationToJSON(op, response)), nil
}

// UpdateProject applies the v1 projects.update. The v1 body is the Project
// resource (name = display name, labels), addressed by the path project; only
// the mutable fields present in the body are applied (a merge), so omitting a
// field leaves it unchanged rather than clearing it. The v1 response is the
// Project, not an operation.
func (p *Provider) UpdateProject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	project := p.project(nr)

	// The v1 Project.parent is mutable through update (the Discovery doc:
	// "Once set, the parent cannot be cleared"), which the core models as a
	// move. Apply it first so a rejected destination leaves metadata untouched.
	if raw, ok := body["parent"]; ok {
		dest := parentString(raw)
		if dest == "" {
			return nil, model.NewProviderError("InvalidArgument", "invalid parent", 400)
		}
		if _, _, err := p.core.MoveProject(ctx, project, dest); err != nil {
			return nil, err
		}
	}

	in := core.UpdateProjectInput{}
	if _, ok := body["name"]; ok {
		in.DisplayName = mapStr(body, "name")
		in.UpdateMask = append(in.UpdateMask, "display_name")
	}
	if _, ok := body["labels"]; ok {
		in.Labels = labelMap(body["labels"])
		in.UpdateMask = append(in.UpdateMask, "labels")
	}
	if len(in.UpdateMask) == 0 {
		// A parent-only (or empty) update: read back the current project.
		proj, err := p.core.GetProject(ctx, project)
		if err != nil {
			return nil, err
		}
		return provider.OK(projectToV1JSON(proj)), nil
	}
	proj, _, err := p.core.UpdateProject(ctx, project, in)
	if err != nil {
		return nil, err
	}
	return provider.OK(projectToV1JSON(proj)), nil
}

// DeleteProject marks a project for deletion. The v1 method returns Empty, not
// an operation.
func (p *Provider) DeleteProject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if _, _, err := p.core.DeleteProject(ctx, p.project(nr)); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// UndeleteProject restores a DELETE_REQUESTED project. The v1 method returns
// Empty, not an operation.
func (p *Provider) UndeleteProject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if _, _, err := p.core.UndeleteProject(ctx, p.project(nr)); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// ResolveOperation resolves a top-level google.longrunning operation name
// (operations/{id}) owned by Cloud Resource Manager. It is consulted by the
// Functions REST provider, which owns the /v1/operations/{id} route the two
// services share (project create operations are top-level). It returns
// handled=false for names outside the top-level shape and for ids that are not
// Resource Manager operations, so a genuine Functions unknown id still 404s
// through that provider.
func (p *Provider) ResolveOperation(ctx context.Context, project, name string) (map[string]any, bool, error) {
	if !isTopLevelOperationName(name) {
		return nil, false, nil
	}
	op, err := p.core.GetOperation(ctx, project, name)
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, true, err
	}
	var response map[string]any
	if op.Done {
		response = projectToV1JSON(op.Project)
	}
	return operationToJSON(op, response), true, nil
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
