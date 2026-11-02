package runui

import (
	"context"

	"jaiscloud/internal/gcp/policy"
	runcore "jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
)

// ProviderInterface is the subset of *run.Service used by the Cloud Run UI
// handlers. The core is transport-neutral and addressable directly, so the UI
// reuses its typed API and rendering helpers rather than going through the REST
// adapter. It keeps the UI decoupled from the core's concrete type.
type ProviderInterface interface {
	// ListAllServices lists every service in a project across all locations,
	// sorted by location then id. It backs the region-optional console list.
	ListAllServices(ctx context.Context, project string) ([]runstore.Service, error)

	// GetService reads a single service in a location.
	GetService(ctx context.Context, project, location, id string) (runstore.Service, error)

	// Revisions of a service.
	ListRevisions(ctx context.Context, project, location, service string) ([]runstore.Revision, error)
	GetRevision(ctx context.Context, project, location, service, id string) (runstore.Revision, error)

	// DeleteService removes a service and its revisions, returning the delete
	// operation. The console issues an unconditional delete (validateOnly false,
	// no etag precondition), but the signature follows the core's OCC/flags API.
	DeleteService(ctx context.Context, project, location, id string, validateOnly bool, etag string) (runstore.Operation, error)

	// CreateService deploys a service from the caller's Service body and returns
	// it as read back after the operation. With validateOnly the core only
	// validates, so an empty record is returned.
	CreateService(ctx context.Context, project, location, id string, body map[string]any, validateOnly bool) (runstore.Service, error)

	// UpdateService patches a service with a comma-separated field mask and
	// returns it as read back. Mask paths are passed to the core verbatim.
	UpdateService(ctx context.Context, project, location, id string, body map[string]any, mask string, validateOnly, allowMissing bool) (runstore.Service, error)

	// Service IAM. Policies cross the seam as decoded maps so the UI does not
	// depend on the policy package.
	ServiceGetIamPolicy(ctx context.Context, project, location, id string) (map[string]any, error)
	ServiceSetIamPolicy(ctx context.Context, project, location, id string, body map[string]any) (map[string]any, error)
	ServiceTestIamPermissions(ctx context.Context, project, location, id string, permissions []string) ([]string, error)
}

// compile-time check that Provider satisfies the UI seam.
var _ ProviderInterface = (*Provider)(nil)

// Provider adapts the Cloud Run core onto ProviderInterface. Mutations discard
// the long-running operation and read the record back.
type Provider struct {
	svc *runcore.Service
}

// NewProvider returns a UI provider over the Cloud Run core.
func NewProvider(svc *runcore.Service) *Provider { return &Provider{svc: svc} }

// ListAllServices implements ProviderInterface.
func (p *Provider) ListAllServices(ctx context.Context, project string) ([]runstore.Service, error) {
	return p.svc.ListAllServices(ctx, project)
}

// GetService implements ProviderInterface.
func (p *Provider) GetService(ctx context.Context, project, location, id string) (runstore.Service, error) {
	return p.svc.GetService(ctx, project, location, id)
}

// ListRevisions implements ProviderInterface.
func (p *Provider) ListRevisions(ctx context.Context, project, location, service string) ([]runstore.Revision, error) {
	return p.svc.ListRevisions(ctx, project, location, service)
}

// GetRevision implements ProviderInterface.
func (p *Provider) GetRevision(ctx context.Context, project, location, service, id string) (runstore.Revision, error) {
	return p.svc.GetRevision(ctx, project, location, service, id)
}

// DeleteService implements ProviderInterface; the operation is discarded.
func (p *Provider) DeleteService(ctx context.Context, project, location, id string, validateOnly bool, etag string) (runstore.Operation, error) {
	return p.svc.DeleteService(ctx, project, location, id, validateOnly, etag)
}

// CreateService implements ProviderInterface; the operation is discarded and
// the created service is read back.
func (p *Provider) CreateService(ctx context.Context, project, location, id string, body map[string]any, validateOnly bool) (runstore.Service, error) {
	if _, err := p.svc.CreateService(ctx, project, location, id, body, validateOnly); err != nil {
		return runstore.Service{}, err
	}
	if validateOnly {
		return runstore.Service{}, nil
	}
	return p.svc.GetService(ctx, project, location, id)
}

// UpdateService implements ProviderInterface; the operation is discarded and
// the updated service is read back.
func (p *Provider) UpdateService(ctx context.Context, project, location, id string, body map[string]any, mask string, validateOnly, allowMissing bool) (runstore.Service, error) {
	if _, err := p.svc.UpdateService(ctx, project, location, id, body, mask, validateOnly, allowMissing); err != nil {
		return runstore.Service{}, err
	}
	if validateOnly {
		return runstore.Service{}, nil
	}
	return p.svc.GetService(ctx, project, location, id)
}

// ServiceGetIamPolicy implements ProviderInterface.
func (p *Provider) ServiceGetIamPolicy(ctx context.Context, project, location, id string) (map[string]any, error) {
	pol, err := p.svc.ServiceGetIamPolicy(ctx, project, location, id)
	if err != nil {
		return nil, err
	}
	return policy.ToMap(pol), nil
}

// ServiceSetIamPolicy implements ProviderInterface.
func (p *Provider) ServiceSetIamPolicy(ctx context.Context, project, location, id string, body map[string]any) (map[string]any, error) {
	pol, err := p.svc.ServiceSetIamPolicy(ctx, project, location, id, body)
	if err != nil {
		return nil, err
	}
	return policy.ToMap(pol), nil
}

// ServiceTestIamPermissions implements ProviderInterface.
func (p *Provider) ServiceTestIamPermissions(ctx context.Context, project, location, id string, permissions []string) ([]string, error) {
	return p.svc.ServiceTestIamPermissions(ctx, project, location, id, permissions)
}
