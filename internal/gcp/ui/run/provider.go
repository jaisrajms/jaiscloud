package runui

import (
	"context"

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
}

// compile-time check that the core satisfies the UI seam.
var _ ProviderInterface = (*runcore.Service)(nil)
