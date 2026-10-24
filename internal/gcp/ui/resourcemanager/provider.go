package resourcemanagerui

import (
	"context"

	resourcemanagercore "jaiscloud/internal/gcp/service/resourcemanager"
)

// ProviderInterface is the subset of *resourcemanager.Service used by the
// Resource Manager UI handlers. The core is transport-neutral and addressable
// directly, so the console reuses its typed API rather than going through the
// REST adapter. It keeps the UI decoupled from the core's concrete type.
type ProviderInterface interface {
	// ListProjects returns a cursor page of projects: every created project
	// unioned with the configured default + extra projects, sorted by id.
	// DELETE_REQUESTED projects are omitted unless showDeleted is set.
	ListProjects(ctx context.Context, pageSize int, pageToken string, showDeleted bool, filter string) ([]resourcemanagercore.Project, string, error)

	// GetProject reads a project, synthesizing an ACTIVE placeholder for an id
	// that was never explicitly created.
	GetProject(ctx context.Context, project string) (resourcemanagercore.Project, error)

	// CreateProject validates and registers a new project.
	CreateProject(ctx context.Context, in resourcemanagercore.CreateProjectInput) (resourcemanagercore.Project, resourcemanagercore.Operation, error)

	// DeleteProject marks a project DELETE_REQUESTED.
	DeleteProject(ctx context.Context, project string) (resourcemanagercore.Project, resourcemanagercore.Operation, error)

	// UndeleteProject restores a DELETE_REQUESTED project to ACTIVE.
	UndeleteProject(ctx context.Context, project string) (resourcemanagercore.Project, resourcemanagercore.Operation, error)
}

// compile-time check that the core satisfies the UI seam.
var _ ProviderInterface = (*resourcemanagercore.Service)(nil)
