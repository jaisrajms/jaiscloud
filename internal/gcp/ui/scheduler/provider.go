package schedulerui

import (
	"context"

	schedcore "jaiscloud/internal/gcp/service/scheduler"
	schedstore "jaiscloud/internal/gcp/store/scheduler"
)

// ProviderInterface is the subset of *scheduler.Service used by the Cloud
// Scheduler UI handlers. The core is transport-neutral and addressable
// directly, so the UI reuses its typed API rather than going through the REST
// adapter. It keeps the UI decoupled from the core's concrete type.
type ProviderInterface interface {
	// ListJobsByProject lists every job in a project across all locations,
	// sorted by location then id. It backs the location-optional console list.
	ListJobsByProject(ctx context.Context, project string) ([]schedstore.Job, error)

	// GetJob reads a single job.
	GetJob(ctx context.Context, project, location, name string) (schedstore.Job, error)

	// CreateJob validates and stores a new job.
	CreateJob(ctx context.Context, project, location string, j schedstore.Job) (schedstore.Job, error)

	// UpdateJob applies an update (an empty mask replaces every mutable field).
	UpdateJob(ctx context.Context, project, location, name string, upd schedstore.Job, mask []string) (schedstore.Job, error)

	// DeleteJob removes a job.
	DeleteJob(ctx context.Context, project, location, name string) error

	// PauseJob / ResumeJob transition a job's state and recompute its next fire.
	PauseJob(ctx context.Context, project, location, name string) (schedstore.Job, error)
	ResumeJob(ctx context.Context, project, location, name string) (schedstore.Job, error)

	// RunJob forces an immediate delivery attempt and records its outcome.
	RunJob(ctx context.Context, project, location, name string) (schedstore.Job, error)
}

// compile-time check that the core satisfies the UI seam.
var _ ProviderInterface = (*schedcore.Service)(nil)
