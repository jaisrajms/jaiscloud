package computeui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *compute.Provider used by the Compute
// Engine UI handlers. It keeps the UI decoupled from the provider package.
type ProviderInterface interface {
	// InstancesAggregatedList lists instances across every zone for a project.
	InstancesAggregatedList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// InstancesGet reads a single zonal instance.
	InstancesGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// InstancesStart / InstancesStop transition a zonal instance's status and
	// return a compute#operation envelope.
	InstancesStart(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	InstancesStop(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// InstancesDelete removes a zonal instance and returns a compute#operation.
	InstancesDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
