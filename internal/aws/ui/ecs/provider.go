package ecsui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *container.ContainerProvider used by UI handlers.
type ProviderInterface interface {
	ListClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DescribeClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListServices(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListTasks(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	RunTask(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	StopTask(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
