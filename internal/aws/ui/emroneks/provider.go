package emroneksui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *emroneks.EMRContainersProvider used by UI handlers.
type ProviderInterface interface {
	ListVirtualClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DescribeVirtualCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateVirtualCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteVirtualCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListJobRuns(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DescribeJobRun(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	StartJobRun(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CancelJobRun(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
