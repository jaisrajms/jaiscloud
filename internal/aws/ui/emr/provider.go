package emrui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *emrprovider.EMRProvider used by UI handlers.
type ProviderInterface interface {
	ListClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DescribeCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	RunJobFlow(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	TerminateJobFlows(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListSteps(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DescribeStep(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	AddJobFlowSteps(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CancelSteps(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
