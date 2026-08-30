package eksui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *eks.EKSProvider used by UI handlers.
type ProviderInterface interface {
	ListClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DescribeCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
