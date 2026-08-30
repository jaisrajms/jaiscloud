package elasticacheui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *cache.CacheProvider used by UI handlers.
type ProviderInterface interface {
	DescribeCacheClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateCacheCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteCacheCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
