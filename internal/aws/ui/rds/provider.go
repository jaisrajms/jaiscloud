package rdsui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *rds.RelationalProvider used by UI handlers.
type ProviderInterface interface {
	DescribeDBInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateDBInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteDBInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	StartDBInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	StopDBInstance(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
