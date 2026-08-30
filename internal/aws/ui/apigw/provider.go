package apigwui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *apigw.GatewayProvider used by UI handlers.
type ProviderInterface interface {
	GetRestApis(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateRestApi(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteRestApi(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetResources(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetDeployments(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateDeployment(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetStages(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
