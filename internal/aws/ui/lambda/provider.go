package lambdaui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *lambda.FunctionProvider used by UI handlers.
type ProviderInterface interface {
	ListFunctions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	InvokeFunction(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	UpdateFunctionConfiguration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetFunctionConfiguration(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
