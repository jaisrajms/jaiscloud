package ssmui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *parameter.ParameterProvider used by SSM UI handlers.
type ProviderInterface interface {
	DescribeParameters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetParameter(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetParametersByPath(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	PutParameter(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteParameter(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetParameterHistory(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
