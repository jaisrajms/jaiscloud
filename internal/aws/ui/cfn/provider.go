package cfnui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *stack.StackProvider used by UI handlers.
type ProviderInterface interface {
	ListStacks(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateStack(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DescribeStacks(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteStack(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
