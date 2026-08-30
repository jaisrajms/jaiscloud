package sfnui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *stepfunctions.Provider used by UI handlers.
type ProviderInterface interface {
	ListStateMachines(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateStateMachine(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteStateMachine(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	StartExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListExecutions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	StopExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetExecutionHistory(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
