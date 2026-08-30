package eventbridgeui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *events.EventBridgeProvider used by UI handlers.
type ProviderInterface interface {
	ListRules(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	PutRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	EnableRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DisableRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListTargetsByRule(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	PutTargets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	RemoveTargets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	PutEvents(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListEventBuses(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateEventBus(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteEventBus(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
