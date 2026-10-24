package pubsubui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *pubsub.Provider used by the Pub/Sub UI
// handlers. It keeps the UI decoupled from the provider package.
type ProviderInterface interface {
	// Topics.
	TopicList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	TopicCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	TopicGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	TopicDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	TopicPublish(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Subscriptions.
	SubscriptionList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	SubscriptionCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	SubscriptionGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	SubscriptionUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	SubscriptionDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// IAM policies.
	TopicGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	TopicSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	SubscriptionGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	SubscriptionSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
