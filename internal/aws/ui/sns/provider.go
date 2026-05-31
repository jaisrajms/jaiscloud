package snsui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *notification.SNSProvider used by SNS UI handlers.
type ProviderInterface interface {
	ListTopics(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetTopicAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	Subscribe(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	Unsubscribe(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListSubscriptionsByTopic(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListSubscriptions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	Publish(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListTagsForResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
