package sqsui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *queue.QueueProvider used by UI handlers.
// Defined here so handler_test.go can mock it without importing the full provider.
type ProviderInterface interface {
	CreateQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListQueues(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetQueueUrl(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetQueueAttributes(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	PurgeQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	SendMessage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ReceiveMessage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteMessage(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListDeadLetterSourceQueues(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListQueueTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	PeekMessages(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
