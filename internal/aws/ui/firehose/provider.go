package firehoseui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *firehose.Provider used by UI handlers.
type ProviderInterface interface {
	ListDeliveryStreams(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateDeliveryStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteDeliveryStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DescribeDeliveryStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
