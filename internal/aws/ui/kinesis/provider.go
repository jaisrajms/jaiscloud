package kinesisui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *kinesis.Provider used by UI handlers.
type ProviderInterface interface {
	ListStreams(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteStream(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DescribeStreamSummary(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
