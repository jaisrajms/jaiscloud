package logsui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *cwlogs.Provider used by UI handlers.
type ProviderInterface interface {
	CreateLogGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteLogGroup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DescribeLogGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DescribeLogStreams(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetLogEvents(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	FilterLogEvents(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	PutRetentionPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	StartQuery(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetQueryResults(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	StopQuery(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
