package cloudwatchui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *cloudwatch.Provider used by UI handlers.
type ProviderInterface interface {
	ListMetrics(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetMetricStatistics(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	PutMetricAlarm(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DescribeAlarms(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteAlarms(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	SetAlarmState(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	EnableAlarmActions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DisableAlarmActions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	PutDashboard(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetDashboard(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListDashboards(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteDashboards(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
