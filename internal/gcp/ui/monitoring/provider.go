package monitoringui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of the Cloud Monitoring REST provider used by
// the Monitoring UI handlers. It keeps the UI decoupled from the provider package.
type ProviderInterface interface {
	// Metrics.
	ListMetricDescriptors(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListTimeSeries(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Alerting.
	ListAlertPolicies(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetAlertPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateAlertPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	UpdateAlertPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteAlertPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Notification channels.
	ListNotificationChannels(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetNotificationChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateNotificationChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	UpdateNotificationChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteNotificationChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListNotificationChannelDescriptors(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Service Monitoring: services.
	ListServices(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	UpdateService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Service Monitoring: service level objectives.
	ListServiceLevelObjectives(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetServiceLevelObjective(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateServiceLevelObjective(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	UpdateServiceLevelObjective(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteServiceLevelObjective(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
