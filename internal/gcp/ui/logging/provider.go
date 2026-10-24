package loggingui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of the Cloud Logging REST provider used by
// the Logging UI handlers. It keeps the UI decoupled from the provider package.
type ProviderInterface interface {
	// Entries and logs.
	EntryList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	LogList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	LogDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Logs-based metrics.
	MetricList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	MetricGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	MetricCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	MetricUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	MetricDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Sinks (log router).
	SinkList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	SinkGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	SinkCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	SinkPatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	SinkDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Resource-level exclusions.
	ExclusionList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ExclusionGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ExclusionCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ExclusionPatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ExclusionDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
