package glueui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *catalog.GlueProvider used by UI handlers.
type ProviderInterface interface {
	GetDatabases(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateDatabase(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteDatabase(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetTables(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetJobs(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	StartJobRun(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetJobRuns(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetCrawlers(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateCrawler(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteCrawler(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	StartCrawler(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
