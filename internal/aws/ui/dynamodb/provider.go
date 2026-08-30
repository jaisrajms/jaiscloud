package dynamodbui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *table.TableProvider used by DynamoDB UI handlers.
type ProviderInterface interface {
	ListTables(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DescribeTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteTable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	Scan(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	Query(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	PutItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteItem(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DescribeTimeToLive(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListTagsOfResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
