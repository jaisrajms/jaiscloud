package kmsui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *key.KeyProvider used by KMS UI handlers.
type ProviderInterface interface {
	ListKeys(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DescribeKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	EnableKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DisableKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ScheduleKeyDeletion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CancelKeyDeletion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListAliases(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteAlias(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListResourceTags(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
