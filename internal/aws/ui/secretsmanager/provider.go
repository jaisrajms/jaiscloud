package secretsmanagerui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *secret.SecretProvider used by SecretsManager UI handlers.
type ProviderInterface interface {
	ListSecrets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DescribeSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetSecretValue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	PutSecretValue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	UpdateSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	RestoreSecret(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListSecretVersionIds(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	TagResource(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
