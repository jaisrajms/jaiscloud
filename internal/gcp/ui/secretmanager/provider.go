package secretmanagerui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *secretmanager.Provider used by the Secret
// Manager UI handlers.
type ProviderInterface interface {
	Create(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	List(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	Get(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	Update(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	Delete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	AddVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	Access(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListVersions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DestroyVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DisableVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	EnableVersion(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	GetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	SetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	TestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
