package iamui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *iam.Provider used by the IAM UI handlers.
// It keeps the UI decoupled from the provider package.
type ProviderInterface interface {
	Create(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	List(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	Get(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	Update(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	Delete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	Disable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	Enable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	GetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	SetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	TestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	ServiceAccountKeyCreate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ServiceAccountKeyList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ServiceAccountKeyGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ServiceAccountKeyDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ServiceAccountKeyDisable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ServiceAccountKeyEnable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	ServiceAccountSignBlob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ServiceAccountSignJwt(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
