package sesui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *ses.Provider used by UI handlers.
type ProviderInterface interface {
	ListIdentities(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	VerifyEmailIdentity(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteIdentity(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetSendQuota(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
