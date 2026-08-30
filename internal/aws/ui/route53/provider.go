package route53ui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *dns.DNSProvider used by UI handlers.
type ProviderInterface interface {
	ListHostedZones(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateHostedZone(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteHostedZone(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListResourceRecordSets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
