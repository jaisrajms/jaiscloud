package elbv2ui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *elbv2.ELBv2Provider used by UI handlers.
type ProviderInterface interface {
	DescribeLoadBalancers(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateLoadBalancer(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteLoadBalancer(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DescribeTargetGroups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
