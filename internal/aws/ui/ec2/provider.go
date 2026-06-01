package ec2ui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *compute.ComputeProvider used by UI handlers.
type ProviderInterface interface {
	DescribeInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	TerminateInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	StartInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	StopInstances(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
