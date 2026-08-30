package iamui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *iam.IAMProvider used by IAM UI handlers.
type ProviderInterface interface {
	// Roles
	ListRoles(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateRole(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetRole(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteRole(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	AttachRolePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DetachRolePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListAttachedRolePolicies(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Users
	ListUsers(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteUser(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListAccessKeys(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateAccessKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteAccessKey(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Policies
	ListPolicies(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreatePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeletePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
