package storageui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *storage.Provider used by the Cloud
// Storage UI handlers. It keeps the UI decoupled from the provider package.
type ProviderInterface interface {
	// Buckets.
	BucketsList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	BucketsInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	BucketsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	BucketsUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	BucketsLockRetentionPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	BucketsDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Objects.
	ObjectsList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ObjectsInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ObjectsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ObjectsGetMedia(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ObjectsPatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ObjectsDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ObjectsRestore(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Bucket IAM + ACL.
	BucketsGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	BucketsSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	BucketACLList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	BucketACLInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)

	// Object IAM + ACL.
	ObjectsGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ObjectsSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ObjectACLList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ObjectACLInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
