package s3ui

import (
	"context"

	"jaiscloud/internal/model"
)

// ProviderInterface is the subset of *object.ObjectProvider used by S3 UI handlers.
type ProviderInterface interface {
	ListBuckets(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CreateBucket(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteBucket(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListObjectsV2(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	HeadObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	PutObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	DeleteObjects(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	CopyObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetBucketVersioning(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	PutBucketVersioning(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListObjectVersions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	ListMultipartUploads(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	AbortMultipartUpload(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	GetBucketTagging(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	PutBucketTagging(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
	HeadBucket(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error)
}
