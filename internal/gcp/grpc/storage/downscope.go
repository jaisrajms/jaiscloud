package storage

import (
	"context"

	"jaiscloud/internal/gcp/downscope"
	"jaiscloud/internal/model"

	"google.golang.org/grpc/metadata"
)

// requireDownscope enforces a downscoped credential's access boundary on one
// gRPC Storage operation by reading the bearer token from the incoming RPC
// metadata. It is a no-op for ordinary requests and when no bearer is present.
func (s *Service) requireDownscope(ctx context.Context, op downscope.Op, bucket, name string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return nil
	}
	if err := downscope.Allowed(vals[0], op, bucket, name); err != nil {
		return mapError(model.NewProviderError("PermissionDenied", "Downscoped token does not allow this GCS operation", 403))
	}
	return nil
}

// requireBucketAdminDownscope denies a bucket-level RPC performed with a
// downscoped credential: an access-boundary rule grants object access only.
func (s *Service) requireBucketAdminDownscope(ctx context.Context) error {
	return s.requireDownscope(ctx, downscope.BucketAdmin, "", "")
}
