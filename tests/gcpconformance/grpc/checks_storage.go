package grpcconformance

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// storageChecks covers the Cloud Storage gRPC v2 surface (google.storage.v2.Storage)
// via the official cloud.google.com/go/storage gRPC client.
func storageChecks() []Check {
	checks := []Check{
		{Service: "storage", RPC: "CreateBucket", KeyField: "success", Run: checkStorageCreateBucket},
		{Service: "storage", RPC: "GetBucket", KeyField: "name", Run: checkStorageGetBucket},
		{Service: "storage", RPC: "ListBuckets", KeyField: "buckets[].name", Run: checkStorageListBuckets},
		{Service: "storage", RPC: "DeleteBucket", KeyField: "success", Run: checkStorageDeleteBucket},
	}
	return append(checks, storageExtraChecks()...)
}

func storageBucketName(cfg Config) string { return cfg.ResourceName("gcpc-grpc-bucket") }

func newStorageClient(ctx context.Context, cfg Config, extra ...option.ClientOption) (*storage.Client, error) {
	opts := []option.ClientOption{
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
		storage.WithDisabledClientMetrics(),
	}
	opts = append(opts, extra...)
	return storage.NewGRPCClient(ctx, opts...)
}

func checkStorageCreateBucket(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	return client.Bucket(storageBucketName(cfg)).Create(ctx, cfg.Project, &storage.BucketAttrs{
		Location: "US",
		Labels:   map[string]string{"suite": "grpc-conformance"},
	})
}

func checkStorageGetBucket(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	name := storageBucketName(cfg)
	attrs, err := client.Bucket(name).Attrs(ctx)
	if err != nil {
		return err
	}
	if attrs.Name != name {
		return fmt.Errorf("GetBucket returned name %q, want %q", attrs.Name, name)
	}
	return nil
}

func checkStorageListBuckets(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	name := storageBucketName(cfg)
	it := client.Buckets(ctx, cfg.Project)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		if attrs.Name == name {
			return nil
		}
	}
	return fmt.Errorf("ListBuckets did not include %q", name)
}

func checkStorageDeleteBucket(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	return client.Bucket(storageBucketName(cfg)).Delete(ctx)
}
