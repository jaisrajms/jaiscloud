package storage

import (
	"context"
	"testing"

	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// TestGRPCCreateBucketValidatesStorageClassAndLocation is the gRPC half of
// TF5-1: the closed sets are the same on both transports, so a create naming an
// undocumented storage class, location or public access prevention mode is
// INVALID_ARGUMENT before anything is stored, and the documented values still
// create.
func TestGRPCCreateBucketValidatesStorageClassAndLocation(t *testing.T) {
	ctx := context.Background()
	client, cleanup := storageTestService(t)
	defer cleanup()

	create := func(id string, b *storagepb.Bucket) error {
		_, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
			Parent:   "projects/_",
			BucketId: id,
			Bucket:   b,
		})
		return err
	}

	for _, tc := range []struct {
		name   string
		bucket *storagepb.Bucket
	}{
		{"invalid storage class", &storagepb.Bucket{Location: "US", StorageClass: "BOGUS"}},
		{"lowercase storage class", &storagepb.Bucket{Location: "US", StorageClass: "standard"}},
		{"invalid location", &storagepb.Bucket{Location: "NOT-A-LOCATION"}},
		{"invalid public access prevention",
			&storagepb.Bucket{Location: "US", IamConfig: &storagepb.Bucket_IamConfig{PublicAccessPrevention: "sometimes"}}},
	} {
		if err := create("grpc-invalid", tc.bucket); status.Code(err) != codes.InvalidArgument {
			t.Errorf("CreateBucket(%s) code = %v, want InvalidArgument (%v)", tc.name, status.Code(err), err)
		}
	}

	// The valid control: documented values on both fields create and read back.
	if err := create("grpc-valid", &storagepb.Bucket{Location: "us-central1", StorageClass: "NEARLINE"}); err != nil {
		t.Fatalf("CreateBucket(valid) = %v, want nil", err)
	}
	got, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: "projects/_/buckets/grpc-valid"})
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if got.GetLocation() != "US-CENTRAL1" || got.GetStorageClass() != "NEARLINE" {
		t.Errorf("created bucket = location %q / storage class %q, want US-CENTRAL1 / NEARLINE",
			got.GetLocation(), got.GetStorageClass())
	}
}

// TestGRPCUpdateBucketValidatesStorageClass: the default storage class is
// mutable over gRPC too (update_mask storage_class), so an invalid value is
// INVALID_ARGUMENT and leaves the stored one unchanged.
func TestGRPCUpdateBucketValidatesStorageClass(t *testing.T) {
	ctx := context.Background()
	client, cleanup := storageTestService(t)
	defer cleanup()

	createBucket(t, client, "grpc-sc-update", "US")
	_, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket: &storagepb.Bucket{
			Name:         "projects/_/buckets/grpc-sc-update",
			StorageClass: "BOGUS",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"storage_class"}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("UpdateBucket(storage_class=BOGUS) code = %v, want InvalidArgument (%v)", status.Code(err), err)
	}

	got, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: "projects/_/buckets/grpc-sc-update"})
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if got.GetStorageClass() != "STANDARD" {
		t.Errorf("rejected update changed storage class to %q, want STANDARD", got.GetStorageClass())
	}

	updated, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket: &storagepb.Bucket{
			Name:         "projects/_/buckets/grpc-sc-update",
			StorageClass: "COLDLINE",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"storage_class"}},
	})
	if err != nil {
		t.Fatalf("UpdateBucket(storage_class=COLDLINE) = %v, want nil", err)
	}
	if updated.GetStorageClass() != "COLDLINE" {
		t.Errorf("valid update storage class = %q, want COLDLINE", updated.GetStorageClass())
	}
}
