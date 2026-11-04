package storage

import (
	"context"
	"testing"

	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// iamConfigOfProto reads the two members the proto Bucket.IamConfig carries.
func iamConfigOfProto(t *testing.T, b *storagepb.Bucket) (uniform bool, pap string) {
	t.Helper()
	if b.GetIamConfig() == nil {
		t.Fatalf("bucket %q has no iam_config", b.GetName())
	}
	return b.GetIamConfig().GetUniformBucketLevelAccess().GetEnabled(),
		b.GetIamConfig().GetPublicAccessPrevention()
}

// TestGRPCBucketIamConfigRoundTrip is the gRPC half of DGD10: a bucket created
// with iam_config reads it back over gRPC, and a bucket created without it keeps
// real GCS's defaults (off / inherited) — the same values REST reports.
func TestGRPCBucketIamConfigRoundTrip(t *testing.T) {
	ctx := context.Background()
	client, cleanup := storageTestService(t)
	defer cleanup()

	created, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "grpc-uba",
		Bucket: &storagepb.Bucket{
			Project:  "projects/test-project",
			Location: "US",
			IamConfig: &storagepb.Bucket_IamConfig{
				UniformBucketLevelAccess: &storagepb.Bucket_IamConfig_UniformBucketLevelAccess{Enabled: true},
				PublicAccessPrevention:   "enforced",
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if uniform, pap := iamConfigOfProto(t, created); !uniform || pap != "enforced" {
		t.Fatalf("create response iam_config = uniform %v / pap %q, want true/enforced", uniform, pap)
	}

	got, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: "projects/_/buckets/grpc-uba"})
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if uniform, pap := iamConfigOfProto(t, got); !uniform || pap != "enforced" {
		t.Fatalf("read-back iam_config = uniform %v / pap %q, want true/enforced", uniform, pap)
	}

	// No iam_config on create: the GCS defaults, without a lock_time.
	plain, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "grpc-plain",
		Bucket:   &storagepb.Bucket{Project: "projects/test-project", Location: "US"},
	})
	if err != nil {
		t.Fatalf("CreateBucket(plain): %v", err)
	}
	if uniform, pap := iamConfigOfProto(t, plain); uniform || pap != "inherited" {
		t.Fatalf("default iam_config = uniform %v / pap %q, want false/inherited", uniform, pap)
	}
	if plain.GetIamConfig().GetUniformBucketLevelAccess().GetLockTime() != nil {
		t.Error("lock_time must not be set: the emulator does not model the 90-day lock window")
	}
}

// TestGRPCUpdateBucketIamConfig covers the masked update paths the official Go
// client sends, one member at a time: toggling uniform bucket-level access must
// leave public access prevention alone and vice versa.
func TestGRPCUpdateBucketIamConfig(t *testing.T) {
	ctx := context.Background()
	client, cleanup := storageTestService(t)
	defer cleanup()

	if _, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "grpc-patch",
		Bucket:   &storagepb.Bucket{Project: "projects/test-project", Location: "US"},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	name := "projects/_/buckets/grpc-patch"

	updated, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket: &storagepb.Bucket{
			Name: name,
			IamConfig: &storagepb.Bucket_IamConfig{
				UniformBucketLevelAccess: &storagepb.Bucket_IamConfig_UniformBucketLevelAccess{Enabled: true},
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"iam_config.uniform_bucket_level_access.enabled"}},
	})
	if err != nil {
		t.Fatalf("UpdateBucket(enable): %v", err)
	}
	if uniform, pap := iamConfigOfProto(t, updated); !uniform || pap != "inherited" {
		t.Fatalf("after enabling: uniform %v / pap %q, want true/inherited", uniform, pap)
	}

	updated, err = client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket: &storagepb.Bucket{
			Name:      name,
			IamConfig: &storagepb.Bucket_IamConfig{PublicAccessPrevention: "enforced"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"iam_config.public_access_prevention"}},
	})
	if err != nil {
		t.Fatalf("UpdateBucket(pap): %v", err)
	}
	if uniform, pap := iamConfigOfProto(t, updated); !uniform || pap != "enforced" {
		t.Fatalf("after enforcing PAP: uniform %v / pap %q, want true/enforced", uniform, pap)
	}

	// A masked update of an unrelated field leaves the configuration intact.
	updated, err = client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket:     &storagepb.Bucket{Name: name, StorageClass: "NEARLINE"},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"storage_class"}},
	})
	if err != nil {
		t.Fatalf("UpdateBucket(storage class): %v", err)
	}
	if uniform, pap := iamConfigOfProto(t, updated); !uniform || pap != "enforced" {
		t.Fatalf("storage-class update changed iam_config: uniform %v / pap %q", uniform, pap)
	}

	// A masked iam_config update that carries no message sets both members to
	// their proto3 zero values instead of keeping the old configuration.
	updated, err = client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket:     &storagepb.Bucket{Name: name},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"iam_config"}},
	})
	if err != nil {
		t.Fatalf("UpdateBucket(clear iam_config): %v", err)
	}
	if uniform, pap := iamConfigOfProto(t, updated); uniform || pap != "inherited" {
		t.Fatalf("after clearing iam_config: uniform %v / pap %q, want false/inherited", uniform, pap)
	}
}

// TestGRPCUpdateBucketIamConfigInvalidPublicAccessPrevention: the proto layer
// maps the provider's 400 onto InvalidArgument rather than accepting a mode GCS
// does not support.
func TestGRPCUpdateBucketIamConfigInvalidPublicAccessPrevention(t *testing.T) {
	ctx := context.Background()
	client, cleanup := storageTestService(t)
	defer cleanup()

	if _, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "grpc-bad-pap",
		Bucket: &storagepb.Bucket{
			Project:   "projects/test-project",
			Location:  "US",
			IamConfig: &storagepb.Bucket_IamConfig{PublicAccessPrevention: "sometimes"},
		},
	}); err == nil {
		t.Fatal("CreateBucket with invalid public_access_prevention: want error, got nil")
	} else if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateBucket error code = %v, want InvalidArgument", status.Code(err))
	}

	if _, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "grpc-bad-pap2",
		Bucket:   &storagepb.Bucket{Project: "projects/test-project", Location: "US"},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	_, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket: &storagepb.Bucket{
			Name:      "projects/_/buckets/grpc-bad-pap2",
			IamConfig: &storagepb.Bucket_IamConfig{PublicAccessPrevention: "sometimes"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"iam_config.public_access_prevention"}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("UpdateBucket error = %v (code %v), want InvalidArgument", err, status.Code(err))
	}
}
