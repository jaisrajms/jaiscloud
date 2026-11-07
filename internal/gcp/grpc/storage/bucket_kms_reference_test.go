package storage

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"

	"jaiscloud/internal/blobfs"
	gcpcrypto "jaiscloud/internal/gcp/crypto"
	storageprovider "jaiscloud/internal/gcp/provider/storage"
	"jaiscloud/internal/gcp/store/gcs"
	"jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/store"
)

// TestCreateBucketCMEKReferenceGRPC is the gRPC half of FC4-1: CreateBucket
// resolves encryption.default_kms_key against the KMS store exactly as the REST
// insert does, so both transports reject the same references.
func TestCreateBucketCMEKReferenceGRPC(t *testing.T) {
	ctx := context.Background()
	keys := kms.NewMemoryStore()
	if err := keys.CreateKeyRing(ctx, "test-project", "us", "ring", kms.KeyRing{ID: "ring"}); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if err := keys.CreateCryptoKey(ctx, "test-project", "us", "ring", "key",
		kms.CryptoKey{ID: "key", Purpose: "ENCRYPT_DECRYPT"}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	objects := gcs.NewMemoryObjectStore()
	resources := store.NewMemoryResourceStore()
	provider := storageprovider.New(objects, resources, blobfs.NewMemoryBlobStore(),
		gcpcrypto.NewEnvelopeEncryptor(keys))
	provider.SetKMSStore(keys)
	svc := NewService(objects, resources, provider, "test-project")

	create := func(id, location, key string) error {
		_, err := svc.CreateBucket(ctx, &storagepb.CreateBucketRequest{
			Parent:   "projects/test-project",
			BucketId: id,
			Bucket: &storagepb.Bucket{
				BucketId:   id,
				Location:   location,
				Encryption: &storagepb.Bucket_Encryption{DefaultKmsKey: key},
			},
		})
		return err
	}

	existing := "projects/test-project/locations/us/keyRings/ring/cryptoKeys/key"
	if err := create("cmek-ok", "US", existing); err != nil {
		t.Fatalf("accept: %v", err)
	}
	if err := create("cmek-missing", "US", "projects/test-project/locations/us/keyRings/ring/cryptoKeys/absent"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing key = %v, want INVALID_ARGUMENT", err)
	}
	if err := create("cmek-wrong-loc", "US", "projects/test-project/locations/europe/keyRings/ring/cryptoKeys/key"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("cross-location key = %v, want INVALID_ARGUMENT", err)
	}
}

// TestUpdateBucketCMEKReferenceGRPC covers the masked gRPC update: a PATCH that
// introduces a missing default key is rejected before the store changes.
func TestUpdateBucketCMEKReferenceGRPC(t *testing.T) {
	ctx := context.Background()
	keys := kms.NewMemoryStore()
	objects := gcs.NewMemoryObjectStore()
	resources := store.NewMemoryResourceStore()
	provider := storageprovider.New(objects, resources, blobfs.NewMemoryBlobStore(),
		gcpcrypto.NewEnvelopeEncryptor(keys))
	provider.SetKMSStore(keys)
	svc := NewService(objects, resources, provider, "test-project")

	if _, err := svc.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent: "projects/test-project", BucketId: "cmek-upd",
		Bucket: &storagepb.Bucket{BucketId: "cmek-upd", Location: "US"},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	_, err := svc.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket: &storagepb.Bucket{
			Name:       "projects/_/buckets/cmek-upd",
			Encryption: &storagepb.Bucket_Encryption{DefaultKmsKey: "projects/test-project/locations/us/keyRings/ring/cryptoKeys/absent"},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"encryption.default_kms_key"}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("update with missing key = %v, want INVALID_ARGUMENT", err)
	}
	got, err := svc.GetBucket(ctx, &storagepb.GetBucketRequest{Name: "projects/_/buckets/cmek-upd"})
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if k := got.GetEncryption().GetDefaultKmsKey(); k != "" {
		t.Fatalf("stored key = %q, want unchanged (empty)", k)
	}
}
