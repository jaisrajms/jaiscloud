package storage

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/auth"
	"jaiscloud/internal/gcp/downscope"
	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestDownscopeEnforcementGRPC verifies the gRPC Storage surface enforces a
// downscoped credential's access boundary read from the incoming metadata, and
// that ordinary requests are unaffected.
func TestDownscopeEnforcementGRPC(t *testing.T) {
	client, svc, cleanup := newStorageTestServer(t)
	defer cleanup()
	createBucket(t, client, "bucket-a", "US")
	writeSingleShot(t, client, testBucket, "allowed/file.txt", "text/plain", []byte("allowed"))

	rules := []downscope.Rule{{
		Bucket:       "bucket-a",
		ObjectPrefix: "allowed/",
		Permissions: []string{
			downscope.PermissionLegacyObjectReader,
			downscope.PermissionObjectViewer,
			downscope.PermissionLegacyBucketWriter,
		},
	}}
	token := auth.MintDownscopedToken("sa@example.com", "proj", rules)
	authCtx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer "+token))

	if _, err := svc.GetObject(authCtx, &storagepb.GetObjectRequest{Bucket: testBucket, Object: "allowed/file.txt"}); err != nil {
		t.Fatalf("allowed GetObject: %v", err)
	}
	if _, err := svc.ListObjects(authCtx, &storagepb.ListObjectsRequest{Parent: testBucket, Prefix: "allowed/"}); err != nil {
		t.Fatalf("allowed ListObjects: %v", err)
	}
	if _, err := svc.GetObject(authCtx, &storagepb.GetObjectRequest{Bucket: testBucket, Object: "allowed_sibling/file.txt"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("sibling GetObject = %v, want PermissionDenied", err)
	}
	if _, err := svc.ListObjects(authCtx, &storagepb.ListObjectsRequest{Parent: testBucket, Prefix: "allowed_sibling/"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("sibling ListObjects = %v, want PermissionDenied", err)
	}
	if _, err := svc.GetBucket(authCtx, &storagepb.GetBucketRequest{Name: testBucket}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("GetBucket = %v, want PermissionDenied", err)
	}

	// An ordinary request (no downscoped credential) is not denied; the object
	// is simply absent.
	if _, err := svc.GetObject(context.Background(), &storagepb.GetObjectRequest{Bucket: testBucket, Object: "allowed_sibling/file.txt"}); status.Code(err) == codes.PermissionDenied {
		t.Fatalf("ordinary GetObject must not be denied: %v", err)
	}
}

// TestDownscopeResumableSessionOps verifies the resumable session RPCs are
// guarded: a token scoped to another bucket cannot query or cancel an upload
// whose ID it might guess.
func TestDownscopeResumableSessionOps(t *testing.T) {
	client, svc, cleanup := newStorageTestServer(t)
	defer cleanup()
	createBucket(t, client, "bucket-a", "US")

	allowedToken := auth.MintDownscopedToken("sa@example.com", "proj", []downscope.Rule{{
		Bucket:      "bucket-a",
		Permissions: []string{downscope.PermissionLegacyBucketWriter},
	}})
	allowedCtx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer "+allowedToken))

	start, err := svc.StartResumableWrite(allowedCtx, &storagepb.StartResumableWriteRequest{
		WriteObjectSpec: &storagepb.WriteObjectSpec{
			Resource: &storagepb.Object{Bucket: testBucket, Name: "allowed/big.bin"},
		},
	})
	if err != nil {
		t.Fatalf("StartResumableWrite: %v", err)
	}
	if _, err := svc.QueryWriteStatus(allowedCtx, &storagepb.QueryWriteStatusRequest{UploadId: start.GetUploadId()}); err != nil {
		t.Fatalf("allowed QueryWriteStatus: %v", err)
	}

	otherToken := auth.MintDownscopedToken("sa@example.com", "proj", []downscope.Rule{{
		Bucket:      "other-bucket",
		Permissions: []string{downscope.PermissionLegacyBucketWriter},
	}})
	otherCtx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer "+otherToken))

	if _, err := svc.QueryWriteStatus(otherCtx, &storagepb.QueryWriteStatusRequest{UploadId: start.GetUploadId()}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-bucket QueryWriteStatus = %v, want PermissionDenied", err)
	}
	if _, err := svc.CancelResumableWrite(otherCtx, &storagepb.CancelResumableWriteRequest{UploadId: start.GetUploadId()}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("cross-bucket CancelResumableWrite = %v, want PermissionDenied", err)
	}
	// The allowed owner can still cancel it.
	if _, err := svc.CancelResumableWrite(allowedCtx, &storagepb.CancelResumableWriteRequest{UploadId: start.GetUploadId()}); err != nil {
		t.Fatalf("allowed CancelResumableWrite: %v", err)
	}
}
