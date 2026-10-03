package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"
)

// writeObjectCSEK stores an object encrypted with the given customer-supplied
// key via the bidirectional write stream (which validates CSEK params).
func writeObjectCSEK(t *testing.T, client storagepb.StorageClient, bucket, object string, key, data []byte) {
	t.Helper()
	w, err := client.BidiWriteObject(context.Background())
	if err != nil {
		t.Fatalf("BidiWriteObject: %v", err)
	}
	if err := w.Send(&storagepb.BidiWriteObjectRequest{
		FirstMessage: &storagepb.BidiWriteObjectRequest_WriteObjectSpec{
			WriteObjectSpec: &storagepb.WriteObjectSpec{
				Resource: &storagepb.Object{Name: object, Bucket: bucket, ContentType: "text/plain"},
			},
		},
		WriteOffset:               0,
		Data:                      &storagepb.BidiWriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: data}},
		FinishWrite:               true,
		CommonObjectRequestParams: &storagepb.CommonObjectRequestParams{EncryptionAlgorithm: "AES256", EncryptionKeyBytes: key},
	}); err != nil {
		t.Fatalf("BidiWriteObject Send: %v", err)
	}
	if _, err := w.Recv(); err != nil {
		t.Fatalf("BidiWriteObject Recv: %v", err)
	}
}

// readObjectErr returns the terminal error of a ReadObject attempt, accepting an
// error either from the stream constructor or the first Recv.
func readObjectErr(t *testing.T, client storagepb.StorageClient, req *storagepb.ReadObjectRequest) error {
	t.Helper()
	stream, err := client.ReadObject(context.Background(), req)
	if err != nil {
		return err
	}
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// TestWriteObjectPreservesContentEncoding covers the gRPC streaming-write half
// of J69: Object.content_encoding set on the write spec survives to the stored
// object and is returned by GetObject.
func TestWriteObjectPreservesContentEncoding(t *testing.T) {
	client, _, cleanup := newStorageTestServer(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	stream, err := client.WriteObject(ctx)
	if err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	if err := stream.Send(&storagepb.WriteObjectRequest{
		FirstMessage: &storagepb.WriteObjectRequest_WriteObjectSpec{
			WriteObjectSpec: &storagepb.WriteObjectSpec{
				Resource: &storagepb.Object{Name: "enc", Bucket: testBucket, ContentType: "text/plain", ContentEncoding: "gzip"},
			},
		},
		WriteOffset: 0,
		Data:        &storagepb.WriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: []byte("data")}},
		FinishWrite: true,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	if resp.GetResource().GetContentEncoding() != "gzip" {
		t.Fatalf("write response contentEncoding = %q, want gzip", resp.GetResource().GetContentEncoding())
	}

	got, err := client.GetObject(ctx, &storagepb.GetObjectRequest{Bucket: testBucket, Object: "enc"})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if got.GetContentEncoding() != "gzip" {
		t.Fatalf("stored contentEncoding = %q, want gzip", got.GetContentEncoding())
	}
}

// TestGetObjectExposesCustomerEncryption covers J63: a gRPC read of a CSEK
// object surfaces customer_encryption (algorithm + key SHA-256) like the REST
// customerEncryption field.
func TestGetObjectExposesCustomerEncryption(t *testing.T) {
	client, _, cleanup := newStorageTestServer(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	key := bytes.Repeat([]byte{0x42}, 32)
	writeObjectCSEK(t, client, testBucket, "cek", key, []byte("secret"))

	obj, err := client.GetObject(ctx, &storagepb.GetObjectRequest{Bucket: testBucket, Object: "cek"})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	ce := obj.GetCustomerEncryption()
	if ce == nil {
		t.Fatalf("expected customer_encryption on CSEK object, got %+v", obj)
	}
	if ce.GetEncryptionAlgorithm() != "AES256" {
		t.Errorf("algorithm = %q, want AES256", ce.GetEncryptionAlgorithm())
	}
	sum := sha256.Sum256(key)
	if !bytes.Equal(ce.GetKeySha256Bytes(), sum[:]) {
		t.Errorf("key_sha256_bytes = %x, want %x", ce.GetKeySha256Bytes(), sum[:])
	}
}

// TestReadObjectRejectsMalformedCSEKParams covers J62: the unary ReadObject path
// validates CommonObjectRequestParams (algorithm and key length) up front —
// including a zero-length read, which returns before any decrypt.
func TestReadObjectRejectsMalformedCSEKParams(t *testing.T) {
	client, _, cleanup := newStorageTestServer(t)
	defer cleanup()
	createBucket(t, client, "bucket-a", "US")
	writeSingleShot(t, client, testBucket, "obj", "text/plain", []byte("hello"))
	writeSingleShot(t, client, testBucket, "empty", "text/plain", nil)

	key := bytes.Repeat([]byte{0x42}, 32)

	// Unsupported algorithm.
	err := readObjectErr(t, client, &storagepb.ReadObjectRequest{
		Bucket: testBucket, Object: "obj",
		CommonObjectRequestParams: &storagepb.CommonObjectRequestParams{
			EncryptionAlgorithm: "AES128", EncryptionKeyBytes: key,
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad algorithm err = %v, want InvalidArgument", err)
	}

	// Malformed key length.
	err = readObjectErr(t, client, &storagepb.ReadObjectRequest{
		Bucket: testBucket, Object: "obj",
		CommonObjectRequestParams: &storagepb.CommonObjectRequestParams{
			EncryptionAlgorithm: "AES256", EncryptionKeyBytes: []byte("short"),
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad key length err = %v, want InvalidArgument", err)
	}

	// Zero-length object: the read returns metadata only, but the malformed
	// params must still be rejected.
	err = readObjectErr(t, client, &storagepb.ReadObjectRequest{
		Bucket: testBucket, Object: "empty",
		CommonObjectRequestParams: &storagepb.CommonObjectRequestParams{
			EncryptionAlgorithm: "AES128", EncryptionKeyBytes: key,
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("zero-length read bad param err = %v, want InvalidArgument", err)
	}
}

// TestUpdateObjectTargetGeneration covers the gRPC half of J57: UpdateObject
// with Object.generation selects a specific (noncurrent) revision to update in
// place.
func TestUpdateObjectTargetGeneration(t *testing.T) {
	client, _, cleanup := newStorageTestServer(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "bucket-a",
		Bucket: &storagepb.Bucket{
			Project:    "projects/test-project",
			Location:   "US",
			Versioning: &storagepb.Bucket_Versioning{Enabled: true},
		},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	v1 := writeSingleShot(t, client, testBucket, "gen-obj", "text/plain", []byte("one"))
	v2 := writeSingleShot(t, client, testBucket, "gen-obj", "text/plain", []byte("two"))
	if v1.GetGeneration() == v2.GetGeneration() {
		t.Fatal("expected distinct generations")
	}

	updated, err := client.UpdateObject(ctx, &storagepb.UpdateObjectRequest{
		Object: &storagepb.Object{
			Name:        "gen-obj",
			Bucket:      testBucket,
			Generation:  v1.GetGeneration(),
			ContentType: "application/json",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"content_type"}},
	})
	if err != nil {
		t.Fatalf("UpdateObject target generation: %v", err)
	}
	if updated.GetGeneration() != v1.GetGeneration() {
		t.Fatalf("updated generation = %d, want %d", updated.GetGeneration(), v1.GetGeneration())
	}
	if updated.GetContentType() != "application/json" {
		t.Fatalf("contentType = %q, want application/json", updated.GetContentType())
	}

	// The selected revision changed; the live generation did not.
	target, err := client.GetObject(ctx, &storagepb.GetObjectRequest{
		Bucket: testBucket, Object: "gen-obj", Generation: v1.GetGeneration(),
	})
	if err != nil {
		t.Fatalf("GetObject(target): %v", err)
	}
	if target.GetContentType() != "application/json" || target.GetMetageneration() != 2 {
		t.Errorf("target = contentType %q metageneration %d, want application/json/2",
			target.GetContentType(), target.GetMetageneration())
	}
	live, err := client.GetObject(ctx, &storagepb.GetObjectRequest{Bucket: testBucket, Object: "gen-obj"})
	if err != nil {
		t.Fatalf("GetObject(live): %v", err)
	}
	if live.GetContentType() != "text/plain" || live.GetMetageneration() != 1 {
		t.Errorf("live mutated: contentType %q metageneration %d", live.GetContentType(), live.GetMetageneration())
	}
}
