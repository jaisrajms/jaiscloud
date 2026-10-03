package storage

import (
	"bytes"
	"context"
	"hash/crc32"
	"io"
	"testing"
	"time"

	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// TestReadObjectChunkCRC32C verifies every streamed data message carries the
// CRC32C of its content field. Real GCS always sets it, and the official Go
// client's zero-copy ReadObject decoder requires a field to follow the content
// when it ends on a message boundary — omitting it panics the client on a
// subsequent Read.
func TestReadObjectChunkCRC32C(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	payload := []byte("chunk-crc-payload")
	writeSingleShot(t, client, testBucket, "crc-obj", "text/plain", payload)

	stream, err := client.ReadObject(ctx, &storagepb.ReadObjectRequest{Bucket: testBucket, Object: "crc-obj"})
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	var got []byte
	dataMsgs := 0
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadObject Recv: %v", err)
		}
		cd := msg.GetChecksummedData()
		if cd == nil {
			continue
		}
		dataMsgs++
		if cd.Crc32C == nil {
			t.Fatalf("data message %d has no crc32c", dataMsgs)
		}
		want := crc32.Checksum(cd.GetContent(), crc32.MakeTable(crc32.Castagnoli))
		if cd.GetCrc32C() != want {
			t.Fatalf("data message %d crc32c = %d, want %d", dataMsgs, cd.GetCrc32C(), want)
		}
		got = append(got, cd.GetContent()...)
	}
	if dataMsgs == 0 {
		t.Fatal("ReadObject returned no data messages")
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("ReadObject content = %q, want %q", got, payload)
	}
}

// TestCreateBucketRetentionPolicy covers setting a retention policy at bucket
// creation: the policy (and the server-assigned effectiveTime the official
// client requires to surface it) round-trips, and LockBucketRetentionPolicy
// then locks it.
func TestCreateBucketRetentionPolicy(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "bucket-a",
		Bucket: &storagepb.Bucket{
			Project:  "projects/test-project",
			Location: "US",
			RetentionPolicy: &storagepb.Bucket_RetentionPolicy{
				RetentionDuration: durationpb.New(time.Hour),
			},
		},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	got, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: testBucket})
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	rp := got.GetRetentionPolicy()
	if rp.GetRetentionDuration().AsDuration() != time.Hour {
		t.Fatalf("retention duration = %v, want 1h", rp.GetRetentionDuration().AsDuration())
	}
	if rp.GetEffectiveTime() == nil {
		t.Fatal("retention effectiveTime not set at creation")
	}

	locked, err := client.LockBucketRetentionPolicy(ctx, &storagepb.LockBucketRetentionPolicyRequest{Bucket: testBucket})
	if err != nil {
		t.Fatalf("LockBucketRetentionPolicy: %v", err)
	}
	if !locked.GetRetentionPolicy().GetIsLocked() {
		t.Fatal("retention policy not locked after lock")
	}
}

// TestUpdateBucketLabelsPerKey covers the dotted "labels.<key>" update-mask
// paths the official GCS clients send: a masked key is merged into the stored
// map, and masking a key absent from the request removes it while preserving
// every other label.
func TestUpdateBucketLabelsPerKey(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "bucket-a",
		Bucket: &storagepb.Bucket{
			Project:  "projects/test-project",
			Location: "US",
			Labels:   map[string]string{"a": "1"},
		},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	if _, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket:     &storagepb.Bucket{Name: testBucket, Labels: map[string]string{"b": "2"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels.b"}},
	}); err != nil {
		t.Fatalf("UpdateBucket labels.b: %v", err)
	}
	merged, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: testBucket})
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if merged.GetLabels()["a"] != "1" || merged.GetLabels()["b"] != "2" {
		t.Fatalf("labels after merge = %v, want a=1,b=2", merged.GetLabels())
	}

	if _, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket:     &storagepb.Bucket{Name: testBucket},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels.a"}},
	}); err != nil {
		t.Fatalf("UpdateBucket labels.a delete: %v", err)
	}
	after, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: testBucket})
	if err != nil {
		t.Fatalf("GetBucket after delete: %v", err)
	}
	if _, ok := after.GetLabels()["a"]; ok {
		t.Fatalf("label a still present after delete: %v", after.GetLabels())
	}
	if after.GetLabels()["b"] != "2" {
		t.Fatalf("label b lost on per-key delete: %v", after.GetLabels())
	}
}
