package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	iampb "cloud.google.com/go/iam/apiv1/iampb"

	"jaiscloud/internal/blobfs"
	gcpcrypto "jaiscloud/internal/gcp/crypto"
	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"
	storageprovider "jaiscloud/internal/gcp/provider/storage"
	"jaiscloud/internal/gcp/store/gcs"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// newStorageTestServer starts an in-process gRPC Storage server and returns the
// client plus the service itself (so tests can inspect in-memory session state,
// e.g. the resumable spill file).
func newStorageTestServer(t *testing.T) (storagepb.StorageClient, *Service, func()) {
	t.Helper()
	objects := gcs.NewMemoryObjectStore()
	blobs := blobfs.NewMemoryBlobStore()
	keys := kmsstore.NewMemoryStore()
	resources := store.NewMemoryResourceStore()
	provider := storageprovider.New(objects, resources, blobs, gcpcrypto.NewEnvelopeEncryptor(keys))
	svc := NewService(objects, resources, provider, "test-project")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	storagepb.RegisterStorageServer(srv, svc)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		conn.Close()
		srv.Stop()
	}
	return storagepb.NewStorageClient(conn), svc, cleanup
}

func storageTestService(t *testing.T) (storagepb.StorageClient, func()) {
	t.Helper()
	client, _, cleanup := newStorageTestServer(t)
	return client, cleanup
}

const testBucket = "projects/_/buckets/bucket-a"

func createBucket(t *testing.T, client storagepb.StorageClient, name, location string) {
	t.Helper()
	if _, err := client.CreateBucket(context.Background(), &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: name,
		Bucket:   &storagepb.Bucket{Project: "projects/test-project", Location: location},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
}

func TestBucketCRUD(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()

	created, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "bucket-a",
		Bucket: &storagepb.Bucket{
			Project:  "projects/test-project",
			Location: "US",
			Labels:   map[string]string{"transport": "grpc"},
		},
	})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if created.GetName() != testBucket {
		t.Fatalf("CreateBucket name = %q, want %q", created.GetName(), testBucket)
	}
	if created.GetLocation() != "US" {
		t.Fatalf("CreateBucket location = %q, want US", created.GetLocation())
	}

	got, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: testBucket})
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	if got.GetLabels()["transport"] != "grpc" {
		t.Fatalf("GetBucket labels = %v, want transport=grpc", got.GetLabels())
	}

	list, err := client.ListBuckets(ctx, &storagepb.ListBucketsRequest{Parent: "projects/test-project"})
	if err != nil {
		t.Fatalf("ListBuckets: %v", err)
	}
	if len(list.GetBuckets()) != 1 || list.GetBuckets()[0].GetName() != testBucket {
		t.Fatalf("ListBuckets = %v, want exactly [%s]", list.GetBuckets(), testBucket)
	}

	if _, err := client.DeleteBucket(ctx, &storagepb.DeleteBucketRequest{Name: testBucket}); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	if _, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: testBucket}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetBucket after delete err = %v, want NotFound", err)
	}
}

// writeSingleShot writes an object via the client-streaming WriteObject RPC.
func writeSingleShot(t *testing.T, client storagepb.StorageClient, bucket, object, contentType string, data []byte) *storagepb.Object {
	t.Helper()
	stream, err := client.WriteObject(context.Background())
	if err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	if err := stream.Send(&storagepb.WriteObjectRequest{
		FirstMessage: &storagepb.WriteObjectRequest_WriteObjectSpec{
			WriteObjectSpec: &storagepb.WriteObjectSpec{
				Resource: &storagepb.Object{Name: object, Bucket: bucket, ContentType: contentType},
			},
		},
		WriteOffset: 0,
		Data:        &storagepb.WriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: data}},
		FinishWrite: true,
	}); err != nil {
		t.Fatalf("WriteObject Send: %v", err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("WriteObject CloseAndRecv: %v", err)
	}
	return resp.GetResource()
}

// readObject reads an object's full content via the server-streaming ReadObject.
func readObject(t *testing.T, client storagepb.StorageClient, bucket, object string) []byte {
	t.Helper()
	stream, err := client.ReadObject(context.Background(), &storagepb.ReadObjectRequest{Bucket: bucket, Object: object})
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	var out []byte
	first := true
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadObject Recv: %v", err)
		}
		if first {
			if msg.GetMetadata() == nil {
				t.Fatal("ReadObject first message missing metadata")
			}
			first = false
		}
		if cd := msg.GetChecksummedData(); cd != nil {
			out = append(out, cd.GetContent()...)
		}
	}
	return out
}

func TestWriteReadRoundTrip(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	payload := []byte("hello grpc storage")
	obj := writeSingleShot(t, client, testBucket, "obj-a", "text/plain", payload)
	if obj.GetGeneration() <= 0 {
		t.Fatalf("WriteObject generation = %d, want > 0", obj.GetGeneration())
	}
	if obj.GetSize() != int64(len(payload)) {
		t.Fatalf("WriteObject size = %d, want %d", obj.GetSize(), len(payload))
	}

	got := readObject(t, client, testBucket, "obj-a")
	if string(got) != string(payload) {
		t.Fatalf("ReadObject content = %q, want %q", got, payload)
	}

	// GetObject reflects the stored metadata.
	meta, err := client.GetObject(ctx, &storagepb.GetObjectRequest{Bucket: testBucket, Object: "obj-a"})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if meta.GetContentType() != "text/plain" {
		t.Fatalf("GetObject contentType = %q, want text/plain", meta.GetContentType())
	}
}

func TestResumableWrite(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	srw, err := client.StartResumableWrite(ctx, &storagepb.StartResumableWriteRequest{
		WriteObjectSpec: &storagepb.WriteObjectSpec{
			Resource: &storagepb.Object{Name: "resumable-a", Bucket: testBucket, ContentType: "application/octet-stream"},
		},
	})
	if err != nil {
		t.Fatalf("StartResumableWrite: %v", err)
	}
	uploadID := srw.GetUploadId()
	if uploadID == "" {
		t.Fatal("StartResumableWrite upload_id is empty")
	}

	payload := []byte("resumable payload bytes")

	stream, err := client.BidiWriteObject(ctx)
	if err != nil {
		t.Fatalf("BidiWriteObject: %v", err)
	}
	if err := stream.Send(&storagepb.BidiWriteObjectRequest{
		FirstMessage: &storagepb.BidiWriteObjectRequest_UploadId{UploadId: uploadID},
		WriteOffset:  0,
		Data:         &storagepb.BidiWriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: payload}},
		FinishWrite:  true,
	}); err != nil {
		t.Fatalf("BidiWriteObject Send: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("BidiWriteObject CloseSend: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("BidiWriteObject Recv: %v", err)
	}
	if resp.GetResource() == nil {
		t.Fatalf("BidiWriteObject response = %v, want Resource", resp.GetWriteStatus())
	}

	got := readObject(t, client, testBucket, "resumable-a")
	if string(got) != string(payload) {
		t.Fatalf("ReadObject resumable content = %q, want %q", got, payload)
	}
}

func TestComposeAndUpdateAndList(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	a := writeSingleShot(t, client, testBucket, "comp-a", "text/plain", []byte("part-a-"))
	b := writeSingleShot(t, client, testBucket, "comp-b", "text/plain", []byte("part-b"))

	composed, err := client.ComposeObject(ctx, &storagepb.ComposeObjectRequest{
		Destination: &storagepb.Object{Name: "composed", Bucket: testBucket},
		SourceObjects: []*storagepb.ComposeObjectRequest_SourceObject{
			{Name: "comp-a", Generation: a.GetGeneration()},
			{Name: "comp-b", Generation: b.GetGeneration()},
		},
	})
	if err != nil {
		t.Fatalf("ComposeObject: %v", err)
	}
	if composed.GetComponentCount() != 2 {
		t.Fatalf("ComposeObject componentCount = %d, want 2", composed.GetComponentCount())
	}
	if got := readObject(t, client, testBucket, "composed"); string(got) != "part-a-part-b" {
		t.Fatalf("composed content = %q, want %q", got, "part-a-part-b")
	}

	// Update metadata.
	updated, err := client.UpdateObject(ctx, &storagepb.UpdateObjectRequest{
		Object:     &storagepb.Object{Name: "comp-a", Bucket: testBucket, Metadata: map[string]string{"updated": "true"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"metadata.updated"}},
	})
	if err != nil {
		t.Fatalf("UpdateObject: %v", err)
	}
	if updated.GetMetadata()["updated"] != "true" {
		t.Fatalf("UpdateObject metadata = %v, want updated=true", updated.GetMetadata())
	}

	// ListObjects sees all three.
	list, err := client.ListObjects(ctx, &storagepb.ListObjectsRequest{Parent: testBucket})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	names := map[string]bool{}
	for _, o := range list.GetObjects() {
		names[o.GetName()] = true
	}
	for _, want := range []string{"comp-a", "comp-b", "composed"} {
		if !names[want] {
			t.Fatalf("ListObjects missing %q (got %v)", want, names)
		}
	}
}

func TestListObjectsLexicographicBounds(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	for _, name := range []string{"lex-a", "lex-b", "lex-c"} {
		writeSingleShot(t, client, testBucket, name, "text/plain", []byte(name))
	}

	listNames := func(req *storagepb.ListObjectsRequest) []string {
		t.Helper()
		resp, err := client.ListObjects(ctx, req)
		if err != nil {
			t.Fatalf("ListObjects: %v", err)
		}
		names := make([]string, 0, len(resp.GetObjects()))
		for _, o := range resp.GetObjects() {
			names = append(names, o.GetName())
		}
		return names
	}

	// LexicographicStart is inclusive.
	got := listNames(&storagepb.ListObjectsRequest{Parent: testBucket, LexicographicStart: "lex-b"})
	if want := []string{"lex-b", "lex-c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("LexicographicStart listing = %v, want %v", got, want)
	}

	// LexicographicEnd is exclusive.
	got = listNames(&storagepb.ListObjectsRequest{Parent: testBucket, LexicographicEnd: "lex-c"})
	if want := []string{"lex-a", "lex-b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("LexicographicEnd listing = %v, want %v", got, want)
	}

	// Both bounds form a half-open range.
	got = listNames(&storagepb.ListObjectsRequest{Parent: testBucket, LexicographicStart: "lex-a", LexicographicEnd: "lex-c"})
	if want := []string{"lex-a", "lex-b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("bounds listing = %v, want %v", got, want)
	}

	// Bounds compose with a prefix.
	got = listNames(&storagepb.ListObjectsRequest{Parent: testBucket, Prefix: "lex-", LexicographicStart: "lex-b", LexicographicEnd: "lex-c"})
	if want := []string{"lex-b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("prefix+bounds listing = %v, want %v", got, want)
	}

	// An empty range (start == end) lists nothing.
	got = listNames(&storagepb.ListObjectsRequest{Parent: testBucket, LexicographicStart: "lex-b", LexicographicEnd: "lex-b"})
	if len(got) != 0 {
		t.Fatalf("empty range listing = %v, want none", got)
	}
}

func TestWriteObjectResumableRemovesSession(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	srw, err := client.StartResumableWrite(ctx, &storagepb.StartResumableWriteRequest{
		WriteObjectSpec: &storagepb.WriteObjectSpec{
			Resource: &storagepb.Object{Name: "resumable-wo", Bucket: testBucket, ContentType: "application/octet-stream"},
		},
	})
	if err != nil {
		t.Fatalf("StartResumableWrite: %v", err)
	}
	uploadID := srw.GetUploadId()

	payload := []byte("client-streaming resumable bytes")

	stream, err := client.WriteObject(ctx)
	if err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	if err := stream.Send(&storagepb.WriteObjectRequest{
		FirstMessage: &storagepb.WriteObjectRequest_UploadId{UploadId: uploadID},
		WriteOffset:  0,
		Data:         &storagepb.WriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: payload}},
		FinishWrite:  true,
	}); err != nil {
		t.Fatalf("WriteObject Send: %v", err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("WriteObject CloseAndRecv: %v", err)
	}
	if resp.GetResource() == nil {
		t.Fatalf("WriteObject response = %v, want Resource", resp.GetWriteStatus())
	}

	// The finished session is removed from the in-memory upload map.
	if _, err := client.QueryWriteStatus(ctx, &storagepb.QueryWriteStatusRequest{UploadId: uploadID}); status.Code(err) != codes.NotFound {
		t.Fatalf("QueryWriteStatus after finish err = %v, want NotFound", err)
	}

	got := readObject(t, client, testBucket, "resumable-wo")
	if string(got) != string(payload) {
		t.Fatalf("ReadObject content = %q, want %q", got, payload)
	}
}

func TestWriteObjectOffsetGapOutOfRange(t *testing.T) {
	client, cleanup := storageTestService(t)
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
				Resource: &storagepb.Object{Name: "gap-obj", Bucket: testBucket, ContentType: "text/plain"},
			},
		},
		WriteOffset: 5, // gap: the first write must begin at offset 0
		Data:        &storagepb.WriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: []byte("hello")}},
		FinishWrite: true,
	}); err != nil {
		t.Fatalf("WriteObject Send: %v", err)
	}
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.OutOfRange {
		t.Fatalf("WriteObject offset gap err = %v, want OutOfRange", err)
	}
}

func TestGetObjectHonorsGeneration(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()

	// Versioned bucket so overwrites retain prior generations.
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

	v1 := writeSingleShot(t, client, testBucket, "gen-obj", "text/plain", []byte("version-one"))
	v2 := writeSingleShot(t, client, testBucket, "gen-obj", "application/json", []byte("version-two"))
	if v1.GetGeneration() == v2.GetGeneration() {
		t.Fatal("expected distinct generations")
	}

	got, err := client.GetObject(ctx, &storagepb.GetObjectRequest{
		Bucket: testBucket, Object: "gen-obj", Generation: v1.GetGeneration(),
	})
	if err != nil {
		t.Fatalf("GetObject(stale generation): %v", err)
	}
	if got.GetGeneration() != v1.GetGeneration() {
		t.Fatalf("GetObject generation = %d, want %d", got.GetGeneration(), v1.GetGeneration())
	}
	if got.GetContentType() != "text/plain" {
		t.Fatalf("GetObject contentType = %q, want text/plain", got.GetContentType())
	}
}

// TestUpdateObjectKeepsVersionedGenerations verifies gRPC UpdateObject modifies
// the live generation's metadata in place: the generation is unchanged, the
// metageneration bumps, and the noncurrent generation survives untouched.
func TestUpdateObjectKeepsVersionedGenerations(t *testing.T) {
	client, cleanup := storageTestService(t)
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

	v1 := writeSingleShot(t, client, testBucket, "upd-obj", "text/plain", []byte("version-one"))
	v2 := writeSingleShot(t, client, testBucket, "upd-obj", "application/json", []byte("version-two"))
	if v1.GetGeneration() == v2.GetGeneration() {
		t.Fatal("expected distinct generations")
	}

	updated, err := client.UpdateObject(ctx, &storagepb.UpdateObjectRequest{
		Object:     &storagepb.Object{Name: "upd-obj", Bucket: testBucket, Metadata: map[string]string{"updated": "true"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"metadata.updated"}},
	})
	if err != nil {
		t.Fatalf("UpdateObject: %v", err)
	}
	if updated.GetGeneration() != v2.GetGeneration() {
		t.Fatalf("UpdateObject changed generation: got %d, want %d", updated.GetGeneration(), v2.GetGeneration())
	}
	if updated.GetMetageneration() <= v2.GetMetageneration() {
		t.Fatalf("UpdateObject metageneration = %d, want > %d", updated.GetMetageneration(), v2.GetMetageneration())
	}
	if updated.GetMetadata()["updated"] != "true" {
		t.Fatalf("UpdateObject metadata = %v, want updated=true", updated.GetMetadata())
	}

	// The noncurrent generation is untouched and still addressable.
	old, err := client.GetObject(ctx, &storagepb.GetObjectRequest{
		Bucket: testBucket, Object: "upd-obj", Generation: v1.GetGeneration(),
	})
	if err != nil {
		t.Fatalf("GetObject(noncurrent): %v", err)
	}
	if old.GetContentType() != "text/plain" || old.GetMetadata()["updated"] != "" {
		t.Fatalf("noncurrent generation mutated: contentType=%q metadata=%v", old.GetContentType(), old.GetMetadata())
	}
}

func TestDeleteAndUpdatePreconditions(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	writeSingleShot(t, client, testBucket, "pre-obj", "text/plain", []byte("data"))

	stale := int64(999999)

	// DeleteObject if_generation_match mismatch.
	if _, err := client.DeleteObject(ctx, &storagepb.DeleteObjectRequest{
		Bucket: testBucket, Object: "pre-obj", IfGenerationMatch: &stale,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteObject precondition err = %v, want FailedPrecondition", err)
	}

	// DeleteObject targeting a generation that does not exist is NotFound —
	// delete-by-generation removes exactly the requested revision, so a
	// missing revision is not a live-generation precondition mismatch.
	if _, err := client.DeleteObject(ctx, &storagepb.DeleteObjectRequest{
		Bucket: testBucket, Object: "pre-obj", Generation: stale,
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("DeleteObject missing-generation err = %v, want NotFound", err)
	}

	// UpdateObject if_metageneration_match mismatch.
	if _, err := client.UpdateObject(ctx, &storagepb.UpdateObjectRequest{
		Object:                &storagepb.Object{Name: "pre-obj", Bucket: testBucket, Metadata: map[string]string{"x": "y"}},
		UpdateMask:            &fieldmaskpb.FieldMask{Paths: []string{"metadata.x"}},
		IfMetagenerationMatch: &stale,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("UpdateObject precondition err = %v, want FailedPrecondition", err)
	}

	// The object is still present (preconditions failed without mutating).
	if got := readObject(t, client, testBucket, "pre-obj"); string(got) != "data" {
		t.Fatalf("ReadObject content = %q, want %q", got, "data")
	}
}

// The compose/write paths below thread their request preconditions into the
// store's atomic *Checked methods — the follow-up #48 deliberately deferred
// (its DeleteObject/ComposeObject/finalize calls passed precondition=nil). A
// stale precondition must be rejected atomically, without mutating the object.

// TestDeleteObjectGenerationRemovesOnlyThatVersion verifies the gRPC
// delete-by-generation contract matches REST's ?generation=: only the requested
// revision is removed, deleting the live revision does not promote a noncurrent
// survivor, and removing the last revision removes the object.
func TestDeleteObjectGenerationRemovesOnlyThatVersion(t *testing.T) {
	client, cleanup := storageTestService(t)
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
	v1 := writeSingleShot(t, client, testBucket, "gen-del", "text/plain", []byte("version-one"))
	v2 := writeSingleShot(t, client, testBucket, "gen-del", "text/plain", []byte("version-two"))

	// Deleting the live v2 must not promote the noncurrent v1: a bare lookup
	// fails, while v1 stays reachable by generation.
	if _, err := client.DeleteObject(ctx, &storagepb.DeleteObjectRequest{
		Bucket: testBucket, Object: "gen-del", Generation: v2.GetGeneration(),
	}); err != nil {
		t.Fatalf("DeleteObject(v2): %v", err)
	}
	if _, err := client.GetObject(ctx, &storagepb.GetObjectRequest{
		Bucket: testBucket, Object: "gen-del",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("bare GetObject after deleting the live generation err = %v, want NotFound", err)
	}
	if _, err := client.GetObject(ctx, &storagepb.GetObjectRequest{
		Bucket: testBucket, Object: "gen-del", Generation: v1.GetGeneration(),
	}); err != nil {
		t.Fatalf("noncurrent v1 must still be reachable by generation: %v", err)
	}

	// Removing the last (noncurrent) revision deletes the object entirely.
	if _, err := client.DeleteObject(ctx, &storagepb.DeleteObjectRequest{
		Bucket: testBucket, Object: "gen-del", Generation: v1.GetGeneration(),
	}); err != nil {
		t.Fatalf("DeleteObject(v1): %v", err)
	}
	if _, err := client.GetObject(ctx, &storagepb.GetObjectRequest{
		Bucket: testBucket, Object: "gen-del", Generation: v1.GetGeneration(),
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetObject(v1) after delete err = %v, want NotFound", err)
	}
}

func TestComposeObjectDestinationPrecondition(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	a := writeSingleShot(t, client, testBucket, "cs-a", "text/plain", []byte("A"))
	b := writeSingleShot(t, client, testBucket, "cs-b", "text/plain", []byte("B"))
	writeSingleShot(t, client, testBucket, "cs-dest", "text/plain", []byte("OLD"))

	sources := []*storagepb.ComposeObjectRequest_SourceObject{
		{Name: "cs-a", Generation: a.GetGeneration()},
		{Name: "cs-b", Generation: b.GetGeneration()},
	}

	// if_generation_match=0 ("create only if absent") against an existing
	// destination is rejected atomically; the existing object is untouched.
	zero := int64(0)
	if _, err := client.ComposeObject(ctx, &storagepb.ComposeObjectRequest{
		Destination:       &storagepb.Object{Name: "cs-dest", Bucket: testBucket},
		SourceObjects:     sources,
		IfGenerationMatch: &zero,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("compose if_generation_match=0 on existing dest: code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if got := readObject(t, client, testBucket, "cs-dest"); string(got) != "OLD" {
		t.Fatalf("destination unchanged after rejected compose, got %q, want OLD", got)
	}

	// The same create-only precondition succeeds against a new destination.
	if _, err := client.ComposeObject(ctx, &storagepb.ComposeObjectRequest{
		Destination:       &storagepb.Object{Name: "cs-new", Bucket: testBucket},
		SourceObjects:     sources,
		IfGenerationMatch: &zero,
	}); err != nil {
		t.Fatalf("compose if_generation_match=0 on new dest: %v", err)
	}
	if got := readObject(t, client, testBucket, "cs-new"); string(got) != "AB" {
		t.Fatalf("composed content = %q, want AB", got)
	}
}

// writeSingleShotPre writes via the client-streaming WriteObject RPC with an
// optional if_generation_match precondition, returning the RPC error.
func writeSingleShotPre(t *testing.T, client storagepb.StorageClient, bucket, object string, data []byte, ifGenMatch *int64) error {
	t.Helper()
	stream, err := client.WriteObject(context.Background())
	if err != nil {
		return err
	}
	if err := stream.Send(&storagepb.WriteObjectRequest{
		FirstMessage: &storagepb.WriteObjectRequest_WriteObjectSpec{
			WriteObjectSpec: &storagepb.WriteObjectSpec{
				Resource:          &storagepb.Object{Name: object, Bucket: bucket, ContentType: "text/plain"},
				IfGenerationMatch: ifGenMatch,
			},
		},
		WriteOffset: 0,
		Data:        &storagepb.WriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: data}},
		FinishWrite: true,
	}); err != nil {
		return err
	}
	_, err = stream.CloseAndRecv()
	return err
}

func TestWriteObjectPrecondition(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	obj := writeSingleShot(t, client, testBucket, "wp", "text/plain", []byte("first"))
	gen := obj.GetGeneration()
	zero := int64(0)

	// Create-only (if_generation_match=0) against an existing object is
	// rejected; the stored bytes are untouched.
	if err := writeSingleShotPre(t, client, testBucket, "wp", []byte("second"), &zero); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("write if_generation_match=0 on existing: code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if got := readObject(t, client, testBucket, "wp"); string(got) != "first" {
		t.Fatalf("object unchanged after rejected write, got %q, want first", got)
	}

	// A matching generation overwrites.
	if err := writeSingleShotPre(t, client, testBucket, "wp", []byte("second"), &gen); err != nil {
		t.Fatalf("write with matching if_generation_match: %v", err)
	}
	if got := readObject(t, client, testBucket, "wp"); string(got) != "second" {
		t.Fatalf("object content = %q, want second", got)
	}

	// Create-only against a new object succeeds.
	if err := writeSingleShotPre(t, client, testBucket, "wp-new", []byte("fresh"), &zero); err != nil {
		t.Fatalf("write if_generation_match=0 on new object: %v", err)
	}

	// A resumable write carries the write spec's precondition through to
	// finalize (StartResumableWrite captures it on the session).
	srw, err := client.StartResumableWrite(ctx, &storagepb.StartResumableWriteRequest{
		WriteObjectSpec: &storagepb.WriteObjectSpec{
			Resource:          &storagepb.Object{Name: "wp", Bucket: testBucket, ContentType: "text/plain"},
			IfGenerationMatch: &zero,
		},
	})
	if err != nil {
		t.Fatalf("StartResumableWrite: %v", err)
	}
	stream, err := client.BidiWriteObject(ctx)
	if err != nil {
		t.Fatalf("BidiWriteObject: %v", err)
	}
	if err := stream.Send(&storagepb.BidiWriteObjectRequest{
		FirstMessage: &storagepb.BidiWriteObjectRequest_UploadId{UploadId: srw.GetUploadId()},
		Data:         &storagepb.BidiWriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: []byte("resumed")}},
	}); err != nil {
		t.Fatalf("BidiWriteObject send data: %v", err)
	}
	if err := stream.Send(&storagepb.BidiWriteObjectRequest{FinishWrite: true}); err != nil {
		t.Fatalf("BidiWriteObject send finish: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("resumable finalize with stale precondition: code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
}

// ─── RewriteObject ────────────────────────────────────────────────────────────

func TestRewriteObjectCopiesBytesAndMetadata(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	src := writeSingleShot(t, client, testBucket, "rw-src", "text/plain", []byte("rewrite-payload"))
	if _, err := client.UpdateObject(ctx, &storagepb.UpdateObjectRequest{
		Object:     &storagepb.Object{Name: "rw-src", Bucket: testBucket, Metadata: map[string]string{"k": "v"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"metadata.k"}},
	}); err != nil {
		t.Fatalf("UpdateObject source: %v", err)
	}

	resp, err := client.RewriteObject(ctx, &storagepb.RewriteObjectRequest{
		SourceBucket:      testBucket,
		SourceObject:      "rw-src",
		DestinationBucket: testBucket,
		DestinationName:   "rw-dst",
	})
	if err != nil {
		t.Fatalf("RewriteObject: %v", err)
	}
	if !resp.GetDone() {
		t.Fatalf("RewriteObject done = false, want true")
	}
	if resp.GetObjectSize() != int64(len("rewrite-payload")) {
		t.Fatalf("RewriteObject objectSize = %d, want %d", resp.GetObjectSize(), len("rewrite-payload"))
	}
	dst := resp.GetResource()
	if dst.GetName() != "rw-dst" {
		t.Fatalf("RewriteObject resource name = %q, want rw-dst", dst.GetName())
	}
	if dst.GetGeneration() == src.GetGeneration() {
		t.Fatalf("destination generation = source generation %d, want a fresh generation", dst.GetGeneration())
	}
	if dst.GetContentType() != "text/plain" {
		t.Fatalf("destination contentType = %q, want text/plain", dst.GetContentType())
	}
	if dst.GetMetadata()["k"] != "v" {
		t.Fatalf("destination metadata = %v, want k=v", dst.GetMetadata())
	}
	if got := readObject(t, client, testBucket, "rw-dst"); string(got) != "rewrite-payload" {
		t.Fatalf("rewritten content = %q, want rewrite-payload", got)
	}
	// Source is untouched by a rewrite.
	if got := readObject(t, client, testBucket, "rw-src"); string(got) != "rewrite-payload" {
		t.Fatalf("source content = %q, want rewrite-payload", got)
	}
}

func TestRewriteObjectDestinationPrecondition(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	writeSingleShot(t, client, testBucket, "rw-p-src", "text/plain", []byte("SRC"))
	writeSingleShot(t, client, testBucket, "rw-p-dst", "text/plain", []byte("OLD"))

	zero := int64(0)
	if _, err := client.RewriteObject(ctx, &storagepb.RewriteObjectRequest{
		SourceBucket:      testBucket,
		SourceObject:      "rw-p-src",
		DestinationBucket: testBucket,
		DestinationName:   "rw-p-dst",
		IfGenerationMatch: &zero,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("rewrite if_generation_match=0 on existing dest: code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if got := readObject(t, client, testBucket, "rw-p-dst"); string(got) != "OLD" {
		t.Fatalf("destination unchanged after rejected rewrite, got %q, want OLD", got)
	}
	if got := readObject(t, client, testBucket, "rw-p-src"); string(got) != "SRC" {
		t.Fatalf("source unchanged after rejected rewrite, got %q, want SRC", got)
	}

	// A matching source generation is honored; a stale if_source_generation_match
	// fails without writing the destination.
	srcObj, err := client.GetObject(ctx, &storagepb.GetObjectRequest{Bucket: testBucket, Object: "rw-p-src"})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if _, err := client.RewriteObject(ctx, &storagepb.RewriteObjectRequest{
		SourceBucket:            testBucket,
		SourceObject:            "rw-p-src",
		DestinationBucket:       testBucket,
		DestinationName:         "rw-p-new",
		IfSourceGenerationMatch: &zero,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("rewrite stale if_source_generation_match: code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if _, err := client.GetObject(ctx, &storagepb.GetObjectRequest{Bucket: testBucket, Object: "rw-p-new"}); status.Code(err) != codes.NotFound {
		t.Fatalf("destination created despite source precondition failure: %v", err)
	}

	if _, err := client.RewriteObject(ctx, &storagepb.RewriteObjectRequest{
		SourceBucket:            testBucket,
		SourceObject:            "rw-p-src",
		DestinationBucket:       testBucket,
		DestinationName:         "rw-p-new",
		IfSourceGenerationMatch: &srcObj.Generation,
	}); err != nil {
		t.Fatalf("rewrite with matching source generation: %v", err)
	}
	if got := readObject(t, client, testBucket, "rw-p-new"); string(got) != "SRC" {
		t.Fatalf("rewritten content = %q, want SRC", got)
	}
}

// ─── MoveObject ───────────────────────────────────────────────────────────────

func TestMoveObjectLeavesSourceGoneAndDestPresent(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	writeSingleShot(t, client, testBucket, "mv-src", "text/plain", []byte("move-payload"))

	moved, err := client.MoveObject(ctx, &storagepb.MoveObjectRequest{
		Bucket:            testBucket,
		SourceObject:      "mv-src",
		DestinationObject: "mv-dst",
	})
	if err != nil {
		t.Fatalf("MoveObject: %v", err)
	}
	if moved.GetName() != "mv-dst" {
		t.Fatalf("MoveObject resource name = %q, want mv-dst", moved.GetName())
	}
	if got := readObject(t, client, testBucket, "mv-dst"); string(got) != "move-payload" {
		t.Fatalf("moved content = %q, want move-payload", got)
	}
	if _, err := client.GetObject(ctx, &storagepb.GetObjectRequest{Bucket: testBucket, Object: "mv-src"}); status.Code(err) != codes.NotFound {
		t.Fatalf("source after move: code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
}

func TestMoveObjectPreconditionsAndMissingSource(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	writeSingleShot(t, client, testBucket, "mv-p-src", "text/plain", []byte("SRC"))
	writeSingleShot(t, client, testBucket, "mv-p-dst", "text/plain", []byte("OLD"))

	// Destination create-only precondition rejects the move; the source stays.
	zero := int64(0)
	if _, err := client.MoveObject(ctx, &storagepb.MoveObjectRequest{
		Bucket:            testBucket,
		SourceObject:      "mv-p-src",
		DestinationObject: "mv-p-dst",
		IfGenerationMatch: &zero,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("move destination precondition: code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if got := readObject(t, client, testBucket, "mv-p-dst"); string(got) != "OLD" {
		t.Fatalf("destination unchanged after rejected move, got %q, want OLD", got)
	}
	if got := readObject(t, client, testBucket, "mv-p-src"); string(got) != "SRC" {
		t.Fatalf("source removed despite rejected move, got %q, want SRC", got)
	}

	// A missing source is NotFound.
	if _, err := client.MoveObject(ctx, &storagepb.MoveObjectRequest{
		Bucket:            testBucket,
		SourceObject:      "mv-p-missing",
		DestinationObject: "mv-p-new",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("move missing source: code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
}

// ─── IAM ──────────────────────────────────────────────────────────────────────

func TestStorageBucketIAMRoundTrip(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	got, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: testBucket})
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if got.GetEtag() == nil {
		t.Fatalf("GetIamPolicy etag is empty, want a default etag")
	}

	set, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: testBucket,
		Policy: &iampb.Policy{
			Bindings: []*iampb.Binding{
				{Role: "roles/storage.objectViewer", Members: []string{"user:alice@example.com"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}
	if len(set.GetBindings()) != 1 || set.GetBindings()[0].GetRole() != "roles/storage.objectViewer" {
		t.Fatalf("SetIamPolicy bindings = %v, want objectViewer", set.GetBindings())
	}

	got, err = client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: testBucket})
	if err != nil {
		t.Fatalf("GetIamPolicy after set: %v", err)
	}
	if len(got.GetBindings()) != 1 || got.GetBindings()[0].GetMembers()[0] != "user:alice@example.com" {
		t.Fatalf("GetIamPolicy round-trip bindings = %v", got.GetBindings())
	}

	perms, err := client.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource:    testBucket,
		Permissions: []string{"storage.buckets.get", "storage.objects.list"},
	})
	if err != nil {
		t.Fatalf("TestIamPermissions: %v", err)
	}
	if len(perms.GetPermissions()) != 2 {
		t.Fatalf("TestIamPermissions = %v, want both permissions", perms.GetPermissions())
	}
}

func TestStorageObjectIAMRoundTrip(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")
	writeSingleShot(t, client, testBucket, "iam-obj", "text/plain", []byte("data"))

	const res = testBucket + "/objects/iam-obj"
	if _, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: res,
		Policy: &iampb.Policy{
			Bindings: []*iampb.Binding{
				{Role: "roles/storage.objectAdmin", Members: []string{"serviceAccount:svc@example.com"}},
			},
		},
	}); err != nil {
		t.Fatalf("SetIamPolicy(object): %v", err)
	}
	got, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: res})
	if err != nil {
		t.Fatalf("GetIamPolicy(object): %v", err)
	}
	if len(got.GetBindings()) != 1 || got.GetBindings()[0].GetRole() != "roles/storage.objectAdmin" {
		t.Fatalf("object IAM bindings = %v, want objectAdmin", got.GetBindings())
	}
	// The bucket policy is stored under a separate id and stays empty.
	bucketPol, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: testBucket})
	if err != nil {
		t.Fatalf("GetIamPolicy(bucket): %v", err)
	}
	if len(bucketPol.GetBindings()) != 0 {
		t.Fatalf("bucket IAM unexpectedly shares object bindings: %v", bucketPol.GetBindings())
	}
}

func TestStorageIAMNotFound(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	if _, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{
		Resource: "projects/_/buckets/missing-bucket",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetIamPolicy missing bucket: code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
	if _, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: testBucket + "/objects/missing-obj",
		Policy:   &iampb.Policy{},
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("SetIamPolicy missing object: code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
	if _, err := client.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource: "projects/_/buckets/missing-bucket",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("TestIamPermissions missing bucket: code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
}

// ─── resumable spill ──────────────────────────────────────────────────────────

func TestResumableUploadSpillsToTempFileAndCleansUp(t *testing.T) {
	client, svc, cleanup := newStorageTestServer(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	srw, err := client.StartResumableWrite(ctx, &storagepb.StartResumableWriteRequest{
		WriteObjectSpec: &storagepb.WriteObjectSpec{
			Resource: &storagepb.Object{Name: "spill-obj", Bucket: testBucket, ContentType: "application/octet-stream"},
		},
	})
	if err != nil {
		t.Fatalf("StartResumableWrite: %v", err)
	}
	uploadID := srw.GetUploadId()

	// Two sub-4-MiB chunks: each is under the gRPC default max message size,
	// but together they cross resumableSpillThreshold and force a spill.
	chunk1 := bytes.Repeat([]byte("x"), 3<<20)
	chunk2 := bytes.Repeat([]byte("y"), 2<<20)
	big := append(append([]byte{}, chunk1...), chunk2...)
	stream, err := client.BidiWriteObject(ctx)
	if err != nil {
		t.Fatalf("BidiWriteObject: %v", err)
	}
	if err := stream.Send(&storagepb.BidiWriteObjectRequest{
		FirstMessage: &storagepb.BidiWriteObjectRequest_UploadId{UploadId: uploadID},
		WriteOffset:  0,
		Data:         &storagepb.BidiWriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: chunk1}},
	}); err != nil {
		t.Fatalf("BidiWriteObject send first chunk: %v", err)
	}
	if err := stream.Send(&storagepb.BidiWriteObjectRequest{
		WriteOffset: int64(len(chunk1)),
		Data:        &storagepb.BidiWriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: chunk2}},
	}); err != nil {
		t.Fatalf("BidiWriteObject send second chunk: %v", err)
	}
	// A state lookup forces the server to process the preceding data messages
	// before we inspect in-memory session state (client Send is asynchronous).
	if err := stream.Send(&storagepb.BidiWriteObjectRequest{StateLookup: true}); err != nil {
		t.Fatalf("BidiWriteObject send state lookup: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("BidiWriteObject state lookup Recv: %v", err)
	}

	// The session must have spilled: bytes live in a temp file, not memory.
	svc.mu.Lock()
	sess := svc.uploads[uploadID]
	spilled := sess != nil && sess.tmpFile != nil && sess.buf == nil
	var tmpPath string
	if spilled {
		tmpPath = sess.tmpPath
	}
	svc.mu.Unlock()
	if !spilled {
		t.Fatal("expected the resumable session to spill to a temp file past the threshold")
	}
	if _, err := os.Stat(tmpPath); err != nil {
		t.Fatalf("expected spill file %s to exist: %v", tmpPath, err)
	}

	if err := stream.Send(&storagepb.BidiWriteObjectRequest{FinishWrite: true}); err != nil {
		t.Fatalf("BidiWriteObject send finish: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("BidiWriteObject Recv: %v", err)
	}
	if resp.GetResource() == nil {
		t.Fatalf("BidiWriteObject response = %v, want Resource", resp.GetWriteStatus())
	}

	if got := readObject(t, client, testBucket, "spill-obj"); !bytes.Equal(got, big) {
		t.Fatalf("spilled round-trip: got %d bytes, want %d", len(got), len(big))
	}
	if _, err := os.Stat(tmpPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("spill file %s not removed after finalize (err=%v)", tmpPath, err)
	}
}

func TestResetRemovesSpillFiles(t *testing.T) {
	client, svc, cleanup := newStorageTestServer(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	srw, err := client.StartResumableWrite(ctx, &storagepb.StartResumableWriteRequest{
		WriteObjectSpec: &storagepb.WriteObjectSpec{
			Resource: &storagepb.Object{Name: "reset-obj", Bucket: testBucket, ContentType: "application/octet-stream"},
		},
	})
	if err != nil {
		t.Fatalf("StartResumableWrite: %v", err)
	}
	uploadID := srw.GetUploadId()

	stream, err := client.BidiWriteObject(ctx)
	if err != nil {
		t.Fatalf("BidiWriteObject: %v", err)
	}
	chunk1 := bytes.Repeat([]byte("y"), 3<<20)
	chunk2 := bytes.Repeat([]byte("z"), 2<<20)
	if err := stream.Send(&storagepb.BidiWriteObjectRequest{
		FirstMessage: &storagepb.BidiWriteObjectRequest_UploadId{UploadId: uploadID},
		WriteOffset:  0,
		Data:         &storagepb.BidiWriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: chunk1}},
	}); err != nil {
		t.Fatalf("BidiWriteObject send first chunk: %v", err)
	}
	if err := stream.Send(&storagepb.BidiWriteObjectRequest{
		WriteOffset: int64(len(chunk1)),
		Data:        &storagepb.BidiWriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: chunk2}},
	}); err != nil {
		t.Fatalf("BidiWriteObject send second chunk: %v", err)
	}
	// Synchronize with the server before inspecting session state.
	if err := stream.Send(&storagepb.BidiWriteObjectRequest{StateLookup: true}); err != nil {
		t.Fatalf("BidiWriteObject send state lookup: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("BidiWriteObject state lookup Recv: %v", err)
	}

	svc.mu.Lock()
	sess := svc.uploads[uploadID]
	var tmpPath string
	if sess != nil {
		tmpPath = sess.tmpPath
	}
	svc.mu.Unlock()
	if tmpPath == "" {
		t.Fatal("expected a spilled session before Reset")
	}

	svc.Reset(ctx)
	if _, err := os.Stat(tmpPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("spill file %s not removed by Reset (err=%v)", tmpPath, err)
	}
	if _, err := client.QueryWriteStatus(ctx, &storagepb.QueryWriteStatusRequest{UploadId: uploadID}); status.Code(err) != codes.NotFound {
		t.Fatalf("QueryWriteStatus after Reset: code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
}

// ─── range reads ──────────────────────────────────────────────────────────────

func TestReadObjectRange(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	payload := []byte("0123456789abcdefghij")
	writeSingleShot(t, client, testBucket, "range-obj", "text/plain", payload)

	stream, err := client.ReadObject(ctx, &storagepb.ReadObjectRequest{
		Bucket: testBucket, Object: "range-obj", ReadOffset: 5, ReadLimit: 4,
	})
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	var out []byte
	first := true
	var cr *storagepb.ContentRange
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadObject Recv: %v", err)
		}
		if first {
			first = false
			cr = msg.GetContentRange()
			if msg.GetMetadata() == nil {
				t.Fatal("ReadObject first message missing metadata")
			}
		}
		if cd := msg.GetChecksummedData(); cd != nil {
			out = append(out, cd.GetContent()...)
		}
	}
	if string(out) != "5678" {
		t.Fatalf("range read = %q, want 5678", out)
	}
	if cr.GetStart() != 5 || cr.GetEnd() != 9 || cr.GetCompleteLength() != int64(len(payload)) {
		t.Fatalf("content range = %+v, want start=5 end=9 complete=%d", cr, len(payload))
	}

	// A whole-object read still round-trips.
	if got := readObject(t, client, testBucket, "range-obj"); !bytes.Equal(got, payload) {
		t.Fatalf("full read = %q, want %q", got, payload)
	}

	// A zero-length range (offset at EOF) returns metadata only and no data
	// chunks; this path deliberately skips the blob read and decryption.
	zero, err := client.ReadObject(ctx, &storagepb.ReadObjectRequest{
		Bucket: testBucket, Object: "range-obj", ReadOffset: int64(len(payload)),
	})
	if err != nil {
		t.Fatalf("ReadObject zero range: %v", err)
	}
	zeroMsgs := 0
	for {
		msg, err := zero.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadObject zero range Recv: %v", err)
		}
		zeroMsgs++
		if msg.GetChecksummedData() != nil {
			t.Fatalf("zero-length range returned data: %q", msg.GetChecksummedData().GetContent())
		}
		cr := msg.GetContentRange()
		if cr.GetStart() != int64(len(payload)) || cr.GetEnd() != int64(len(payload)) {
			t.Fatalf("zero-length content range = %+v, want start=end=%d", cr, len(payload))
		}
	}
	if zeroMsgs != 1 {
		t.Fatalf("zero-length range messages = %d, want 1 (metadata only)", zeroMsgs)
	}
}

func TestUpdateBucketLabelsAndPrecondition(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	initial, err := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: testBucket})
	if err != nil {
		t.Fatalf("GetBucket: %v", err)
	}
	initialMeta := initial.GetMetageneration()
	if initialMeta != 1 {
		t.Fatalf("fresh bucket metageneration = %d, want 1", initialMeta)
	}

	updated, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket:     &storagepb.Bucket{Name: testBucket, Labels: map[string]string{"env": "prod"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	if err != nil {
		t.Fatalf("UpdateBucket: %v", err)
	}
	if updated.GetLabels()["env"] != "prod" {
		t.Fatalf("UpdateBucket labels = %v, want env=prod", updated.GetLabels())
	}
	if updated.GetMetageneration() != initialMeta+1 {
		t.Fatalf("UpdateBucket metageneration = %d, want %d", updated.GetMetageneration(), initialMeta+1)
	}

	// A stale metageneration precondition must fail without mutating.
	if _, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket:                &storagepb.Bucket{Name: testBucket, Labels: map[string]string{"env": "dev"}},
		UpdateMask:            &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		IfMetagenerationMatch: &initialMeta,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("UpdateBucket stale precondition err = %v, want FailedPrecondition", err)
	}
	if got, _ := client.GetBucket(ctx, &storagepb.GetBucketRequest{Name: testBucket}); got.GetLabels()["env"] != "prod" {
		t.Fatalf("stale-precondition update mutated labels: %v", got.GetLabels())
	}

	// The current metageneration satisfies the precondition.
	current := updated.GetMetageneration()
	again, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket:                &storagepb.Bucket{Name: testBucket, Labels: map[string]string{"env": "dev"}},
		UpdateMask:            &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
		IfMetagenerationMatch: &current,
	})
	if err != nil {
		t.Fatalf("UpdateBucket matching precondition: %v", err)
	}
	if again.GetLabels()["env"] != "dev" {
		t.Fatalf("UpdateBucket labels = %v, want env=dev", again.GetLabels())
	}

	// An update_mask is required.
	if _, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket: &storagepb.Bucket{Name: testBucket, Labels: map[string]string{"env": "x"}},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("UpdateBucket missing mask err = %v, want InvalidArgument", err)
	}
}

func TestRestoreObject(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()

	// Versioned bucket so overwrites retain (tombstone) prior generations.
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

	v1 := writeSingleShot(t, client, testBucket, "restore-obj", "text/plain", []byte("version-one"))
	v2 := writeSingleShot(t, client, testBucket, "restore-obj", "text/plain", []byte("version-two"))
	if v1.GetGeneration() == v2.GetGeneration() {
		t.Fatal("expected distinct generations")
	}

	// v1 is now non-live (tombstoned) after the v2 write.
	dead, err := client.GetObject(ctx, &storagepb.GetObjectRequest{
		Bucket: testBucket, Object: "restore-obj", Generation: v1.GetGeneration(),
	})
	if err != nil {
		t.Fatalf("GetObject(v1): %v", err)
	}
	if dead.GetDeleteTime() == nil {
		t.Fatal("expected v1 to carry a delete_time before restore")
	}

	restored, err := client.RestoreObject(ctx, &storagepb.RestoreObjectRequest{
		Bucket: testBucket, Object: "restore-obj", Generation: v1.GetGeneration(),
	})
	if err != nil {
		t.Fatalf("RestoreObject: %v", err)
	}
	if restored.GetGeneration() != v1.GetGeneration() {
		t.Fatalf("RestoreObject generation = %d, want %d", restored.GetGeneration(), v1.GetGeneration())
	}
	if restored.GetDeleteTime() != nil {
		t.Fatalf("RestoreObject delete_time = %v, want nil", restored.GetDeleteTime())
	}

	// v1 is the live generation again (reads resolve to it).
	if got := readObject(t, client, testBucket, "restore-obj"); string(got) != "version-one" {
		t.Fatalf("read after restore = %q, want version-one", got)
	}
	// v2 was superseded and is now non-live.
	superseded, err := client.GetObject(ctx, &storagepb.GetObjectRequest{
		Bucket: testBucket, Object: "restore-obj", Generation: v2.GetGeneration(),
	})
	if err != nil {
		t.Fatalf("GetObject(v2): %v", err)
	}
	if superseded.GetDeleteTime() == nil {
		t.Fatal("expected v2 to be non-live after restoring v1")
	}

	// Precondition mismatch on the current live generation fails without mutating.
	stale := int64(99999999)
	if _, err := client.RestoreObject(ctx, &storagepb.RestoreObjectRequest{
		Bucket: testBucket, Object: "restore-obj", Generation: v2.GetGeneration(), IfGenerationMatch: &stale,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RestoreObject precondition err = %v, want FailedPrecondition", err)
	}
	if got := readObject(t, client, testBucket, "restore-obj"); string(got) != "version-one" {
		t.Fatalf("failed restore mutated live object: %q", got)
	}

	// An unknown generation is NotFound.
	if _, err := client.RestoreObject(ctx, &storagepb.RestoreObjectRequest{
		Bucket: testBucket, Object: "restore-obj", Generation: v1.GetGeneration() + v2.GetGeneration() + 1,
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("RestoreObject unknown generation err = %v, want NotFound", err)
	}
}

func TestLockBucketRetentionPolicy(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	// No retention policy yet: lock returns NotFound.
	if _, err := client.LockBucketRetentionPolicy(ctx, &storagepb.LockBucketRetentionPolicyRequest{
		Bucket: testBucket, IfMetagenerationMatch: 1,
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("lock without retention policy err = %v, want NotFound", err)
	}

	// Set an unlocked retention policy via UpdateBucket.
	upd, err := client.UpdateBucket(ctx, &storagepb.UpdateBucketRequest{
		Bucket: &storagepb.Bucket{
			Name:            testBucket,
			RetentionPolicy: &storagepb.Bucket_RetentionPolicy{RetentionDuration: durationpb.New(time.Hour)},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retention_policy"}},
	})
	if err != nil {
		t.Fatalf("UpdateBucket retention policy: %v", err)
	}
	if got := upd.GetRetentionPolicy().GetRetentionDuration().AsDuration(); got != time.Hour {
		t.Fatalf("retention duration = %v, want 1h", got)
	}
	if upd.GetRetentionPolicy().GetIsLocked() {
		t.Fatal("retention policy should start unlocked")
	}
	unlockedMeta := upd.GetMetageneration()

	locked, err := client.LockBucketRetentionPolicy(ctx, &storagepb.LockBucketRetentionPolicyRequest{
		Bucket: testBucket, IfMetagenerationMatch: unlockedMeta,
	})
	if err != nil {
		t.Fatalf("LockBucketRetentionPolicy: %v", err)
	}
	if !locked.GetRetentionPolicy().GetIsLocked() {
		t.Fatal("retention policy not locked after lock")
	}
	if locked.GetMetageneration() != unlockedMeta+1 {
		t.Fatalf("metageneration after lock = %d, want %d", locked.GetMetageneration(), unlockedMeta+1)
	}

	// Locking again is harmless: still locked, metageneration unchanged.
	lockedAgain, err := client.LockBucketRetentionPolicy(ctx, &storagepb.LockBucketRetentionPolicyRequest{
		Bucket: testBucket, IfMetagenerationMatch: locked.GetMetageneration(),
	})
	if err != nil {
		t.Fatalf("second LockBucketRetentionPolicy: %v", err)
	}
	if !lockedAgain.GetRetentionPolicy().GetIsLocked() {
		t.Fatal("retention policy lost its lock on a second attempt")
	}
	if lockedAgain.GetMetageneration() != locked.GetMetageneration() {
		t.Fatalf("second lock bumped metageneration to %d, want %d", lockedAgain.GetMetageneration(), locked.GetMetageneration())
	}

	// The precondition is enforced.
	stale := int64(1)
	if _, err := client.LockBucketRetentionPolicy(ctx, &storagepb.LockBucketRetentionPolicyRequest{
		Bucket: testBucket, IfMetagenerationMatch: stale,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("lock stale precondition err = %v, want FailedPrecondition", err)
	}
}

func TestCancelResumableWrite(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	srw, err := client.StartResumableWrite(ctx, &storagepb.StartResumableWriteRequest{
		WriteObjectSpec: &storagepb.WriteObjectSpec{
			Resource: &storagepb.Object{Name: "cancel-me", Bucket: testBucket, ContentType: "text/plain"},
		},
	})
	if err != nil {
		t.Fatalf("StartResumableWrite: %v", err)
	}
	uploadID := srw.GetUploadId()
	if _, err := client.QueryWriteStatus(ctx, &storagepb.QueryWriteStatusRequest{UploadId: uploadID}); err != nil {
		t.Fatalf("QueryWriteStatus before cancel: %v", err)
	}

	if _, err := client.CancelResumableWrite(ctx, &storagepb.CancelResumableWriteRequest{UploadId: uploadID}); err != nil {
		t.Fatalf("CancelResumableWrite: %v", err)
	}
	if _, err := client.QueryWriteStatus(ctx, &storagepb.QueryWriteStatusRequest{UploadId: uploadID}); status.Code(err) != codes.NotFound {
		t.Fatalf("QueryWriteStatus after cancel err = %v, want NotFound", err)
	}

	// Idempotent for the same id and for an unknown id.
	if _, err := client.CancelResumableWrite(ctx, &storagepb.CancelResumableWriteRequest{UploadId: uploadID}); err != nil {
		t.Fatalf("CancelResumableWrite (idempotent): %v", err)
	}
	if _, err := client.CancelResumableWrite(ctx, &storagepb.CancelResumableWriteRequest{UploadId: "does-not-exist"}); err != nil {
		t.Fatalf("CancelResumableWrite (unknown id): %v", err)
	}

	// An empty upload_id is invalid.
	if _, err := client.CancelResumableWrite(ctx, &storagepb.CancelResumableWriteRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CancelResumableWrite empty id err = %v, want InvalidArgument", err)
	}

	// Cancelling the id of an already-completed upload must not remove the
	// committed object (the session is already gone by completion).
	done, err := client.StartResumableWrite(ctx, &storagepb.StartResumableWriteRequest{
		WriteObjectSpec: &storagepb.WriteObjectSpec{
			Resource: &storagepb.Object{Name: "completed-obj", Bucket: testBucket, ContentType: "text/plain"},
		},
	})
	if err != nil {
		t.Fatalf("StartResumableWrite (completed): %v", err)
	}
	payload := []byte("already committed")
	stream, err := client.WriteObject(ctx)
	if err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	if err := stream.Send(&storagepb.WriteObjectRequest{
		FirstMessage: &storagepb.WriteObjectRequest_UploadId{UploadId: done.GetUploadId()},
		WriteOffset:  0,
		Data:         &storagepb.WriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: payload}},
		FinishWrite:  true,
	}); err != nil {
		t.Fatalf("WriteObject Send: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("WriteObject CloseAndRecv: %v", err)
	}
	if _, err := client.CancelResumableWrite(ctx, &storagepb.CancelResumableWriteRequest{UploadId: done.GetUploadId()}); err != nil {
		t.Fatalf("CancelResumableWrite (completed): %v", err)
	}
	if got := readObject(t, client, testBucket, "completed-obj"); !bytes.Equal(got, payload) {
		t.Fatalf("completed object content = %q, want %q", got, payload)
	}
}

// TestComposeObjectCSEKSources verifies ComposeObject reads its source objects
// with the customer-supplied encryption key in common_object_request_params and
// encrypts the composite destination with that key (J38), matching real GCS:
// all CSEK components must use the same key and the result inherits it.
func TestComposeObjectCSEKSources(t *testing.T) {
	client, _, cleanup := newStorageTestServer(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	key := bytes.Repeat([]byte{0x42}, 32)
	names := []string{"csek-a", "csek-b"}
	payloads := [][]byte{[]byte("part-a-"), []byte("part-b")}

	for i, name := range names {
		w, err := client.BidiWriteObject(ctx)
		if err != nil {
			t.Fatalf("BidiWriteObject: %v", err)
		}
		if err := w.Send(&storagepb.BidiWriteObjectRequest{
			FirstMessage: &storagepb.BidiWriteObjectRequest_WriteObjectSpec{
				WriteObjectSpec: &storagepb.WriteObjectSpec{
					Resource: &storagepb.Object{Name: name, Bucket: testBucket, ContentType: "text/plain"},
				},
			},
			WriteOffset:               0,
			Data:                      &storagepb.BidiWriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: payloads[i]}},
			FinishWrite:               true,
			CommonObjectRequestParams: &storagepb.CommonObjectRequestParams{EncryptionAlgorithm: "AES256", EncryptionKeyBytes: key},
		}); err != nil {
			t.Fatalf("BidiWriteObject Send: %v", err)
		}
		if _, err := w.Recv(); err != nil {
			t.Fatalf("BidiWriteObject Recv: %v", err)
		}
	}

	sources := []*storagepb.ComposeObjectRequest_SourceObject{{Name: "csek-a"}, {Name: "csek-b"}}
	csekParams := func(k []byte) *storagepb.CommonObjectRequestParams {
		return &storagepb.CommonObjectRequestParams{EncryptionAlgorithm: "AES256", EncryptionKeyBytes: k}
	}

	// Without the key a CSEK source cannot be read.
	if _, err := client.ComposeObject(ctx, &storagepb.ComposeObjectRequest{
		Destination:   &storagepb.Object{Name: "csek-composed", Bucket: testBucket},
		SourceObjects: sources,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ComposeObject without key err = %v, want InvalidArgument", err)
	}
	// A different key is an incorrect source key.
	if _, err := client.ComposeObject(ctx, &storagepb.ComposeObjectRequest{
		Destination:               &storagepb.Object{Name: "csek-composed", Bucket: testBucket},
		SourceObjects:             sources,
		CommonObjectRequestParams: csekParams(bytes.Repeat([]byte{0x24}, 32)),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ComposeObject wrong key err = %v, want InvalidArgument", err)
	}
	// A key on a non-CSEK source is rejected (J36 semantics).
	writeSingleShot(t, client, testBucket, "plain-src", "text/plain", []byte("plain"))
	if _, err := client.ComposeObject(ctx, &storagepb.ComposeObjectRequest{
		Destination:               &storagepb.Object{Name: "plain-composed", Bucket: testBucket},
		SourceObjects:             []*storagepb.ComposeObjectRequest_SourceObject{{Name: "plain-src"}},
		CommonObjectRequestParams: csekParams(key),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ComposeObject plain source with key err = %v, want InvalidArgument", err)
	}

	// With the key the composite is created.
	composed, err := client.ComposeObject(ctx, &storagepb.ComposeObjectRequest{
		Destination:               &storagepb.Object{Name: "csek-composed", Bucket: testBucket},
		SourceObjects:             sources,
		CommonObjectRequestParams: csekParams(key),
	})
	if err != nil {
		t.Fatalf("ComposeObject: %v", err)
	}
	if composed.GetComponentCount() != 2 {
		t.Fatalf("componentCount = %d, want 2", composed.GetComponentCount())
	}

	// The composite is encrypted with the key: readable with it, not without.
	if got := readObjectCSEK(t, client, testBucket, "csek-composed", key); string(got) != "part-a-part-b" {
		t.Fatalf("composed content = %q, want %q", got, "part-a-part-b")
	}
	stream, err := client.ReadObject(ctx, &storagepb.ReadObjectRequest{Bucket: testBucket, Object: "csek-composed"})
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("read without key err = %v, want InvalidArgument", err)
	}
}

// readObjectCSEK reads an object's full content via ReadObject, carrying the
// customer-supplied encryption key in common_object_request_params.
func readObjectCSEK(t *testing.T, client storagepb.StorageClient, bucket, object string, key []byte) []byte {
	t.Helper()
	stream, err := client.ReadObject(context.Background(), &storagepb.ReadObjectRequest{
		Bucket:                    bucket,
		Object:                    object,
		CommonObjectRequestParams: &storagepb.CommonObjectRequestParams{EncryptionAlgorithm: "AES256", EncryptionKeyBytes: key},
	})
	if err != nil {
		t.Fatalf("ReadObject with CSEK: %v", err)
	}
	var out []byte
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadObject Recv: %v", err)
		}
		if cd := msg.GetChecksummedData(); cd != nil {
			out = append(out, cd.GetContent()...)
		}
	}
	return out
}
