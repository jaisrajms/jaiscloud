package storage

import (
	"bytes"
	"context"
	"io"
	"testing"

	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// bidiResult collects one BidiReadObject stream's first-message metadata/handle
// and every ObjectRangeData, keyed by read_id.
type bidiResult struct {
	metadata *storagepb.Object
	handle   []byte
	data     map[int64][]byte
	end      map[int64]bool
	// dataMsgs counts ObjectRangeData messages that carried content, so tests
	// can assert a large range was split across multiple chunks.
	dataMsgs int
}

// drainBidi sends req then half-closes the stream and reads every response.
func drainBidi(t *testing.T, stream storagepb.Storage_BidiReadObjectClient, req *storagepb.BidiReadObjectRequest) *bidiResult {
	t.Helper()
	if err := stream.Send(req); err != nil {
		t.Fatalf("BidiReadObject Send: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("BidiReadObject CloseSend: %v", err)
	}
	res := &bidiResult{
		data: map[int64][]byte{},
		end:  map[int64]bool{},
	}
	first := true
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("BidiReadObject Recv: %v", err)
		}
		if first {
			res.metadata = msg.GetMetadata()
			res.handle = msg.GetReadHandle().GetHandle()
			first = false
		}
		for _, odr := range msg.GetObjectDataRanges() {
			id := odr.GetReadRange().GetReadId()
			if cd := odr.GetChecksummedData(); cd != nil {
				if len(cd.GetContent()) > 0 {
					res.dataMsgs++
				}
				res.data[id] = append(res.data[id], cd.GetContent()...)
			}
			if odr.GetRangeEnd() {
				res.end[id] = true
			}
		}
	}
	return res
}

func bidiObjectSpec(bucket, object string) *storagepb.BidiReadObjectSpec {
	return &storagepb.BidiReadObjectSpec{Bucket: bucket, Object: object}
}

// recvBidiError drains a stream until it fails, returning the terminal error.
// BidiReadObject sends the object metadata before serving ranges, so a range
// error arrives after any already-queued response messages.
func recvBidiError(t *testing.T, stream storagepb.Storage_BidiReadObjectClient) error {
	t.Helper()
	for {
		if _, err := stream.Recv(); err != nil {
			return err
		}
	}
}

func TestBidiReadObjectFullObject(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	createBucket(t, client, "bucket-a", "US")

	payload := []byte("0123456789abcdefghij")
	writeSingleShot(t, client, testBucket, "bidi-full", "text/plain", payload)

	stream, err := client.BidiReadObject(context.Background())
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	res := drainBidi(t, stream, &storagepb.BidiReadObjectRequest{
		ReadObjectSpec: bidiObjectSpec(testBucket, "bidi-full"),
		ReadRanges:     []*storagepb.ReadRange{{ReadOffset: 0, ReadId: 1}},
	})
	if res.metadata == nil {
		t.Fatal("first BidiReadObject response missing metadata")
	}
	if res.metadata.GetSize() != int64(len(payload)) {
		t.Fatalf("metadata size = %d, want %d", res.metadata.GetSize(), len(payload))
	}
	if len(res.handle) == 0 {
		t.Fatal("first BidiReadObject response missing read_handle")
	}
	if !bytes.Equal(res.data[1], payload) {
		t.Fatalf("range 1 = %q, want %q", res.data[1], payload)
	}
	if !res.end[1] {
		t.Fatalf("range 1 missing range_end: %+v", res)
	}
}

func TestBidiReadObjectMultipleRanges(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	createBucket(t, client, "bucket-a", "US")

	payload := []byte("abcdefghijklmnopqrstuvwxyz")
	writeSingleShot(t, client, testBucket, "bidi-multi", "text/plain", payload)

	stream, err := client.BidiReadObject(context.Background())
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	res := drainBidi(t, stream, &storagepb.BidiReadObjectRequest{
		ReadObjectSpec: bidiObjectSpec(testBucket, "bidi-multi"),
		ReadRanges: []*storagepb.ReadRange{
			{ReadOffset: 0, ReadLength: 3, ReadId: 7},
			{ReadOffset: 23, ReadLength: 3, ReadId: 8},
			{ReadOffset: 10, ReadId: 9},
		},
	})
	if got := string(res.data[7]); got != "abc" {
		t.Fatalf("range 7 = %q, want abc", got)
	}
	if got := string(res.data[8]); got != "xyz" {
		t.Fatalf("range 8 = %q, want xyz", got)
	}
	if got := string(res.data[9]); got != "klmnopqrstuvwxyz" {
		t.Fatalf("range 9 = %q, want klmnopqrstuvwxyz", got)
	}
	for _, id := range []int64{7, 8, 9} {
		if !res.end[id] {
			t.Fatalf("range %d missing range_end", id)
		}
	}
}

func TestBidiReadObjectNegativeOffsetAndZeroLength(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	createBucket(t, client, "bucket-a", "US")

	payload := []byte("0123456789") // size 10
	writeSingleShot(t, client, testBucket, "bidi-neg", "text/plain", payload)

	stream, err := client.BidiReadObject(context.Background())
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	res := drainBidi(t, stream, &storagepb.BidiReadObjectRequest{
		ReadObjectSpec: bidiObjectSpec(testBucket, "bidi-neg"),
		ReadRanges: []*storagepb.ReadRange{
			{ReadOffset: -5, ReadLength: 3, ReadId: 1},
			{ReadOffset: -100, ReadId: 2}, // magnitude > size → whole object
		},
	})
	if got := string(res.data[1]); got != "567" {
		t.Fatalf("negative offset range = %q, want 567", got)
	}
	if !bytes.Equal(res.data[2], payload) {
		t.Fatalf("clamped negative offset range = %q, want %q", res.data[2], payload)
	}
}

func TestBidiReadObjectRangeErrors(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	createBucket(t, client, "bucket-a", "US")
	writeSingleShot(t, client, testBucket, "bidi-err", "text/plain", []byte("0123456789"))

	cases := []struct {
		name string
		rng  *storagepb.ReadRange
	}{
		{"offset past EOF", &storagepb.ReadRange{ReadOffset: 11, ReadId: 1}},
		{"negative length", &storagepb.ReadRange{ReadOffset: 0, ReadLength: -1, ReadId: 2}},
	}
	for _, tc := range cases {
		stream, err := client.BidiReadObject(context.Background())
		if err != nil {
			t.Fatalf("%s: BidiReadObject: %v", tc.name, err)
		}
		if err := stream.Send(&storagepb.BidiReadObjectRequest{
			ReadObjectSpec: bidiObjectSpec(testBucket, "bidi-err"),
			ReadRanges:     []*storagepb.ReadRange{tc.rng},
		}); err != nil {
			t.Fatalf("%s: Send: %v", tc.name, err)
		}
		if got := recvBidiError(t, stream); status.Code(got) != codes.OutOfRange {
			t.Fatalf("%s: code = %v, want OutOfRange (err=%v)", tc.name, status.Code(got), got)
		}
	}
}

func TestBidiReadObjectPreconditionsAndGeneration(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := client.CreateBucket(ctx, &storagepb.CreateBucketRequest{
		Parent:   "projects/_",
		BucketId: "bucket-a",
		Bucket:   &storagepb.Bucket{Project: "projects/test-project", Location: "US", Versioning: &storagepb.Bucket_Versioning{Enabled: true}},
	}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	v1 := writeSingleShot(t, client, testBucket, "bidi-gen", "text/plain", []byte("version-one"))
	v2 := writeSingleShot(t, client, testBucket, "bidi-gen", "text/plain", []byte("version-two"))

	// Reading a specific generation returns that revision.
	stream, err := client.BidiReadObject(ctx)
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	res := drainBidi(t, stream, &storagepb.BidiReadObjectRequest{
		ReadObjectSpec: &storagepb.BidiReadObjectSpec{Bucket: testBucket, Object: "bidi-gen", Generation: v1.GetGeneration()},
		ReadRanges:     []*storagepb.ReadRange{{ReadId: 1}},
	})
	if got := string(res.data[1]); got != "version-one" {
		t.Fatalf("generation read = %q, want version-one", got)
	}

	// A stale if_generation_match fails loud.
	stale := v1.GetGeneration()
	stream, err = client.BidiReadObject(ctx)
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	if err := stream.Send(&storagepb.BidiReadObjectRequest{
		ReadObjectSpec: &storagepb.BidiReadObjectSpec{Bucket: testBucket, Object: "bidi-gen", IfGenerationMatch: &stale},
		ReadRanges:     []*storagepb.ReadRange{{ReadId: 1}},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale if_generation_match: code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}

	// A matching if_generation_match succeeds.
	live := v2.GetGeneration()
	stream, err = client.BidiReadObject(ctx)
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	res = drainBidi(t, stream, &storagepb.BidiReadObjectRequest{
		ReadObjectSpec: &storagepb.BidiReadObjectSpec{Bucket: testBucket, Object: "bidi-gen", IfGenerationMatch: &live},
		ReadRanges:     []*storagepb.ReadRange{{ReadId: 1}},
	})
	if got := string(res.data[1]); got != "version-two" {
		t.Fatalf("matching if_generation_match read = %q, want version-two", got)
	}
}

func TestBidiReadObjectReadHandleFlow(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	createBucket(t, client, "bucket-a", "US")

	payload := []byte("handle-flow-payload")
	writeSingleShot(t, client, testBucket, "bidi-handle", "text/plain", payload)

	// First stream: normal spec, capture the minted handle.
	first, err := client.BidiReadObject(context.Background())
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	firstRes := drainBidi(t, first, &storagepb.BidiReadObjectRequest{
		ReadObjectSpec: bidiObjectSpec(testBucket, "bidi-handle"),
		ReadRanges:     []*storagepb.ReadRange{{ReadId: 1}},
	})
	if len(firstRes.handle) == 0 {
		t.Fatal("first stream did not mint a read handle")
	}
	if firstRes.metadata == nil {
		t.Fatal("first stream should carry metadata")
	}

	// Second stream: handle only, no bucket/object/generation. Metadata is
	// deliberately omitted, matching real GCS.
	second, err := client.BidiReadObject(context.Background())
	if err != nil {
		t.Fatalf("BidiReadObject(handle): %v", err)
	}
	secondRes := drainBidi(t, second, &storagepb.BidiReadObjectRequest{
		ReadObjectSpec: &storagepb.BidiReadObjectSpec{ReadHandle: &storagepb.BidiReadHandle{Handle: firstRes.handle}},
		ReadRanges:     []*storagepb.ReadRange{{ReadOffset: 7, ReadLength: 4, ReadId: 1}},
	})
	if secondRes.metadata != nil {
		t.Fatalf("handle-opened stream returned metadata: %+v", secondRes.metadata)
	}
	if got := string(secondRes.data[1]); got != "flow" {
		t.Fatalf("handle-opened read = %q, want flow", got)
	}

	// An invalid handle is rejected.
	bad, err := client.BidiReadObject(context.Background())
	if err != nil {
		t.Fatalf("BidiReadObject(bad handle): %v", err)
	}
	if err := bad.Send(&storagepb.BidiReadObjectRequest{
		ReadObjectSpec: &storagepb.BidiReadObjectSpec{ReadHandle: &storagepb.BidiReadHandle{Handle: []byte("not-a-handle")}},
		ReadRanges:     []*storagepb.ReadRange{{ReadId: 1}},
	}); err != nil {
		t.Fatalf("bad handle Send: %v", err)
	}
	if _, err := bad.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid handle: code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}

	// A handle that disagrees with the explicit spec is rejected.
	mismatch, err := client.BidiReadObject(context.Background())
	if err != nil {
		t.Fatalf("BidiReadObject(mismatch): %v", err)
	}
	if err := mismatch.Send(&storagepb.BidiReadObjectRequest{
		ReadObjectSpec: &storagepb.BidiReadObjectSpec{
			Bucket:     testBucket,
			Object:     "other-object",
			ReadHandle: &storagepb.BidiReadHandle{Handle: firstRes.handle},
		},
		ReadRanges: []*storagepb.ReadRange{{ReadId: 1}},
	}); err != nil {
		t.Fatalf("mismatch Send: %v", err)
	}
	if _, err := mismatch.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("handle/spec mismatch: code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

func TestBidiReadObjectValidation(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	createBucket(t, client, "bucket-a", "US")
	writeSingleShot(t, client, testBucket, "bidi-valid", "text/plain", []byte("data"))

	t.Run("missing spec on first message", func(t *testing.T) {
		stream, err := client.BidiReadObject(context.Background())
		if err != nil {
			t.Fatalf("BidiReadObject: %v", err)
		}
		if err := stream.Send(&storagepb.BidiReadObjectRequest{ReadRanges: []*storagepb.ReadRange{{ReadId: 1}}}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
		}
	})

	t.Run("duplicate read_id", func(t *testing.T) {
		stream, err := client.BidiReadObject(context.Background())
		if err != nil {
			t.Fatalf("BidiReadObject: %v", err)
		}
		if err := stream.Send(&storagepb.BidiReadObjectRequest{
			ReadObjectSpec: bidiObjectSpec(testBucket, "bidi-valid"),
			ReadRanges:     []*storagepb.ReadRange{{ReadId: 5}, {ReadId: 5}},
		}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		if got := recvBidiError(t, stream); status.Code(got) != codes.InvalidArgument {
			t.Fatalf("code = %v, want InvalidArgument (err=%v)", status.Code(got), got)
		}
	})

	t.Run("spec on later message", func(t *testing.T) {
		stream, err := client.BidiReadObject(context.Background())
		if err != nil {
			t.Fatalf("BidiReadObject: %v", err)
		}
		if err := stream.Send(&storagepb.BidiReadObjectRequest{ReadObjectSpec: bidiObjectSpec(testBucket, "bidi-valid")}); err != nil {
			t.Fatalf("Send first: %v", err)
		}
		if _, err := stream.Recv(); err != nil {
			t.Fatalf("Recv first response: %v", err)
		}
		if err := stream.Send(&storagepb.BidiReadObjectRequest{
			ReadObjectSpec: bidiObjectSpec(testBucket, "bidi-valid"),
			ReadRanges:     []*storagepb.ReadRange{{ReadId: 1}},
		}); err != nil {
			t.Fatalf("Send second: %v", err)
		}
		if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
		}
	})

	t.Run("read_mask unsupported", func(t *testing.T) {
		stream, err := client.BidiReadObject(context.Background())
		if err != nil {
			t.Fatalf("BidiReadObject: %v", err)
		}
		spec := bidiObjectSpec(testBucket, "bidi-valid")
		spec.ReadMask = &fieldmaskpb.FieldMask{Paths: []string{"name"}}
		if err := stream.Send(&storagepb.BidiReadObjectRequest{ReadObjectSpec: spec}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		if _, err := stream.Recv(); status.Code(err) != codes.Unimplemented {
			t.Fatalf("code = %v, want Unimplemented (err=%v)", status.Code(err), err)
		}
	})

	t.Run("routing_token unsupported", func(t *testing.T) {
		stream, err := client.BidiReadObject(context.Background())
		if err != nil {
			t.Fatalf("BidiReadObject: %v", err)
		}
		token := "route-me"
		spec := bidiObjectSpec(testBucket, "bidi-valid")
		spec.RoutingToken = &token
		if err := stream.Send(&storagepb.BidiReadObjectRequest{ReadObjectSpec: spec}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		if _, err := stream.Recv(); status.Code(err) != codes.Unimplemented {
			t.Fatalf("code = %v, want Unimplemented (err=%v)", status.Code(err), err)
		}
	})
}

func TestBidiReadObjectNotFound(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	createBucket(t, client, "bucket-a", "US")

	stream, err := client.BidiReadObject(context.Background())
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	if err := stream.Send(&storagepb.BidiReadObjectRequest{
		ReadObjectSpec: bidiObjectSpec(testBucket, "missing"),
		ReadRanges:     []*storagepb.ReadRange{{ReadId: 1}},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound (err=%v)", status.Code(err), err)
	}
}

func TestBidiReadObjectCSEK(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")

	key := bytes.Repeat([]byte{0x42}, 32)
	payload := []byte("customer-supplied-key-payload")

	// Write through BidiWriteObject with the CSEK, then read it back through
	// BidiReadObject with the same key.
	w, err := client.BidiWriteObject(ctx)
	if err != nil {
		t.Fatalf("BidiWriteObject: %v", err)
	}
	if err := w.Send(&storagepb.BidiWriteObjectRequest{
		FirstMessage: &storagepb.BidiWriteObjectRequest_WriteObjectSpec{
			WriteObjectSpec: &storagepb.WriteObjectSpec{
				Resource: &storagepb.Object{Name: "bidi-csek", Bucket: testBucket, ContentType: "text/plain"},
			},
		},
		WriteOffset:               0,
		Data:                      &storagepb.BidiWriteObjectRequest_ChecksummedData{ChecksummedData: &storagepb.ChecksummedData{Content: payload}},
		FinishWrite:               true,
		CommonObjectRequestParams: &storagepb.CommonObjectRequestParams{EncryptionAlgorithm: "AES256", EncryptionKeyBytes: key},
	}); err != nil {
		t.Fatalf("BidiWriteObject Send: %v", err)
	}
	if _, err := w.Recv(); err != nil {
		t.Fatalf("BidiWriteObject Recv: %v", err)
	}

	stream, err := client.BidiReadObject(ctx)
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	res := drainBidi(t, stream, &storagepb.BidiReadObjectRequest{
		ReadObjectSpec: &storagepb.BidiReadObjectSpec{
			Bucket:                    testBucket,
			Object:                    "bidi-csek",
			CommonObjectRequestParams: &storagepb.CommonObjectRequestParams{EncryptionAlgorithm: "AES256", EncryptionKeyBytes: key},
		},
		ReadRanges: []*storagepb.ReadRange{{ReadId: 1}},
	})
	if !bytes.Equal(res.data[1], payload) {
		t.Fatalf("CSEK read = %q, want %q", res.data[1], payload)
	}
}

func TestBidiReadObjectHandleWithExplicitObjectKeepsMetadata(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	createBucket(t, client, "bucket-a", "US")

	payload := []byte("handle-and-object")
	writeSingleShot(t, client, testBucket, "bidi-handle-obj", "text/plain", payload)

	first, err := client.BidiReadObject(context.Background())
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	firstRes := drainBidi(t, first, &storagepb.BidiReadObjectRequest{
		ReadObjectSpec: bidiObjectSpec(testBucket, "bidi-handle-obj"),
		ReadRanges:     []*storagepb.ReadRange{{ReadId: 1}},
	})

	// The official client always sends bucket/object alongside a read handle.
	// Metadata must still be present so the client can size a negative-offset
	// read (an absent metadata makes the SDK terminate at size 0).
	second, err := client.BidiReadObject(context.Background())
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	res := drainBidi(t, second, &storagepb.BidiReadObjectRequest{
		ReadObjectSpec: &storagepb.BidiReadObjectSpec{
			Bucket:     testBucket,
			Object:     "bidi-handle-obj",
			ReadHandle: &storagepb.BidiReadHandle{Handle: firstRes.handle},
		},
		ReadRanges: []*storagepb.ReadRange{{ReadOffset: -6, ReadLength: 6, ReadId: 1}},
	})
	if res.metadata == nil {
		t.Fatal("handle+explicit-object stream omitted metadata")
	}
	if got := string(res.data[1]); got != "object" {
		t.Fatalf("handle+explicit-object read = %q, want object", got)
	}
}

func TestBidiReadObjectEmptyWindowAtEOF(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	createBucket(t, client, "bucket-a", "US")

	payload := []byte("short")
	writeSingleShot(t, client, testBucket, "bidi-eof", "text/plain", payload)

	stream, err := client.BidiReadObject(context.Background())
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	// offset == size is an empty window and is allowed; only offset > size is
	// OutOfRange.
	res := drainBidi(t, stream, &storagepb.BidiReadObjectRequest{
		ReadObjectSpec: bidiObjectSpec(testBucket, "bidi-eof"),
		ReadRanges:     []*storagepb.ReadRange{{ReadOffset: int64(len(payload)), ReadId: 1}},
	})
	if len(res.data[1]) != 0 {
		t.Fatalf("empty window returned %q, want no data", res.data[1])
	}
	if !res.end[1] {
		t.Fatal("empty window did not terminate the range")
	}
}

func TestBidiReadObjectTooManyRanges(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	createBucket(t, client, "bucket-a", "US")
	writeSingleShot(t, client, testBucket, "bidi-many", "text/plain", []byte("data"))

	stream, err := client.BidiReadObject(context.Background())
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	ranges := make([]*storagepb.ReadRange, 0, maxBidiReadRanges+1)
	for i := 0; i <= maxBidiReadRanges; i++ {
		ranges = append(ranges, &storagepb.ReadRange{ReadOffset: 0, ReadLength: 1, ReadId: int64(i)})
	}
	if err := stream.Send(&storagepb.BidiReadObjectRequest{
		ReadObjectSpec: bidiObjectSpec(testBucket, "bidi-many"),
		ReadRanges:     ranges,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := recvBidiError(t, stream); status.Code(got) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument (err=%v)", status.Code(got), got)
	}
}

func TestBidiReadObjectBadEncryptionAlgorithm(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	createBucket(t, client, "bucket-a", "US")
	writeSingleShot(t, client, testBucket, "bidi-alg", "text/plain", []byte("data"))

	stream, err := client.BidiReadObject(context.Background())
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	spec := bidiObjectSpec(testBucket, "bidi-alg")
	spec.CommonObjectRequestParams = &storagepb.CommonObjectRequestParams{EncryptionAlgorithm: "AES128"}
	if err := stream.Send(&storagepb.BidiReadObjectRequest{ReadObjectSpec: spec}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

func TestBidiReadObjectHandleGenerationMismatch(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	createBucket(t, client, "bucket-a", "US")
	writeSingleShot(t, client, testBucket, "bidi-gen-mismatch", "text/plain", []byte("data"))

	first, err := client.BidiReadObject(context.Background())
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	firstRes := drainBidi(t, first, &storagepb.BidiReadObjectRequest{
		ReadObjectSpec: bidiObjectSpec(testBucket, "bidi-gen-mismatch"),
		ReadRanges:     []*storagepb.ReadRange{{ReadId: 1}},
	})

	stream, err := client.BidiReadObject(context.Background())
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	if err := stream.Send(&storagepb.BidiReadObjectRequest{
		ReadObjectSpec: &storagepb.BidiReadObjectSpec{
			Bucket:     testBucket,
			Object:     "bidi-gen-mismatch",
			Generation: 99999999,
			ReadHandle: &storagepb.BidiReadHandle{Handle: firstRes.handle},
		},
		ReadRanges: []*storagepb.ReadRange{{ReadId: 1}},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

func TestBidiReadObjectMultiChunk(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	createBucket(t, client, "bucket-a", "US")

	payload := bytes.Repeat([]byte("x"), bidiReadChunkSize+123)
	writeSingleShot(t, client, testBucket, "bidi-chunked", "text/plain", payload)

	stream, err := client.BidiReadObject(context.Background())
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	res := drainBidi(t, stream, &storagepb.BidiReadObjectRequest{
		ReadObjectSpec: bidiObjectSpec(testBucket, "bidi-chunked"),
		ReadRanges:     []*storagepb.ReadRange{{ReadOffset: 0, ReadId: 1}},
	})
	if res.dataMsgs < 2 {
		t.Fatalf("large range delivered %d data message(s), want >1 chunk", res.dataMsgs)
	}
	if !bytes.Equal(res.data[1], payload) {
		t.Fatalf("chunked read length = %d, want %d", len(res.data[1]), len(payload))
	}
	if !res.end[1] {
		t.Fatal("chunked read did not terminate")
	}
}

func TestBidiReadObjectIncrementalRanges(t *testing.T) {
	client, cleanup := storageTestService(t)
	defer cleanup()
	ctx := context.Background()
	createBucket(t, client, "bucket-a", "US")
	writeSingleShot(t, client, testBucket, "bidi-incremental", "text/plain", []byte("0123456789"))

	stream, err := client.BidiReadObject(ctx)
	if err != nil {
		t.Fatalf("BidiReadObject: %v", err)
	}
	// Message 1: spec only (as MultiRangeDownloader sends).
	if err := stream.Send(&storagepb.BidiReadObjectRequest{ReadObjectSpec: bidiObjectSpec(testBucket, "bidi-incremental")}); err != nil {
		t.Fatalf("Send spec: %v", err)
	}
	meta, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv metadata: %v", err)
	}
	if meta.GetMetadata() == nil {
		t.Fatal("first response missing metadata")
	}
	// Message 2: the first range, sent after the stream is already open.
	if err := stream.Send(&storagepb.BidiReadObjectRequest{
		ReadRanges: []*storagepb.ReadRange{{ReadOffset: 2, ReadLength: 3, ReadId: 1}},
	}); err != nil {
		t.Fatalf("Send range: %v", err)
	}
	var out []byte
	for {
		msg, rerr := stream.Recv()
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			t.Fatalf("Recv range: %v", rerr)
		}
		done := false
		for _, odr := range msg.GetObjectDataRanges() {
			if odr.GetReadRange().GetReadId() != 1 {
				continue
			}
			out = append(out, odr.GetChecksummedData().GetContent()...)
			if odr.GetRangeEnd() {
				done = true
			}
		}
		if done {
			break
		}
	}
	if string(out) != "234" {
		t.Fatalf("incremental range = %q, want 234", out)
	}
	_ = stream.CloseSend()
}
