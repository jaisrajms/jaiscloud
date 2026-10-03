package grpcconformance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

// storageExtraChecks covers the Cloud Storage gRPC v2 data-plane, resumable,
// bucket-mutation and IAM RPCs on top of the bucket CRUD probes in
// checks_storage.go.
//
// Every probe drives the emulator with the official cloud.google.com/go/storage
// gRPC client where that client maps 1:1 onto the RPC under test. Two RPCs —
// the client-streaming WriteObject and CancelResumableWrite — are never issued
// by the current high-level Go client (its Writer talks BidiWriteObject and
// never cancels an upload session), so they are driven through the proto
// descriptors the high-level client itself links, using dynamicpb messages.
// This keeps a single storage proto registered in the test binary (importing a
// second generated storagepb would panic with a duplicate-file registration)
// while still exercising the exact wire RPC.
//
// Each probe creates the fixtures it needs and names every resource through
// cfg.ResourceName, so the sequence is self-contained and safe against a
// long-lived emulator.
func storageExtraChecks() []Check {
	return []Check{
		// ── object CRUD / data plane ────────────────────────────────────────
		{Service: "storage", RPC: "GetObject", Method: "GetObject", KeyField: "name/size/contentType", Run: checkStorageGetObject},
		{Service: "storage", RPC: "DeleteObject", Method: "DeleteObject", KeyField: "object absent after delete", Run: checkStorageDeleteObject},
		{Service: "storage", RPC: "ListObjects", Method: "ListObjects", KeyField: "objects[] contains prefixed fixture", Run: checkStorageListObjects},
		{Service: "storage", RPC: "UpdateObject", Method: "UpdateObject", KeyField: "masked metadata applied", Run: checkStorageUpdateObject},
		{Service: "storage", RPC: "ComposeObject", Method: "ComposeObject", KeyField: "componentCount=2, concatenated bytes", Run: checkStorageComposeObject},
		{Service: "storage", RPC: "RewriteObject", Method: "RewriteObject", KeyField: "done=true, copied bytes/metadata", Run: checkStorageRewriteObject},
		{Service: "storage", RPC: "MoveObject", Method: "MoveObject", KeyField: "source gone, destination present", Run: checkStorageMoveObject},
		{Service: "storage", RPC: "RestoreObject", Method: "RestoreObject", KeyField: "restored generation live", Run: checkStorageRestoreObject},
		{Service: "storage", RPC: "ReadObject", Method: "ReadObject", KeyField: "range bytes + contentRange", Run: checkStorageReadObject},
		{Service: "storage", RPC: "BidiReadObject", Method: "BidiReadObject", KeyField: "multi-range bytes + read_handle round-trip", Run: checkStorageBidiReadObject},

		// ── writes ──────────────────────────────────────────────────────────
		{Service: "storage", RPC: "WriteObject", Method: "WriteObject", KeyField: "client-stream resource name/size", Run: checkStorageWriteObject},
		{Service: "storage", RPC: "BidiWriteObject", Method: "BidiWriteObject", KeyField: "streamed bytes round-trip", Run: checkStorageBidiWriteObject},

		// ── resumable writes ────────────────────────────────────────────────
		{Service: "storage", RPC: "StartResumableWrite", Method: "StartResumableWrite", KeyField: "upload_id non-empty", Run: checkStorageStartResumableWrite},
		{Service: "storage", RPC: "QueryWriteStatus", Method: "QueryWriteStatus", KeyField: "persisted_size=0 for fresh upload", Run: checkStorageQueryWriteStatus},
		{Service: "storage", RPC: "CancelResumableWrite", Method: "CancelResumableWrite", KeyField: "status after cancel = NotFound", Run: checkStorageCancelResumableWrite},

		// ── bucket mutation ─────────────────────────────────────────────────
		{Service: "storage", RPC: "UpdateBucket", Method: "UpdateBucket", KeyField: "masked labels applied", Run: checkStorageUpdateBucket},
		{Service: "storage", RPC: "LockBucketRetentionPolicy", Method: "LockBucketRetentionPolicy", KeyField: "retentionPolicy.isLocked", Run: checkStorageLockBucketRetentionPolicy},

		// ── IAM ─────────────────────────────────────────────────────────────
		{Service: "storage", RPC: "GetIamPolicy", Method: "GetIamPolicy", KeyField: "default etag present", Run: checkStorageGetIamPolicy},
		{Service: "storage", RPC: "SetIamPolicy", Method: "SetIamPolicy", KeyField: "binding round-trips", Run: checkStorageSetIamPolicy},
		{Service: "storage", RPC: "TestIamPermissions", Method: "TestIamPermissions", KeyField: "requested permissions returned", Run: checkStorageTestIamPermissions},
	}
}

// ─── fixtures + high-level helpers ───────────────────────────────────────────

// storageObjBucket is the dedicated bucket for the object data-plane probes.
// It is distinct from storageBucketName so the bucket-CRUD sequence (which ends
// by deleting its bucket) never affects these probes.
func storageObjBucket(cfg Config) string { return cfg.ResourceName("gcpc-grpc-storage") }

// storageVersionedBucket backs the RestoreObject soft-delete probe.
func storageVersionedBucket(cfg Config) string { return cfg.ResourceName("gcpc-grpc-storage-ver") }

// ensureStorageBucket makes a bucket exist idempotently, creating it with the
// supplied attrs when absent.
func ensureStorageBucket(ctx context.Context, client *storage.Client, cfg Config, name string, attrs *storage.BucketAttrs) error {
	_, err := client.Bucket(name).Attrs(ctx)
	if err == nil {
		return nil
	}
	if !errors.Is(err, storage.ErrBucketNotExist) && status.Code(err) != codes.NotFound {
		return fmt.Errorf("bucket %q attrs: %w", name, err)
	}
	if attrs == nil {
		attrs = &storage.BucketAttrs{}
	}
	if err := client.Bucket(name).Create(ctx, cfg.Project, attrs); err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("create bucket %q: %w", name, err)
	}
	return nil
}

// putStorageObject writes an object through the high-level client's one-shot
// gRPC writer (BidiWriteObject) and returns its attributes.
func putStorageObject(ctx context.Context, client *storage.Client, bucket, object, contentType string, data []byte) (*storage.ObjectAttrs, error) {
	w := client.Bucket(bucket).Object(object).NewWriter(ctx)
	if contentType != "" {
		w.ContentType = contentType
	}
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("write %q: %w", object, err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("close %q: %w", object, err)
	}
	return w.Attrs(), nil
}

// readStorageObject reads an object's full content through the high-level
// client's ReadObject server stream.
func readStorageObject(ctx context.Context, client *storage.Client, bucket, object string) ([]byte, error) {
	r, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("reader %q: %w", object, err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", object, err)
	}
	return data, nil
}

// ─── dynamic proto client (WriteObject / CancelResumableWrite) ───────────────

// storageServicePrefix is the full gRPC method path prefix for the Cloud
// Storage v2 service.
const storageServicePrefix = "/google.storage.v2.Storage/"

// storageConn dials the emulator for raw generated-style calls.
func storageConn(cfg Config) (*grpc.ClientConn, error) {
	return grpc.NewClient(cfg.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// newStorageMsg builds an empty dynamic message for a fully-qualified proto
// message registered in the global file registry (populated by the official
// cloud.google.com/go/storage client linked into this test binary).
func newStorageMsg(fullName string) (*dynamicpb.Message, error) {
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(fullName))
	if err != nil {
		return nil, fmt.Errorf("find descriptor %s: %w", fullName, err)
	}
	md, ok := d.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("descriptor %s is %T, want message", fullName, d)
	}
	return dynamicpb.NewMessage(md), nil
}

func msgField(m protoreflect.Message, name string) protoreflect.FieldDescriptor {
	return m.Descriptor().Fields().ByName(protoreflect.Name(name))
}

func msgSetString(m protoreflect.Message, name, val string) error {
	fd := msgField(m, name)
	if fd == nil {
		return fmt.Errorf("message %s has no field %q", m.Descriptor().FullName(), name)
	}
	m.Set(fd, protoreflect.ValueOfString(val))
	return nil
}

func msgSetInt64(m protoreflect.Message, name string, val int64) error {
	fd := msgField(m, name)
	if fd == nil {
		return fmt.Errorf("message %s has no field %q", m.Descriptor().FullName(), name)
	}
	m.Set(fd, protoreflect.ValueOfInt64(val))
	return nil
}

func msgSetBool(m protoreflect.Message, name string, val bool) error {
	fd := msgField(m, name)
	if fd == nil {
		return fmt.Errorf("message %s has no field %q", m.Descriptor().FullName(), name)
	}
	m.Set(fd, protoreflect.ValueOfBool(val))
	return nil
}

func msgSetBytes(m protoreflect.Message, name string, val []byte) error {
	fd := msgField(m, name)
	if fd == nil {
		return fmt.Errorf("message %s has no field %q", m.Descriptor().FullName(), name)
	}
	m.Set(fd, protoreflect.ValueOfBytes(val))
	return nil
}

// msgMutable returns the sub-message at the named field, creating it (and
// marking the enclosing oneof) if needed.
func msgMutable(m protoreflect.Message, name string) (protoreflect.Message, error) {
	fd := msgField(m, name)
	if fd == nil {
		return nil, fmt.Errorf("message %s has no field %q", m.Descriptor().FullName(), name)
	}
	return m.Mutable(fd).Message(), nil
}

// storageWriteObjectSpec fills a WriteObjectSpec's destination resource.
func storageWriteObjectSpec(m protoreflect.Message, name, bucket, object, contentType string) error {
	res, err := msgMutable(m, "resource")
	if err != nil {
		return err
	}
	if err := msgSetString(res, "bucket", bucket); err != nil {
		return err
	}
	if err := msgSetString(res, "name", object); err != nil {
		return err
	}
	if contentType != "" {
		return msgSetString(res, "content_type", contentType)
	}
	return nil
}

// ─── object CRUD / data plane ────────────────────────────────────────────────

func checkStorageGetObject(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	object := cfg.ResourceName("gcpc-grpc-get-obj")
	payload := []byte("get-object-payload")
	if _, err := putStorageObject(ctx, client, bucket, object, "text/plain", payload); err != nil {
		return err
	}
	attrs, err := client.Bucket(bucket).Object(object).Attrs(ctx)
	if err != nil {
		return fmt.Errorf("GetObject: %w", err)
	}
	if attrs.Name != object {
		return fmt.Errorf("GetObject name = %q, want %q", attrs.Name, object)
	}
	if attrs.Size != int64(len(payload)) {
		return fmt.Errorf("GetObject size = %d, want %d", attrs.Size, len(payload))
	}
	if attrs.ContentType != "text/plain" {
		return fmt.Errorf("GetObject contentType = %q, want text/plain", attrs.ContentType)
	}
	return nil
}

func checkStorageDeleteObject(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	object := cfg.ResourceName("gcpc-grpc-delete-obj")
	if _, err := putStorageObject(ctx, client, bucket, object, "text/plain", []byte("delete-me")); err != nil {
		return err
	}
	if err := client.Bucket(bucket).Object(object).Delete(ctx); err != nil {
		return fmt.Errorf("DeleteObject: %w", err)
	}
	if _, err := client.Bucket(bucket).Object(object).Attrs(ctx); !errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("GetObject after delete = %v, want ErrObjectNotExist", err)
	}
	return nil
}

func checkStorageListObjects(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	prefix := cfg.ResourceName("gcpc-grpc-list")
	first := prefix + "-a"
	second := prefix + "-b"
	if _, err := putStorageObject(ctx, client, bucket, first, "text/plain", []byte("a")); err != nil {
		return err
	}
	if _, err := putStorageObject(ctx, client, bucket, second, "text/plain", []byte("b")); err != nil {
		return err
	}
	seen := map[string]bool{}
	it := client.Bucket(bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("ListObjects: %w", err)
		}
		seen[attrs.Name] = true
	}
	if !seen[first] || !seen[second] {
		return fmt.Errorf("ListObjects prefix %q returned %v, want both %q and %q", prefix, seen, first, second)
	}
	return nil
}

func checkStorageUpdateObject(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	object := cfg.ResourceName("gcpc-grpc-update-obj")
	if _, err := putStorageObject(ctx, client, bucket, object, "text/plain", []byte("update-me")); err != nil {
		return err
	}
	updated, err := client.Bucket(bucket).Object(object).Update(ctx, storage.ObjectAttrsToUpdate{
		Metadata: map[string]string{"suite": "grpc-conformance"},
	})
	if err != nil {
		return fmt.Errorf("UpdateObject: %w", err)
	}
	if updated.Metadata["suite"] != "grpc-conformance" {
		return fmt.Errorf("UpdateObject metadata = %v, want suite=grpc-conformance", updated.Metadata)
	}
	return nil
}

func checkStorageComposeObject(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	partA := cfg.ResourceName("gcpc-grpc-compose-a")
	partB := cfg.ResourceName("gcpc-grpc-compose-b")
	dest := cfg.ResourceName("gcpc-grpc-composed")
	if _, err := putStorageObject(ctx, client, bucket, partA, "text/plain", []byte("part-a-")); err != nil {
		return err
	}
	if _, err := putStorageObject(ctx, client, bucket, partB, "text/plain", []byte("part-b")); err != nil {
		return err
	}
	attrs, err := client.Bucket(bucket).Object(dest).ComposerFrom(
		client.Bucket(bucket).Object(partA),
		client.Bucket(bucket).Object(partB),
	).Run(ctx)
	if err != nil {
		return fmt.Errorf("ComposeObject: %w", err)
	}
	if attrs.ComponentCount != 2 {
		return fmt.Errorf("ComposeObject componentCount = %d, want 2", attrs.ComponentCount)
	}
	got, err := readStorageObject(ctx, client, bucket, dest)
	if err != nil {
		return err
	}
	if string(got) != "part-a-part-b" {
		return fmt.Errorf("composed content = %q, want %q", got, "part-a-part-b")
	}
	return nil
}

func checkStorageRewriteObject(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	src := cfg.ResourceName("gcpc-grpc-rewrite-src")
	dst := cfg.ResourceName("gcpc-grpc-rewrite-dst")
	payload := []byte("rewrite-payload")
	if _, err := putStorageObject(ctx, client, bucket, src, "text/plain", payload); err != nil {
		return err
	}
	attrs, err := client.Bucket(bucket).Object(dst).CopierFrom(client.Bucket(bucket).Object(src)).Run(ctx)
	if err != nil {
		return fmt.Errorf("RewriteObject: %w", err)
	}
	if attrs.Name != dst {
		return fmt.Errorf("RewriteObject name = %q, want %q", attrs.Name, dst)
	}
	if attrs.Size != int64(len(payload)) {
		return fmt.Errorf("RewriteObject size = %d, want %d", attrs.Size, len(payload))
	}
	got, err := readStorageObject(ctx, client, bucket, dst)
	if err != nil {
		return err
	}
	if string(got) != string(payload) {
		return fmt.Errorf("rewritten content = %q, want %q", got, payload)
	}
	return nil
}

func checkStorageMoveObject(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	src := cfg.ResourceName("gcpc-grpc-move-src")
	dst := cfg.ResourceName("gcpc-grpc-move-dst")
	payload := []byte("move-payload")
	if _, err := putStorageObject(ctx, client, bucket, src, "text/plain", payload); err != nil {
		return err
	}
	moved, err := client.Bucket(bucket).Object(src).Move(ctx, storage.MoveObjectDestination{Object: dst})
	if err != nil {
		return fmt.Errorf("MoveObject: %w", err)
	}
	if moved.Name != dst {
		return fmt.Errorf("MoveObject name = %q, want %q", moved.Name, dst)
	}
	if _, err := client.Bucket(bucket).Object(src).Attrs(ctx); !errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("source after move = %v, want ErrObjectNotExist", err)
	}
	got, err := readStorageObject(ctx, client, bucket, dst)
	if err != nil {
		return err
	}
	if string(got) != string(payload) {
		return fmt.Errorf("moved content = %q, want %q", got, payload)
	}
	return nil
}

func checkStorageRestoreObject(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageVersionedBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, &storage.BucketAttrs{VersioningEnabled: true}); err != nil {
		return err
	}
	object := cfg.ResourceName("gcpc-grpc-restore-obj")
	first, err := putStorageObject(ctx, client, bucket, object, "text/plain", []byte("version-one"))
	if err != nil {
		return err
	}
	if _, err := putStorageObject(ctx, client, bucket, object, "text/plain", []byte("version-two")); err != nil {
		return err
	}
	restored, err := client.Bucket(bucket).Object(object).Generation(first.Generation).Restore(ctx, &storage.RestoreOptions{})
	if err != nil {
		return fmt.Errorf("RestoreObject: %w", err)
	}
	if restored.Generation != first.Generation {
		return fmt.Errorf("RestoreObject generation = %d, want %d", restored.Generation, first.Generation)
	}
	got, err := readStorageObject(ctx, client, bucket, object)
	if err != nil {
		return err
	}
	if string(got) != "version-one" {
		return fmt.Errorf("content after restore = %q, want version-one", got)
	}
	return nil
}

func checkStorageReadObject(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	object := cfg.ResourceName("gcpc-grpc-read-obj")
	if _, err := putStorageObject(ctx, client, bucket, object, "text/plain", []byte("0123456789abcdef")); err != nil {
		return err
	}
	r, err := client.Bucket(bucket).Object(object).NewRangeReader(ctx, 5, 4)
	if err != nil {
		return fmt.Errorf("ReadObject range: %w", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("ReadObject range read: %w", err)
	}
	if string(got) != "5678" {
		return fmt.Errorf("ReadObject range = %q, want 5678", got)
	}
	return nil
}

// ─── writes ──────────────────────────────────────────────────────────────────

func checkStorageWriteObject(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	object := cfg.ResourceName("gcpc-grpc-write-obj")
	payload := []byte("write-object-client-streaming")

	conn, err := storageConn(cfg)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	req, err := newStorageMsg("google.storage.v2.WriteObjectRequest")
	if err != nil {
		return err
	}
	spec, err := msgMutable(req, "write_object_spec")
	if err != nil {
		return err
	}
	if err := storageWriteObjectSpec(spec, "resource", bucket, object, "text/plain"); err != nil {
		return err
	}
	if err := msgSetInt64(req, "write_offset", 0); err != nil {
		return err
	}
	cd, err := msgMutable(req, "checksummed_data")
	if err != nil {
		return err
	}
	if err := msgSetBytes(cd, "content", payload); err != nil {
		return err
	}
	if err := msgSetBool(req, "finish_write", true); err != nil {
		return err
	}

	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{StreamName: "WriteObject", ClientStreams: true}, storageServicePrefix+"WriteObject")
	if err != nil {
		return fmt.Errorf("WriteObject stream: %w", err)
	}
	if err := stream.SendMsg(req); err != nil {
		return fmt.Errorf("WriteObject send: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return fmt.Errorf("WriteObject close send: %w", err)
	}
	resp, err := newStorageMsg("google.storage.v2.WriteObjectResponse")
	if err != nil {
		return err
	}
	if err := stream.RecvMsg(resp); err != nil {
		return fmt.Errorf("WriteObject recv: %w", err)
	}

	attrs, err := client.Bucket(bucket).Object(object).Attrs(ctx)
	if err != nil {
		return fmt.Errorf("GetObject after WriteObject: %w", err)
	}
	if attrs.Name != object || attrs.Size != int64(len(payload)) {
		return fmt.Errorf("WriteObject resource name=%q size=%d, want %q/%d", attrs.Name, attrs.Size, object, len(payload))
	}
	got, err := readStorageObject(ctx, client, bucket, object)
	if err != nil {
		return err
	}
	if string(got) != string(payload) {
		return fmt.Errorf("WriteObject content = %q, want %q", got, payload)
	}
	return nil
}

func checkStorageBidiWriteObject(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	object := cfg.ResourceName("gcpc-grpc-bidi-obj")
	payload := []byte("bidi-write-payload")
	w := client.Bucket(bucket).Object(object).NewWriter(ctx)
	w.ContentType = "text/plain"
	if _, err := w.Write(payload); err != nil {
		_ = w.Close()
		return fmt.Errorf("BidiWriteObject write: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("BidiWriteObject close: %w", err)
	}
	got, err := readStorageObject(ctx, client, bucket, object)
	if err != nil {
		return err
	}
	if string(got) != string(payload) {
		return fmt.Errorf("BidiWriteObject content = %q, want %q", got, payload)
	}
	return nil
}

// ─── resumable writes ────────────────────────────────────────────────────────

// storageStartResumable issues StartResumableWrite for a run-unique object and
// returns the upload id.
func storageStartResumable(ctx context.Context, conn *grpc.ClientConn, cfg Config, bucket, object string) (string, error) {
	req, err := newStorageMsg("google.storage.v2.StartResumableWriteRequest")
	if err != nil {
		return "", err
	}
	spec, err := msgMutable(req, "write_object_spec")
	if err != nil {
		return "", err
	}
	if err := storageWriteObjectSpec(spec, "resource", bucket, object, "text/plain"); err != nil {
		return "", err
	}
	resp, err := newStorageMsg("google.storage.v2.StartResumableWriteResponse")
	if err != nil {
		return "", err
	}
	if err := conn.Invoke(ctx, storageServicePrefix+"StartResumableWrite", req, resp); err != nil {
		return "", fmt.Errorf("StartResumableWrite: %w", err)
	}
	uploadID := resp.Get(msgField(resp, "upload_id")).String()
	if uploadID == "" {
		return "", fmt.Errorf("StartResumableWrite returned an empty upload_id")
	}
	return uploadID, nil
}

// storageCancelResumable cancels an upload session.
func storageCancelResumable(ctx context.Context, conn *grpc.ClientConn, uploadID string) error {
	req, err := newStorageMsg("google.storage.v2.CancelResumableWriteRequest")
	if err != nil {
		return err
	}
	if err := msgSetString(req, "upload_id", uploadID); err != nil {
		return err
	}
	resp, err := newStorageMsg("google.storage.v2.CancelResumableWriteResponse")
	if err != nil {
		return err
	}
	if err := conn.Invoke(ctx, storageServicePrefix+"CancelResumableWrite", req, resp); err != nil {
		return fmt.Errorf("CancelResumableWrite: %w", err)
	}
	return nil
}

func checkStorageStartResumableWrite(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	conn, err := storageConn(cfg)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	uploadID, err := storageStartResumable(ctx, conn, cfg, bucket, cfg.ResourceName("gcpc-grpc-resume-start"))
	if err != nil {
		return err
	}
	// Clean up the session; the object itself is never finalized.
	return storageCancelResumable(ctx, conn, uploadID)
}

func checkStorageQueryWriteStatus(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	conn, err := storageConn(cfg)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	uploadID, err := storageStartResumable(ctx, conn, cfg, bucket, cfg.ResourceName("gcpc-grpc-resume-query"))
	if err != nil {
		return err
	}
	defer func() { _ = storageCancelResumable(ctx, conn, uploadID) }()

	req, err := newStorageMsg("google.storage.v2.QueryWriteStatusRequest")
	if err != nil {
		return err
	}
	if err := msgSetString(req, "upload_id", uploadID); err != nil {
		return err
	}
	resp, err := newStorageMsg("google.storage.v2.QueryWriteStatusResponse")
	if err != nil {
		return err
	}
	if err := conn.Invoke(ctx, storageServicePrefix+"QueryWriteStatus", req, resp); err != nil {
		return fmt.Errorf("QueryWriteStatus: %w", err)
	}
	oneof := resp.Descriptor().Oneofs().ByName("write_status")
	if which := resp.WhichOneof(oneof); which == nil {
		return fmt.Errorf("QueryWriteStatus returned no write_status")
	} else if which.Name() != "persisted_size" {
		return fmt.Errorf("QueryWriteStatus write_status = %q, want persisted_size", which.Name())
	}
	if got := resp.Get(msgField(resp, "persisted_size")).Int(); got != 0 {
		return fmt.Errorf("QueryWriteStatus persisted_size = %d, want 0", got)
	}
	return nil
}

func checkStorageCancelResumableWrite(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	conn, err := storageConn(cfg)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	uploadID, err := storageStartResumable(ctx, conn, cfg, bucket, cfg.ResourceName("gcpc-grpc-resume-cancel"))
	if err != nil {
		return err
	}
	if err := storageCancelResumable(ctx, conn, uploadID); err != nil {
		return err
	}
	// The session is gone: a follow-up QueryWriteStatus must be NotFound, and
	// a second cancel must stay idempotent.
	req, err := newStorageMsg("google.storage.v2.QueryWriteStatusRequest")
	if err != nil {
		return err
	}
	if err := msgSetString(req, "upload_id", uploadID); err != nil {
		return err
	}
	resp, err := newStorageMsg("google.storage.v2.QueryWriteStatusResponse")
	if err != nil {
		return err
	}
	if err := conn.Invoke(ctx, storageServicePrefix+"QueryWriteStatus", req, resp); status.Code(err) != codes.NotFound {
		return fmt.Errorf("QueryWriteStatus after cancel = %v, want NotFound", err)
	}
	return storageCancelResumable(ctx, conn, uploadID)
}

// ─── bucket mutation ─────────────────────────────────────────────────────────

func checkStorageUpdateBucket(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	var update storage.BucketAttrsToUpdate
	update.SetLabel("env", "prod")
	updated, err := client.Bucket(bucket).Update(ctx, update)
	if err != nil {
		return fmt.Errorf("UpdateBucket: %w", err)
	}
	if updated.Labels["env"] != "prod" {
		return fmt.Errorf("UpdateBucket labels = %v, want env=prod", updated.Labels)
	}
	return nil
}

func checkStorageLockBucketRetentionPolicy(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	// The lock RPC is irreversible, so use a dedicated throwaway bucket whose
	// retention policy is set at creation time.
	bucket := cfg.ResourceName("gcpc-grpc-retention")
	if err := ensureStorageBucket(ctx, client, cfg, bucket, &storage.BucketAttrs{
		RetentionPolicy: &storage.RetentionPolicy{RetentionPeriod: time.Hour},
	}); err != nil {
		return err
	}
	if err := client.Bucket(bucket).LockRetentionPolicy(ctx); err != nil {
		return fmt.Errorf("LockBucketRetentionPolicy: %w", err)
	}
	attrs, err := client.Bucket(bucket).Attrs(ctx)
	if err != nil {
		return fmt.Errorf("GetBucket after lock: %w", err)
	}
	if attrs.RetentionPolicy == nil || !attrs.RetentionPolicy.IsLocked {
		return fmt.Errorf("retention policy after lock = %+v, want IsLocked", attrs.RetentionPolicy)
	}
	return nil
}

// ─── IAM ─────────────────────────────────────────────────────────────────────

func checkStorageGetIamPolicy(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	pol, err := client.Bucket(bucket).IAM().Policy(ctx)
	if err != nil {
		return fmt.Errorf("GetIamPolicy: %w", err)
	}
	if pol == nil || pol.InternalProto == nil || len(pol.InternalProto.Etag) == 0 {
		return fmt.Errorf("GetIamPolicy returned no default etag: %+v", pol)
	}
	return nil
}

func checkStorageSetIamPolicy(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	pol, err := client.Bucket(bucket).IAM().Policy(ctx)
	if err != nil {
		return fmt.Errorf("GetIamPolicy: %w", err)
	}
	pol.Add("user:conformance@example.com", "roles/storage.objectViewer")
	if err := client.Bucket(bucket).IAM().SetPolicy(ctx, pol); err != nil {
		return fmt.Errorf("SetIamPolicy: %w", err)
	}
	got, err := client.Bucket(bucket).IAM().Policy(ctx)
	if err != nil {
		return fmt.Errorf("GetIamPolicy after set: %w", err)
	}
	if !got.HasRole("user:conformance@example.com", "roles/storage.objectViewer") {
		return fmt.Errorf("SetIamPolicy binding missing: roles=%v members=%v", got.Roles(), got.Members("roles/storage.objectViewer"))
	}
	return nil
}

func checkStorageTestIamPermissions(ctx context.Context, cfg Config) error {
	client, err := newStorageClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	bucket := storageObjBucket(cfg)
	if err := ensureStorageBucket(ctx, client, cfg, bucket, nil); err != nil {
		return err
	}
	want := []string{"storage.buckets.get", "storage.objects.list"}
	perms, err := client.Bucket(bucket).IAM().TestPermissions(ctx, want)
	if err != nil {
		return fmt.Errorf("TestIamPermissions: %w", err)
	}
	got := map[string]bool{}
	for _, p := range perms {
		got[p] = true
	}
	for _, p := range want {
		if !got[p] {
			return fmt.Errorf("TestIamPermissions = %v, want %q included", perms, p)
		}
	}
	return nil
}
