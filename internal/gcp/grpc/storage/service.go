// Package storage implements the Cloud Storage v2 gRPC service
// (google.storage.v2.Storage) over the same gcs.ObjectStore, blobfs.BlobStore,
// and crypto.EnvelopeEncryptor backing the REST provider, so REST and gRPC share
// object state and stay byte-compatible.
package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/clock"
	grpcutil "jaiscloud/internal/gcp/grpc"
	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"
	storageprovider "jaiscloud/internal/gcp/provider/storage"
	"jaiscloud/internal/gcp/store/gcs"
	"jaiscloud/internal/model"

	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service implements storagepb.StorageServer. It reuses the REST provider's
// generation counter, encryption, and blob write/read helpers so REST and gRPC
// objects are byte-compatible, and the gcs.ObjectStore for bucket/object
// metadata CRUD.
type Service struct {
	storagepb.UnimplementedStorageServer

	objects     gcs.ObjectStore
	provider    *storageprovider.Provider
	defaultProj string

	mu      sync.Mutex
	uploads map[string]*uploadSession // in-progress resumable uploads (in-memory)
}

// uploadSession accumulates the bytes of an in-progress resumable write.
type uploadSession struct {
	bucket       string
	object       string
	contentType  string
	metadata     map[string]string
	kmsKeyName   string
	cseKey       []byte
	cseKeySHA256 string
	buf          []byte
	length       int64
	lastAccess   time.Time
}

// NewService returns a Cloud Storage v2 gRPC service backed by the shared
// object store and the REST provider (generation counter + byte/encryption
// helpers). defaultProj is the config-default project used when a request
// carries none.
func NewService(objects gcs.ObjectStore, provider *storageprovider.Provider, defaultProj string) *Service {
	return &Service{
		objects:     objects,
		provider:    provider,
		defaultProj: defaultProj,
		uploads:     make(map[string]*uploadSession),
	}
}

func mapError(err error) error { return grpcutil.GRPCStatus(err) }

// ─── resource-name helpers ───────────────────────────────────────────────────

func bucketResourceName(bucket string) string { return "projects/_/buckets/" + bucket }

// parseBucketName returns the bucket name from a full resource name
// ("projects/_/buckets/{bucket}" → "{bucket}") or a bare name.
func parseBucketName(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
}

// parseProject returns the project from a "projects/{project}" resource name,
// or the raw string if it lacks the prefix.
func parseProject(resource string) string {
	return strings.TrimPrefix(resource, "projects/")
}

// projectForBucket resolves the project that owns a bucket (the account scope
// used for envelope encryption), falling back to the request/default project.
func (s *Service) projectForBucket(ctx context.Context, bucket string) string {
	if m, err := s.objects.GetBucket(ctx, bucket); err == nil {
		if pid, _ := m["projectId"].(string); pid != "" {
			return pid
		}
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// ─── proto ↔ store transcoding ───────────────────────────────────────────────

func genToInt64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func int64ToGen(i int64) string { return strconv.FormatInt(i, 10) }

// decodeCRC32C decodes a base64 (big-endian uint32) CRC32C string.
func decodeCRC32C(b64 string) (uint32, bool) {
	d, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || len(d) != 4 {
		return 0, false
	}
	return uint32(d[0])<<24 | uint32(d[1])<<16 | uint32(d[2])<<8 | uint32(d[3]), true
}

func ts(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

// objectToProto converts stored object metadata into the proto Object, filling
// the bucket as a full resource name (projects/_/buckets/{bucket}).
func objectToProto(m gcs.ObjectMeta) *storagepb.Object {
	o := &storagepb.Object{
		Name:           m.Name,
		Bucket:         bucketResourceName(m.Bucket),
		Etag:           "CAE=",
		Generation:     genToInt64(m.Generation),
		Metageneration: genToInt64(m.Metageneration),
		StorageClass:   m.StorageClass,
		Size:           m.Size,
		ContentType:    m.ContentType,
		ComponentCount: int32(m.ComponentCount),
		Metadata:       m.Metadata,
		TemporaryHold:  m.TemporaryHold,
		KmsKey:         m.KmsKeyName,
		CreateTime:     ts(m.TimeCreated),
		UpdateTime:     ts(m.Updated),
	}
	if m.EventBasedHold {
		o.EventBasedHold = &m.EventBasedHold
	}
	if m.TimeDeleted != nil {
		o.DeleteTime = ts(*m.TimeDeleted)
	}
	if m.CRC32C != "" {
		if v, ok := decodeCRC32C(m.CRC32C); ok {
			o.Checksums = &storagepb.ObjectChecksums{Crc32C: &v}
		}
	}
	if m.MD5Hash != "" {
		if b, err := base64.StdEncoding.DecodeString(m.MD5Hash); err == nil {
			if o.Checksums == nil {
				o.Checksums = &storagepb.ObjectChecksums{}
			}
			o.Checksums.Md5Hash = b
		}
	}
	return o
}

// bucketToProto converts a stored bucket map into the proto Bucket.
func bucketToProto(m map[string]any) *storagepb.Bucket {
	name, _ := m["name"].(string)
	b := &storagepb.Bucket{
		Name:           bucketResourceName(name),
		BucketId:       name,
		Metageneration: 1,
	}
	if v, _ := m["location"].(string); v != "" {
		b.Location = v
	}
	if v, _ := m["storageClass"].(string); v != "" {
		b.StorageClass = v
	}
	switch l := m["labels"].(type) {
	case map[string]string:
		b.Labels = l
	case map[string]any:
		if len(l) > 0 {
			b.Labels = make(map[string]string, len(l))
			for k, v := range l {
				if sv, ok := v.(string); ok {
					b.Labels[k] = sv
				}
			}
		}
	}
	if v, ok := m["versioning"].(map[string]any); ok {
		if en, _ := v["enabled"].(bool); en {
			b.Versioning = &storagepb.Bucket_Versioning{Enabled: true}
		}
	}
	if tc, _ := m["timeCreated"].(string); tc != "" {
		if t, err := time.Parse(time.RFC3339Nano, tc); err == nil {
			b.CreateTime = timestamppb.New(t)
		}
	}
	if u, _ := m["updated"].(string); u != "" {
		if t, err := time.Parse(time.RFC3339Nano, u); err == nil {
			b.UpdateTime = timestamppb.New(t)
		}
	}
	return b
}

// protoResourceToMeta converts a WriteObjectSpec's resource (proto Object) plus
// the resolved bucket/object into store metadata with a fresh generation.
func protoResourceToMeta(resource *storagepb.Object, bucket, object, generation string, now time.Time) gcs.ObjectMeta {
	meta := gcs.ObjectMeta{
		Bucket:         bucket,
		Name:           object,
		Generation:     generation,
		Metageneration: "1",
		StorageClass:   "STANDARD",
		TimeCreated:    now,
		Updated:        now,
	}
	if resource != nil {
		meta.ContentType = resource.GetContentType()
		if meta.ContentType == "" {
			meta.ContentType = "application/octet-stream"
		}
		meta.StorageClass = resource.GetStorageClass()
		if meta.StorageClass == "" {
			meta.StorageClass = "STANDARD"
		}
		meta.Metadata = resource.GetMetadata()
		meta.TemporaryHold = resource.GetTemporaryHold()
		if resource.EventBasedHold != nil {
			meta.EventBasedHold = *resource.EventBasedHold
		}
		meta.KmsKeyName = resource.GetKmsKey()
	}
	return meta
}

// cseKeyFromParams extracts a CSEK key from the CommonObjectRequestParams
// (raw key bytes + raw sha256 bytes), returning the key and its base64 sha256.
func cseKeyFromParams(params *storagepb.CommonObjectRequestParams) ([]byte, string) {
	if params == nil {
		return nil, ""
	}
	key := params.GetEncryptionKeyBytes()
	if len(key) != 32 {
		return nil, ""
	}
	sum := sha256.Sum256(key)
	return key, base64.StdEncoding.EncodeToString(sum[:])
}

// ─── buckets ──────────────────────────────────────────────────────────────────

func (s *Service) CreateBucket(ctx context.Context, req *storagepb.CreateBucketRequest) (*storagepb.Bucket, error) {
	name := req.GetBucketId()
	if name == "" {
		name = req.GetBucket().GetBucketId()
	}
	if name == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "missing bucket name", 400))
	}
	project := parseProject(req.GetBucket().GetProject())
	if project == "" {
		project = grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
	}

	location := req.GetBucket().GetLocation()
	if location == "" {
		location = "US"
	}
	storageClass := req.GetBucket().GetStorageClass()
	if storageClass == "" {
		storageClass = "STANDARD"
	}
	now := clock.Now().Format(time.RFC3339Nano)
	meta := map[string]any{
		"name":         name,
		"location":     location,
		"storageClass": storageClass,
		"timeCreated":  now,
		"updated":      now,
	}
	if labels := req.GetBucket().GetLabels(); len(labels) > 0 {
		meta["labels"] = labels
	}
	if req.GetBucket().GetVersioning() != nil && req.GetBucket().GetVersioning().GetEnabled() {
		meta["versioning"] = map[string]any{"enabled": true}
	}

	if err := s.objects.CreateBucket(ctx, project, name, meta); err != nil {
		if errors.Is(err, gcs.ErrAlreadyExists) {
			return nil, mapError(model.NewProviderError("AlreadyExists", "bucket already exists", 409))
		}
		return nil, mapError(err)
	}
	return bucketToProto(meta), nil
}

func (s *Service) GetBucket(ctx context.Context, req *storagepb.GetBucketRequest) (*storagepb.Bucket, error) {
	name := parseBucketName(req.GetName())
	meta, err := s.objects.GetBucket(ctx, name)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return nil, mapError(model.NewProviderError("NotFound", "bucket not found", 404))
		}
		return nil, mapError(err)
	}
	return bucketToProto(meta), nil
}

func (s *Service) DeleteBucket(ctx context.Context, req *storagepb.DeleteBucketRequest) (*emptypb.Empty, error) {
	name := parseBucketName(req.GetName())
	if err := s.objects.DeleteBucket(ctx, name); err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return nil, mapError(model.NewProviderError("NotFound", "bucket not found", 404))
		}
		if errors.Is(err, gcs.ErrBucketNotEmpty) {
			return nil, mapError(model.NewProviderError("FailedPrecondition", "bucket is not empty", 409))
		}
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Service) ListBuckets(ctx context.Context, req *storagepb.ListBucketsRequest) (*storagepb.ListBucketsResponse, error) {
	project := parseProject(req.GetParent())
	if project == "" {
		project = grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
	}
	buckets, err := s.objects.ListBuckets(ctx, project)
	if err != nil {
		return nil, mapError(err)
	}
	// Apply prefix filter, sort by name, then page.
	filtered := buckets[:0]
	for _, m := range buckets {
		name, _ := m["name"].(string)
		if req.GetPrefix() == "" || strings.HasPrefix(name, req.GetPrefix()) {
			filtered = append(filtered, m)
		}
	}
	sort.Slice(filtered, func(i, j int) bool {
		ni, _ := filtered[i]["name"].(string)
		nj, _ := filtered[j]["name"].(string)
		return ni < nj
	})
	page, next := pageBuckets(filtered, int(req.GetPageSize()), req.GetPageToken())
	out := make([]*storagepb.Bucket, 0, len(page))
	for _, m := range page {
		out = append(out, bucketToProto(m))
	}
	return &storagepb.ListBucketsResponse{Buckets: out, NextPageToken: next}, nil
}

// pageBuckets applies cursor pagination over a name-sorted bucket slice.
func pageBuckets(buckets []map[string]any, pageSize int, pageToken string) ([]map[string]any, string) {
	start := 0
	if pageToken != "" {
		if cursor := decodeCursor(pageToken); cursor != "" {
			for start < len(buckets) {
				n, _ := buckets[start]["name"].(string)
				if n > cursor {
					break
				}
				start++
			}
		}
	}
	if pageSize <= 0 {
		pageSize = len(buckets)
	}
	end := start + pageSize
	if end > len(buckets) {
		end = len(buckets)
	}
	page := buckets[start:end]
	next := ""
	if end < len(buckets) {
		n, _ := page[len(page)-1]["name"].(string)
		next = encodeCursor(n)
	}
	return page, next
}

// ─── objects ──────────────────────────────────────────────────────────────────

func (s *Service) GetObject(ctx context.Context, req *storagepb.GetObjectRequest) (*storagepb.Object, error) {
	bucket := parseBucketName(req.GetBucket())
	var meta gcs.ObjectMeta
	var err error
	if req.GetGeneration() > 0 {
		meta, err = s.objects.GetObjectGeneration(ctx, bucket, req.GetObject(), int64ToGen(req.GetGeneration()))
	} else {
		meta, err = s.objects.GetObjectMeta(ctx, bucket, req.GetObject())
	}
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return nil, mapError(model.NewProviderError("NotFound", "object not found", 404))
		}
		return nil, mapError(err)
	}
	return objectToProto(meta), nil
}

func (s *Service) ListObjects(ctx context.Context, req *storagepb.ListObjectsRequest) (*storagepb.ListObjectsResponse, error) {
	bucket := parseBucketName(req.GetParent())
	var objs []gcs.ObjectMeta
	var err error
	if req.GetVersions() {
		objs, err = s.objects.ListObjectVersions(ctx, bucket)
	} else {
		objs, err = s.objects.ListObjects(ctx, bucket)
	}
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return nil, mapError(model.NewProviderError("NotFound", "bucket not found", 404))
		}
		return nil, mapError(err)
	}

	// Prefix filter.
	if pfx := req.GetPrefix(); pfx != "" {
		filtered := objs[:0]
		for _, m := range objs {
			if strings.HasPrefix(m.Name, pfx) {
				filtered = append(filtered, m)
			}
		}
		objs = filtered
	}

	// Cursor pagination over object names (already name-sorted by the store).
	pageSize := int(req.GetPageSize())
	start := 0
	if req.GetPageToken() != "" {
		if cursor := decodeCursor(req.GetPageToken()); cursor != "" {
			for start < len(objs) && objs[start].Name <= cursor {
				start++
			}
		}
	}
	if pageSize <= 0 {
		pageSize = len(objs)
	}
	end := start + pageSize
	if end > len(objs) {
		end = len(objs)
	}
	page := objs[start:end]
	next := ""
	if end < len(objs) {
		next = encodeCursor(page[len(page)-1].Name)
	}

	out := make([]*storagepb.Object, 0, len(page))
	for _, m := range page {
		out = append(out, objectToProto(m))
	}
	return &storagepb.ListObjectsResponse{Objects: out, NextPageToken: next}, nil
}

func (s *Service) DeleteObject(ctx context.Context, req *storagepb.DeleteObjectRequest) (*emptypb.Empty, error) {
	bucket := parseBucketName(req.GetBucket())
	object := req.GetObject()

	// Preconditions are evaluated against the live object. The optional
	// `generation` field selects the revision to delete; the store's delete
	// operates on the live revision, so a non-live target is a precondition
	// failure rather than a partial delete.
	live, err := s.objects.GetObjectMeta(ctx, bucket, object)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return nil, mapError(model.NewProviderError("NotFound", "object not found", 404))
		}
		return nil, mapError(err)
	}
	if req.GetGeneration() > 0 && genToInt64(live.Generation) != req.GetGeneration() {
		return nil, mapError(preconditionErr("generation precondition failed"))
	}
	if err := checkObjectPreconditions(live, req.IfGenerationMatch, req.IfGenerationNotMatch, req.IfMetagenerationMatch, req.IfMetagenerationNotMatch); err != nil {
		return nil, mapError(err)
	}

	// precondition: nil — checkObjectPreconditions above already validated
	// the request's preconditions against a separately-fetched read. That
	// check-then-write isn't atomic the way the REST path's is (see
	// storage.DeleteObjectData's *Checked call) — a real but narrower,
	// pre-existing gap, left as a follow-up rather than duplicating the
	// check here.
	if err := s.provider.DeleteObjectData(ctx, bucket, object, nil); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// preconditionErr returns a FAILED_PRECONDITION provider error (HTTP 400),
// matching the canonical precondition-failure shape used across GCP services.
func preconditionErr(msg string) error {
	return &model.ProviderError{Code: "FailedPrecondition", Message: msg, HTTPStatus: 400, Status: "FAILED_PRECONDITION"}
}

// checkObjectPreconditions validates the four GCS object preconditions against
// the live object metadata, returning a FAILED_PRECONDITION error on mismatch.
// A nil pointer means the corresponding precondition is unset. For
// if_generation_match/if_generation_not_match a value of 0 carries the special
// GCS meaning of "no live versions" / "a live version exists", which falls out
// naturally from comparing against the live generation.
func checkObjectPreconditions(live gcs.ObjectMeta, ifGenMatch, ifGenNotMatch, ifMetaMatch, ifMetaNotMatch *int64) error {
	gen := genToInt64(live.Generation)
	metaGen := genToInt64(live.Metageneration)
	if ifGenMatch != nil && gen != *ifGenMatch {
		return preconditionErr("if_generation_match precondition failed")
	}
	if ifGenNotMatch != nil && gen == *ifGenNotMatch {
		return preconditionErr("if_generation_not_match precondition failed")
	}
	if ifMetaMatch != nil && metaGen != *ifMetaMatch {
		return preconditionErr("if_metageneration_match precondition failed")
	}
	if ifMetaNotMatch != nil && metaGen == *ifMetaNotMatch {
		return preconditionErr("if_metageneration_not_match precondition failed")
	}
	return nil
}

// UpdateObject applies a patch update to an object's metadata.
func (s *Service) UpdateObject(ctx context.Context, req *storagepb.UpdateObjectRequest) (*storagepb.Object, error) {
	bucket := parseBucketName(req.GetObject().GetBucket())
	object := req.GetObject().GetName()
	meta, err := s.objects.GetObjectMeta(ctx, bucket, object)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return nil, mapError(model.NewProviderError("NotFound", "object not found", 404))
		}
		return nil, mapError(err)
	}

	if err := checkObjectPreconditions(meta, req.IfGenerationMatch, req.IfGenerationNotMatch, req.IfMetagenerationMatch, req.IfMetagenerationNotMatch); err != nil {
		return nil, mapError(err)
	}

	pb := req.GetObject()
	mask := req.GetUpdateMask()

	// Metadata uses dot notation ("metadata.<key>"); merge keys when present,
	// clear when the bare "metadata" path is specified.
	if maskIncludes(mask, "metadata") || (pb.GetMetadata() != nil && len(pb.GetMetadata()) == 0) {
		meta.Metadata = nil
	}
	if pb.GetMetadata() != nil && len(pb.GetMetadata()) > 0 {
		if meta.Metadata == nil {
			meta.Metadata = make(map[string]string)
		}
		for k, v := range pb.GetMetadata() {
			meta.Metadata[k] = v
		}
	}
	if maskIncludes(mask, "content_type") {
		meta.ContentType = pb.GetContentType()
	}
	if maskIncludes(mask, "storage_class") && pb.GetStorageClass() != "" {
		meta.StorageClass = pb.GetStorageClass()
	}
	if maskIncludes(mask, "temporary_hold") {
		meta.TemporaryHold = pb.GetTemporaryHold()
	}
	if maskIncludes(mask, "event_based_hold") && pb.EventBasedHold != nil {
		meta.EventBasedHold = *pb.EventBasedHold
	}

	meta.Metageneration = bumpMeta(meta.Metageneration)
	meta.Updated = clock.Now()
	if err := s.objects.PutObjectMeta(ctx, bucket, object, meta); err != nil {
		return nil, mapError(err)
	}
	return objectToProto(meta), nil
}

func (s *Service) ComposeObject(ctx context.Context, req *storagepb.ComposeObjectRequest) (*storagepb.Object, error) {
	dest := req.GetDestination()
	bucket := parseBucketName(dest.GetBucket())
	object := dest.GetName()
	if bucket == "" || object == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "compose requires a destination object name", 400))
	}
	sources := req.GetSourceObjects()
	if len(sources) == 0 {
		return nil, mapError(model.NewProviderError("InvalidArgument", "compose requires source objects", 400))
	}

	project := s.projectForBucket(ctx, bucket)

	var buf bytes.Buffer
	for _, src := range sources {
		if src.GetName() == "" {
			return nil, mapError(model.NewProviderError("InvalidArgument", "compose source object missing name", 400))
		}
		gen := ""
		if src.GetGeneration() > 0 {
			gen = int64ToGen(src.GetGeneration())
		}
		_, raw, err := s.provider.GetObjectData(ctx, project, bucket, src.GetName(), gen, nil)
		if err != nil {
			return nil, mapError(err)
		}
		buf.Write(raw)
	}

	versioned := s.provider.BucketVersioned(ctx, bucket)
	priorBlobKey := ""
	if !versioned {
		if prev, err := s.objects.GetObjectMeta(ctx, bucket, object); err == nil && prev.Generation != "" {
			priorBlobKey = storageprovider.BlobKey(bucket, object, prev.Generation)
		}
	}
	now := clock.Now()
	meta := protoResourceToMeta(dest, bucket, object, s.provider.NextGen(), now)
	meta.ComponentCount = int64(len(sources))

	// precondition: nil — this gRPC path doesn't yet parse
	// WriteObjectSpec.if_generation_match/if_metageneration_match; the REST
	// ObjectsInsert/ObjectsRewrite path does (see storage.objectPrecondition).
	finalMeta, err := s.provider.PutObjectData(ctx, project, meta, buf.Bytes(), versioned, priorBlobKey, false, meta.KmsKeyName, nil, "", nil)
	if err != nil {
		return nil, mapError(err)
	}
	return objectToProto(finalMeta), nil
}

// ─── resumable writes ─────────────────────────────────────────────────────────

func (s *Service) StartResumableWrite(ctx context.Context, req *storagepb.StartResumableWriteRequest) (*storagepb.StartResumableWriteResponse, error) {
	spec := req.GetWriteObjectSpec()
	resource := spec.GetResource()
	bucket := parseBucketName(resource.GetBucket())
	object := resource.GetName()
	if bucket == "" || object == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "missing bucket or object name", 400))
	}

	id := s.provider.NextGen()
	now := clock.RealNow()

	sess := &uploadSession{
		bucket:      bucket,
		object:      object,
		contentType: resource.GetContentType(),
		metadata:    resource.GetMetadata(),
		kmsKeyName:  resource.GetKmsKey(),
		lastAccess:  now,
	}
	if sess.contentType == "" {
		sess.contentType = "application/octet-stream"
	}
	sess.cseKey, sess.cseKeySHA256 = cseKeyFromParams(req.GetCommonObjectRequestParams())

	s.mu.Lock()
	s.uploads[id] = sess
	s.mu.Unlock()

	// Mirror the session to the durable store so it survives a restart (bytes
	// stay in memory, matching the REST provider's spill model).
	_ = s.objects.InitResumable(ctx, gcs.ResumableSession{
		UploadID: id, Bucket: bucket, Name: object, ContentType: sess.contentType, LastAccess: now,
	})

	return &storagepb.StartResumableWriteResponse{UploadId: id}, nil
}

func (s *Service) QueryWriteStatus(ctx context.Context, req *storagepb.QueryWriteStatusRequest) (*storagepb.QueryWriteStatusResponse, error) {
	s.mu.Lock()
	sess, ok := s.uploads[req.GetUploadId()]
	var persisted int64
	if ok {
		sess.lastAccess = clock.RealNow()
		persisted = sess.length
	}
	s.mu.Unlock()
	if !ok {
		return nil, mapError(model.NewProviderError("NotFound", "unknown upload_id", 404))
	}
	return &storagepb.QueryWriteStatusResponse{
		WriteStatus: &storagepb.QueryWriteStatusResponse_PersistedSize{PersistedSize: persisted},
	}, nil
}

// ─── writes ───────────────────────────────────────────────────────────────────

// appendData appends content at writeOffset, enforcing that writeOffset equals
// the session's persisted length (a resume continues exactly where the prior
// stream left off). A gap or overlap is an OutOfRange error, matching real
// GCS. It runs under s.mu so a concurrent QueryWriteStatus read of sess.length
// never races with an active write stream.
func (s *Service) appendData(sess *uploadSession, writeOffset int64, content []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if writeOffset != sess.length {
		return &model.ProviderError{Code: "OutOfRange", Message: "write offset does not match the persisted size", HTTPStatus: 400, Status: "OUT_OF_RANGE"}
	}
	sess.buf = append(sess.buf, content...)
	sess.length += int64(len(content))
	return nil
}

// finalize persists the accumulated bytes as a new object generation and
// returns its proto form.
func (s *Service) finalize(ctx context.Context, project string, sess *uploadSession) (*storagepb.Object, error) {
	versioned := s.provider.BucketVersioned(ctx, sess.bucket)
	priorBlobKey := ""
	if !versioned {
		if prev, err := s.objects.GetObjectMeta(ctx, sess.bucket, sess.object); err == nil && prev.Generation != "" {
			priorBlobKey = storageprovider.BlobKey(sess.bucket, sess.object, prev.Generation)
		}
	}
	meta := gcs.ObjectMeta{
		Bucket:         sess.bucket,
		Name:           sess.object,
		Generation:     s.provider.NextGen(),
		Metageneration: "1",
		ContentType:    sess.contentType,
		StorageClass:   "STANDARD",
		Metadata:       sess.metadata,
		KmsKeyName:     sess.kmsKeyName,
		TimeCreated:    clock.Now(),
		Updated:        clock.Now(),
	}
	if meta.ContentType == "" {
		meta.ContentType = "application/octet-stream"
	}
	finalMeta, err := s.provider.PutObjectData(ctx, project, meta, sess.buf, versioned, priorBlobKey, true, sess.kmsKeyName, sess.cseKey, sess.cseKeySHA256, nil)
	if err != nil {
		return nil, err
	}
	return objectToProto(finalMeta), nil
}

// WriteObject implements the client-streaming (non-bidi) write.
func (s *Service) WriteObject(stream storagepb.Storage_WriteObjectServer) error {
	ctx := stream.Context()
	var sess *uploadSession
	var uploadID string
	project := ""
	resumable := false

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if sess == nil {
			switch fm := req.GetFirstMessage().(type) {
			case *storagepb.WriteObjectRequest_WriteObjectSpec:
				res := fm.WriteObjectSpec.GetResource()
				sess = &uploadSession{
					bucket:      parseBucketName(res.GetBucket()),
					object:      res.GetName(),
					contentType: res.GetContentType(),
					metadata:    res.GetMetadata(),
					kmsKeyName:  res.GetKmsKey(),
				}
				project = s.projectForBucket(ctx, sess.bucket)
			case *storagepb.WriteObjectRequest_UploadId:
				resumable = true
				uploadID = fm.UploadId
				s.mu.Lock()
				existing, ok := s.uploads[fm.UploadId]
				s.mu.Unlock()
				if !ok {
					return mapError(model.NewProviderError("NotFound", "unknown upload_id", 404))
				}
				sess = existing
				project = s.projectForBucket(ctx, sess.bucket)
			default:
				return mapError(model.NewProviderError("InvalidArgument", "missing write object spec", 400))
			}
		}
		if cd := req.GetChecksummedData(); cd != nil {
			if err := s.appendData(sess, req.GetWriteOffset(), cd.GetContent()); err != nil {
				return mapError(err)
			}
		}
		if req.GetFinishWrite() {
			obj, err := s.finalize(ctx, project, sess)
			if err != nil {
				return mapError(err)
			}
			if resumable {
				s.mu.Lock()
				delete(s.uploads, uploadID)
				s.mu.Unlock()
				_ = s.objects.DeleteResumable(ctx, uploadID)
			}
			return stream.SendAndClose(&storagepb.WriteObjectResponse{
				WriteStatus: &storagepb.WriteObjectResponse_Resource{Resource: obj},
			})
		}
	}
	// Stream closed without finish_write.
	if !resumable {
		return mapError(model.NewProviderError("InvalidArgument", "non-resumable write missing finish_write", 400))
	}
	return nil
}

// BidiWriteObject implements the bidirectional write used by the Go SDK's
// writer (both single-shot ChunkSize=0 and resumable ChunkSize>0 paths).
func (s *Service) BidiWriteObject(stream storagepb.Storage_BidiWriteObjectServer) error {
	ctx := stream.Context()
	var sess *uploadSession
	var uploadID string
	project := ""
	resumable := false

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if sess == nil {
			switch fm := req.GetFirstMessage().(type) {
			case *storagepb.BidiWriteObjectRequest_WriteObjectSpec:
				res := fm.WriteObjectSpec.GetResource()
				sess = &uploadSession{
					bucket:      parseBucketName(res.GetBucket()),
					object:      res.GetName(),
					contentType: res.GetContentType(),
					metadata:    res.GetMetadata(),
					kmsKeyName:  res.GetKmsKey(),
				}
				if sess.contentType == "" {
					sess.contentType = "application/octet-stream"
				}
				sess.cseKey, sess.cseKeySHA256 = cseKeyFromParams(req.GetCommonObjectRequestParams())
				project = s.projectForBucket(ctx, sess.bucket)
			case *storagepb.BidiWriteObjectRequest_UploadId:
				resumable = true
				uploadID = fm.UploadId
				s.mu.Lock()
				existing, ok := s.uploads[uploadID]
				if ok {
					existing.lastAccess = clock.RealNow()
				}
				s.mu.Unlock()
				if !ok {
					return mapError(model.NewProviderError("NotFound", "unknown upload_id", 404))
				}
				sess = existing
				project = s.projectForBucket(ctx, sess.bucket)
			default:
				return mapError(model.NewProviderError("InvalidArgument", "missing write object spec", 400))
			}
		}
		if cd := req.GetChecksummedData(); cd != nil {
			if err := s.appendData(sess, req.GetWriteOffset(), cd.GetContent()); err != nil {
				return mapError(err)
			}
		}
		if req.GetFinishWrite() {
			obj, err := s.finalize(ctx, project, sess)
			if err != nil {
				return mapError(err)
			}
			if resumable {
				s.mu.Lock()
				delete(s.uploads, uploadID)
				s.mu.Unlock()
				_ = s.objects.DeleteResumable(ctx, uploadID)
			}
			return stream.Send(&storagepb.BidiWriteObjectResponse{
				WriteStatus: &storagepb.BidiWriteObjectResponse_Resource{Resource: obj},
			})
		}
		if req.GetFlush() || req.GetStateLookup() {
			s.mu.Lock()
			persisted := sess.length
			s.mu.Unlock()
			if err := stream.Send(&storagepb.BidiWriteObjectResponse{
				WriteStatus: &storagepb.BidiWriteObjectResponse_PersistedSize{PersistedSize: persisted},
			}); err != nil {
				return err
			}
		}
	}
	if !resumable {
		return mapError(model.NewProviderError("InvalidArgument", "non-resumable write missing finish_write", 400))
	}
	return nil
}

// ─── reads ────────────────────────────────────────────────────────────────────

func (s *Service) ReadObject(req *storagepb.ReadObjectRequest, stream storagepb.Storage_ReadObjectServer) error {
	ctx := stream.Context()
	bucket := parseBucketName(req.GetBucket())
	project := s.projectForBucket(ctx, bucket)

	gen := ""
	if req.GetGeneration() > 0 {
		gen = int64ToGen(req.GetGeneration())
	}
	var cseKey []byte
	if cop := req.GetCommonObjectRequestParams(); cop != nil {
		cseKey, _ = cseKeyFromParams(cop)
	}
	meta, plain, err := s.provider.GetObjectData(ctx, project, bucket, req.GetObject(), gen, cseKey)
	if err != nil {
		return mapError(err)
	}

	// Honor read_offset (negative = from end) and read_limit.
	start, end := readRange(int64(len(plain)), req.GetReadOffset(), req.GetReadLimit())
	data := plain[start:end]

	// First message carries metadata + object checksums + content range.
	first := &storagepb.ReadObjectResponse{
		Metadata:        objectToProto(meta),
		ObjectChecksums: objectChecksumsFromMeta(meta),
		ContentRange: &storagepb.ContentRange{
			Start:          start,
			End:            end,
			CompleteLength: int64(len(plain)),
		},
	}
	if err := stream.Send(first); err != nil {
		return err
	}

	// Stream the data in bounded chunks.
	const chunk = 2 << 20 // 2 MiB
	for len(data) > 0 {
		n := chunk
		if n > len(data) {
			n = len(data)
		}
		if err := stream.Send(&storagepb.ReadObjectResponse{
			ChecksummedData: &storagepb.ChecksummedData{Content: data[:n]},
		}); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// readRange resolves read_offset/read_limit into a [start,end) byte range.
func readRange(size, offset, limit int64) (int64, int64) {
	start := offset
	if start < 0 {
		start = size + start
		if start < 0 {
			start = 0
		}
	}
	if start > size {
		start = size
	}
	end := size
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	return start, end
}

func objectChecksumsFromMeta(m gcs.ObjectMeta) *storagepb.ObjectChecksums {
	if m.CRC32C == "" {
		return nil
	}
	v, ok := decodeCRC32C(m.CRC32C)
	if !ok {
		return nil
	}
	out := &storagepb.ObjectChecksums{Crc32C: &v}
	if m.MD5Hash != "" {
		if b, err := base64.StdEncoding.DecodeString(m.MD5Hash); err == nil {
			out.Md5Hash = b
		}
	}
	return out
}

// ─── shared helpers ───────────────────────────────────────────────────────────

func maskIncludes(mask *fieldmaskpb.FieldMask, path string) bool {
	if mask == nil {
		return false
	}
	for _, p := range mask.GetPaths() {
		if p == "*" || p == path {
			return true
		}
	}
	return false
}

func bumpMeta(m string) string {
	n, err := strconv.Atoi(m)
	if err != nil {
		return "1"
	}
	return strconv.Itoa(n + 1)
}

func encodeCursor(id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id))
}

func decodeCursor(token string) string {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return ""
	}
	return string(b)
}
