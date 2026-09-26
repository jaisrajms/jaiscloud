// Package storage implements the Cloud Storage v2 gRPC service
// (google.storage.v2.Storage) over the same gcs.ObjectStore, blobfs.BlobStore,
// and crypto.EnvelopeEncryptor backing the REST provider, so REST and gRPC share
// object state and stay byte-compatible.
//
// Implemented RPCs: bucket CRUD + UpdateBucket/LockBucketRetentionPolicy, object
// CRUD + RestoreObject, compose/rewrite/move, Get/Update/DeleteObject, the
// resumable-write surface (StartResumableWrite/WriteObject/BidiWriteObject/
// QueryWriteStatus/CancelResumableWrite), the read surface (ReadObject and the
// bidirectional streaming BidiReadObject), and bucket/object IAM.
package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	iampb "cloud.google.com/go/iam/apiv1/iampb"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/downscope"
	grpcutil "jaiscloud/internal/gcp/grpc"
	storagepb "jaiscloud/internal/gcp/grpc/storage/storagepb"
	"jaiscloud/internal/gcp/policy"
	storageprovider "jaiscloud/internal/gcp/provider/storage"
	"jaiscloud/internal/gcp/store/gcs"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"

	"google.golang.org/protobuf/types/known/durationpb"
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

	objects gcs.ObjectStore
	// resources is the generic ResourceStore holding IAM policies. It is the
	// same store the REST provider uses, so bucket/object policies set through
	// either transport are visible to both.
	resources   store.ResourceStore
	provider    *storageprovider.Provider
	defaultProj string

	mu      sync.Mutex
	uploads map[string]*uploadSession // in-progress resumable uploads (in-memory)
}

// uploadSession accumulates the bytes of an in-progress resumable write. Bytes
// stay in buf up to resumableSpillThreshold; past that they spill to private
// tmpFile so a large upload never buffers fully in memory (mirroring the REST
// provider's spill model).
type uploadSession struct {
	bucket       string
	object       string
	contentType  string
	metadata     map[string]string
	kmsKeyName   string
	cseKey       []byte
	cseKeySHA256 string
	// temporaryHold/eventBasedHold mirror the write spec's Object hold fields.
	// eventBasedHold is a *bool so an omitted field (nil) can inherit the
	// bucket's defaultEventBasedHold while an explicit false overrides it.
	temporaryHold  bool
	eventBasedHold *bool
	precondition   *gcs.Precondition // from WriteObjectSpec's if_* fields; checked atomically at finalize
	buf            []byte
	tmpPath        string   // spill file path once the threshold is exceeded
	tmpFile        *os.File // open handle for appending spilled bytes
	length         int64
	lastAccess     time.Time
}

// closeSpill closes and removes the session's spill file, if any. It is
// idempotent and safe to call on both the finalize and error/reset paths.
func (sess *uploadSession) closeSpill() {
	if sess.tmpFile != nil {
		sess.tmpFile.Close()
		os.Remove(sess.tmpPath)
		sess.tmpFile = nil
		sess.tmpPath = ""
	}
}

// resumableSpillThreshold is the in-memory buffer size beyond which a
// resumable upload session spills its accumulated bytes to a temp file. It
// matches the REST provider's threshold so both transports behave alike.
const resumableSpillThreshold = 4 << 20 // 4 MiB

// NewService returns a Cloud Storage v2 gRPC service backed by the shared
// object store, the generic ResourceStore (IAM policies), and the REST
// provider (generation counter + byte/encryption helpers). defaultProj is the
// config-default project used when a request carries none.
func NewService(objects gcs.ObjectStore, resources store.ResourceStore, provider *storageprovider.Provider, defaultProj string) *Service {
	return &Service{
		objects:     objects,
		resources:   resources,
		provider:    provider,
		defaultProj: defaultProj,
		uploads:     make(map[string]*uploadSession),
	}
}

// Reset closes and removes every in-progress resumable session's spill file
// and clears the session map. Implements admin.Resetter so /_jaiscloud/reset
// does not leak temp files across test runs.
func (s *Service) Reset(_ context.Context) {
	s.mu.Lock()
	for _, sess := range s.uploads {
		sess.closeSpill()
	}
	s.uploads = make(map[string]*uploadSession)
	s.mu.Unlock()
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
	// The proto field is optional but, per the API contract, always set in a
	// response (true or false), so clients can distinguish it from "unknown".
	o.EventBasedHold = &m.EventBasedHold
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
		Metageneration: genToInt64(gcs.BucketMetageneration(m)),
	}
	if v, _ := m["location"].(string); v != "" {
		b.Location = v
	}
	if v, _ := m["storageClass"].(string); v != "" {
		b.StorageClass = v
	}
	b.DefaultEventBasedHold, _ = m["defaultEventBasedHold"].(bool)
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
	if rp, ok := m["retentionPolicy"].(map[string]any); ok && len(rp) > 0 {
		policy := &storagepb.Bucket_RetentionPolicy{}
		if locked, _ := rp["isLocked"].(bool); locked {
			policy.IsLocked = true
		}
		if period, _ := rp["retentionPeriod"].(string); period != "" {
			if n, err := strconv.ParseInt(period, 10, 64); err == nil && n > 0 {
				policy.RetentionDuration = durationpb.New(time.Duration(n) * time.Second)
			}
		}
		if et, _ := rp["effectiveTime"].(string); et != "" {
			if t, err := time.Parse(time.RFC3339Nano, et); err == nil {
				policy.EffectiveTime = timestamppb.New(t)
			}
		}
		b.RetentionPolicy = policy
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

// ─── IAM (bucket + object policies over the shared ResourceStore) ─────────────

// parseIamResource resolves a gRPC Storage IAM resource name to the bucket and
// (for object-scoped policies) the object. The recognized shapes are
// "projects/_/buckets/{bucket}", "projects/_/buckets/{bucket}/objects/{object}",
// and a bare "{bucket}". Managed folders are not supported.
func parseIamResource(resource string) (bucket, object string, isObject, ok bool) {
	rest, found := strings.CutPrefix(resource, "projects/_/buckets/")
	if !found {
		if resource != "" && !strings.Contains(resource, "/") {
			return resource, "", false, true
		}
		return "", "", false, false
	}
	b, o, found := strings.Cut(rest, "/objects/")
	if found {
		if b == "" || o == "" {
			return "", "", false, false
		}
		return b, o, true, true
	}
	if rest == "" || strings.Contains(rest, "/") {
		return "", "", false, false
	}
	return rest, "", false, true
}

// iamPolicyKey returns the policy resource type and id for a bucket- or
// object-scoped request. The ids match the REST provider's ("{bucket}" and
// "{bucket}/{object}") so the two transports share stored policies.
func iamPolicyKey(bucket, object string) (resourceType, id string) {
	if object == "" {
		return storageprovider.ResourceTypeBucketIAM, bucket
	}
	return storageprovider.ResourceTypeObjectIAM, bucket + "/" + object
}

// requireIamResource verifies the bucket or object backing an IAM request
// exists, returning a NotFound provider error otherwise.
func (s *Service) requireIamResource(ctx context.Context, bucket, object string) error {
	if object != "" {
		if _, err := s.objects.GetObjectMeta(ctx, bucket, object); err != nil {
			if errors.Is(err, gcs.ErrNoSuchObject) {
				return model.NewProviderError("NotFound", "object not found", 404)
			}
			return err
		}
		return nil
	}
	if _, err := s.objects.GetBucket(ctx, bucket); err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return err
	}
	return nil
}

func (s *Service) GetIamPolicy(ctx context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	if err := s.requireBucketAdminDownscope(ctx); err != nil {
		return nil, err
	}

	bucket, object, _, ok := parseIamResource(req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.requireIamResource(ctx, bucket, object); err != nil {
		return nil, mapError(err)
	}
	rt, id := iamPolicyKey(bucket, object)
	return policyToProto(policy.Load(ctx, s.resources, s.projectForBucket(ctx, bucket), rt, id)), nil
}

func (s *Service) SetIamPolicy(ctx context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if err := s.requireBucketAdminDownscope(ctx); err != nil {
		return nil, err
	}

	bucket, object, _, ok := parseIamResource(req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.requireIamResource(ctx, bucket, object); err != nil {
		return nil, mapError(err)
	}
	rt, id := iamPolicyKey(bucket, object)
	pol, err := policy.Set(ctx, s.resources, s.projectForBucket(ctx, bucket), rt, id, protoPolicyToBody(req.GetPolicy()))
	if err != nil {
		return nil, mapError(err)
	}
	return policyToProto(pol), nil
}

func (s *Service) TestIamPermissions(ctx context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	if err := s.requireBucketAdminDownscope(ctx); err != nil {
		return nil, err
	}

	bucket, object, _, ok := parseIamResource(req.GetResource())
	if !ok {
		return nil, mapError(model.NewProviderError("InvalidArgument", "invalid resource name", 400))
	}
	if err := s.requireIamResource(ctx, bucket, object); err != nil {
		return nil, mapError(err)
	}
	return &iampb.TestIamPermissionsResponse{Permissions: policy.TestPermissions(req.GetPermissions())}, nil
}

// protoPolicyToBody converts a proto IAM policy into the map shape the shared
// policy package persists (mirrors the KMS/Pub/Sub/Secret Manager services).
func protoPolicyToBody(p *iampb.Policy) map[string]any {
	body := map[string]any{}
	if p == nil {
		return body
	}
	bindings := make([]any, 0, len(p.GetBindings()))
	for _, b := range p.GetBindings() {
		members := make([]any, 0, len(b.GetMembers()))
		for _, m := range b.GetMembers() {
			members = append(members, m)
		}
		bindings = append(bindings, map[string]any{"role": b.GetRole(), "members": members})
	}
	body["bindings"] = bindings
	if et := p.GetEtag(); len(et) > 0 {
		body["etag"] = string(et)
	}
	if p.GetVersion() != 0 {
		body["version"] = int(p.GetVersion())
	}
	return body
}

// policyToProto renders a stored policy as the proto IAM policy.
func policyToProto(p policy.Policy) *iampb.Policy {
	out := &iampb.Policy{Version: int32(p.Version)}
	if p.Etag != "" {
		out.Etag = []byte(p.Etag)
	}
	for _, b := range p.Bindings {
		m, ok := b.(map[string]any)
		if !ok {
			continue
		}
		binding := &iampb.Binding{}
		binding.Role, _ = m["role"].(string)
		for _, v := range toStrings(m["members"]) {
			binding.Members = append(binding.Members, v)
		}
		out.Bindings = append(out.Bindings, binding)
	}
	return out
}

func toStrings(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// ─── buckets ──────────────────────────────────────────────────────────────────

func (s *Service) CreateBucket(ctx context.Context, req *storagepb.CreateBucketRequest) (*storagepb.Bucket, error) {
	if err := s.requireBucketAdminDownscope(ctx); err != nil {
		return nil, err
	}

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
	if rp := bucketRetentionToMap(req.GetBucket().GetRetentionPolicy()); rp != nil {
		meta["retentionPolicy"] = rp
	}
	meta["defaultEventBasedHold"] = req.GetBucket().GetDefaultEventBasedHold()

	if err := s.objects.CreateBucket(ctx, project, name, meta); err != nil {
		if errors.Is(err, gcs.ErrAlreadyExists) {
			return nil, mapError(model.NewProviderError("AlreadyExists", "bucket already exists", 409))
		}
		return nil, mapError(err)
	}
	return bucketToProto(meta), nil
}

func (s *Service) GetBucket(ctx context.Context, req *storagepb.GetBucketRequest) (*storagepb.Bucket, error) {
	if err := s.requireBucketAdminDownscope(ctx); err != nil {
		return nil, err
	}

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
	if err := s.requireBucketAdminDownscope(ctx); err != nil {
		return nil, err
	}

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
	if err := s.requireBucketAdminDownscope(ctx); err != nil {
		return nil, err
	}

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

// ─── buckets: update / retention lock ─────────────────────────────────────────

// mapBucketMutationError maps the bucket-store sentinels returned by
// UpdateBucketMetaAtomic to the canonical gRPC-facing provider errors.
func mapBucketMutationError(err error) error {
	if errors.Is(err, gcs.ErrNoSuchBucket) {
		return model.NewProviderError("NotFound", "bucket not found", 404)
	}
	if errors.Is(err, gcs.ErrPreconditionFailed) {
		return model.NewProviderError("FailedPrecondition", "At least one of the pre-conditions you specified did not hold", 412)
	}
	return err
}

// bucketRetentionToMap converts a proto bucket retention policy into the map
// shape the REST provider persists (decimal-second retentionPeriod string,
// optional effectiveTime RFC 3339, isLocked bool). Returns nil for a nil policy.
// GCS populates effectiveTime server-side when a policy is applied, so an
// absent request value is defaulted to now — the official Go client only
// surfaces a retention policy whose effectiveTime is set.
func bucketRetentionToMap(rp *storagepb.Bucket_RetentionPolicy) map[string]any {
	if rp == nil {
		return nil
	}
	out := map[string]any{}
	if d := rp.GetRetentionDuration(); d != nil {
		out["retentionPeriod"] = strconv.FormatInt(int64(d.AsDuration().Seconds()), 10)
	}
	if et := rp.GetEffectiveTime(); et != nil {
		out["effectiveTime"] = et.AsTime().Format(time.RFC3339Nano)
	} else {
		out["effectiveTime"] = clock.Now().Format(time.RFC3339Nano)
	}
	if rp.GetIsLocked() {
		out["isLocked"] = true
	}
	return out
}

// applyBucketMask overlays the request's masked Bucket fields onto the stored
// bucket metadata. Only the field paths this service models (labels, versioning,
// storageClass, location, retentionPolicy) are applied; "*" applies all of them.
// An absent/empty masked field clears the stored key where GCS treats the field
// as a map/message (labels, versioning, retentionPolicy) and is otherwise left
// untouched.
func applyBucketMask(meta map[string]any, pb *storagepb.Bucket, mask *fieldmaskpb.FieldMask) {
	applyBucketLabelsMask(meta, pb, mask)
	if maskIncludes(mask, "versioning") {
		if v := pb.GetVersioning(); v != nil && v.GetEnabled() {
			meta["versioning"] = map[string]any{"enabled": true}
		} else {
			delete(meta, "versioning")
		}
	}
	if sc := pb.GetStorageClass(); sc != "" && maskIncludes(mask, "storage_class") {
		meta["storageClass"] = sc
	}
	if loc := pb.GetLocation(); loc != "" && maskIncludes(mask, "location") {
		meta["location"] = loc
	}
	if maskIncludes(mask, "retention_policy") {
		if rp := bucketRetentionToMap(pb.GetRetentionPolicy()); rp != nil {
			meta["retentionPolicy"] = rp
		} else {
			delete(meta, "retentionPolicy")
		}
	}
	if maskIncludes(mask, "default_event_based_hold") {
		if pb.GetDefaultEventBasedHold() {
			meta["defaultEventBasedHold"] = true
		} else {
			delete(meta, "defaultEventBasedHold")
		}
	}
}

// applyBucketLabelsMask applies the labels-related update-mask paths. The bare
// "labels" (or "*") replaces the whole label map; per-key paths of the form
// "labels.<key>" — the shape the official GCS clients send — merge an
// individual label, or delete it when the request omits the key.
func applyBucketLabelsMask(meta map[string]any, pb *storagepb.Bucket, mask *fieldmaskpb.FieldMask) {
	whole := false
	var keys []string
	for _, p := range mask.GetPaths() {
		if p == "*" || p == "labels" {
			whole = true
			continue
		}
		if key, ok := strings.CutPrefix(p, "labels."); ok && key != "" {
			keys = append(keys, key)
		}
	}
	if whole {
		if labels := pb.GetLabels(); len(labels) > 0 {
			meta["labels"] = labels
		} else {
			delete(meta, "labels")
		}
		return
	}
	if len(keys) == 0 {
		return
	}
	labels := bucketLabels(meta)
	if labels == nil {
		labels = map[string]string{}
	}
	for _, key := range keys {
		if v, ok := pb.GetLabels()[key]; ok {
			labels[key] = v
		} else {
			delete(labels, key)
		}
	}
	if len(labels) > 0 {
		meta["labels"] = labels
	} else {
		delete(meta, "labels")
	}
}

// bucketLabels normalizes the stored label map — map[string]string from a
// create, or map[string]any after a snapshot/JSON round-trip — to string values.
func bucketLabels(meta map[string]any) map[string]string {
	switch v := meta["labels"].(type) {
	case map[string]string:
		out := make(map[string]string, len(v))
		for k, val := range v {
			out[k] = val
		}
		return out
	case map[string]any:
		out := make(map[string]string, len(v))
		for k, val := range v {
			if s, ok := val.(string); ok {
				out[k] = s
			}
		}
		return out
	default:
		return nil
	}
}

// UpdateBucket applies the request's update_mask to the stored bucket metadata.
// The metageneration precondition (if_metageneration_match /
// if_metageneration_not_match) is validated inside the store's atomic
// read-modify-write, then the metageneration is bumped — the same
// UpdateBucketMetaAtomic path the REST BucketsUpdate uses, so the two transports
// share bucket metageneration state.
func (s *Service) UpdateBucket(ctx context.Context, req *storagepb.UpdateBucketRequest) (*storagepb.Bucket, error) {
	if err := s.requireBucketAdminDownscope(ctx); err != nil {
		return nil, err
	}

	bucket := parseBucketName(req.GetBucket().GetName())
	if bucket == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "missing bucket name", 400))
	}
	mask := req.GetUpdateMask()
	if mask == nil || len(mask.GetPaths()) == 0 {
		return nil, mapError(model.NewProviderError("InvalidArgument", "update_mask is required", 400))
	}
	var pre *gcs.Precondition
	if req.IfMetagenerationMatch != nil || req.IfMetagenerationNotMatch != nil {
		pre = &gcs.Precondition{
			MetagenerationMatch:    req.IfMetagenerationMatch,
			MetagenerationNotMatch: req.IfMetagenerationNotMatch,
		}
	}
	updated, err := s.objects.UpdateBucketMetaAtomic(ctx, bucket, func(meta map[string]any) (map[string]any, error) {
		if !gcs.BucketMetagenerationMatches(meta, pre) {
			return nil, gcs.ErrPreconditionFailed
		}
		applyBucketMask(meta, req.GetBucket(), mask)
		meta["metageneration"] = bumpMeta(gcs.BucketMetageneration(meta))
		meta["updated"] = clock.Now().Format(time.RFC3339Nano)
		return meta, nil
	})
	if err != nil {
		return nil, mapError(mapBucketMutationError(err))
	}
	return bucketToProto(updated), nil
}

// LockBucketRetentionPolicy sets the bucket's retention policy isLocked flag
// permanently (GCS retention locks cannot be undone) and bumps the bucket
// metageneration, honoring the required if_metageneration_match precondition.
// A missing bucket or a bucket with no retention policy is NotFound; a bucket
// whose policy is already locked is returned unchanged (locking is idempotent).
func (s *Service) LockBucketRetentionPolicy(ctx context.Context, req *storagepb.LockBucketRetentionPolicyRequest) (*storagepb.Bucket, error) {
	if err := s.requireBucketAdminDownscope(ctx); err != nil {
		return nil, err
	}

	bucket := parseBucketName(req.GetBucket())
	if bucket == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "missing bucket name", 400))
	}
	var pre *gcs.Precondition
	if v := req.GetIfMetagenerationMatch(); v != 0 {
		pre = &gcs.Precondition{MetagenerationMatch: &v}
	}
	updated, err := s.objects.UpdateBucketMetaAtomic(ctx, bucket, func(meta map[string]any) (map[string]any, error) {
		if !gcs.BucketMetagenerationMatches(meta, pre) {
			return nil, gcs.ErrPreconditionFailed
		}
		rp, _ := meta["retentionPolicy"].(map[string]any)
		if len(rp) == 0 {
			return nil, model.NewProviderError("NotFound", "bucket has no retention policy", 404)
		}
		if locked, _ := rp["isLocked"].(bool); locked {
			return meta, nil
		}
		next := make(map[string]any, len(rp)+1)
		for k, v := range rp {
			next[k] = v
		}
		next["isLocked"] = true
		if _, ok := next["effectiveTime"]; !ok {
			next["effectiveTime"] = clock.Now().Format(time.RFC3339Nano)
		}
		meta["retentionPolicy"] = next
		meta["metageneration"] = bumpMeta(gcs.BucketMetageneration(meta))
		meta["updated"] = clock.Now().Format(time.RFC3339Nano)
		return meta, nil
	})
	if err != nil {
		return nil, mapError(mapBucketMutationError(err))
	}
	return bucketToProto(updated), nil
}

// ─── objects ──────────────────────────────────────────────────────────────────

func (s *Service) GetObject(ctx context.Context, req *storagepb.GetObjectRequest) (*storagepb.Object, error) {
	bucket := parseBucketName(req.GetBucket())
	if err := s.requireDownscope(ctx, downscope.ReadObject, bucket, req.GetObject()); err != nil {
		return nil, err
	}
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
	if err := s.requireDownscope(ctx, downscope.List, bucket, req.GetPrefix()); err != nil {
		return nil, err
	}
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
	if err := s.requireDownscope(ctx, downscope.DeleteObject, bucket, object); err != nil {
		return nil, err
	}

	// Preconditions are threaded into the store's *Checked delete so the check
	// and the delete happen under one lock/transaction — the same atomic path
	// the REST ObjectsDelete uses (see storage.objectPrecondition).
	pre := grpcObjectPrecondition(req.IfGenerationMatch, req.IfGenerationNotMatch, req.IfMetagenerationMatch, req.IfMetagenerationNotMatch)
	// An explicit `generation` selects the revision to delete; the provider
	// removes exactly that generation (a noncurrent survivor is not promoted),
	// matching REST's ?generation= contract.
	gen := ""
	if req.GetGeneration() > 0 {
		gen = int64ToGen(req.GetGeneration())
	}
	if err := s.provider.DeleteObjectData(ctx, bucket, object, gen, pre); err != nil {
		return nil, mapError(preconditionResult(err))
	}
	return &emptypb.Empty{}, nil
}

// preconditionErr returns a FAILED_PRECONDITION provider error (HTTP 400),
// matching the canonical precondition-failure shape used across GCP services.
func preconditionErr(msg string) error {
	return &model.ProviderError{Code: "FailedPrecondition", Message: msg, HTTPStatus: 400, Status: "FAILED_PRECONDITION"}
}

// grpcObjectPrecondition builds a gcs.Precondition from a gRPC request's four
// optional if_*_match fields, returning nil when none is set (the common case).
// The proto fields are optional (*int64), so nil already means "unset" and a
// present 0 keeps GCS's special "no live version" meaning — the same shape the
// store's *Checked methods expect.
func grpcObjectPrecondition(ifGenMatch, ifGenNotMatch, ifMetaMatch, ifMetaNotMatch *int64) *gcs.Precondition {
	if ifGenMatch == nil && ifGenNotMatch == nil && ifMetaMatch == nil && ifMetaNotMatch == nil {
		return nil
	}
	return &gcs.Precondition{
		GenerationMatch:        ifGenMatch,
		GenerationNotMatch:     ifGenNotMatch,
		MetagenerationMatch:    ifMetaMatch,
		MetagenerationNotMatch: ifMetaNotMatch,
	}
}

// preconditionResult maps the store's raw gcs.ErrPreconditionFailed (returned
// by the *Checked object-write methods) to the canonical FAILED_PRECONDITION
// provider error. Callers still run the result through mapError. The provider's
// delete path instead returns a 412 PreconditionFailed provider error, which
// passes through here and is resolved by mapError's 412→FAILED_PRECONDITION
// status mapping.
func preconditionResult(err error) error {
	if errors.Is(err, gcs.ErrPreconditionFailed) {
		return preconditionErr("At least one of the pre-conditions you specified did not hold")
	}
	return err
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

// RestoreObject restores a non-live (soft-deleted/tombstoned) generation to
// live. The emulator models GCS soft-delete as versioning tombstones (TimeDeleted
// set), so this delegates to the store's atomic RestoreObjectGeneration, which
// flips the targeted generation live and marks the current live generation
// non-live. The if_* preconditions are validated against the current live
// generation inside that same atomic mutation. copy_source_acl and restore_token
// are accepted but ignored (the emulator has no ACL plane and no hierarchical
// namespaces).
func (s *Service) RestoreObject(ctx context.Context, req *storagepb.RestoreObjectRequest) (*storagepb.Object, error) {
	bucket := parseBucketName(req.GetBucket())
	object := req.GetObject()
	if err := s.requireDownscope(ctx, downscope.WriteObject, bucket, object); err != nil {
		return nil, err
	}
	if bucket == "" || object == "" || req.GetGeneration() <= 0 {
		return nil, mapError(model.NewProviderError("InvalidArgument", "restore requires bucket, object, and a positive generation", 400))
	}
	pre := grpcObjectPrecondition(req.IfGenerationMatch, req.IfGenerationNotMatch, req.IfMetagenerationMatch, req.IfMetagenerationNotMatch)
	meta, err := s.objects.RestoreObjectGeneration(ctx, bucket, object, int64ToGen(req.GetGeneration()), pre)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return nil, mapError(model.NewProviderError("NotFound", "bucket not found", 404))
		}
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return nil, mapError(model.NewProviderError("NotFound", "object generation not found", 404))
		}
		return nil, mapError(preconditionResult(err))
	}
	return objectToProto(meta), nil
}

// UpdateObject applies a patch update to an object's metadata.
func (s *Service) UpdateObject(ctx context.Context, req *storagepb.UpdateObjectRequest) (*storagepb.Object, error) {
	bucket := parseBucketName(req.GetObject().GetBucket())
	object := req.GetObject().GetName()
	if err := s.requireDownscope(ctx, downscope.WriteObject, bucket, object); err != nil {
		return nil, err
	}
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
	if err := s.requireDownscope(ctx, downscope.WriteObject, bucket, object); err != nil {
		return nil, err
	}
	for _, src := range sources {
		if err := s.requireDownscope(ctx, downscope.ReadObject, bucket, src.GetName()); err != nil {
			return nil, err
		}
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
	// Inherit the bucket default only when the destination resource did not
	// explicitly set event_based_hold.
	if dest == nil || dest.EventBasedHold == nil {
		s.applyDefaultEventBasedHold(ctx, bucket, &meta)
	}

	// The compose destination's if_generation_match/if_metageneration_match
	// (ComposeObjectRequest has no not-match variants) guard the write
	// atomically via the store's *Checked path.
	pre := grpcObjectPrecondition(req.IfGenerationMatch, nil, req.IfMetagenerationMatch, nil)
	finalMeta, err := s.provider.PutObjectData(ctx, project, meta, buf.Bytes(), versioned, priorBlobKey, false, meta.KmsKeyName, nil, "", pre)
	if err != nil {
		return nil, mapError(preconditionResult(err))
	}
	return objectToProto(finalMeta), nil
}

// copySourceKey extracts the raw CSEK from a RewriteObjectRequest's
// copy_source_encryption_key_bytes field (32 bytes); nil when absent. The
// provider verifies it against the source object's stored key hash.
func copySourceKey(req *storagepb.RewriteObjectRequest) []byte {
	key := req.GetCopySourceEncryptionKeyBytes()
	if len(key) != 32 {
		return nil
	}
	return key
}

// rewriteDestinationMeta builds the destination metadata for a server-side
// copy: a copy of the source metadata with a fresh generation, overridden by
// the request's optional destination resource (contentType/metadata/
// storageClass). kmsKey is the destination key (empty = server-DEK); the
// emulator re-encrypts, so encryption fields from the source are cleared.
func rewriteDestinationMeta(src gcs.ObjectMeta, dest *storagepb.Object, dstBucket, dstObject, generation, kmsKey string) gcs.ObjectMeta {
	now := clock.Now()
	meta := src
	meta.Bucket = dstBucket
	meta.Name = dstObject
	meta.Generation = generation
	meta.Metageneration = "1"
	meta.TimeCreated = now
	meta.Updated = now
	meta.TimeDeleted = nil
	meta.Retention = nil
	meta.WrappedDEK = nil
	meta.CSEKeySHA256 = ""
	meta.KmsKeyName = kmsKey
	if dest != nil {
		if ct := dest.GetContentType(); ct != "" {
			meta.ContentType = ct
		}
		if dest.GetMetadata() != nil {
			meta.Metadata = dest.GetMetadata()
		}
		if sc := dest.GetStorageClass(); sc != "" {
			meta.StorageClass = sc
		}
	}
	return meta
}

// priorBlobKeyFor returns the blob key of the destination object's existing
// live generation when the bucket is not versioned (so the overwrite can drop
// the superseded blob); empty when versioned or absent.
func (s *Service) priorBlobKeyFor(ctx context.Context, bucket, object string) string {
	if s.provider.BucketVersioned(ctx, bucket) {
		return ""
	}
	if prev, err := s.objects.GetObjectMeta(ctx, bucket, object); err == nil && prev.Generation != "" {
		return storageprovider.BlobKey(bucket, object, prev.Generation)
	}
	return ""
}

// RewriteObject implements the server-side copy RPC. The emulator re-encrypts
// on write, so it performs a full in-process copy of the source bytes +
// metadata and records it as a single fresh destination generation
// (done=true, no rewrite-token chunking). Destination preconditions are
// enforced atomically by the provider's guarded write; source generation
// selection and source preconditions are validated against the source meta.
func (s *Service) RewriteObject(ctx context.Context, req *storagepb.RewriteObjectRequest) (*storagepb.RewriteResponse, error) {
	srcBucket := parseBucketName(req.GetSourceBucket())
	srcObject := req.GetSourceObject()
	dstBucket := parseBucketName(req.GetDestinationBucket())
	dstObject := req.GetDestinationName()
	if srcBucket == "" || srcObject == "" || dstBucket == "" || dstObject == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "rewrite requires source and destination object names", 400))
	}
	if err := s.requireDownscope(ctx, downscope.ReadObject, srcBucket, srcObject); err != nil {
		return nil, err
	}
	if err := s.requireDownscope(ctx, downscope.WriteObject, dstBucket, dstObject); err != nil {
		return nil, err
	}

	srcProject := s.projectForBucket(ctx, srcBucket)
	srcGen := ""
	if req.GetSourceGeneration() > 0 {
		srcGen = int64ToGen(req.GetSourceGeneration())
	}
	srcKey := copySourceKey(req)
	srcMeta, raw, err := s.provider.GetObjectData(ctx, srcProject, srcBucket, srcObject, srcGen, srcKey)
	if err != nil {
		return nil, mapError(err)
	}
	if err := checkObjectPreconditions(srcMeta, req.IfSourceGenerationMatch, req.IfSourceGenerationNotMatch, req.IfSourceMetagenerationMatch, req.IfSourceMetagenerationNotMatch); err != nil {
		return nil, mapError(err)
	}

	dstProject := s.projectForBucket(ctx, dstBucket)
	versioned := s.provider.BucketVersioned(ctx, dstBucket)
	dstKey, dstKeySHA := cseKeyFromParams(req.GetCommonObjectRequestParams())
	meta := rewriteDestinationMeta(srcMeta, req.GetDestination(), dstBucket, dstObject, s.provider.NextGen(), req.GetDestinationKmsKey())

	pre := grpcObjectPrecondition(req.IfGenerationMatch, req.IfGenerationNotMatch, req.IfMetagenerationMatch, req.IfMetagenerationNotMatch)
	finalMeta, err := s.provider.PutObjectData(ctx, dstProject, meta, raw, versioned, s.priorBlobKeyFor(ctx, dstBucket, dstObject), true, meta.KmsKeyName, dstKey, dstKeySHA, pre)
	if err != nil {
		return nil, mapError(preconditionResult(err))
	}
	obj := objectToProto(finalMeta)
	return &storagepb.RewriteResponse{
		TotalBytesRewritten: finalMeta.Size,
		ObjectSize:          finalMeta.Size,
		Done:                true,
		Resource:            obj,
	}, nil
}

// MoveObject implements the move RPC for the single-live-generation emulator as
// a copy-then-delete: the source bytes+metadata are written to the destination
// under the destination preconditions, then the source is deleted under the
// source preconditions. Both mutations go through the store's atomic *Checked
// path (via the provider), so a precondition mismatch on either side is
// rejected WITHOUT mutating that side. The two steps are not one transaction,
// so a concurrent source mutation between them can leave both objects present;
// the write is ordered first so a failure never loses the source's bytes.
func (s *Service) MoveObject(ctx context.Context, req *storagepb.MoveObjectRequest) (*storagepb.Object, error) {
	bucket := parseBucketName(req.GetBucket())
	srcObject := req.GetSourceObject()
	dstObject := req.GetDestinationObject()
	if bucket == "" || srcObject == "" || dstObject == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "move requires bucket, source and destination object names", 400))
	}
	if srcObject == dstObject {
		return nil, mapError(model.NewProviderError("InvalidArgument", "source and destination object must differ", 400))
	}
	if err := s.requireDownscope(ctx, downscope.ReadObject, bucket, srcObject); err != nil {
		return nil, err
	}
	if err := s.requireDownscope(ctx, downscope.DeleteObject, bucket, srcObject); err != nil {
		return nil, err
	}
	if err := s.requireDownscope(ctx, downscope.WriteObject, bucket, dstObject); err != nil {
		return nil, err
	}

	project := s.projectForBucket(ctx, bucket)
	srcMeta, raw, err := s.provider.GetObjectData(ctx, project, bucket, srcObject, "", nil)
	if err != nil {
		return nil, mapError(err)
	}

	srcPre := grpcObjectPrecondition(req.IfSourceGenerationMatch, req.IfSourceGenerationNotMatch, req.IfSourceMetagenerationMatch, req.IfSourceMetagenerationNotMatch)
	if err := checkObjectPreconditions(srcMeta, req.IfSourceGenerationMatch, req.IfSourceGenerationNotMatch, req.IfSourceMetagenerationMatch, req.IfSourceMetagenerationNotMatch); err != nil {
		return nil, mapError(err)
	}

	versioned := s.provider.BucketVersioned(ctx, bucket)
	meta := rewriteDestinationMeta(srcMeta, nil, bucket, dstObject, s.provider.NextGen(), "")
	destPre := grpcObjectPrecondition(req.IfGenerationMatch, req.IfGenerationNotMatch, req.IfMetagenerationMatch, req.IfMetagenerationNotMatch)
	finalMeta, err := s.provider.PutObjectData(ctx, project, meta, raw, versioned, s.priorBlobKeyFor(ctx, bucket, dstObject), true, "", nil, "", destPre)
	if err != nil {
		return nil, mapError(preconditionResult(err))
	}

	if err := s.provider.DeleteObjectData(ctx, bucket, srcObject, "", srcPre); err != nil {
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
	if err := s.requireDownscope(ctx, downscope.WriteObject, bucket, object); err != nil {
		return nil, err
	}

	id := s.provider.NextGen()
	now := clock.RealNow()

	// Preconditions from the write spec are carried on the session and checked
	// atomically when the resumable write is finalized.
	var pre *gcs.Precondition
	if spec != nil {
		pre = grpcObjectPrecondition(spec.IfGenerationMatch, spec.IfGenerationNotMatch, spec.IfMetagenerationMatch, spec.IfMetagenerationNotMatch)
	}

	sess := &uploadSession{
		bucket:         bucket,
		object:         object,
		contentType:    resource.GetContentType(),
		metadata:       resource.GetMetadata(),
		kmsKeyName:     resource.GetKmsKey(),
		temporaryHold:  resource.GetTemporaryHold(),
		eventBasedHold: resource.EventBasedHold,
		precondition:   pre,
		lastAccess:     now,
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
	if err := s.requireDownscope(ctx, downscope.WriteObject, sess.bucket, sess.object); err != nil {
		return nil, err
	}
	return &storagepb.QueryWriteStatusResponse{
		WriteStatus: &storagepb.QueryWriteStatusResponse_PersistedSize{PersistedSize: persisted},
	}, nil
}

// CancelResumableWrite discards an in-progress resumable upload session. It
// removes the in-memory session (closing any spill file) and the durable store
// record. Both are idempotent for an unknown id — real GCS returns OK for an
// upload_id it no longer knows — so cancelling an already-completed upload is a
// harmless no-op that never touches the committed object.
func (s *Service) CancelResumableWrite(ctx context.Context, req *storagepb.CancelResumableWriteRequest) (*storagepb.CancelResumableWriteResponse, error) {
	id := req.GetUploadId()
	if id == "" {
		return nil, mapError(model.NewProviderError("InvalidArgument", "missing upload_id", 400))
	}
	s.mu.Lock()
	sess, ok := s.uploads[id]
	if ok {
		if err := s.requireDownscope(ctx, downscope.WriteObject, sess.bucket, sess.object); err != nil {
			s.mu.Unlock()
			return nil, err
		}
		sess.closeSpill()
		delete(s.uploads, id)
	}
	s.mu.Unlock()
	if err := s.objects.DeleteResumable(ctx, id); err != nil && !errors.Is(err, gcs.ErrNoSuchUpload) {
		return nil, mapError(err)
	}
	return &storagepb.CancelResumableWriteResponse{}, nil
}

// ─── writes ───────────────────────────────────────────────────────────────────

// appendData appends content at writeOffset, enforcing that writeOffset equals
// the session's persisted length (a resume continues exactly where the prior
// stream left off). A gap or overlap is an OutOfRange error, matching real
// GCS. It runs under s.mu so a concurrent QueryWriteStatus read of sess.length
// never races with an active write stream.
//
// Bytes accumulate in memory until they would cross resumableSpillThreshold, at
// which point the buffer is flushed to a temp file and subsequent bytes are
// appended there (mirroring the REST provider's spill model), so a large upload
// never holds its whole payload in memory.
func (s *Service) appendData(sess *uploadSession, writeOffset int64, content []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if writeOffset != sess.length {
		return &model.ProviderError{Code: "OutOfRange", Message: "write offset does not match the persisted size", HTTPStatus: 400, Status: "OUT_OF_RANGE"}
	}
	if sess.tmpFile != nil {
		if _, err := sess.tmpFile.Write(content); err != nil {
			return fmt.Errorf("resumable upload: write spill file: %w", err)
		}
	} else if int64(len(sess.buf))+int64(len(content)) > resumableSpillThreshold {
		if err := spillUploadSession(sess); err != nil {
			return err
		}
		if _, err := sess.tmpFile.Write(content); err != nil {
			return fmt.Errorf("resumable upload: write spill file: %w", err)
		}
	} else {
		sess.buf = append(sess.buf, content...)
	}
	sess.length += int64(len(content))
	return nil
}

// spillUploadSession flushes the in-memory buffer to a temp file and switches
// the session to file-backed accumulation. The caller must hold s.mu.
func spillUploadSession(sess *uploadSession) error {
	f, err := os.CreateTemp("", "jaiscloud-gcp-grpc-resumable-*")
	if err != nil {
		return fmt.Errorf("resumable upload: create spill file: %w", err)
	}
	if _, err := f.Write(sess.buf); err != nil {
		f.Close()
		os.Remove(f.Name())
		return fmt.Errorf("resumable upload: flush spill file: %w", err)
	}
	sess.tmpFile = f
	sess.tmpPath = f.Name()
	sess.buf = nil
	return nil
}

// sessionBytes returns the session's accumulated bytes, reading them back from
// the spill file when the session spilled. It runs under s.mu so a concurrent
// resume stream's append cannot race the read (appendData takes the same lock).
func (s *Service) sessionBytes(sess *uploadSession) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess.tmpFile == nil {
		return sess.buf, nil
	}
	if _, err := sess.tmpFile.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("resumable upload: seek spill file: %w", err)
	}
	data, err := io.ReadAll(sess.tmpFile)
	if err != nil {
		return nil, fmt.Errorf("resumable upload: read spill file: %w", err)
	}
	return data, nil
}

// applyDefaultEventBasedHold sets meta.EventBasedHold from the bucket's
// defaultEventBasedHold when the caller did not explicitly set it. It is the
// gRPC analogue of the REST ObjectsInsert default inheritance.
func (s *Service) applyDefaultEventBasedHold(ctx context.Context, bucket string, meta *gcs.ObjectMeta) {
	bmeta, err := s.objects.GetBucket(ctx, bucket)
	if err != nil {
		return
	}
	if def, _ := bmeta["defaultEventBasedHold"].(bool); def {
		meta.EventBasedHold = true
	}
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
		TemporaryHold:  sess.temporaryHold,
		TimeCreated:    clock.Now(),
		Updated:        clock.Now(),
	}
	if sess.eventBasedHold != nil {
		meta.EventBasedHold = *sess.eventBasedHold
	} else {
		s.applyDefaultEventBasedHold(ctx, sess.bucket, &meta)
	}
	if meta.ContentType == "" {
		meta.ContentType = "application/octet-stream"
	}
	raw, err := s.sessionBytes(sess)
	if err != nil {
		return nil, err
	}
	finalMeta, err := s.provider.PutObjectData(ctx, project, meta, raw, versioned, priorBlobKey, true, sess.kmsKeyName, sess.cseKey, sess.cseKeySHA256, sess.precondition)
	if err != nil {
		// The caller maps the result to a gRPC status.
		return nil, preconditionResult(err)
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

	// A non-resumable session is local to this stream, so its spill file (if
	// any) is removed when the stream ends, success or error. A resumable
	// session stays in the upload map for a later resume and is cleaned up on
	// finish (below) or Reset.
	defer func() {
		if sess != nil && !resumable {
			sess.closeSpill()
		}
	}()

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
					bucket:         parseBucketName(res.GetBucket()),
					object:         res.GetName(),
					contentType:    res.GetContentType(),
					metadata:       res.GetMetadata(),
					kmsKeyName:     res.GetKmsKey(),
					temporaryHold:  res.GetTemporaryHold(),
					eventBasedHold: res.EventBasedHold,
					precondition:   grpcObjectPrecondition(fm.WriteObjectSpec.IfGenerationMatch, fm.WriteObjectSpec.IfGenerationNotMatch, fm.WriteObjectSpec.IfMetagenerationMatch, fm.WriteObjectSpec.IfMetagenerationNotMatch),
				}
				project = s.projectForBucket(ctx, sess.bucket)
				if err := s.requireDownscope(ctx, downscope.WriteObject, sess.bucket, sess.object); err != nil {
					return err
				}
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
				if err := s.requireDownscope(ctx, downscope.WriteObject, sess.bucket, sess.object); err != nil {
					return err
				}
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
				sess.closeSpill()
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

	// A non-resumable session is local to this stream, so its spill file (if
	// any) is removed when the stream ends, success or error. A resumable
	// session stays in the upload map for a later resume and is cleaned up on
	// finish (below) or Reset.
	defer func() {
		if sess != nil && !resumable {
			sess.closeSpill()
		}
	}()

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
					bucket:         parseBucketName(res.GetBucket()),
					object:         res.GetName(),
					contentType:    res.GetContentType(),
					metadata:       res.GetMetadata(),
					kmsKeyName:     res.GetKmsKey(),
					temporaryHold:  res.GetTemporaryHold(),
					eventBasedHold: res.EventBasedHold,
					precondition:   grpcObjectPrecondition(fm.WriteObjectSpec.IfGenerationMatch, fm.WriteObjectSpec.IfGenerationNotMatch, fm.WriteObjectSpec.IfMetagenerationMatch, fm.WriteObjectSpec.IfMetagenerationNotMatch),
				}
				if sess.contentType == "" {
					sess.contentType = "application/octet-stream"
				}
				sess.cseKey, sess.cseKeySHA256 = cseKeyFromParams(req.GetCommonObjectRequestParams())
				project = s.projectForBucket(ctx, sess.bucket)
				if err := s.requireDownscope(ctx, downscope.WriteObject, sess.bucket, sess.object); err != nil {
					return err
				}
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
				if err := s.requireDownscope(ctx, downscope.WriteObject, sess.bucket, sess.object); err != nil {
					return err
				}
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
				sess.closeSpill()
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

// ReadObject streams an object (or a byte range of it) to the client.
//
// Range handling note: objects are stored as a single AES-256-GCM blob
// (iv || ciphertext || tag) for both CSEK and the server/CMEK envelope. GCM
// authenticates the entire ciphertext with one tag, so the requested window
// cannot be decrypted without processing the whole blob — true partial
// decryption is not feasible for the on-disk format. The closest safe
// improvement is therefore: (1) decrypt exactly once per request (the provider
// returns the full plaintext and the range is sliced as a view, with no copy),
// and (2) detect a zero-byte range from the metadata and skip the blob read and
// decryption entirely. Both paths preserve correctness.
func (s *Service) ReadObject(req *storagepb.ReadObjectRequest, stream storagepb.Storage_ReadObjectServer) error {
	ctx := stream.Context()
	bucket := parseBucketName(req.GetBucket())
	project := s.projectForBucket(ctx, bucket)
	if err := s.requireDownscope(ctx, downscope.ReadObject, bucket, req.GetObject()); err != nil {
		return err
	}
	gen := ""
	if req.GetGeneration() > 0 {
		gen = int64ToGen(req.GetGeneration())
	}

	// Resolve the byte range from metadata before touching the blob so a
	// zero-length read never decrypts the object.
	var meta gcs.ObjectMeta
	var err error
	if req.GetGeneration() > 0 {
		meta, err = s.objects.GetObjectGeneration(ctx, bucket, req.GetObject(), gen)
	} else {
		meta, err = s.objects.GetObjectMeta(ctx, bucket, req.GetObject())
	}
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return mapError(model.NewProviderError("NotFound", "object not found", 404))
		}
		return mapError(err)
	}
	start, end := readRange(meta.Size, req.GetReadOffset(), req.GetReadLimit())

	// First message carries metadata + object checksums + content range.
	first := &storagepb.ReadObjectResponse{
		Metadata:        objectToProto(meta),
		ObjectChecksums: objectChecksumsFromMeta(meta),
		ContentRange: &storagepb.ContentRange{
			Start:          start,
			End:            end,
			CompleteLength: meta.Size,
		},
	}
	if start == end {
		// Zero-byte range: metadata-only response; no blob read/decrypt.
		return stream.Send(first)
	}

	var cseKey []byte
	if cop := req.GetCommonObjectRequestParams(); cop != nil {
		cseKey, _ = cseKeyFromParams(cop)
	}
	readMeta, plain, err := s.provider.GetObjectData(ctx, project, bucket, req.GetObject(), gen, cseKey)
	if err != nil {
		return mapError(err)
	}
	meta = readMeta
	// Defensive clamp: metadata size and the decrypted length should agree, but
	// never index past the plaintext actually in hand.
	if int64(len(plain)) < end {
		end = int64(len(plain))
	}
	if start > end {
		start = end
	}
	// Rebuild the envelope from the decrypted plaintext (the authoritative
	// length) rather than the pre-decrypt metadata snapshot.
	first.Metadata = objectToProto(meta)
	first.ObjectChecksums = objectChecksumsFromMeta(meta)
	first.ContentRange = &storagepb.ContentRange{Start: start, End: end, CompleteLength: int64(len(plain))}
	if err := stream.Send(first); err != nil {
		return err
	}
	data := plain[start:end]

	// Stream the data in bounded chunks. Each chunk carries its own CRC32C of
	// the content field: real GCS always sets it, and the official Go client's
	// zero-copy ReadObject decoder requires a field to follow the content when
	// the content ends exactly on a message boundary (otherwise it advances its
	// buffer cursor past the last buffer and panics on the next read).
	const chunk = 2 << 20 // 2 MiB
	for len(data) > 0 {
		n := chunk
		if n > len(data) {
			n = len(data)
		}
		content := data[:n]
		crc := crc32cOf(content)
		if err := stream.Send(&storagepb.ReadObjectResponse{
			ChecksummedData: &storagepb.ChecksummedData{Content: content, Crc32C: &crc},
		}); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// crc32cOf returns the CRC32C-Castagnoli checksum of data, the digest GCS
// carries in ChecksummedData.crc32c.
func crc32cOf(data []byte) uint32 {
	return crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
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
