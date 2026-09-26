// Package storage implements the Google Cloud Storage provider (buckets and
// objects) on top of the shared ResourceStore (metadata) and BlobStore (bytes).
package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/downscope"
	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/gcp/store/gcs"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// Resource types used in the generic ResourceStore (IAM + ACL; buckets and
// objects live in the dedicated gcs.ObjectStore). The two IAM types are
// exported so the gRPC Storage service keys bucket/object policies identically
// and both transports share one policy store.
const (
	ResourceTypeBucketIAM = "gcs_bucket_iam"
	ResourceTypeObjectIAM = "gcs_object_iam"

	rtBucketIAM = ResourceTypeBucketIAM
	rtObjectIAM = ResourceTypeObjectIAM
	rtACL       = "gcs_acl"
	// rtNotification is the generic-store resource type for per-bucket Cloud
	// Pub/Sub notification configurations (GCS notificationConfigs).
	rtNotification = "gcs_notification"
)

// EventPublisher publishes one Cloud Pub/Sub message on behalf of another
// provider. It is satisfied by the Pub/Sub provider, letting the storage
// provider reuse Pub/Sub's envelope encryption, per-subscription fan-out and
// push delivery without importing the provider/pubsub package (no
// provider→provider dependency).
type EventPublisher interface {
	// PublishEvent delivers data (plaintext bytes) with attributes to the given
	// topic (a bare ID, "projects/{p}/topics/{t}", or the fully-qualified
	// "//pubsub.googleapis.com/..." form) and returns the message ID.
	PublishEvent(ctx context.Context, accountID, topic string, data []byte, attributes map[string]string) (string, error)
	// TopicExists reports whether the topic exists in the project (real GCS
	// rejects a notificationConfigs.insert for a missing topic with 404).
	TopicExists(ctx context.Context, accountID, topic string) (bool, error)
}

// notificationConfig is a stored GCS notificationConfigs entry. The JSON field
// names match the GCS JSON API schema (storage#notification), which uses
// snake_case for payload_format/event_types/custom_attributes/object_name_prefix
// (per the Discovery document) and camelCase for selfLink.
type notificationConfig struct {
	Kind             string            `json:"kind,omitempty"`
	ID               string            `json:"id,omitempty"`
	SelfLink         string            `json:"selfLink,omitempty"`
	Topic            string            `json:"topic,omitempty"`
	PayloadFormat    string            `json:"payload_format,omitempty"`
	EventTypes       []string          `json:"event_types,omitempty"`
	ObjectNamePrefix string            `json:"object_name_prefix,omitempty"`
	CustomAttributes map[string]string `json:"custom_attributes,omitempty"`
	Etag             string            `json:"etag,omitempty"`
}

// blobsNamespace is the BlobStore namespace ("bucket") used for GCS object bytes.
const blobsNamespace = "gcs"

// maxUploadSessions caps concurrent resumable-upload sessions (DoS guard).
const maxUploadSessions = 1000

// resumableSessionTTL bounds how long an inactive resumable session is kept
// before the periodic sweep removes it (DoS guard; real GCS keeps sessions
// for about a week).
const resumableSessionTTL = 24 * time.Hour

// resumableSpillThreshold is the in-memory buffer size beyond which a
// resumable upload session spills its accumulated bytes to a temp file so that
// large uploads never buffer fully in memory.
const resumableSpillThreshold = 4 << 20 // 4 MiB

// Provider implements the GCS JSON + media API.
type Provider struct {
	objects   gcs.ObjectStore     // dedicated store: buckets + objects (jc_gcs_*)
	resources store.ResourceStore // generic store: IAM + ACL (jc_resources)
	blobs     blobfs.BlobStore    // object bytes
	encryptor crypto.EnvelopeEncryptor

	mu      sync.Mutex
	uploads map[string]*uploadSession // resumable upload sessions (in-memory)
	// completed tombstones for finished resumable uploads, so a post-completion
	// status query returns 200 + the object. Bounded and TTL-swept; lost on
	// restart (an in-memory concern, not durably mirrored).
	completed map[string]*completedSession

	genMu sync.Mutex
	gen   int64 // monotonically-increasing object generation counter

	notifMu   sync.Mutex // serialises notification ID allocation
	publisher EventPublisher
}

// SetEventPublisher wires the Pub/Sub publisher used to fan object events out
// to a bucket's notificationConfigs. Called once at startup; nil disables
// notification delivery (unit tests that don't exercise fan-out).
func (p *Provider) SetEventPublisher(ev EventPublisher) { p.publisher = ev }

// completedSession is a lightweight tombstone for a finished resumable upload,
// holding the finalized object resource so a post-completion status query can
// replay it.
type completedSession struct {
	Bucket     string
	Object     string
	objectJSON map[string]any
	lastAccess time.Time
}

// uploadSession holds the state of an in-progress resumable upload.
type uploadSession struct {
	Bucket      string
	Object      string
	ContentType string
	Metadata    map[string]string // custom object metadata captured at session start
	buf         []byte            // in-memory bytes up to resumableSpillThreshold
	tmpPath     string            // spill file path once threshold is exceeded
	tmpFile     *os.File          // open handle for appending spilled bytes
	length      int64             // total accumulated bytes across chunks
	lastAccess  time.Time         // last chunk/status-query time, for the TTL sweep
}

// New returns a GCS provider backed by the dedicated object store (buckets +
// objects), the generic resource store (IAM + ACL), the blob store (bytes),
// and an envelope encryptor (CMEK/CSEK/server-DEK).
func New(objects gcs.ObjectStore, resources store.ResourceStore, blobs blobfs.BlobStore, encryptor crypto.EnvelopeEncryptor) *Provider {
	p := &Provider{
		objects:   objects,
		resources: resources,
		blobs:     blobs,
		encryptor: encryptor,
		uploads:   make(map[string]*uploadSession),
		completed: make(map[string]*completedSession),
		gen:       clock.Now().UnixNano(),
	}
	p.seedGeneration(context.Background())
	return p
}

// seedGeneration bumps the generation counter past the highest stored
// generation so generations remain monotonic across restarts (--dsn).
func (p *Provider) seedGeneration(ctx context.Context) {
	maxGenStr, err := p.objects.MaxGeneration(ctx)
	if err != nil {
		return
	}
	maxGen, err := strconv.ParseInt(maxGenStr, 10, 64)
	if err != nil {
		return
	}
	p.genMu.Lock()
	if maxGen >= p.gen {
		p.gen = maxGen
	}
	p.genMu.Unlock()
}

// SeedGeneration re-reads the highest stored generation and bumps the in-memory
// counter past it. In memory mode the state.json restore runs after the
// provider is constructed, so the construction-time seed (New) alone would miss
// restored generations under a frozen clock; the startup path calls this again
// after restore to keep generations monotonic.
func (p *Provider) SeedGeneration(ctx context.Context) {
	p.seedGeneration(ctx)
}

// Name satisfies admin.PostRestoreHook.
func (p *Provider) Name() string { return "gcs-generation" }

// OnRestore re-seeds the generation counter after an admin import so that
// generations stay monotonic past the highest generation present in the
// imported snapshot (mirrors the startup-time SeedGeneration call).
func (p *Provider) OnRestore(ctx context.Context) error {
	p.SeedGeneration(ctx)
	return nil
}

// nextGen returns a unique, monotonically-increasing object generation.
func (p *Provider) nextGen() string {
	p.genMu.Lock()
	p.gen++
	g := p.gen
	p.genMu.Unlock()
	return strconv.FormatInt(g, 10)
}

// NextGen returns a fresh monotonically-increasing object generation. Exported
// so the gRPC Storage service shares the REST provider's generation counter.
func (p *Provider) NextGen() string { return p.nextGen() }

// Reset clears in-progress resumable-upload sessions. Implements admin.Resetter
// so /_jaiscloud/reset does not leak upload state across test runs.
func (p *Provider) Reset(_ context.Context) {
	p.mu.Lock()
	for _, sess := range p.uploads {
		if sess.tmpFile != nil {
			sess.tmpFile.Close()
			os.Remove(sess.tmpPath)
		}
	}
	p.uploads = make(map[string]*uploadSession)
	p.completed = make(map[string]*completedSession)
	p.mu.Unlock()
}

// sweepSessions removes stale in-memory resumable sessions, closing and
// deleting any spill files, and evicts expired completion tombstones. The
// caller must hold p.mu.
func (p *Provider) sweepSessions() {
	now := clock.RealNow()
	for id, sess := range p.uploads {
		if now.Sub(sess.lastAccess) > resumableSessionTTL {
			if sess.tmpFile != nil {
				sess.tmpFile.Close()
				os.Remove(sess.tmpPath)
			}
			delete(p.uploads, id)
		}
	}
	for id, done := range p.completed {
		if now.Sub(done.lastAccess) > resumableSessionTTL {
			delete(p.completed, id)
		}
	}
}

// sweepStaleStore deletes resumable sessions in the durable store whose
// last_access predates the TTL (best-effort; also catches orphans left behind
// after a process restart, when the in-memory map is empty).
func (p *Provider) sweepStaleStore(ctx context.Context) {
	stale, err := p.objects.ListStaleResumable(ctx, clock.RealNow().Add(-resumableSessionTTL))
	if err != nil {
		return
	}
	for _, s := range stale {
		_ = p.objects.DeleteResumable(ctx, s.UploadID)
	}
}

// requireDownscope enforces a downscoped credential's access boundary on one
// GCS operation. It is a no-op when the request carries no downscoped bearer
// token (ordinary emulator requests stay unrestricted) and for synthetic
// requests without an underlying HTTP request (in-process dispatch, unit tests).
func requireDownscope(nr *model.NormalizedRequest, op downscope.Op, bucket, name string) error {
	if nr == nil || nr.Raw == nil {
		return nil
	}
	if err := downscope.Allowed(nr.Raw.Header.Get("Authorization"), op, bucket, name); err != nil {
		return model.NewProviderError("PermissionDenied", "Downscoped token does not allow this GCS operation", 403)
	}
	return nil
}

// requireBucketAdmin denies any bucket-level operation performed with a
// downscoped credential: a rule grants object access only.
func requireBucketAdmin(nr *model.NormalizedRequest) error {
	return requireDownscope(nr, downscope.BucketAdmin, "", "")
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Storage.BucketsList":                 p.BucketsList,
		"Storage.BucketsInsert":               p.BucketsInsert,
		"Storage.BucketsGet":                  p.BucketsGet,
		"Storage.BucketsUpdate":               p.BucketsUpdate,
		"Storage.BucketsLockRetentionPolicy":  p.BucketsLockRetentionPolicy,
		"Storage.BucketsGetStorageLayout":     p.BucketsGetStorageLayout,
		"Storage.NotificationsInsert":         p.NotificationsInsert,
		"Storage.NotificationsList":           p.NotificationsList,
		"Storage.NotificationsGet":            p.NotificationsGet,
		"Storage.NotificationsDelete":         p.NotificationsDelete,
		"Storage.BucketsDelete":               p.BucketsDelete,
		"Storage.BucketsGetIamPolicy":         p.BucketsGetIamPolicy,
		"Storage.BucketsSetIamPolicy":         p.BucketsSetIamPolicy,
		"Storage.BucketACLList":               p.BucketACLList,
		"Storage.BucketACLInsert":             p.BucketACLInsert,
		"Storage.ObjectsList":                 p.ObjectsList,
		"Storage.ObjectsInsert":               p.ObjectsInsert,
		"Storage.ObjectsGet":                  p.ObjectsGet,
		"Storage.ObjectsGetMedia":             p.ObjectsGetMedia,
		"Storage.ObjectsUpdate":               p.ObjectsUpdate,
		"Storage.ObjectsPatch":                p.ObjectsPatch,
		"Storage.ObjectsGetIamPolicy":         p.ObjectsGetIamPolicy,
		"Storage.ObjectsSetIamPolicy":         p.ObjectsSetIamPolicy,
		"Storage.ObjectsDelete":               p.ObjectsDelete,
		"Storage.ObjectsRewrite":              p.ObjectsRewrite,
		"Storage.ObjectsCopy":                 p.ObjectsCopy,
		"Storage.ObjectsMove":                 p.ObjectsMove,
		"Storage.ObjectsRestore":              p.ObjectsRestore,
		"Storage.ObjectsCompose":              p.ObjectsCompose,
		"Storage.ObjectACLList":               p.ObjectACLList,
		"Storage.ObjectACLInsert":             p.ObjectACLInsert,
		"Storage.ObjectsInsertStartResumable": p.ObjectsInsertStartResumable,
		"Storage.ObjectsInsertResumable":      p.ObjectsInsertResumable,
	}
}

// ─── metadata types ───────────────────────────────────────────────────────────

type bucketMeta struct {
	Name            string         `json:"name"`
	Location        string         `json:"location,omitempty"`
	StorageClass    string         `json:"storageClass,omitempty"`
	TimeCreated     string         `json:"timeCreated,omitempty"`
	Updated         string         `json:"updated,omitempty"`
	Metageneration  string         `json:"metageneration,omitempty"`
	Versioning      map[string]any `json:"versioning,omitempty"`
	RetentionPolicy map[string]any `json:"retentionPolicy,omitempty"`
	Lifecycle       map[string]any `json:"lifecycle,omitempty"`
	Encryption      map[string]any `json:"encryption,omitempty"`
	// Cors is the bucket's cross-origin resource sharing config (GCS
	// Bucket.cors): a list of {origin[], method[], responseHeader[],
	// maxAgeSeconds} rules. Stored verbatim so it round-trips through the
	// bucket-meta JSON on both the memory and Postgres stores.
	Cors []any `json:"cors,omitempty"`
	// SoftDeletePolicy is the bucket's soft-delete config (GCS
	// Bucket.softDeletePolicy). Nil means the GCS default (7 days) applies.
	SoftDeletePolicy map[string]any `json:"softDeletePolicy,omitempty"`
	// Labels is the bucket's user-defined label set (GCS Bucket.labels).
	Labels map[string]string `json:"labels,omitempty"`
	// DefaultEventBasedHold is inherited by newly created objects that do not
	// explicitly set eventBasedHold (GCS Bucket.defaultEventBasedHold).
	DefaultEventBasedHold bool `json:"defaultEventBasedHold,omitempty"`
}

type objectMeta struct {
	Kind           string            `json:"kind"`
	ID             string            `json:"id,omitempty"`
	Name           string            `json:"name"`
	Bucket         string            `json:"bucket"`
	Size           string            `json:"size,omitempty"`
	ContentType    string            `json:"contentType,omitempty"`
	Md5Hash        string            `json:"md5Hash,omitempty"`
	Crc32c         string            `json:"crc32c,omitempty"`
	Etag           string            `json:"etag,omitempty"`
	SelfLink       string            `json:"selfLink,omitempty"`
	MediaLink      string            `json:"mediaLink,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	Generation     string            `json:"generation,omitempty"`
	Metageneration string            `json:"metageneration,omitempty"`
	StorageClass   string            `json:"storageClass,omitempty"`
	TimeCreated    string            `json:"timeCreated,omitempty"`
	Updated        string            `json:"updated,omitempty"`
	// TimeFinalized is when the object's content was finalized (upload
	// complete). TimeStorageClassUpdated is when the storage class was last
	// set. The emulator does not track storage-class transitions separately,
	// so both derive from the object's creation time.
	TimeFinalized           string `json:"timeFinalized,omitempty"`
	TimeStorageClassUpdated string `json:"timeStorageClassUpdated,omitempty"`
	// ComponentCount is the number of source objects accumulated by compose
	// operations (GCS Object.componentCount). Zero for non-composite objects.
	ComponentCount int64 `json:"componentCount,omitempty"`
	// Retention is the object-level retention policy (Object.retention).
	Retention *objectRetention `json:"retention,omitempty"`
	// RetentionExpirationTime is the server-determined expiry (RFC 3339).
	RetentionExpirationTime string `json:"retentionExpirationTime,omitempty"`
	TemporaryHold           bool   `json:"temporaryHold,omitempty"`
	EventBasedHold          bool   `json:"eventBasedHold,omitempty"`
	// TimeDeleted is set on non-live generations (versioning).
	TimeDeleted string `json:"timeDeleted,omitempty"`
	// KmsKeyName is the CMEK key name (empty when server-DEK or CSEK encrypted).
	KmsKeyName string `json:"kmsKeyName,omitempty"`
	// CustomerEncryption describes a CSEK-encrypted object.
	CustomerEncryption *customerEncryption `json:"customerEncryption,omitempty"`
}

type customerEncryption struct {
	EncryptionAlgorithm string `json:"encryptionAlgorithm,omitempty"`
	KeySha256           string `json:"keySha256,omitempty"`
}

type objectRetention struct {
	RetainUntilTime string `json:"retainUntilTime,omitempty"`
	Mode            string `json:"mode,omitempty"`
}

// applyObjectHolds resolves a created/overwritten object's hold flags to match
// GCS: explicit temporaryHold/eventBasedHold fields in the request body win; an
// absent eventBasedHold inherits the bucket's defaultEventBasedHold. Shared by
// the REST insert/compose/copy paths (the gRPC write path applies the same rule
// via Service.applyDefaultEventBasedHold).
func applyObjectHolds(body map[string]any, bmeta map[string]any, o *objectMeta) {
	if body != nil {
		if h, ok := body["temporaryHold"].(bool); ok {
			o.TemporaryHold = h
		}
		if h, ok := body["eventBasedHold"].(bool); ok {
			o.EventBasedHold = h
			return
		}
	}
	if def, _ := bmeta["defaultEventBasedHold"].(bool); def {
		o.EventBasedHold = true
	}
}

// toStoreObject converts the wire objectMeta into the store's ObjectMeta.
func toStoreObject(o objectMeta) gcs.ObjectMeta {
	tc, _ := time.Parse(time.RFC3339Nano, o.TimeCreated)
	up, _ := time.Parse(time.RFC3339Nano, o.Updated)
	size, _ := strconv.ParseInt(o.Size, 10, 64)
	m := gcs.ObjectMeta{
		Bucket:         o.Bucket,
		Name:           o.Name,
		Generation:     o.Generation,
		Metageneration: o.Metageneration,
		ContentType:    o.ContentType,
		Size:           size,
		MD5Hash:        o.Md5Hash,
		CRC32C:         o.Crc32c,
		StorageClass:   o.StorageClass,
		Metadata:       o.Metadata,
		ComponentCount: o.ComponentCount,
		TimeCreated:    tc,
		Updated:        up,
		TemporaryHold:  o.TemporaryHold,
		EventBasedHold: o.EventBasedHold,
		KmsKeyName:     o.KmsKeyName,
	}
	if o.CustomerEncryption != nil {
		m.CSEKeySHA256 = o.CustomerEncryption.KeySha256
	}
	if o.Retention != nil && (o.Retention.Mode != "" || o.Retention.RetainUntilTime != "") {
		rt, _ := time.Parse(time.RFC3339Nano, o.Retention.RetainUntilTime)
		m.Retention = &gcs.ObjectRetention{RetainUntilTime: rt, Mode: o.Retention.Mode}
	}
	if o.TimeDeleted != "" {
		if td, err := time.Parse(time.RFC3339Nano, o.TimeDeleted); err == nil {
			m.TimeDeleted = &td
		}
	}
	return m
}

// baseURL returns the request's absolute base ("scheme://host") for building
// self/media links. Falls back to the emulator default when absent (unit tests
// build requests without a base).
func baseURL(nr *model.NormalizedRequest) string {
	if b, _ := nr.Params[wire.BaseURLKey].(string); b != "" {
		return b
	}
	return "http://localhost:8080"
}

func objectSelfLink(base, bucket, name string) string {
	return base + "/storage/v1/b/" + bucket + "/o/" + url.PathEscape(name)
}

func objectMediaLink(base, bucket, name string) string {
	return base + "/download/storage/v1/b/" + bucket + "/o/" + url.PathEscape(name) + "?alt=media"
}

func bucketSelfLink(base, name string) string {
	return base + "/storage/v1/b/" + name
}

// fromStoreObject converts a stored ObjectMeta back into the wire objectMeta,
// re-deriving the derived fields (kind/id/etag/selfLink/mediaLink).
func fromStoreObject(nr *model.NormalizedRequest, m gcs.ObjectMeta) objectMeta {
	o := objectMeta{
		Kind:           "storage#object",
		Name:           m.Name,
		Bucket:         m.Bucket,
		Size:           strconv.FormatInt(m.Size, 10),
		ContentType:    m.ContentType,
		Md5Hash:        m.MD5Hash,
		Crc32c:         m.CRC32C,
		Etag:           "CAE=",
		Metadata:       m.Metadata,
		Generation:     m.Generation,
		Metageneration: m.Metageneration,
		StorageClass:   m.StorageClass,
		ComponentCount: m.ComponentCount,
		TemporaryHold:  m.TemporaryHold,
		EventBasedHold: m.EventBasedHold,
	}
	if !m.TimeCreated.IsZero() {
		o.TimeCreated = m.TimeCreated.Format(time.RFC3339Nano)
		o.TimeFinalized = o.TimeCreated
		o.TimeStorageClassUpdated = o.TimeCreated
	}
	if !m.Updated.IsZero() {
		o.Updated = m.Updated.Format(time.RFC3339Nano)
	}
	if m.Retention != nil {
		o.Retention = &objectRetention{Mode: m.Retention.Mode}
		if !m.Retention.RetainUntilTime.IsZero() {
			o.Retention.RetainUntilTime = m.Retention.RetainUntilTime.Format(time.RFC3339Nano)
			o.RetentionExpirationTime = o.Retention.RetainUntilTime
		}
	}
	if m.TimeDeleted != nil {
		o.TimeDeleted = m.TimeDeleted.Format(time.RFC3339Nano)
	}
	if m.KmsKeyName != "" {
		o.KmsKeyName = m.KmsKeyName
	}
	if m.CSEKeySHA256 != "" {
		o.CustomerEncryption = &customerEncryption{EncryptionAlgorithm: "AES256", KeySha256: m.CSEKeySHA256}
	}
	o.ID = m.Bucket + "/" + m.Name + "/" + m.Generation
	base := baseURL(nr)
	o.SelfLink = objectSelfLink(base, m.Bucket, m.Name)
	o.MediaLink = objectMediaLink(base, m.Bucket, m.Name)
	return o
}

// blobKey returns the blob store key for an object generation. Every object
// (versioned or not) is stored under a generation-specific key so overwriting a
// versioned object never clobbers a prior generation's bytes.
func blobKey(bucket, object, generation string) string {
	return bucket + "/" + object + "/" + generation
}

// BlobKey returns the blob store key for an object generation. Exported so the
// gRPC Storage service computes the same keys as the REST provider.
func BlobKey(bucket, object, generation string) string {
	return blobKey(bucket, object, generation)
}

// objectPrecondition parses GCS's real ifGenerationMatch/ifGenerationNotMatch/
// ifMetagenerationMatch/ifMetagenerationNotMatch query params — the codec's
// generic queryToParams already copies these into nr.Params as strings, but
// nothing previously read them, so a client's conditional write (most
// commonly ifGenerationMatch=0 for "create only if this object doesn't
// already exist") was silently applied unconditionally. Returns nil when the
// request carries none of them (the overwhelmingly common case).
func objectPrecondition(nr *model.NormalizedRequest) *gcs.Precondition {
	var pre gcs.Precondition
	set := false
	if v, ok := parseInt64Param(nr, "ifGenerationMatch"); ok {
		pre.GenerationMatch = &v
		set = true
	}
	if v, ok := parseInt64Param(nr, "ifGenerationNotMatch"); ok {
		pre.GenerationNotMatch = &v
		set = true
	}
	if v, ok := parseInt64Param(nr, "ifMetagenerationMatch"); ok {
		pre.MetagenerationMatch = &v
		set = true
	}
	if v, ok := parseInt64Param(nr, "ifMetagenerationNotMatch"); ok {
		pre.MetagenerationNotMatch = &v
		set = true
	}
	if !set {
		return nil
	}
	return &pre
}

// bucketPrecondition parses the bucket-level OCC preconditions GCS honors on
// buckets.update: ifMetagenerationMatch/ifMetagenerationNotMatch. Returns nil
// when neither is present (the common case).
func bucketPrecondition(nr *model.NormalizedRequest) *gcs.Precondition {
	var pre gcs.Precondition
	set := false
	if v, ok := parseInt64Param(nr, "ifMetagenerationMatch"); ok {
		pre.MetagenerationMatch = &v
		set = true
	}
	if v, ok := parseInt64Param(nr, "ifMetagenerationNotMatch"); ok {
		pre.MetagenerationNotMatch = &v
		set = true
	}
	if !set {
		return nil
	}
	return &pre
}

func parseInt64Param(nr *model.NormalizedRequest, key string) (int64, bool) {
	s, ok := nr.Params[key].(string)
	if !ok || s == "" {
		return 0, false
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// ─── buckets ──────────────────────────────────────────────────────────────────

// bucketToMap converts a bucketMeta into the map stored by the ObjectStore.
func bucketToMap(b bucketMeta) map[string]any {
	data, _ := json.Marshal(b)
	var m map[string]any
	json.Unmarshal(data, &m)
	return m
}

// mapToBucket converts a stored bucket map back into a bucketMeta.
func mapToBucket(m map[string]any) bucketMeta {
	data, _ := json.Marshal(m)
	var b bucketMeta
	json.Unmarshal(data, &b)
	return b
}

// paginateBuckets applies pageToken/maxResults cursor pagination over a
// name-sorted bucket list.
func paginateBuckets(buckets []map[string]any, params map[string]any) ([]map[string]any, string) {
	sort.Slice(buckets, func(i, j int) bool {
		ni, _ := buckets[i]["name"].(string)
		nj, _ := buckets[j]["name"].(string)
		return ni < nj
	})
	limit := maxResults(params)
	start := 0
	if tok, _ := params["pageToken"].(string); tok != "" {
		if cursor := decodeCursor(tok); cursor != "" {
			for start < len(buckets) {
				n, _ := buckets[start]["name"].(string)
				if n > cursor {
					break
				}
				start++
			}
		}
	}
	end := start + limit
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

func (p *Provider) BucketsList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	buckets, err := p.objects.ListBuckets(ctx, nr.AccountID)
	if err != nil {
		return nil, err
	}
	page, nextToken := paginateBuckets(buckets, nr.Params)
	items := make([]any, 0, len(page))
	for _, m := range page {
		items = append(items, toBucketMap(nr, mapToBucket(m)))
	}
	resp := map[string]any{"kind": "storage#buckets", "items": items}
	if nextToken != "" {
		resp["nextPageToken"] = nextToken
	}
	return provider.OK(resp), nil
}

func (p *Provider) BucketsInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	name, _ := body["name"].(string)
	if name == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing bucket name", 400)
	}
	b := bucketMeta{Name: name}
	if loc, _ := body["location"].(string); loc != "" {
		b.Location = loc
	} else {
		b.Location = "US"
	}
	if sc, _ := body["storageClass"].(string); sc != "" {
		b.StorageClass = sc
	} else {
		b.StorageClass = "STANDARD"
	}
	b.Versioning = bodyMap(body, "versioning")
	b.RetentionPolicy = bodyMap(body, "retentionPolicy")
	b.Lifecycle = bodyMap(body, "lifecycle")
	b.Encryption = bodyMap(body, "encryption")
	b.Cors = bodySlice(body, "cors")
	b.SoftDeletePolicy = bodyMap(body, "softDeletePolicy")
	b.Labels = bodyStringMap(body, "labels")
	if h, ok := body["defaultEventBasedHold"].(bool); ok {
		b.DefaultEventBasedHold = h
	}
	b.TimeCreated = clock.Now().Format(time.RFC3339Nano)
	b.Updated = b.TimeCreated
	b.Metageneration = "1"

	if err := p.objects.CreateBucket(ctx, nr.AccountID, name, bucketToMap(b)); err != nil {
		if errors.Is(err, gcs.ErrAlreadyExists) {
			return nil, model.NewProviderError("Conflict", "bucket already exists", 409)
		}
		return nil, err
	}
	return provider.OK(toBucketMap(nr, b)), nil
}

func (p *Provider) BucketsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	name, _ := nr.Params["bucket"].(string)
	meta, err := p.objects.GetBucket(ctx, name)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	return provider.OK(toBucketMap(nr, mapToBucket(meta))), nil
}

// BucketsGetStorageLayout implements buckets.getStorageLayout. The emulator
// models no hierarchical namespace, so every bucket reports it disabled. This
// is the resource `gcloud storage cp` (and the GCS SDKs) reads before uploading,
// and its absence previously aborted those clients.
func (p *Provider) BucketsGetStorageLayout(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	name, _ := nr.Params["bucket"].(string)
	meta, err := p.objects.GetBucket(ctx, name)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	layout := map[string]any{
		"kind":                  "storage#storageLayout",
		"bucket":                name,
		"hierarchicalNamespace": map[string]any{"enabled": false},
	}
	if loc, _ := meta["location"].(string); loc != "" {
		layout["location"] = loc
	}
	return provider.OK(layout), nil
}

func (p *Provider) BucketsUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	name, _ := nr.Params["bucket"].(string)
	body, _ := nr.Params["body"].(map[string]any)
	pre := bucketPrecondition(nr)
	// The read-modify-write runs atomically in the store: the metageneration
	// precondition is validated against the bucket's current meta, then the
	// metageneration is bumped and the requested fields are applied, all under
	// the same lock/transaction — so a concurrent update can't be lost and a
	// stale precondition (checked against the same snapshot) can't slip through.
	updated, err := p.objects.UpdateBucketMetaAtomic(ctx, name, func(meta map[string]any) (map[string]any, error) {
		if !gcs.BucketMetagenerationMatches(meta, pre) {
			return nil, gcs.ErrPreconditionFailed
		}
		b := mapToBucket(meta)
		// Preserve timeCreated; update only fields present in the request body.
		if loc, _ := body["location"].(string); loc != "" {
			b.Location = loc
		}
		if sc, _ := body["storageClass"].(string); sc != "" {
			b.StorageClass = sc
		}
		if _, ok := body["versioning"]; ok {
			b.Versioning = bodyMap(body, "versioning")
		}
		if _, ok := body["retentionPolicy"]; ok {
			newRP := bodyMap(body, "retentionPolicy")
			oldRP, _ := meta["retentionPolicy"].(map[string]any)
			if locked, _ := oldRP["isLocked"].(bool); locked {
				// Once a bucket's retention policy is locked it can be
				// extended but never removed or shortened (GCS makes the lock
				// irreversible). Preserve the lock and its effectiveTime.
				if newRP == nil || retentionPeriodSeconds(newRP) < retentionPeriodSeconds(oldRP) {
					return nil, model.NewProviderError("InvalidRequest",
						"Bucket retention policy is locked and cannot be removed or shortened", 400)
				}
				newRP["isLocked"] = true
				if _, ok := newRP["effectiveTime"]; !ok {
					if et, ok := oldRP["effectiveTime"]; ok {
						newRP["effectiveTime"] = et
					}
				}
			}
			b.RetentionPolicy = newRP
		}
		if _, ok := body["lifecycle"]; ok {
			b.Lifecycle = bodyMap(body, "lifecycle")
		}
		if _, ok := body["encryption"]; ok {
			b.Encryption = bodyMap(body, "encryption")
		}
		if _, ok := body["cors"]; ok {
			b.Cors = bodySlice(body, "cors")
		}
		if _, ok := body["softDeletePolicy"]; ok {
			b.SoftDeletePolicy = bodyMap(body, "softDeletePolicy")
		}
		if _, ok := body["labels"]; ok {
			b.Labels = bodyStringMap(body, "labels")
		}
		if h, ok := body["defaultEventBasedHold"].(bool); ok {
			b.DefaultEventBasedHold = h
		}
		b.Metageneration = bumpMeta(gcs.BucketMetageneration(meta))
		b.Updated = clock.Now().Format(time.RFC3339Nano)
		next := bucketToMap(b)
		// projectId is store-owned (set by CreateBucket) and not part of
		// bucketMeta, so re-attach it after round-tripping through the struct —
		// otherwise the memory ObjectStore's project-scoped ListBuckets stops
		// returning a bucket once it has been updated.
		if pid, ok := meta["projectId"]; ok {
			next["projectId"] = pid
		}
		return next, nil
	})
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		if errors.Is(err, gcs.ErrPreconditionFailed) {
			return nil, model.NewProviderError("PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412)
		}
		return nil, err
	}
	return provider.OK(toBucketMap(nr, mapToBucket(updated))), nil
}

// BucketsLockRetentionPolicy implements buckets.lockRetentionPolicy. Locking is
// irreversible: the bucket's retentionPolicy.isLocked is set true (with an
// effectiveTime) and the metageneration bumped. The optional
// ifMetagenerationMatch is validated atomically with the mutation. A bucket
// with no retention policy, or an unknown bucket, is NotFound; an already
// locked policy is returned unchanged (locking is idempotent).
func (p *Provider) BucketsLockRetentionPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	name, _ := nr.Params["bucket"].(string)
	if name == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing bucket name", 400)
	}
	var pre *gcs.Precondition
	if v, ok := parseInt64Param(nr, "ifMetagenerationMatch"); ok {
		pre = &gcs.Precondition{MetagenerationMatch: &v}
	}
	updated, err := p.objects.UpdateBucketMetaAtomic(ctx, name, func(meta map[string]any) (map[string]any, error) {
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
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		if errors.Is(err, gcs.ErrPreconditionFailed) {
			return nil, model.NewProviderError("PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412)
		}
		return nil, err
	}
	return provider.OK(toBucketMap(nr, mapToBucket(updated))), nil
}

func (p *Provider) BucketsDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	name, _ := nr.Params["bucket"].(string)
	project := p.bucketProject(ctx, name)
	if err := p.objects.DeleteBucket(ctx, name); err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		if errors.Is(err, gcs.ErrBucketNotEmpty) {
			return nil, model.NewProviderError("bucketNotEmpty", "bucket is not empty", 409)
		}
		return nil, err
	}
	// Real GCS drops a bucket's notification configs with the bucket.
	if entries, err := p.listBucketNotifications(ctx, project, name); err == nil {
		for _, e := range entries {
			_ = p.resources.Delete(ctx, project, store.GlobalRegion, rtNotification, e.ID)
		}
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// ─── notification configs ──────────────────────────────────────────────────────

// NotificationsInsert implements storage.notifications.insert: POST
// /storage/v1/b/{bucket}/notificationConfigs.
func (p *Provider) NotificationsInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	bucket, _ := nr.Params["bucket"].(string)
	if bucket == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing bucket", 400)
	}
	if err := p.scopeToBucket(ctx, nr, bucket); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	if body == nil {
		return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
	}
	topic, _ := body["topic"].(string)
	if topic == "" {
		return nil, model.NewProviderError("InvalidArgument", "topic is required", 400)
	}
	if p.publisher != nil {
		exists, err := p.publisher.TopicExists(ctx, nr.AccountID, topic)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, model.NewProviderError("NotFound", "The topic does not exist.", 404)
		}
	}
	payloadFormat := firstString(body, "payload_format", "payloadFormat")
	if payloadFormat == "" {
		payloadFormat = "JSON_API_V1"
	}
	if payloadFormat != "JSON_API_V1" && payloadFormat != "NONE" {
		return nil, model.NewProviderError("InvalidArgument",
			"payload_format must be one of JSON_API_V1, NONE", 400)
	}
	eventTypes := firstStringSlice(body, "event_types", "eventTypes")
	for _, et := range eventTypes {
		if !validNotificationEventType(et) {
			return nil, model.NewProviderError("InvalidArgument",
				"invalid event type: "+et, 400)
		}
	}
	cfg := notificationConfig{
		Kind:             "storage#notification",
		Topic:            topic,
		PayloadFormat:    payloadFormat,
		EventTypes:       eventTypes,
		ObjectNamePrefix: firstString(body, "object_name_prefix", "objectNamePrefix"),
		CustomAttributes: firstStringMap(body, "custom_attributes", "customAttributes"),
		Etag:             "CAE=",
	}
	p.notifMu.Lock()
	defer p.notifMu.Unlock()
	id, err := p.nextNotificationID(ctx, nr.AccountID, bucket)
	if err != nil {
		return nil, err
	}
	cfg.ID = id
	cfg.SelfLink = baseURL(nr) + "/storage/v1/b/" + bucket + "/notificationConfigs/" + id
	data, _ := json.Marshal(cfg)
	if err := p.resources.Create(ctx, nr.AccountID, store.GlobalRegion,
		store.ResourceEntry{Type: rtNotification, ID: notificationKey(bucket, id), Data: data}); err != nil {
		return nil, err
	}
	// Real GCS increments the bucket metageneration when a notification config
	// is created or deleted.
	p.bumpBucketMetageneration(ctx, bucket)
	return provider.OK(notificationToMap(cfg)), nil
}

// validNotificationEventType reports whether et is a GCS notification event
// type (the documented set; OBJECT_INITIALIZE is zonal-bucket-only).
func validNotificationEventType(et string) bool {
	switch et {
	case "OBJECT_FINALIZE", "OBJECT_INITIALIZE", "OBJECT_METADATA_UPDATE",
		"OBJECT_DELETE", "OBJECT_ARCHIVE":
		return true
	default:
		return false
	}
}

// NotificationsList implements storage.notifications.list: GET
// /storage/v1/b/{bucket}/notificationConfigs.
func (p *Provider) NotificationsList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	bucket, _ := nr.Params["bucket"].(string)
	if bucket == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing bucket", 400)
	}
	if err := p.scopeToBucket(ctx, nr, bucket); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	items := make([]any, 0)
	entries, err := p.listBucketNotifications(ctx, nr.AccountID, bucket)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		var cfg notificationConfig
		if json.Unmarshal(e.Data, &cfg) == nil {
			items = append(items, notificationToMap(cfg))
		}
	}
	return provider.OK(map[string]any{"kind": "storage#notifications", "items": items}), nil
}

// NotificationsGet implements storage.notifications.get: GET
// /storage/v1/b/{bucket}/notificationConfigs/{notification}.
func (p *Provider) NotificationsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	bucket, id, err := p.notificationTarget(ctx, nr)
	if err != nil {
		return nil, err
	}
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtNotification, notificationKey(bucket, id))
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "notification not found", 404)
		}
		return nil, err
	}
	var cfg notificationConfig
	if err := json.Unmarshal(e.Data, &cfg); err != nil {
		return nil, err
	}
	return provider.OK(notificationToMap(cfg)), nil
}

// NotificationsDelete implements storage.notifications.delete: DELETE
// /storage/v1/b/{bucket}/notificationConfigs/{notification}.
func (p *Provider) NotificationsDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	bucket, id, err := p.notificationTarget(ctx, nr)
	if err != nil {
		return nil, err
	}
	if err := p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtNotification, notificationKey(bucket, id)); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "notification not found", 404)
		}
		return nil, err
	}
	p.bumpBucketMetageneration(ctx, bucket)
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// notificationTarget resolves and validates the bucket + notification ID for a
// notificationConfigs get/delete, scoping the request to the bucket's project.
func (p *Provider) notificationTarget(ctx context.Context, nr *model.NormalizedRequest) (bucket, id string, err error) {
	bucket, _ = nr.Params["bucket"].(string)
	id, _ = nr.Params["notification"].(string)
	if bucket == "" || id == "" {
		return "", "", model.NewProviderError("InvalidRequest", "missing bucket or notification id", 400)
	}
	if err := p.scopeToBucket(ctx, nr, bucket); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return "", "", model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return "", "", err
	}
	return bucket, id, nil
}

// nextNotificationID allocates the next notification ID for a bucket (the
// emulator's analogue of GCS's per-bucket increasing numeric IDs).
func (p *Provider) nextNotificationID(ctx context.Context, account, bucket string) (string, error) {
	entries, err := p.listBucketNotifications(ctx, account, bucket)
	if err != nil {
		return "", err
	}
	max := 0
	for _, e := range entries {
		if n, err := strconv.Atoi(strings.TrimPrefix(e.ID, bucket+"/")); err == nil && n > max {
			max = n
		}
	}
	return strconv.Itoa(max + 1), nil
}

// listBucketNotifications returns the notification entries for one bucket,
// scoped to the owning project.
func (p *Provider) listBucketNotifications(ctx context.Context, account, bucket string) ([]store.ResourceEntry, error) {
	entries, err := p.resources.List(ctx, account, store.GlobalRegion, rtNotification, "")
	if err != nil {
		return nil, err
	}
	prefix := bucket + "/"
	out := make([]store.ResourceEntry, 0, len(entries))
	for _, e := range entries {
		if strings.HasPrefix(e.ID, prefix) {
			out = append(out, e)
		}
	}
	return out, nil
}

// bumpBucketMetageneration increments a bucket's metageneration, as real GCS
// does when a notification config is created or deleted. Best-effort.
func (p *Provider) bumpBucketMetageneration(ctx context.Context, bucket string) {
	_, _ = p.objects.UpdateBucketMetaAtomic(ctx, bucket, func(meta map[string]any) (map[string]any, error) {
		meta["metageneration"] = bumpMeta(gcs.BucketMetageneration(meta))
		meta["updated"] = clock.Now().Format(time.RFC3339Nano)
		return meta, nil
	})
}

// notificationResourceName is the `notificationConfig` event-attribute value:
// projects/_/buckets/{bucket}/notificationConfigs/{id}.
func notificationResourceName(bucket, id string) string {
	return resource.ResourceID("")("gcs-notification", bucket+"/notificationConfigs/"+id)
}

// notificationKey is the generic-store entry ID for a bucket notification.
func notificationKey(bucket, id string) string { return bucket + "/" + id }

// publishObjectEvent fans an object event out to every notification config on
// the bucket whose eventTypes and objectNamePrefix match. eventTime is the time
// the event took place (defaults to now when zero); extra carries event-specific
// attributes such as overwroteGeneration/overwrittenByGeneration. Best-effort:
// publish errors are swallowed.
func (p *Provider) publishObjectEvent(ctx context.Context, account, bucket, object, eventType string, meta gcs.ObjectMeta, eventTime time.Time, extra map[string]string) {
	if p.publisher == nil {
		return
	}
	entries, err := p.listBucketNotifications(ctx, account, bucket)
	if err != nil {
		return
	}
	if eventTime.IsZero() {
		eventTime = clock.Now()
	}
	for _, e := range entries {
		var cfg notificationConfig
		if json.Unmarshal(e.Data, &cfg) != nil || cfg.Topic == "" {
			continue
		}
		if !notificationEventMatches(cfg.EventTypes, eventType) {
			continue
		}
		if cfg.ObjectNamePrefix != "" && !strings.HasPrefix(object, cfg.ObjectNamePrefix) {
			continue
		}
		format := cfg.PayloadFormat
		if format == "" {
			format = "JSON_API_V1"
		}
		// Custom attributes first so the reserved attributes always win.
		attrs := map[string]string{}
		for k, v := range cfg.CustomAttributes {
			attrs[k] = v
		}
		attrs["eventType"] = eventType
		attrs["payloadFormat"] = format
		attrs["bucketId"] = bucket
		attrs["objectId"] = object
		attrs["objectGeneration"] = meta.Generation
		attrs["notificationConfig"] = notificationResourceName(bucket, cfg.ID)
		attrs["eventTime"] = eventTime.UTC().Format(time.RFC3339Nano)
		for k, v := range extra {
			attrs[k] = v
		}
		var data []byte
		if format == "JSON_API_V1" {
			data = objectEventData(meta)
		}
		_, _ = p.publisher.PublishEvent(ctx, account, cfg.Topic, data, attrs)
	}
}

// notificationEventMatches reports whether an event type passes a config's
// eventTypes filter. Per the Discovery schema, an empty filter matches every
// event type.
func notificationEventMatches(types []string, eventType string) bool {
	if len(types) == 0 {
		return true
	}
	for _, t := range types {
		if t == eventType {
			return true
		}
	}
	return false
}

// objectEventData renders the JSON_API_V1 notification payload: the
// storage#object resource for the object that changed.
func objectEventData(m gcs.ObjectMeta) []byte {
	o := objectMeta{
		Kind:           "storage#object",
		ID:             m.Bucket + "/" + m.Name + "/" + m.Generation,
		Name:           m.Name,
		Bucket:         m.Bucket,
		Size:           strconv.FormatInt(m.Size, 10),
		ContentType:    m.ContentType,
		Md5Hash:        m.MD5Hash,
		Crc32c:         m.CRC32C,
		Etag:           "CAE=",
		Metadata:       m.Metadata,
		Generation:     m.Generation,
		Metageneration: m.Metageneration,
		StorageClass:   m.StorageClass,
	}
	if !m.TimeCreated.IsZero() {
		o.TimeCreated = m.TimeCreated.Format(time.RFC3339Nano)
		o.TimeFinalized = o.TimeCreated
	}
	if !m.Updated.IsZero() {
		o.Updated = m.Updated.Format(time.RFC3339Nano)
	}
	b, _ := json.Marshal(o)
	return b
}

// firstString returns the first non-empty string value among the given keys
// (accepting both the Discovery snake_case and the lowerCamelCase spelling).
func firstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

// firstStringSlice returns the first string-array value among the given keys.
func firstStringSlice(m map[string]any, keys ...string) []string {
	for _, k := range keys {
		items, ok := m[k].([]any)
		if !ok {
			continue
		}
		out := make([]string, 0, len(items))
		for _, it := range items {
			if s, ok := it.(string); ok {
				out = append(out, s)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return nil
}

// firstStringMap returns the first string→string map value among the given
// keys.
func firstStringMap(m map[string]any, keys ...string) map[string]string {
	for _, k := range keys {
		raw, ok := m[k].(map[string]any)
		if !ok {
			continue
		}
		out := make(map[string]string, len(raw))
		for kk, vv := range raw {
			if s, ok := vv.(string); ok {
				out[kk] = s
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return nil
}

// ─── objects ──────────────────────────────────────────────────────────────────

// scopeToBucket resolves the project that owns the given bucket and sets it as
// the request's account scope. Buckets/objects live in the dedicated store
// (keyed by globally-unique bucket name); IAM/ACL live in the generic store,
// scoped by the bucket's owning project.
func (p *Provider) scopeToBucket(ctx context.Context, nr *model.NormalizedRequest, bucket string) error {
	meta, err := p.objects.GetBucket(ctx, bucket)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return store.ErrNotFound
		}
		return err
	}
	if pid, _ := meta["projectId"].(string); pid != "" {
		nr.AccountID = pid
	}
	return nil
}

func (p *Provider) ObjectsList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	if bucket == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing bucket", 400)
	}
	versions, _ := nr.Params["versions"].(string)
	var objs []gcs.ObjectMeta
	var err error
	if versions == "true" {
		// ?versions=true lists every generation, including non-live ones.
		objs, err = p.objects.ListObjectVersions(ctx, bucket)
	} else {
		objs, err = p.objects.ListObjects(ctx, bucket)
		if err == nil {
			objs = p.filterLifecycle(ctx, bucket, objs)
		}
	}
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}

	pfx, _ := nr.Params["prefix"].(string)
	if err := requireDownscope(nr, downscope.List, bucket, pfx); err != nil {
		return nil, err
	}
	delim, _ := nr.Params["delimiter"].(string)
	// startOffset filters the listing to names lexicographically equal to or
	// after it (GCS objects.list). It composes with prefix/delimiter and is
	// applied before the pageToken cursor.
	startOffset, _ := nr.Params["startOffset"].(string)
	if versions == "true" {
		delim = "" // versions listing does not group prefixes
	}

	// With a delimiter, group objects into common prefixes (folders). The
	// combined item+prefix result set is paginated with the same cursor
	// semantics as the plain listing (GCS supports maxResults/pageToken
	// together with delimiter).
	if delim != "" {
		type listed struct {
			name string
			item map[string]any // non-nil for objects
		}
		all := make([]listed, 0)
		seen := map[string]bool{}
		for _, m := range objs {
			if !strings.HasPrefix(m.Name, pfx) {
				continue
			}
			// startOffset filters object names before common prefixes are
			// derived, so a prefix survives if any of its objects is in range.
			if startOffset != "" && m.Name < startOffset {
				continue
			}
			rest := m.Name[len(pfx):]
			if i := strings.Index(rest, delim); i >= 0 {
				common := pfx + rest[:i+len(delim)]
				if !seen[common] {
					seen[common] = true
					all = append(all, listed{name: common})
				}
				continue
			}
			o := fromStoreObject(nr, m)
			all = append(all, listed{name: m.Name, item: toMap(o)})
		}
		sort.Slice(all, func(i, j int) bool { return all[i].name < all[j].name })

		limit := maxResults(nr.Params)
		start := 0
		if tok, _ := nr.Params["pageToken"].(string); tok != "" {
			if cursor := decodeCursor(tok); cursor != "" {
				for start < len(all) && all[start].name <= cursor {
					start++
				}
			}
		}
		end := start + limit
		if end > len(all) {
			end = len(all)
		}
		page := all[start:end]
		next := ""
		if end < len(all) {
			next = encodeCursor(page[len(page)-1].name)
		}

		items := make([]any, 0)
		prefixes := make([]string, 0)
		for _, l := range page {
			if l.item != nil {
				items = append(items, l.item)
			} else {
				prefixes = append(prefixes, l.name)
			}
		}
		resp := map[string]any{"kind": "storage#objects", "items": items}
		if len(prefixes) > 0 {
			resp["prefixes"] = prefixes
		}
		if next != "" {
			resp["nextPageToken"] = next
		}
		return provider.OK(resp), nil
	}

	// No delimiter: prefix filter + startOffset + pagination.
	var filtered []gcs.ObjectMeta
	for _, m := range objs {
		if pfx == "" || strings.HasPrefix(m.Name, pfx) {
			filtered = append(filtered, m)
		}
	}
	if startOffset != "" {
		kept := 0
		for _, m := range filtered {
			if m.Name >= startOffset {
				filtered[kept] = m
				kept++
			}
		}
		filtered = filtered[:kept]
	}

	// Cursor pagination over object names. The versions listing is ordered by
	// name ascending, then generation descending, so its cursor must also carry
	// the generation: a page boundary landing in the middle of a name's
	// generations would otherwise skip every remaining generation of that name.
	limit := maxResults(nr.Params)
	start := 0
	if tok, _ := nr.Params["pageToken"].(string); tok != "" {
		if versions == "true" {
			if cur, ok := decodeVersionCursor(tok); ok {
				for start < len(filtered) {
					m := filtered[start]
					if m.Name < cur.Name {
						start++
						continue
					}
					if m.Name == cur.Name {
						mg, _ := strconv.ParseInt(m.Generation, 10, 64)
						cg, _ := strconv.ParseInt(cur.Generation, 10, 64)
						if mg >= cg {
							start++
							continue
						}
					}
					break
				}
			}
		} else if cursor := decodeCursor(tok); cursor != "" {
			for start < len(filtered) && filtered[start].Name <= cursor {
				start++
			}
		}
	}
	end := start + limit
	if end > len(filtered) {
		end = len(filtered)
	}
	page := filtered[start:end]
	next := ""
	if end < len(filtered) {
		if versions == "true" {
			last := page[len(page)-1]
			next = encodeVersionCursor(last.Name, last.Generation)
		} else {
			next = encodeCursor(page[len(page)-1].Name)
		}
	}

	items := make([]any, 0, len(page))
	for _, m := range page {
		items = append(items, toMap(fromStoreObject(nr, m)))
	}
	resp := map[string]any{"kind": "storage#objects", "items": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) ObjectsInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if signed, _ := nr.Params[wire.SignedURLKey].(bool); signed {
		if perr := p.validateSignedURL(nr); perr != nil {
			return nil, perr
		}
	}
	bucket, _ := nr.Params["bucket"].(string)
	if bucket == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing bucket", 400)
	}
	if err := p.scopeToBucket(ctx, nr, bucket); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}

	object, _ := nr.Params["object"].(string)
	if body, ok := nr.Params["body"].(map[string]any); ok && object == "" {
		if n, _ := body["name"].(string); n != "" {
			object = n
		}
	}
	if object == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing object name", 400)
	}
	if err := requireDownscope(nr, downscope.WriteObject, bucket, object); err != nil {
		return nil, err
	}

	media, _ := nr.Params[wire.MediaKey].([]byte)
	contentType, _ := nr.Params[wire.ContentTypeKey].(string)
	body, _ := nr.Params["body"].(map[string]any)
	if contentType == "" && body != nil {
		contentType, _ = body["contentType"].(string)
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	now := clock.Now()
	generation := p.nextGen()

	// Versioning + retention + holds are all driven by bucket config and the
	// request body (GCP-native: bucket versioning.enabled, bucket
	// retentionPolicy.retentionPeriod, object temporaryHold/eventBasedHold).
	var versioned bool
	var retention *objectRetention
	bmeta, _ := p.objects.GetBucket(ctx, bucket)
	if v, ok := bmeta["versioning"].(map[string]any); ok {
		if en, _ := v["enabled"].(bool); en {
			versioned = true
		}
	}
	if rp, ok := bmeta["retentionPolicy"].(map[string]any); ok {
		if period, _ := rp["retentionPeriod"].(string); period != "" {
			if d := parseRetentionPeriod(period); d > 0 {
				retention = &objectRetention{
					RetainUntilTime: now.Add(d).Format(time.RFC3339Nano),
					Mode:            "Unlocked",
				}
			}
		}
	}

	o := objectMeta{
		Kind:           "storage#object",
		Name:           object,
		Bucket:         bucket,
		ContentType:    contentType,
		Generation:     generation,
		Metageneration: "1",
		StorageClass:   "STANDARD",
		TimeCreated:    now.Format(time.RFC3339Nano),
		Updated:        now.Format(time.RFC3339Nano),
		Retention:      retention,
	}
	if retention != nil {
		o.RetentionExpirationTime = retention.RetainUntilTime
	}
	o.Metadata = uploadMetadata(nr.Params)
	// GCS applies the bucket's defaultEventBasedHold to a new object unless the
	// insert explicitly sets eventBasedHold (explicit false overrides).
	applyObjectHolds(body, bmeta, &o)
	o.ID = bucket + "/" + object + "/" + o.Generation
	o.Etag = "CAE="
	base := baseURL(nr)
	o.SelfLink = objectSelfLink(base, bucket, object)
	o.MediaLink = objectMediaLink(base, bucket, object)

	// Capture the prior live generation's blob key so a non-versioned overwrite
	// can clean it up after the new generation is durably stored.
	var priorBlobKey string
	if !versioned {
		if prev, err := p.objects.GetObjectMeta(ctx, bucket, object); err == nil && prev.Generation != generation {
			priorBlobKey = blobKey(bucket, object, prev.Generation)
		}
	}

	// Read the raw object bytes. Envelope encryption is applied whole-object
	// with AES-GCM, so the body is buffered; TODO(streaming-AEAD): stream very
	// large objects through a streaming AEAD (e.g. Tink StreamingAead) rather
	// than buffering the full ciphertext in memory.
	var raw []byte
	if stream, ok := nr.Params[wire.StreamKey].(io.Reader); ok {
		b, rerr := io.ReadAll(stream)
		if rerr != nil {
			return nil, rerr
		}
		raw = b
	} else {
		raw = media
	}

	final, err := p.writeObjectRaw(ctx, nr, bucket, object, o, raw, versioned, priorBlobKey, true)
	if err != nil {
		return nil, err
	}
	return provider.OK(toMap(final)), nil
}

// writeObjectRaw computes checksums over the plaintext, envelope-encrypts it,
// and persists a new object generation (metadata + blob). md5Enabled controls
// whether an MD5 checksum is computed (compose leaves MD5 empty). It returns the
// finalized objectMeta with checksums/size populated.
func (p *Provider) writeObjectRaw(ctx context.Context, nr *model.NormalizedRequest, bucket, object string, o objectMeta, raw []byte, versioned bool, priorBlobKey string, md5Enabled bool) (objectMeta, error) {
	bmeta, _ := p.objects.GetBucket(ctx, bucket)
	kmsKeyName, cseKey, cseKeySHA256, err := p.resolveWriteKey(nr, bucket, bmeta)
	if err != nil {
		return o, err
	}
	finalMeta, err := p.PutObjectData(ctx, nr.AccountID, toStoreObject(o), raw, versioned, priorBlobKey, md5Enabled, kmsKeyName, cseKey, cseKeySHA256, objectPrecondition(nr))
	if err != nil {
		if errors.Is(err, gcs.ErrPreconditionFailed) {
			return o, model.NewProviderError("PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412)
		}
		return o, err
	}
	return fromStoreObject(nr, finalMeta), nil
}

// PutObjectData writes an object's plaintext bytes through the shared envelope-
// encryption + blob-store path and persists the object metadata. It is the
// transport-agnostic core shared by the REST ObjectsInsert and the gRPC
// WriteObject, so both transports stay byte-compatible. meta carries the new
// generation and metadata; checksums/size are computed here and stamped onto
// the returned metadata. project is the owning project (account scope) used for
// envelope DEK wrapping.
// precondition is GCS's ifGenerationMatch/ifGenerationNotMatch/
// ifMetagenerationMatch/ifMetagenerationNotMatch, checked atomically with the
// metadata write (see gcs.ObjectStore's *Checked methods) — nil for a caller
// (currently: the gRPC Storage service) that doesn't yet parse one.
func (p *Provider) PutObjectData(ctx context.Context, project string, meta gcs.ObjectMeta, raw []byte, versioned bool, priorBlobKey string, md5Enabled bool, kmsKeyName string, cseKey []byte, cseKeySHA256 string, precondition *gcs.Precondition) (gcs.ObjectMeta, error) {
	// A write always re-encrypts, so encryption metadata inherited from a
	// source object (copy/rewrite/move) or a prior generation must not leak onto
	// the new object. The key branches below re-populate exactly the metadata
	// for the chosen encryption (CSEK/CMEK/server-DEK).
	meta.CSEKeySHA256 = ""
	meta.KmsKeyName = ""
	meta.WrappedDEK = nil

	// Capture the prior live generation before writing any bytes: it is needed
	// both to reject an overwrite of a held/retention-protected object without
	// leaving an orphaned blob, and to emit the replacement events below.
	var prevMeta *gcs.ObjectMeta
	if existing, gerr := p.objects.GetObjectMeta(ctx, meta.Bucket, meta.Name); gerr == nil && existing.Generation != meta.Generation {
		if perr := objectWriteBlockedError(existing); perr != nil {
			return meta, perr
		}
		prev := existing
		prevMeta = &prev
	}
	// Plaintext checksums/size (GCS reports the logical object, not the
	// ciphertext).
	if md5Enabled {
		sum := md5.Sum(raw)
		meta.MD5Hash = base64.StdEncoding.EncodeToString(sum[:])
	}
	crc := crc32.Checksum(raw, crc32.MakeTable(crc32.Castagnoli))
	crcBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(crcBytes, crc)
	meta.CRC32C = base64.StdEncoding.EncodeToString(crcBytes)
	meta.Size = int64(len(raw))

	// Encrypt and store the ciphertext blob.
	id := blobKey(meta.Bucket, meta.Name, meta.Generation)
	var wrappedDEK []byte
	if cseKey != nil {
		ciphertext, err := kmsstore.EncryptData(cseKey, raw, nil)
		if err != nil {
			return meta, err
		}
		if err := p.blobs.Put(ctx, blobsNamespace, id, ciphertext); err != nil {
			return meta, err
		}
		meta.CSEKeySHA256 = cseKeySHA256
	} else {
		rawDEK, wd, err := p.encryptor.Wrap(ctx, project, kmsKeyName)
		if err != nil {
			return meta, err
		}
		wrappedDEK = wd
		ciphertext, err := kmsstore.EncryptData(rawDEK, raw, nil)
		if err != nil {
			return meta, err
		}
		if err := p.blobs.Put(ctx, blobsNamespace, id, ciphertext); err != nil {
			return meta, err
		}
		meta.KmsKeyName = kmsKeyName
	}

	meta.WrappedDEK = wrappedDEK
	var err error
	if versioned {
		err = p.objects.PutObjectGenerationChecked(ctx, meta.Bucket, meta.Name, meta, precondition)
	} else {
		err = p.objects.PutObjectMetaChecked(ctx, meta.Bucket, meta.Name, meta, precondition)
	}
	if err != nil {
		// Roll back the just-written blob so a failed metadata write does not
		// leave an orphaned object (metadata absent, blob present).
		_ = p.blobs.Delete(ctx, blobsNamespace, id)
		return meta, err
	}
	if priorBlobKey != "" {
		_ = p.blobs.Delete(ctx, blobsNamespace, priorBlobKey)
	}
	// Publish the replacement events and the OBJECT_FINALIZE for the new
	// generation. Best-effort: a delivery failure must not fail the upload.
	finalizeAttrs := map[string]string{}
	if prevMeta != nil {
		finalizeAttrs["overwroteGeneration"] = prevMeta.Generation
		replacedEvent := "OBJECT_DELETE"
		if versioned {
			replacedEvent = "OBJECT_ARCHIVE"
		}
		p.publishObjectEvent(ctx, project, meta.Bucket, meta.Name, replacedEvent, *prevMeta, prevMeta.Updated,
			map[string]string{"overwrittenByGeneration": meta.Generation})
	}
	p.publishObjectEvent(ctx, project, meta.Bucket, meta.Name, "OBJECT_FINALIZE", meta, meta.TimeCreated, finalizeAttrs)
	return meta, nil
}

func (p *Provider) ObjectsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.objectResponse(ctx, nr)
}

// validateSignedURL enforces V4 signed-URL parameter format and expiry. It
// deliberately does not verify the cryptographic signature: the emulator
// accepts any well-formed, unexpired signed URL (consistent with jaiscloud's
// auth-bypass model, and the only approach compatible with SDK-signed URLs
// whose key material never reaches the emulator).
func (p *Provider) validateSignedURL(nr *model.NormalizedRequest) *model.ProviderError {
	expiresStr, _ := nr.Params["X-Goog-Expires"].(string)
	expires, err := strconv.ParseInt(expiresStr, 10, 64)
	if err != nil || expires <= 0 || expires > 604800 {
		return malformedSecurityHeader("Expires")
	}
	dateStr, _ := nr.Params["X-Goog-Date"].(string)
	date, err := time.Parse("20060102T150405Z", dateStr)
	if err != nil {
		return malformedSecurityHeader("Date")
	}
	if date.Add(time.Duration(expires) * time.Second).Before(clock.Now()) {
		return model.NewProviderError("ExpiredToken", "The provided token has expired.", 400).
			WithData(map[string]any{"errorFormat": "xml"})
	}
	return nil
}

// malformedSecurityHeader builds the XML-API 400 error for a malformed signed
// URL parameter (test-derived code/message shape).
func malformedSecurityHeader(param string) *model.ProviderError {
	return model.NewProviderError("MalformedSecurityHeader", "Your request has a malformed header.", 400).
		WithData(map[string]any{"errorFormat": "xml", "parameterName": param})
}

func (p *Provider) ObjectsGetMedia(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if signed, _ := nr.Params[wire.SignedURLKey].(bool); signed {
		if perr := p.validateSignedURL(nr); perr != nil {
			return nil, perr
		}
	}
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	if err := requireDownscope(nr, downscope.ReadObject, bucket, object); err != nil {
		return nil, err
	}
	// Metadata first: metadata gone → 404; metadata present + blob absent → 404
	// (a tombstoned/deleted version whose data was dropped, not corruption).
	meta, err := p.getObjectForRead(ctx, bucket, object, nr.Params)
	if err != nil {
		return nil, err
	}
	if resp, perr := checkReadPreconditions(meta, objectPrecondition(nr)); resp != nil || perr != nil {
		return resp, perr
	}
	id := blobKey(bucket, object, meta.Generation)
	rc, err := p.blobs.GetStream(ctx, blobsNamespace, id, 0, -1)
	if err != nil {
		return nil, model.NewProviderError("NotFound", "object not found", 404)
	}
	// Decrypt the ciphertext. Buffered AES-GCM (see TODO(streaming-AEAD) in
	// ObjectsInsert); a CSEK-encrypted object requires the matching key header.
	ciphertext, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return nil, err
	}
	plain, err := p.decryptObject(ctx, nr, meta, ciphertext)
	if err != nil {
		return nil, err
	}
	status := 200
	headers := mediaHeaders(meta)
	body := plain
	// Honor HTTP Range requests (the gcs-connector uses them to fetch object
	// footers and read large objects in chunks); otherwise the gcs-connector's
	// GoogleCloudStorageReadChannel NPEs on a 200-with-full-body response.
	if nr.Raw != nil {
		if rng := nr.Raw.Header.Get("Range"); rng != "" {
			total := int64(len(plain))
			start, end, res := parseByteRange(rng, total)
			switch res {
			case rangeOK:
				body = plain[start : end+1]
				status = http.StatusPartialContent
				headers["Content-Range"] = fmt.Sprintf("bytes %d-%d/%d", start, end, total)
				headers["Content-Length"] = strconv.Itoa(len(body))
			case rangeUnsatisfiable:
				// A syntactically valid but unsatisfiable range (start at or past
				// the end, or a zero-length suffix) is 416 Range Not Satisfiable
				// with the unsatisfied-range form of Content-Range. A malformed
				// Range falls through to the full 200 body (RFC 7233).
				headers["Content-Range"] = fmt.Sprintf("bytes */%d", total)
				headers["Content-Length"] = "0"
				return &model.ProviderResponse{
					HTTPStatus: http.StatusRequestedRangeNotSatisfiable,
					Data: map[string]any{
						"_stream":           io.NopCloser(bytes.NewReader(nil)),
						wire.ContentTypeKey: meta.ContentType,
						wire.HeadersKey:     headers,
					},
				}, nil
			}
		}
	}
	return &model.ProviderResponse{
		HTTPStatus: status,
		Data: map[string]any{
			"_stream":           io.NopCloser(bytes.NewReader(body)),
			wire.ContentTypeKey: meta.ContentType,
			wire.HeadersKey:     headers,
		},
	}, nil
}

// rangeParse is the outcome of parsing an HTTP Range header.
type rangeParse int

const (
	// rangeOK: a satisfiable range; start/end are the clamped inclusive bounds.
	rangeOK rangeParse = iota
	// rangeMalformed: the header is not a usable bytes range. Per RFC 7233 it is
	// ignored, yielding a full 200 response.
	rangeMalformed
	// rangeUnsatisfiable: a syntactically valid bytes range that cannot be
	// satisfied against the representation, e.g. a start offset at or past the
	// end, a zero-length suffix, or any range against an empty representation.
	// The response is 416.
	rangeUnsatisfiable
)

// parseByteRange parses an HTTP Range header ("bytes=start-end", "bytes=start-",
// or "bytes=-suffix") against a total length and returns the inclusive byte
// bounds plus the outcome. Satisfiable ranges are clamped to the object bounds.
func parseByteRange(rng string, total int64) (start, end int64, res rangeParse) {
	const prefix = "bytes="
	if !strings.HasPrefix(rng, prefix) {
		return 0, 0, rangeMalformed
	}
	spec := strings.TrimPrefix(rng, prefix)
	i := strings.IndexByte(spec, '-')
	if i < 0 {
		return 0, 0, rangeMalformed
	}
	startStr := spec[:i]
	endStr := spec[i+1:]
	switch {
	case startStr == "" && endStr == "": // bytes= — malformed
		return 0, 0, rangeMalformed
	case startStr == "": // bytes=-suffix
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n < 0 {
			return 0, 0, rangeMalformed
		}
		if n == 0 || total <= 0 {
			return 0, 0, rangeUnsatisfiable
		}
		start = total - n
		if start < 0 {
			start = 0
		}
		return start, total - 1, rangeOK
	default: // bytes=start-end or bytes=start-
		s, err := strconv.ParseInt(startStr, 10, 64)
		if err != nil || s < 0 {
			return 0, 0, rangeMalformed
		}
		if s >= total {
			return 0, 0, rangeUnsatisfiable
		}
		start = s
		if endStr == "" {
			return start, total - 1, rangeOK
		}
		e, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || e < start {
			return 0, 0, rangeMalformed
		}
		if e >= total {
			e = total - 1
		}
		return start, e, rangeOK
	}
}

// mediaHeaders builds the GCS media-download response headers for an object so
// the Go SDK's Reader.Attrs (and reader.Metadata()) populate: x-goog-hash,
// x-goog-generation, x-goog-metageneration, x-goog-stored-content-length, and
// one x-goog-meta-<key> header per custom metadata entry.
func mediaHeaders(m gcs.ObjectMeta) map[string]string {
	h := map[string]string{
		"x-goog-generation":            m.Generation,
		"x-goog-metageneration":        m.Metageneration,
		"x-goog-stored-content-length": strconv.FormatInt(m.Size, 10),
	}
	if m.StorageClass != "" {
		h["x-goog-storage-class"] = m.StorageClass
	}
	// Real GCS media downloads carry an ETag header; the emulator uses the same
	// constant object etag as the JSON metadata ("CAE=").
	h["ETag"] = "CAE="
	var hashes []string
	if m.CRC32C != "" {
		hashes = append(hashes, "crc32c="+m.CRC32C)
	}
	if m.MD5Hash != "" {
		hashes = append(hashes, "md5="+m.MD5Hash)
	}
	if len(hashes) > 0 {
		h["x-goog-hash"] = strings.Join(hashes, ",")
	}
	// The XML API surfaces a CSEK object's algorithm and key hash on reads so
	// clients can identify which key is required (the JSON API returns the same
	// data as customerEncryption on the object metadata).
	if m.CSEKeySHA256 != "" {
		h["x-goog-encryption-algorithm"] = "AES256"
		h["x-goog-encryption-key-sha256"] = m.CSEKeySHA256
	}
	for k, v := range m.Metadata {
		h["x-goog-meta-"+canonicalMetaKey(k)] = v
	}
	return h
}

// canonicalMetaKey canonicalises a metadata key the way net/http canonicalises
// header names: lowercased key with the first rune title-cased
// (e.g. "originalname" → "Originalname").
func canonicalMetaKey(key string) string {
	k := strings.ToLower(key)
	if k == "" {
		return k
	}
	r := []rune(k)
	r[0] = unicode.ToUpper(r[0])
	return string(r)
}

// uploadMetadata merges custom object metadata from the request body's
// "metadata" map and the x-goog-meta-* request headers (wire.MetaHeadersKey).
func uploadMetadata(params map[string]any) map[string]string {
	out := bodyMetadata(params)
	hdr, _ := params[wire.MetaHeadersKey].(map[string]string)
	if len(hdr) == 0 {
		return out
	}
	if out == nil {
		out = make(map[string]string, len(hdr))
	}
	for k, v := range hdr {
		out[k] = v
	}
	return out
}

// bucketVersioned reports whether the bucket has versioning.enabled.
func (p *Provider) bucketVersioned(ctx context.Context, bucket string) bool {
	bmeta, _ := p.objects.GetBucket(ctx, bucket)
	if v, ok := bmeta["versioning"].(map[string]any); ok {
		if en, _ := v["enabled"].(bool); en {
			return true
		}
	}
	return false
}

// BucketVersioned reports whether the bucket has versioning enabled. Exported
// for the gRPC Storage service.
func (p *Provider) BucketVersioned(ctx context.Context, bucket string) bool {
	return p.bucketVersioned(ctx, bucket)
}

// GetBucketCORSRules returns the stored CORS rules for a bucket, in the shape
// the gateway CORS interceptor expects. It is wired into the gateway via
// WithGCSCORSLookup and is deliberately context-free (the gateway calls it
// outside a request context). Nil when the bucket or its cors config is absent.
func (p *Provider) GetBucketCORSRules(bucket string) []map[string]any {
	meta, err := p.objects.GetBucket(context.Background(), bucket)
	if err != nil {
		return nil
	}
	switch v := meta["cors"].(type) {
	case []map[string]any:
		return v
	case []any:
		out := make([]map[string]any, 0, len(v))
		for _, r := range v {
			if m, ok := r.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

// readSourceRaw reads and decrypts an object's plaintext bytes.
func (p *Provider) readSourceRaw(ctx context.Context, nr *model.NormalizedRequest, bucket, object string, params map[string]any) (gcs.ObjectMeta, []byte, error) {
	meta, err := p.getObjectForRead(ctx, bucket, object, params)
	if err != nil {
		return gcs.ObjectMeta{}, nil, err
	}
	plain, err := p.readDecrypt(ctx, nr.AccountID, bucket, object, meta, params)
	if err != nil {
		return gcs.ObjectMeta{}, nil, err
	}
	return meta, plain, nil
}

// GetObjectData reads and decrypts an object's plaintext bytes, returning its
// metadata and the full plaintext. generation selects a specific revision when
// non-empty (empty = live); cseKey is required when the object is CSEK-
// encrypted. Shared by the REST read path and the gRPC ReadObject.
func (p *Provider) GetObjectData(ctx context.Context, project, bucket, object, generation string, cseKey []byte) (gcs.ObjectMeta, []byte, error) {
	params := map[string]any{}
	if generation != "" {
		params["generation"] = generation
	}
	meta, err := p.getObjectForRead(ctx, bucket, object, params)
	if err != nil {
		return gcs.ObjectMeta{}, nil, err
	}
	id := blobKey(bucket, object, meta.Generation)
	rc, err := p.blobs.GetStream(ctx, blobsNamespace, id, 0, -1)
	if err != nil {
		return gcs.ObjectMeta{}, nil, model.NewProviderError("NotFound", "object not found", 404)
	}
	ciphertext, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return gcs.ObjectMeta{}, nil, err
	}
	plain, err := p.decryptObjectWithKey(ctx, project, meta, ciphertext, cseKey)
	if err != nil {
		return gcs.ObjectMeta{}, nil, err
	}
	return meta, plain, nil
}

// readDecrypt reads the ciphertext blob and decrypts it to plaintext, resolving
// the CSEK key from params (empty when the object is server-DEK/CMEK encrypted).
func (p *Provider) readDecrypt(ctx context.Context, project, bucket, object string, meta gcs.ObjectMeta, params map[string]any) ([]byte, error) {
	id := blobKey(bucket, object, meta.Generation)
	rc, err := p.blobs.GetStream(ctx, blobsNamespace, id, 0, -1)
	if err != nil {
		return nil, model.NewProviderError("NotFound", "object not found", 404)
	}
	ciphertext, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return nil, err
	}
	return p.decryptObject(ctx, &model.NormalizedRequest{AccountID: project, Params: params}, meta, ciphertext)
}

// ObjectsRewrite implements objects.rewrite (the GCS JSON API copy action used
// by the Go SDK's Object.CopierFrom). It copies the source object's bytes and
// metadata to the destination under a new generation, returning the
// storage#rewriteResponse envelope.
func (p *Provider) ObjectsRewrite(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	final, err := p.copyObject(ctx, nr)
	if err != nil {
		return nil, err
	}
	resp := map[string]any{
		"kind":                "storage#rewriteResponse",
		"totalBytesRewritten": final.Size,
		"objectSize":          final.Size,
		"done":                true,
		"resource":            toMap(final),
	}
	return provider.OK(resp), nil
}

// ObjectsCopy implements objects.copyTo: same copy as rewrite but returning the
// destination storage#object directly (the GCS JSON API objects.copy action,
// used by the Python SDK's Blob.copy_blob and the localgcp copy test).
func (p *Provider) ObjectsCopy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	final, err := p.copyObject(ctx, nr)
	if err != nil {
		return nil, err
	}
	return provider.OK(toMap(final)), nil
}

// copyObject performs the shared copy/rewrite work: copy the source object's
// bytes and metadata to the destination under a fresh generation, applying the
// request body's writable overrides.
func (p *Provider) copyObject(ctx context.Context, nr *model.NormalizedRequest) (objectMeta, error) {
	srcBucket, _ := nr.Params["sourceBucket"].(string)
	srcObject, _ := nr.Params["sourceObject"].(string)
	dstBucket, _ := nr.Params["destinationBucket"].(string)
	dstObject, _ := nr.Params["destinationObject"].(string)
	body, _ := nr.Params["body"].(map[string]any)
	if dstObject == "" && body != nil {
		dstObject, _ = body["name"].(string)
	}
	if srcBucket == "" || srcObject == "" || dstBucket == "" || dstObject == "" {
		return objectMeta{}, model.NewProviderError("InvalidRequest", "copy requires source and destination object names", 400)
	}
	if err := requireDownscope(nr, downscope.ReadObject, srcBucket, srcObject); err != nil {
		return objectMeta{}, err
	}
	if err := requireDownscope(nr, downscope.WriteObject, dstBucket, dstObject); err != nil {
		return objectMeta{}, err
	}
	if err := p.scopeToBucket(ctx, nr, dstBucket); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return objectMeta{}, model.NewProviderError("NotFound", "destination bucket not found", 404)
		}
		return objectMeta{}, err
	}

	srcParams := copySourceCSEKParams(nr)
	if g, _ := nr.Params["sourceGeneration"].(string); g != "" {
		srcParams["generation"] = g
	}
	srcMeta, raw, err := p.readSourceRaw(ctx, nr, srcBucket, srcObject, srcParams)
	if err != nil {
		return objectMeta{}, err
	}
	if !objectMetaPreconditionMatches(srcMeta, sourcePrecondition(nr)) {
		return objectMeta{}, model.NewProviderError("PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412)
	}

	// Destination metadata starts as a copy of the source, overridden by the
	// request body's writable fields, then stamped with a fresh generation.
	now := clock.Now()
	o := fromStoreObject(nr, srcMeta)
	o.Name = dstObject
	o.Bucket = dstBucket
	o.Generation = p.nextGen()
	o.Metageneration = "1"
	o.TimeCreated = now.Format(time.RFC3339Nano)
	o.Updated = o.TimeCreated
	o.Retention = nil
	o.RetentionExpirationTime = ""
	if body != nil {
		if ct, _ := body["contentType"].(string); ct != "" {
			o.ContentType = ct
		}
		if _, ok := body["metadata"]; ok {
			o.Metadata = bodyMetadata(nr.Params)
		}
		if sc, _ := body["storageClass"].(string); sc != "" {
			o.StorageClass = sc
		}
	}
	// The destination inherits the destination bucket's defaultEventBasedHold
	// unless the copy body overrides it; an explicitly requested hold wins over
	// the source object's hold state.
	bmeta, _ := p.objects.GetBucket(ctx, dstBucket)
	applyObjectHolds(body, bmeta, &o)
	o.ID = dstBucket + "/" + dstObject + "/" + o.Generation
	o.Etag = "CAE="
	base := baseURL(nr)
	o.SelfLink = objectSelfLink(base, dstBucket, dstObject)
	o.MediaLink = objectMediaLink(base, dstBucket, dstObject)

	final, err := p.writeObjectRaw(ctx, nr, dstBucket, dstObject, o, raw, p.bucketVersioned(ctx, dstBucket), "", true)
	if err != nil {
		return objectMeta{}, err
	}
	return final, nil
}

// sourcePrecondition parses the ifSourceGenerationMatch/NotMatch and
// ifSourceMetagenerationMatch/NotMatch query params used by objects.move to
// guard the source object. Nil when none is present.
func sourcePrecondition(nr *model.NormalizedRequest) *gcs.Precondition {
	var pre gcs.Precondition
	set := false
	if v, ok := parseInt64Param(nr, "ifSourceGenerationMatch"); ok {
		pre.GenerationMatch = &v
		set = true
	}
	if v, ok := parseInt64Param(nr, "ifSourceGenerationNotMatch"); ok {
		pre.GenerationNotMatch = &v
		set = true
	}
	if v, ok := parseInt64Param(nr, "ifSourceMetagenerationMatch"); ok {
		pre.MetagenerationMatch = &v
		set = true
	}
	if v, ok := parseInt64Param(nr, "ifSourceMetagenerationNotMatch"); ok {
		pre.MetagenerationNotMatch = &v
		set = true
	}
	if !set {
		return nil
	}
	return &pre
}

// objectMetaPreconditionMatches reports whether p is satisfied by the given
// (live) object metadata. A nil p always matches. Mirrors the store's
// unexported objectPreconditionMatches for callers holding an already-read
// ObjectMeta, so it must only be called with a resolved (existing) object —
// the missing-object rules (e.g. ifGenerationNotMatch failing on absence) are
// enforced by the store's *Checked write methods, not here.
func objectMetaPreconditionMatches(m gcs.ObjectMeta, p *gcs.Precondition) bool {
	if p == nil {
		return true
	}
	gen, _ := strconv.ParseInt(m.Generation, 10, 64)
	metagen, _ := strconv.ParseInt(m.Metageneration, 10, 64)
	if p.GenerationMatch != nil && gen != *p.GenerationMatch {
		return false
	}
	if p.GenerationNotMatch != nil && gen == *p.GenerationNotMatch {
		return false
	}
	if p.MetagenerationMatch != nil && metagen != *p.MetagenerationMatch {
		return false
	}
	if p.MetagenerationNotMatch != nil && metagen == *p.MetagenerationNotMatch {
		return false
	}
	return true
}

// checkReadPreconditions evaluates the GCS read (GET) preconditions on an
// already-resolved object: a generation/metageneration *match* mismatch is 412
// Precondition Failed, while a generation/metageneration *not-match* match is
// 304 Not Modified (the conditional-download idiom). Returns (nil, nil) when
// the read may proceed, a 304 ProviderResponse when not-modified, or a 412
// ProviderError otherwise.
func checkReadPreconditions(m gcs.ObjectMeta, pre *gcs.Precondition) (*model.ProviderResponse, error) {
	if pre == nil {
		return nil, nil
	}
	gen, _ := strconv.ParseInt(m.Generation, 10, 64)
	metagen, _ := strconv.ParseInt(m.Metageneration, 10, 64)
	if (pre.GenerationMatch != nil && gen != *pre.GenerationMatch) ||
		(pre.MetagenerationMatch != nil && metagen != *pre.MetagenerationMatch) {
		return nil, model.NewProviderError("PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412)
	}
	if (pre.GenerationNotMatch != nil && gen == *pre.GenerationNotMatch) ||
		(pre.MetagenerationNotMatch != nil && metagen == *pre.MetagenerationNotMatch) {
		return &model.ProviderResponse{HTTPStatus: http.StatusNotModified, Data: map[string]any{}}, nil
	}
	return nil, nil
}

// ObjectsMove implements objects.move. It copies the source object's bytes and
// metadata to the destination under a fresh generation, then deletes the source
// — a copy-then-delete, matching the gRPC MoveObject semantics. The destination
// write goes first so a failure never loses the source's bytes. Destination
// preconditions (ifGenerationMatch/...) are enforced atomically by the write;
// source preconditions (ifSourceGenerationMatch/...) are validated against the
// source metadata before the copy.
func (p *Provider) ObjectsMove(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	srcBucket, _ := nr.Params["sourceBucket"].(string)
	srcObject, _ := nr.Params["sourceObject"].(string)
	dstBucket, _ := nr.Params["destinationBucket"].(string)
	dstObject, _ := nr.Params["destinationObject"].(string)
	if dstBucket == "" {
		dstBucket = srcBucket
	}
	if srcBucket == "" || srcObject == "" || dstBucket == "" || dstObject == "" {
		return nil, model.NewProviderError("InvalidRequest", "move requires source and destination object names", 400)
	}
	if srcBucket == dstBucket && srcObject == dstObject {
		return nil, model.NewProviderError("InvalidRequest", "source and destination object must differ", 400)
	}
	if err := requireDownscope(nr, downscope.ReadObject, srcBucket, srcObject); err != nil {
		return nil, err
	}
	if err := requireDownscope(nr, downscope.DeleteObject, srcBucket, srcObject); err != nil {
		return nil, err
	}
	if err := requireDownscope(nr, downscope.WriteObject, dstBucket, dstObject); err != nil {
		return nil, err
	}
	if err := p.scopeToBucket(ctx, nr, dstBucket); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "destination bucket not found", 404)
		}
		return nil, err
	}

	srcMeta, raw, err := p.readSourceRaw(ctx, nr, srcBucket, srcObject, copySourceCSEKParams(nr))
	if err != nil {
		return nil, err
	}
	if !objectMetaPreconditionMatches(srcMeta, sourcePrecondition(nr)) {
		return nil, model.NewProviderError("PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412)
	}

	now := clock.Now()
	o := fromStoreObject(nr, srcMeta)
	o.Name = dstObject
	o.Bucket = dstBucket
	o.Generation = p.nextGen()
	o.Metageneration = "1"
	o.TimeCreated = now.Format(time.RFC3339Nano)
	o.Updated = o.TimeCreated
	o.Retention = nil
	o.RetentionExpirationTime = ""
	o.ID = dstBucket + "/" + dstObject + "/" + o.Generation
	o.Etag = "CAE="
	base := baseURL(nr)
	o.SelfLink = objectSelfLink(base, dstBucket, dstObject)
	o.MediaLink = objectMediaLink(base, dstBucket, dstObject)

	final, err := p.writeObjectRaw(ctx, nr, dstBucket, dstObject, o, raw, p.bucketVersioned(ctx, dstBucket), "", true)
	if err != nil {
		return nil, err
	}
	if err := p.DeleteObjectData(ctx, srcBucket, srcObject, "", objectPrecondition(nr)); err != nil {
		return nil, err
	}
	return provider.OK(toMap(final)), nil
}

// ObjectsRestore implements objects.restore: a non-live (soft-deleted) object
// generation is made live again, superseding the current live generation if
// any. The store's RestoreObjectGeneration validates the ifGeneration*/
// ifMetageneration* preconditions atomically with the mutation.
func (p *Provider) ObjectsRestore(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	generation, _ := nr.Params["generation"].(string)
	if bucket == "" || object == "" || generation == "" {
		return nil, model.NewProviderError("InvalidRequest", "restore requires bucket, object, and generation", 400)
	}
	if err := requireDownscope(nr, downscope.WriteObject, bucket, object); err != nil {
		return nil, err
	}
	meta, err := p.objects.RestoreObjectGeneration(ctx, bucket, object, generation, objectPrecondition(nr))
	if err != nil {
		switch {
		case errors.Is(err, gcs.ErrNoSuchBucket):
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		case errors.Is(err, gcs.ErrNoSuchObject):
			return nil, model.NewProviderError("NotFound", "object generation not found", 404)
		case errors.Is(err, gcs.ErrPreconditionFailed):
			return nil, model.NewProviderError("PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412)
		default:
			return nil, err
		}
	}
	return provider.OK(toMap(fromStoreObject(nr, meta))), nil
}

// ObjectsCompose implements objects.compose (the GCS JSON API compose action
// used by the Go SDK's Object.ComposerFrom). It concatenates the source objects'
// bytes in sourceObjects order, sets componentCount, computes CRC32C over the
// concatenated bytes, and leaves MD5 empty.
func (p *Provider) ObjectsCompose(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	body, _ := nr.Params["body"].(map[string]any)
	if err := p.scopeToBucket(ctx, nr, bucket); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}

	var dest map[string]any
	if body != nil {
		dest, _ = body["destination"].(map[string]any)
	}
	if object == "" && dest != nil {
		object, _ = dest["name"].(string)
	}
	if bucket == "" || object == "" {
		return nil, model.NewProviderError("InvalidRequest", "compose requires a destination object name", 400)
	}
	if err := requireDownscope(nr, downscope.WriteObject, bucket, object); err != nil {
		return nil, err
	}

	sources, _ := body["sourceObjects"].([]any)
	if len(sources) == 0 {
		return nil, model.NewProviderError("InvalidRequest", "compose requires source objects", 400)
	}
	for _, src := range sources {
		m, _ := src.(map[string]any)
		name, _ := m["name"].(string)
		if err := requireDownscope(nr, downscope.ReadObject, bucket, name); err != nil {
			return nil, err
		}
	}

	var buf bytes.Buffer
	for _, s := range sources {
		sm, _ := s.(map[string]any)
		name, _ := sm["name"].(string)
		if name == "" {
			return nil, model.NewProviderError("InvalidRequest", "compose source object missing name", 400)
		}
		srcParams := map[string]any{}
		if g, _ := sm["generation"].(string); g != "" {
			srcParams["generation"] = g
		}
		_, raw, err := p.readSourceRaw(ctx, nr, bucket, name, srcParams)
		if err != nil {
			return nil, err
		}
		buf.Write(raw)
	}

	now := clock.Now()
	o := objectMeta{
		Kind:           "storage#object",
		Name:           object,
		Bucket:         bucket,
		ContentType:    "application/octet-stream",
		Generation:     p.nextGen(),
		Metageneration: "1",
		StorageClass:   "STANDARD",
		TimeCreated:    now.Format(time.RFC3339Nano),
		Updated:        now.Format(time.RFC3339Nano),
		ComponentCount: int64(len(sources)),
	}
	if dest != nil {
		if ct, _ := dest["contentType"].(string); ct != "" {
			o.ContentType = ct
		}
		if md, ok := dest["metadata"].(map[string]any); ok {
			o.Metadata = make(map[string]string, len(md))
			for k, v := range md {
				if s, ok := v.(string); ok {
					o.Metadata[k] = s
				}
			}
		}
		if sc, _ := dest["storageClass"].(string); sc != "" {
			o.StorageClass = sc
		}
	}
	// Holds: explicit destination values win, otherwise inherit the bucket's
	// defaultEventBasedHold — matching the gRPC ComposeObject path.
	bmeta, _ := p.objects.GetBucket(ctx, bucket)
	applyObjectHolds(dest, bmeta, &o)
	o.ID = bucket + "/" + object + "/" + o.Generation
	o.Etag = "CAE="
	base := baseURL(nr)
	o.SelfLink = objectSelfLink(base, bucket, object)
	o.MediaLink = objectMediaLink(base, bucket, object)

	final, err := p.writeObjectRaw(ctx, nr, bucket, object, o, buf.Bytes(), p.bucketVersioned(ctx, bucket), "", false)
	if err != nil {
		return nil, err
	}
	return provider.OK(toMap(final)), nil
}

// resolveWriteKey determines the DEK for an object write: CSEK (header) wins,
// then CMEK (per-object kmsKeyName query param, else the bucket's
// encryption.defaultKmsKeyName), else empty (server DEK via Wrap).
func (p *Provider) resolveWriteKey(nr *model.NormalizedRequest, bucket string, bmeta map[string]any) (kmsKeyName string, cseKey []byte, cseKeySHA256 string, err error) {
	cseKey, cseKeySHA256, err = resolveCSEK(nr.Params, wire.CSEKAlgorithm, wire.CSEKKey, wire.CSEKKeySHA256)
	if err != nil {
		return "", nil, "", err
	}
	if cseKey != nil {
		return "", cseKey, cseKeySHA256, nil
	}
	if k, _ := nr.Params["kmsKeyName"].(string); k != "" {
		return k, nil, "", nil
	}
	if enc, ok := bmeta["encryption"].(map[string]any); ok {
		if dk, _ := enc["defaultKmsKeyName"].(string); dk != "" {
			return dk, nil, "", nil
		}
	}
	return "", nil, "", nil
}

// resolveCSEK validates a customer-supplied encryption key header set and
// returns the decoded 32-byte AES-256 key plus its computed base64 SHA-256.
// The param names are explicit so the same helper validates the destination key
// (x-goog-encryption-*) and a copy/rewrite source key
// (x-goog-copy-source-encryption-*). It returns (nil, "", nil) when no key
// material is present at all (the object is server-DEK/CMEK encrypted).
//
// Per the GCS contract, the algorithm must be AES256 when supplied, the key
// must be base64 of exactly 32 bytes, and the caller-supplied SHA-256 must
// match the key.
func resolveCSEK(params map[string]any, algKey, keyKey, shaKey string) ([]byte, string, error) {
	alg, _ := params[algKey].(string)
	keyB64, _ := params[keyKey].(string)
	gotSHA, _ := params[shaKey].(string)
	if alg == "" && keyB64 == "" && gotSHA == "" {
		return nil, "", nil
	}
	if alg != "" && !strings.EqualFold(alg, "AES256") {
		return nil, "", model.NewProviderError("InvalidArgument", "customer-supplied encryption algorithm must be AES256", 400)
	}
	if keyB64 == "" {
		return nil, "", model.NewProviderError("InvalidArgument", "missing customer-supplied encryption key", 400)
	}
	key, err := base64.StdEncoding.DecodeString(keyB64)
	if err != nil || len(key) != 32 {
		return nil, "", model.NewProviderError("InvalidArgument", "invalid customer-supplied encryption key", 400)
	}
	if gotSHA == "" {
		return nil, "", model.NewProviderError("InvalidArgument", "missing customer-supplied encryption key sha256", 400)
	}
	sum := sha256.Sum256(key)
	expected := base64.StdEncoding.EncodeToString(sum[:])
	if gotSHA != expected {
		return nil, "", model.NewProviderError("InvalidArgument", "customer-supplied encryption key hash mismatch", 400)
	}
	return key, expected, nil
}

// copySourceCSEKParams builds the source-object read params from the
// x-goog-copy-source-encryption-* headers, keyed the way decryptObject expects
// (wire.CSEKKey/wire.CSEKKeySHA256). Empty when the request carried no
// copy-source key (the source is server-DEK/CMEK encrypted).
func copySourceCSEKParams(nr *model.NormalizedRequest) map[string]any {
	params := map[string]any{}
	if v, _ := nr.Params[wire.CopySourceCSEKAlgorithm].(string); v != "" {
		params[wire.CSEKAlgorithm] = v
	}
	if v, _ := nr.Params[wire.CopySourceCSEKKey].(string); v != "" {
		params[wire.CSEKKey] = v
	}
	if v, _ := nr.Params[wire.CopySourceCSEKKeySHA256].(string); v != "" {
		params[wire.CSEKKeySHA256] = v
	}
	return params
}

// decryptObject returns the plaintext for a stored ciphertext, using the CSEK
// key (when the object is CSEK-encrypted) or the envelope DEK otherwise.
func (p *Provider) decryptObject(ctx context.Context, nr *model.NormalizedRequest, meta gcs.ObjectMeta, ciphertext []byte) ([]byte, error) {
	var cseKey []byte
	if meta.CSEKeySHA256 != "" {
		if alg, _ := nr.Params[wire.CSEKAlgorithm].(string); alg != "" && !strings.EqualFold(alg, "AES256") {
			return nil, model.NewProviderError("InvalidArgument", "customer-supplied encryption algorithm must be AES256", 400)
		}
		keyB64, _ := nr.Params[wire.CSEKKey].(string)
		if keyB64 == "" {
			return nil, model.NewProviderError("InvalidArgument", "missing customer-supplied encryption key", 400)
		}
		key, err := base64.StdEncoding.DecodeString(keyB64)
		if err != nil || len(key) != 32 {
			return nil, model.NewProviderError("InvalidArgument", "invalid customer-supplied encryption key", 400)
		}
		// A caller-supplied sha256, when present, must match the provided key.
		// The stored hash is separately checked by decryptObjectWithKey.
		if gotSHA, _ := nr.Params[wire.CSEKKeySHA256].(string); gotSHA != "" {
			sum := sha256.Sum256(key)
			if base64.StdEncoding.EncodeToString(sum[:]) != gotSHA {
				return nil, model.NewProviderError("InvalidArgument", "customer-supplied encryption key sha256 mismatch", 400)
			}
		}
		cseKey = key
	}
	return p.decryptObjectWithKey(ctx, nr.AccountID, meta, ciphertext, cseKey)
}

// decryptObjectWithKey returns the plaintext for a stored ciphertext given the
// already-resolved CSEK key (nil for server-DEK/CMEK-encrypted objects). This
// is the transport-agnostic core shared by the REST read path and the gRPC
// ReadObject.
func (p *Provider) decryptObjectWithKey(ctx context.Context, project string, meta gcs.ObjectMeta, ciphertext []byte, cseKey []byte) ([]byte, error) {
	if meta.CSEKeySHA256 != "" {
		if len(cseKey) != 32 {
			return nil, model.NewProviderError("InvalidArgument", "invalid customer-supplied encryption key", 400)
		}
		sum := sha256.Sum256(cseKey)
		if base64.StdEncoding.EncodeToString(sum[:]) != meta.CSEKeySHA256 {
			return nil, model.NewProviderError("InvalidArgument", "customer-supplied encryption key mismatch", 400)
		}
		return kmsstore.DecryptData(cseKey, ciphertext, nil)
	}
	rawDEK, err := p.encryptor.Unwrap(ctx, project, meta.KmsKeyName, meta.WrappedDEK)
	if err != nil {
		return nil, err
	}
	return kmsstore.DecryptData(rawDEK, ciphertext, nil)
}

// ObjectsUpdate implements objects.update (HTTP PUT). GCS PUT semantics are a
// strict replacement of the object's writable metadata: fields omitted from
// the request body are cleared (or reset to defaults), not preserved.
func (p *Provider) ObjectsUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	if err := requireDownscope(nr, downscope.WriteObject, bucket, object); err != nil {
		return nil, err
	}
	meta, err := p.objects.GetObjectMeta(ctx, bucket, object)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return nil, model.NewProviderError("NotFound", "object not found", 404)
		}
		return nil, err
	}
	o := fromStoreObject(nr, meta)

	// Strict replace: writable metadata is taken verbatim from the request —
	// omitted fields are cleared.
	o.ContentType, _ = bodyString(nr.Params, "contentType")
	o.Metadata = bodyMetadata(nr.Params)
	o.StorageClass, _ = bodyString(nr.Params, "storageClass")
	if o.StorageClass == "" {
		o.StorageClass = "STANDARD"
	}
	// Holds are writable via objects.update too; omitted booleans reset to false
	// under PUT's full-replacement semantics.
	o.TemporaryHold, _ = bodyBool(nr.Params, "temporaryHold")
	o.EventBasedHold, _ = bodyBool(nr.Params, "eventBasedHold")

	o.Metageneration = bumpMeta(o.Metageneration)
	o.Updated = clock.Now().Format(time.RFC3339Nano)
	if err := p.objects.PutObjectMetaChecked(ctx, bucket, object, toStoreObject(o), objectPrecondition(nr)); err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		if errors.Is(err, gcs.ErrPreconditionFailed) {
			return nil, model.NewProviderError("PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412)
		}
		return nil, err
	}
	return provider.OK(toMap(o)), nil
}

// ObjectsPatch implements objects.patch (HTTP PATCH). Patch semantics merge:
// only the fields present in the request are updated; omitted fields are
// preserved.
func (p *Provider) ObjectsPatch(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	if err := requireDownscope(nr, downscope.WriteObject, bucket, object); err != nil {
		return nil, err
	}
	meta, err := p.objects.GetObjectMeta(ctx, bucket, object)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return nil, model.NewProviderError("NotFound", "object not found", 404)
		}
		return nil, err
	}
	// Preserve size/md5Hash/generation/storageClass/timeCreated; update only
	// the fields present in the request and bump metageneration.
	o := fromStoreObject(nr, meta)
	if body, ok := nr.Params["body"].(map[string]any); ok {
		if ct, _ := body["contentType"].(string); ct != "" {
			o.ContentType = ct
		}
		if sc, _ := body["storageClass"].(string); sc != "" {
			o.StorageClass = sc
		}
		if _, ok := body["metadata"]; ok {
			o.Metadata = bodyMetadata(nr.Params)
		}
		if h, ok := bodyBool(nr.Params, "temporaryHold"); ok {
			o.TemporaryHold = h
		}
		if h, ok := bodyBool(nr.Params, "eventBasedHold"); ok {
			o.EventBasedHold = h
		}
	}
	o.Metageneration = bumpMeta(o.Metageneration)
	o.Updated = clock.Now().Format(time.RFC3339Nano)
	if err := p.objects.PutObjectMetaChecked(ctx, bucket, object, toStoreObject(o), objectPrecondition(nr)); err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		if errors.Is(err, gcs.ErrPreconditionFailed) {
			return nil, model.NewProviderError("PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412)
		}
		return nil, err
	}
	return provider.OK(toMap(o)), nil
}

// bodyString returns the named string field from the request body.
func bodyString(params map[string]any, key string) (string, bool) {
	body, ok := params["body"].(map[string]any)
	if !ok {
		return "", false
	}
	s, ok := body[key].(string)
	return s, ok
}

// bodyBool returns the named boolean field from the request body. The second
// return distinguishes "present (true/false)" from "absent" so callers can
// implement strict-replace vs merge semantics.
func bodyBool(params map[string]any, key string) (bool, bool) {
	body, ok := params["body"].(map[string]any)
	if !ok {
		return false, false
	}
	b, ok := body[key].(bool)
	return b, ok
}

// bodyMetadata returns the metadata map from the request body, or nil when the
// body carries none.
func bodyMetadata(params map[string]any) map[string]string {
	body, ok := params["body"].(map[string]any)
	if !ok {
		return nil
	}
	md, ok := body["metadata"].(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(md))
	for k, v := range md {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

// bumpMeta increments a metageneration string ("N" → "N+1"; default "1").
func bumpMeta(m string) string {
	n, err := strconv.Atoi(m)
	if err != nil {
		return "1"
	}
	return strconv.Itoa(n + 1)
}

func (p *Provider) ObjectsDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	if err := requireDownscope(nr, downscope.DeleteObject, bucket, object); err != nil {
		return nil, err
	}
	// objects.delete honours an explicit ?generation= to delete only that
	// revision; with no generation it deletes the live object (tombstoning it
	// in a versioned bucket).
	generation, _ := nr.Params["generation"].(string)
	if err := p.DeleteObjectData(ctx, bucket, object, generation, objectPrecondition(nr)); err != nil {
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// DeleteObjectData deletes an object, honoring retention holds and bucket
// versioning. With an empty generation it deletes the live object: in a
// non-versioned bucket every generation is hard-deleted, in a versioned bucket
// the live generation is tombstoned and its bytes dropped. When generation is
// supplied (objects.delete?generation=) only that revision is removed — live or
// non-live — and its bytes dropped; removing the live revision does not promote
// a noncurrent survivor, so the name then resolves only by generation. Shared
// by the REST ObjectsDelete and the gRPC DeleteObject. precondition is GCS's
// ifGenerationMatch/ifGenerationNotMatch (the common "safe delete" idiom — the
// store validates it against the live generation), checked atomically with the
// delete.
func (p *Provider) DeleteObjectData(ctx context.Context, bucket, object, generation string, precondition *gcs.Precondition) error {
	if generation != "" {
		return p.deleteObjectGeneration(ctx, bucket, object, generation, precondition)
	}
	// Holds/retention check first: a held or retention-active object cannot be
	// deleted (GCS returns PERMISSION_DENIED). Note this GetObjectMeta read is
	// separate from the precondition check inside the *Checked delete call
	// below — a hold added/removed in between is a pre-existing, narrower race
	// unrelated to (and not widened by) the precondition fix here.
	meta, err := p.objects.GetObjectMeta(ctx, bucket, object)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return model.NewProviderError("NotFound", "object not found", 404)
		}
		return err
	}
	if perr := objectDeleteBlockedError(meta); perr != nil {
		return perr
	}

	if p.bucketVersioned(ctx, bucket) {
		// Versioned bucket: delete only the live generation, leaving a
		// non-live tombstone that appears in ?versions=true listings.
		if _, err := p.objects.TombstoneObjectMetaChecked(ctx, bucket, object, precondition); err != nil {
			if errors.Is(err, gcs.ErrNoSuchObject) {
				return model.NewProviderError("NotFound", "object not found", 404)
			}
			if errors.Is(err, gcs.ErrPreconditionFailed) {
				return model.NewProviderError("PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412)
			}
			return err
		}
		_ = p.blobs.Delete(ctx, blobsNamespace, blobKey(bucket, object, meta.Generation))
		// A versioned delete makes the live version noncurrent, which real GCS
		// reports as OBJECT_ARCHIVE (not OBJECT_DELETE).
		p.publishObjectEvent(ctx, p.bucketProject(ctx, bucket), bucket, object, "OBJECT_ARCHIVE", meta, clock.Now(), nil)
		return nil
	}

	// Non-versioned bucket: hard-delete every generation (there should only
	// be one) and its bytes.
	var blobKeys []string
	if gens, err := p.objects.ListObjectVersions(ctx, bucket); err == nil {
		for _, m := range gens {
			if m.Name == object {
				blobKeys = append(blobKeys, blobKey(bucket, object, m.Generation))
			}
		}
	}
	if err := p.objects.DeleteObjectMetaChecked(ctx, bucket, object, precondition); err != nil {
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return model.NewProviderError("NotFound", "object not found", 404)
		}
		if errors.Is(err, gcs.ErrPreconditionFailed) {
			return model.NewProviderError("PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412)
		}
		return err
	}
	for _, id := range blobKeys {
		_ = p.blobs.Delete(ctx, blobsNamespace, id)
	}
	p.publishObjectEvent(ctx, p.bucketProject(ctx, bucket), bucket, object, "OBJECT_DELETE", meta, clock.Now(), nil)
	return nil
}

// deleteObjectGeneration removes one specific object revision (the target of
// objects.delete?generation=), enforcing holds/retention on that revision and
// dropping its bytes. Removing the live revision does not promote a noncurrent
// survivor (the store leaves remaining revisions noncurrent).
func (p *Provider) deleteObjectGeneration(ctx context.Context, bucket, object, generation string, precondition *gcs.Precondition) error {
	if _, perr := strconv.ParseInt(generation, 10, 64); perr != nil {
		return model.NewProviderError("InvalidRequest", "invalid generation: "+generation, 400)
	}
	// The hold/retention read below is separate from the store's atomic
	// DeleteObjectGeneration; a hold added in between is a pre-existing narrow
	// race, same as the live-delete path.
	target, err := p.objects.GetObjectGeneration(ctx, bucket, object, generation)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return model.NewProviderError("NotFound", "object generation not found", 404)
		}
		return err
	}
	if perr := objectDeleteBlockedError(target); perr != nil {
		return perr
	}
	removed, err := p.objects.DeleteObjectGeneration(ctx, bucket, object, generation, precondition)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return model.NewProviderError("NotFound", "object generation not found", 404)
		}
		if errors.Is(err, gcs.ErrPreconditionFailed) {
			return model.NewProviderError("PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412)
		}
		return err
	}
	_ = p.blobs.Delete(ctx, blobsNamespace, blobKey(bucket, object, removed.Generation))
	p.publishObjectEvent(ctx, p.bucketProject(ctx, bucket), bucket, object, "OBJECT_DELETE", removed, clock.Now(), nil)
	return nil
}

// bucketProject returns the project that owns a bucket (the account scope used
// for notification storage and envelope encryption), falling back to "" when
// the bucket metadata cannot be read.
func (p *Provider) bucketProject(ctx context.Context, bucket string) string {
	if m, err := p.objects.GetBucket(ctx, bucket); err == nil {
		if pid, _ := m["projectId"].(string); pid != "" {
			return pid
		}
	}
	return ""
}

// objectDeleteBlockedError returns the GCS error that blocks deleting an object
// because of a temporary/event-based hold or an active retention policy, or nil
// when the object may be deleted. The message names the specific protection so
// clients (and the Java compat suite) can distinguish holds from retention.
func objectDeleteBlockedError(m gcs.ObjectMeta) *model.ProviderError {
	return objectBlockedError(m, "deleted")
}

// objectWriteBlockedError returns the GCS error that blocks overwriting an
// object because of a hold or an active retention policy, or nil when the write
// may proceed.
func objectWriteBlockedError(m gcs.ObjectMeta) *model.ProviderError {
	return objectBlockedError(m, "overwritten")
}

// objectBlockedError builds the 403 that blocks mutating a protected object,
// naming the specific hold or the retention policy and the attempted verb.
func objectBlockedError(m gcs.ObjectMeta, verb string) *model.ProviderError {
	if m.TemporaryHold {
		return model.NewProviderError("PermissionDenied", "Object is under a temporary hold and cannot be "+verb, 403)
	}
	if m.EventBasedHold {
		return model.NewProviderError("PermissionDenied", "Object is under an event-based hold and cannot be "+verb, 403)
	}
	if retentionActive(m) {
		return model.NewProviderError("PermissionDenied", "Object is under an active retention policy and cannot be "+verb, 403)
	}
	return nil
}

// retentionActive reports whether the object's retention policy still forbids
// deletion/overwrite at the current (possibly frozen) clock time.
func retentionActive(m gcs.ObjectMeta) bool {
	return m.Retention != nil && !m.Retention.RetainUntilTime.IsZero() && clock.Now().Before(m.Retention.RetainUntilTime)
}

// filterLifecycle lazily applies bucket lifecycle Delete rules: an object whose
// age (in days) reaches condition.age is dropped from the listing. This is the
// emulator's analogue of the AWS S3 lazy-retention sweep.
func (p *Provider) filterLifecycle(ctx context.Context, bucket string, objs []gcs.ObjectMeta) []gcs.ObjectMeta {
	bmeta, err := p.objects.GetBucket(ctx, bucket)
	if err != nil {
		return objs
	}
	lc, _ := bmeta["lifecycle"].(map[string]any)
	if lc == nil {
		return objs
	}
	out := objs[:0]
	for _, m := range objs {
		if !objectLifecycleExpired(lc, m.TimeCreated) {
			out = append(out, m)
		}
	}
	return out
}

// objectLifecycleExpired reports whether a Delete rule's condition.age matches
// the object's creation time.
func objectLifecycleExpired(lc map[string]any, created time.Time) bool {
	rules, _ := lc["rule"].([]any)
	now := clock.Now()
	for _, rr := range rules {
		rule, ok := rr.(map[string]any)
		if !ok {
			continue
		}
		action, _ := rule["action"].(map[string]any)
		if actionType, _ := action["type"].(string); actionType != "Delete" {
			continue
		}
		cond, _ := rule["condition"].(map[string]any)
		ageDays, _ := cond["age"].(float64)
		if ageDays <= 0 {
			continue
		}
		if now.Sub(created) >= time.Duration(ageDays)*24*time.Hour {
			return true
		}
	}
	return false
}

// toMap converts an objectMeta struct into a map for JSON-encoding by the codec.
func toMap(o objectMeta) map[string]any {
	b, _ := json.Marshal(o)
	var m map[string]any
	json.Unmarshal(b, &m)
	return m
}

// notificationToMap converts a notificationConfig into a wire map (honouring the
// GCS JSON API's snake_case field names).
func notificationToMap(cfg notificationConfig) map[string]any {
	b, _ := json.Marshal(cfg)
	var m map[string]any
	json.Unmarshal(b, &m)
	return m
}

// toBucketMap converts a bucketMeta struct into a map for JSON-encoding.
func toBucketMap(nr *model.NormalizedRequest, b bucketMeta) map[string]any {
	versioning := b.Versioning
	if versioning == nil {
		versioning = map[string]any{"enabled": false}
	}
	metageneration := b.Metageneration
	if metageneration == "" {
		metageneration = "1"
	}
	out := map[string]any{
		"kind":           "storage#bucket",
		"id":             b.Name,
		"name":           b.Name,
		"location":       b.Location,
		"locationType":   "multi-region",
		"storageClass":   b.StorageClass,
		"timeCreated":    b.TimeCreated,
		"updated":        b.Updated,
		"generation":     "0",
		"metageneration": metageneration,
		"projectNumber":  resource.ProjectNumber(nr.AccountID),
		"selfLink":       bucketSelfLink(baseURL(nr), b.Name),
		"etag":           "CAE=",
		"versioning":     versioning,
		"iamConfiguration": map[string]any{
			"uniformBucketLevelAccess": map[string]any{"enabled": false},
			"bucketPolicyOnly":         map[string]any{"enabled": false},
			"publicAccessPrevention":   "inherited",
		},
	}
	if b.RetentionPolicy != nil {
		out["retentionPolicy"] = b.RetentionPolicy
	}
	if b.Lifecycle != nil {
		out["lifecycle"] = b.Lifecycle
	}
	if b.Encryption != nil {
		out["encryption"] = b.Encryption
	}
	if b.Cors != nil {
		out["cors"] = b.Cors
	}
	// Real GCS enables soft delete on every bucket with a 7-day retention
	// window by default; effectiveTime is when the policy took effect. An
	// explicit policy supplied at create/update is preserved.
	softDelete := b.SoftDeletePolicy
	if softDelete == nil {
		softDelete = map[string]any{"retentionDurationSeconds": "604800"}
	}
	if _, ok := softDelete["effectiveTime"]; !ok && b.TimeCreated != "" {
		softDelete["effectiveTime"] = b.TimeCreated
	}
	out["softDeletePolicy"] = softDelete
	if b.Labels != nil {
		out["labels"] = b.Labels
	}
	out["defaultEventBasedHold"] = b.DefaultEventBasedHold
	return out
}

// bodyMap returns the named object-valued field from the request body, or nil.
func bodyMap(body map[string]any, key string) map[string]any {
	if body == nil {
		return nil
	}
	m, _ := body[key].(map[string]any)
	return m
}

// bodyStringMap returns the named object-valued field from the request body as
// a string map (GCS labels carry string keys and values), or nil. Non-string
// values are skipped; a nil/absent field yields nil so callers can distinguish
// "not supplied" from an explicitly empty map.
func bodyStringMap(body map[string]any, key string) map[string]string {
	m := bodyMap(body, key)
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

// bodySlice returns the named array-valued field from the request body, or nil.
func bodySlice(body map[string]any, key string) []any {
	if body == nil {
		return nil
	}
	v, _ := body[key].([]any)
	return v
}

// retentionPeriodSeconds extracts a retention policy's period in seconds.
// GCS carries retentionPeriod as a decimal-second string; a Go-style duration
// is also accepted for tolerance. Returns 0 when absent/unparseable.
func retentionPeriodSeconds(rp map[string]any) int64 {
	if rp == nil {
		return 0
	}
	period, _ := rp["retentionPeriod"].(string)
	if period == "" {
		return 0
	}
	if n, err := strconv.ParseInt(period, 10, 64); err == nil {
		return n
	}
	if d, err := time.ParseDuration(period); err == nil {
		return int64(d.Seconds())
	}
	return 0
}

// parseRetentionPeriod parses a retentionPeriod string. GCS carries this as a
// decimal-second string ("86400"), while clients may also send a Go-style
// duration ("86400s"); both are accepted.
func parseRetentionPeriod(s string) time.Duration {
	if s == "" {
		return 0
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Duration(n) * time.Second
	}
	return 0
}

// objectResponse returns the JSON metadata for an object, or 404.
func (p *Provider) objectResponse(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	if err := requireDownscope(nr, downscope.ReadObject, bucket, object); err != nil {
		return nil, err
	}
	meta, err := p.getObjectForRead(ctx, bucket, object, nr.Params)
	if err != nil {
		return nil, err
	}
	if resp, perr := checkReadPreconditions(meta, objectPrecondition(nr)); resp != nil || perr != nil {
		return resp, perr
	}
	o := fromStoreObject(nr, meta)
	return provider.OK(toMap(o)), nil
}

// getObjectForRead resolves the object metadata for a read, honouring the
// ?generation= param (specific generation) or defaulting to the live
// generation, and applying lazy lifecycle deletion for live reads.
func (p *Provider) getObjectForRead(ctx context.Context, bucket, object string, params map[string]any) (gcs.ObjectMeta, error) {
	generation, _ := params["generation"].(string)
	var meta gcs.ObjectMeta
	var err error
	if generation != "" {
		meta, err = p.objects.GetObjectGeneration(ctx, bucket, object, generation)
	} else {
		meta, err = p.objects.GetObjectMeta(ctx, bucket, object)
	}
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return gcs.ObjectMeta{}, model.NewProviderError("NotFound", "object not found", 404)
		}
		return gcs.ObjectMeta{}, err
	}
	// Lazy lifecycle: drop a live object whose age exceeds a Delete rule.
	if generation == "" {
		if bmeta, berr := p.objects.GetBucket(ctx, bucket); berr == nil {
			if objectLifecycleExpired(bmeta, meta.TimeCreated) {
				return gcs.ObjectMeta{}, model.NewProviderError("NotFound", "object not found", 404)
			}
		}
	}
	return meta, nil
}

// ─── pagination helpers ────────────────────────────────────────────────────────

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

// versionCursor is the composite cursor for ?versions=true listings, which are
// ordered by name ascending then generation descending. Object names may
// contain any character, so the pair is JSON-encoded before base64 rather than
// concatenated with a delimiter.
type versionCursor struct {
	Name       string `json:"n"`
	Generation string `json:"g"`
}

func encodeVersionCursor(name, generation string) string {
	b, err := json.Marshal(versionCursor{Name: name, Generation: generation})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeVersionCursor(token string) (versionCursor, bool) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return versionCursor{}, false
	}
	var c versionCursor
	if err := json.Unmarshal(b, &c); err != nil {
		return versionCursor{}, false
	}
	return c, true
}

func maxResults(params map[string]any) int {
	if v, ok := params["maxResults"].(string); ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			// Real GCS caps the page at "this parameter or 1,000 items,
			// whichever is smaller".
			if n > 1000 {
				return 1000
			}
			return n
		}
	}
	return 1000
}

// ─── IAM policy ───────────────────────────────────────────────────────────────

type iamBinding struct {
	Role    string   `json:"role"`
	Members []string `json:"members"`
}

type iamPolicy struct {
	Kind       string       `json:"kind"`
	ResourceID string       `json:"resourceId,omitempty"`
	Bindings   []iamBinding `json:"bindings"`
	Etag       string       `json:"etag"`
	Version    int          `json:"version"`
}

func defaultPolicy(bucket, project, resourceID string) iamPolicy {
	return iamPolicy{
		Kind:       "storage#policy",
		ResourceID: resourceID,
		Bindings: []iamBinding{
			{Role: "roles/storage.legacyBucketOwner", Members: []string{"projectEditor:" + project, "projectOwner:" + project}},
			{Role: "roles/storage.legacyBucketReader", Members: []string{"projectViewer:" + project}},
		},
		Etag:    "CAE=",
		Version: 1,
	}
}

func (p *Provider) BucketsGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	bucket, _ := nr.Params["bucket"].(string)
	if err := p.scopeToBucket(ctx, nr, bucket); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	pol := defaultPolicy(bucket, nr.AccountID, nr.ResourceID("gcs-bucket-policy", bucket))
	if e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtBucketIAM, bucket); err == nil {
		json.Unmarshal(e.Data, &pol)
	}
	return provider.OK(map[string]any{
		"kind":       pol.Kind,
		"resourceId": pol.ResourceID,
		"bindings":   pol.Bindings,
		"etag":       pol.Etag,
		"version":    pol.Version,
	}), nil
}

// BucketsSetIamPolicy implements buckets.setIamPolicy with optimistic
// concurrency control: a request carrying an etag that does not match the
// stored policy is rejected with 409.
func (p *Provider) BucketsSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	bucket, _ := nr.Params["bucket"].(string)
	if err := p.scopeToBucket(ctx, nr, bucket); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	return p.setIamPolicy(ctx, nr, rtBucketIAM, bucket, nr.ResourceID("gcs-bucket-policy", bucket))
}

// ObjectsGetIamPolicy returns the object-level IAM policy (defaulting to the
// project's legacy bindings when none has been stored).
func (p *Provider) ObjectsGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	id := bucket + "/" + object
	if err := p.scopeToBucket(ctx, nr, bucket); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	if err := p.requireObject(ctx, nr, id); err != nil {
		return nil, err
	}
	pol := defaultPolicy(id, nr.AccountID, nr.ResourceID("gcs-object-policy", bucket+"/objects/"+object))
	if e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtObjectIAM, id); err == nil {
		json.Unmarshal(e.Data, &pol)
	}
	return provider.OK(map[string]any{
		"kind":       pol.Kind,
		"resourceId": pol.ResourceID,
		"bindings":   pol.Bindings,
		"etag":       pol.Etag,
		"version":    pol.Version,
	}), nil
}

// ObjectsSetIamPolicy implements objects.setIamPolicy with the same etag-based
// optimistic concurrency control as the bucket-level method.
func (p *Provider) ObjectsSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	id := bucket + "/" + object
	if err := p.scopeToBucket(ctx, nr, bucket); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	if err := p.requireObject(ctx, nr, id); err != nil {
		return nil, err
	}
	return p.setIamPolicy(ctx, nr, rtObjectIAM, id, nr.ResourceID("gcs-object-policy", bucket+"/objects/"+object))
}

// requireObject returns a 404 ProviderError when the object's metadata is
// absent, mirroring the GCS JSON API behavior for object-scoped endpoints.
func (p *Provider) requireObject(ctx context.Context, nr *model.NormalizedRequest, id string) error {
	bucket, object, _ := strings.Cut(id, "/")
	if _, err := p.objects.GetObjectMeta(ctx, bucket, object); err != nil {
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return model.NewProviderError("NotFound", "object not found", 404)
		}
		return err
	}
	return nil
}

// setIamPolicy is the shared setIamPolicy flow: OCC etag check (409 on
// mismatch), full bindings replacement, a fresh etag, and persist.
func (p *Provider) setIamPolicy(ctx context.Context, nr *model.NormalizedRequest, rt, id, resourceID string) (*model.ProviderResponse, error) {
	body, _ := nr.Params["body"].(map[string]any)

	existing := defaultPolicy(id, nr.AccountID, resourceID)
	if e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rt, id); err == nil {
		json.Unmarshal(e.Data, &existing)
	}
	// Optimistic concurrency control: reject when the client supplies an etag
	// that doesn't match the stored policy.
	if reqEtag, _ := body["etag"].(string); reqEtag != "" && reqEtag != existing.Etag {
		return nil, model.NewProviderError("Conflict", "etag mismatch: optimistic concurrency control failed", 409)
	}

	pol := iamPolicy{Kind: "storage#policy", ResourceID: resourceID, Version: 1}
	if v, ok := body["version"].(float64); ok {
		pol.Version = int(v)
	}
	if bs, ok := body["bindings"].([]any); ok {
		for _, b := range bs {
			bm, _ := b.(map[string]any)
			role, _ := bm["role"].(string)
			ib := iamBinding{Role: role}
			if ms, ok := bm["members"].([]any); ok {
				for _, m := range ms {
					if s, ok := m.(string); ok {
						ib.Members = append(ib.Members, s)
					}
				}
			}
			pol.Bindings = append(pol.Bindings, ib)
		}
	}
	pol.Etag = policyEtag(pol.Bindings)

	data, _ := json.Marshal(pol)
	_ = p.resources.Upsert(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rt, ID: id, Data: data})
	return provider.OK(map[string]any{
		"kind":       pol.Kind,
		"resourceId": pol.ResourceID,
		"bindings":   pol.Bindings,
		"etag":       pol.Etag,
		"version":    pol.Version,
	}), nil
}

// policyEtag derives a stable etag from the policy bindings so the etag only
// changes when the policy changes (matching GCS's optimistic concurrency
// semantics).
func policyEtag(bindings []iamBinding) string {
	h := sha1.New()
	for _, b := range bindings {
		io.WriteString(h, b.Role)
		io.WriteString(h, "\x00")
		for _, m := range b.Members {
			io.WriteString(h, m)
			io.WriteString(h, "\x00")
		}
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// ─── ACLs ─────────────────────────────────────────────────────────────────────

type aclEntry struct {
	Kind   string `json:"kind"`
	ID     string `json:"id"`
	Entity string `json:"entity"`
	Role   string `json:"role"`
	Bucket string `json:"bucket,omitempty"`
	Object string `json:"object,omitempty"`
	ETag   string `json:"etag"`
}

func defaultACL(bucket, object, kind string) []aclEntry {
	entries := []aclEntry{
		{Kind: kind, ID: bucket + "/owners", Entity: "project-owners-jaiscloud", Role: "OWNER", Bucket: bucket, ETag: "CAE="},
		{Kind: kind, ID: bucket + "/editors", Entity: "project-editors-jaiscloud", Role: "OWNER", Bucket: bucket, ETag: "CAE="},
		{Kind: kind, ID: bucket + "/viewers", Entity: "project-viewers-jaiscloud", Role: "READER", Bucket: bucket, ETag: "CAE="},
	}
	if object != "" {
		for i := range entries {
			entries[i].Object = object
		}
	}
	return entries
}

func (p *Provider) listACL(ctx context.Context, nr *model.NormalizedRequest, bucket, object, kind string) (*model.ProviderResponse, error) {
	aclID := bucket
	if object != "" {
		aclID = bucket + "/" + object
	}
	entries := defaultACL(bucket, object, kind)
	if e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtACL, aclID); err == nil {
		var stored []aclEntry
		if json.Unmarshal(e.Data, &stored) == nil {
			entries = stored
		}
	}
	return provider.OK(map[string]any{"kind": kind, "items": entries}), nil
}

func (p *Provider) insertACL(ctx context.Context, nr *model.NormalizedRequest, bucket, object, kind string) (*model.ProviderResponse, error) {
	body, _ := nr.Params["body"].(map[string]any)
	entity, _ := body["entity"].(string)
	role, _ := body["role"].(string)
	if entity == "" || role == "" {
		return nil, model.NewProviderError("InvalidRequest", "ACL insert requires entity and role", 400)
	}
	aclID := bucket
	if object != "" {
		aclID = bucket + "/" + object
	}
	entries := defaultACL(bucket, object, kind)
	if e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtACL, aclID); err == nil {
		var stored []aclEntry
		if json.Unmarshal(e.Data, &stored) == nil {
			entries = stored
		}
	}
	entry := aclEntry{Kind: kind, ID: aclID + "/" + entity, Entity: entity, Role: role, Bucket: bucket, Object: object, ETag: "CAE="}
	entries = append(entries, entry)
	data, _ := json.Marshal(entries)
	_ = p.resources.Upsert(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtACL, ID: aclID, Data: data})
	resp := map[string]any{
		"kind":   kind,
		"id":     entry.ID,
		"entity": entry.Entity,
		"role":   entry.Role,
		"bucket": entry.Bucket,
		"etag":   entry.ETag,
	}
	if entry.Object != "" {
		resp["object"] = entry.Object
	}
	return provider.OK(resp), nil
}

func (p *Provider) BucketACLList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	bucket, _ := nr.Params["bucket"].(string)
	if err := p.scopeToBucket(ctx, nr, bucket); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	return p.listACL(ctx, nr, bucket, "", "storage#bucketAccessControls")
}

func (p *Provider) BucketACLInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	bucket, _ := nr.Params["bucket"].(string)
	if err := p.scopeToBucket(ctx, nr, bucket); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	return p.insertACL(ctx, nr, bucket, "", "storage#bucketAccessControl")
}

func (p *Provider) ObjectACLList(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	if err := p.scopeToBucket(ctx, nr, bucket); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	return p.listACL(ctx, nr, bucket, object, "storage#objectAccessControls")
}

func (p *Provider) ObjectACLInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := requireBucketAdmin(nr); err != nil {
		return nil, err
	}
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	if err := p.scopeToBucket(ctx, nr, bucket); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	return p.insertACL(ctx, nr, bucket, object, "storage#objectAccessControl")
}

// ─── resumable uploads ────────────────────────────────────────────────────────

func (p *Provider) ObjectsInsertStartResumable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	if object == "" {
		if body, ok := nr.Params["body"].(map[string]any); ok {
			object, _ = body["name"].(string)
		}
	}
	if bucket == "" || object == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing bucket or object name", 400)
	}
	if err := requireDownscope(nr, downscope.WriteObject, bucket, object); err != nil {
		return nil, err
	}
	ct, _ := nr.Params[wire.ContentTypeKey].(string)

	id := p.nextGen() // atomic + monotonic, collision-free under concurrency
	now := clock.RealNow()
	p.mu.Lock()
	p.sweepSessions()
	if len(p.uploads) >= maxUploadSessions {
		p.mu.Unlock()
		return nil, model.NewProviderError("InvalidRequest", "too many active resumable uploads", 429)
	}
	p.uploads[id] = &uploadSession{Bucket: bucket, Object: object, ContentType: ct, Metadata: uploadMetadata(nr.Params), lastAccess: now}
	p.mu.Unlock()

	// Sweep stale sessions from the durable store (may hold orphans from a
	// prior process restart that are no longer in the in-memory map).
	p.sweepStaleStore(ctx)

	if err := p.objects.InitResumable(ctx, gcs.ResumableSession{
		UploadID: id, Bucket: bucket, Name: object, ContentType: ct, LastAccess: now,
	}); err != nil {
		p.mu.Lock()
		delete(p.uploads, id)
		p.mu.Unlock()
		return nil, err
	}

	base, _ := nr.Params[wire.BaseURLKey].(string)
	if base == "" {
		base = "http://localhost"
	}
	loc := fmt.Sprintf("%s/upload/storage/v1/b/%s/o?uploadType=resumable&upload_id=%s", base, bucket, id)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{wire.LocationKey: loc}}, nil
}

func (p *Provider) ObjectsInsertResumable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	uploadID, _ := nr.Params["upload_id"].(string)
	media, _ := nr.Params[wire.MediaKey].([]byte)
	cr, _ := nr.Params["contentRange"].(string)

	p.mu.Lock()
	sess, ok := p.uploads[uploadID]
	if !ok {
		if done, ok := p.completed[uploadID]; ok {
			if err := requireDownscope(nr, downscope.WriteObject, done.Bucket, done.Object); err != nil {
				p.mu.Unlock()
				return nil, err
			}
			p.mu.Unlock()
			return provider.OK(done.objectJSON), nil
		}
		p.mu.Unlock()
		return nil, model.NewProviderError("NotFound", "unknown upload_id", 404)
	}
	sess.lastAccess = clock.RealNow()
	if err := requireDownscope(nr, downscope.WriteObject, sess.Bucket, sess.Object); err != nil {
		p.mu.Unlock()
		return nil, err
	}
	if ct, _ := nr.Params[wire.ContentTypeKey].(string); ct != "" {
		sess.ContentType = ct
	}
	start, end, total, hasTotal, isStatus, endUnknown := parseContentRange(cr)
	complete := false
	if isStatus {
		// Status query (bytes */N): finalize once all N bytes are received, so a
		// client whose final chunk was sent without a known total can discover
		// completion and receive the object resource.
		complete = hasTotal && sess.length >= total
	} else {
		// A chunk starting past the persisted byte count is a client-side
		// data-loss condition: reject with 503 (plain text).
		if start > sess.length {
			p.mu.Unlock()
			return nil, offsetGapError(start, sess.length)
		}
		// Only append a chunk that begins exactly where the accumulated bytes
		// end; a rewind (start < length) ignores re-sent bytes.
		if start == sess.length {
			if err := p.appendChunk(sess, media); err != nil {
				p.mu.Unlock()
				return nil, err
			}
			sess.length += int64(len(media))
		}
		if endUnknown {
			// Terminal unknown-end chunk (bytes <start>-*/<total>, emitted by the
			// Node SDK in single-request mode): the whole object is in this
			// request. Validate the declared total, if any, then finalize.
			if hasTotal && sess.length != total {
				p.mu.Unlock()
				return nil, model.NewProviderError("InvalidRequest", "Invalid Content-Range", 400)
			}
			complete = true
		} else {
			complete = hasTotal && end+1 >= total && sess.length >= total
		}
	}
	if complete {
		delete(p.uploads, uploadID)
	}
	bucket := sess.Bucket
	object := sess.Object
	contentType := sess.ContentType
	metadata := sess.Metadata
	length := sess.length
	tmpPath := sess.tmpPath
	var stream io.Reader
	var closeStream func()
	if complete {
		if sess.tmpFile != nil {
			if _, err := sess.tmpFile.Seek(0, io.SeekStart); err != nil {
				p.mu.Unlock()
				return nil, fmt.Errorf("resumable upload: seek spill file: %w", err)
			}
			stream = sess.tmpFile
			closeStream = func() {
				sess.tmpFile.Close()
				os.Remove(sess.tmpPath)
			}
		} else {
			stream = bytes.NewReader(sess.buf)
		}
	}
	p.mu.Unlock()

	// Mirror the session metadata to the durable store so the session survives
	// a process restart (the temp file lives on the local filesystem, matching
	// the blobfs persistence model).
	if complete {
		_ = p.objects.DeleteResumable(ctx, uploadID)
	} else {
		_ = p.objects.UpdateResumable(ctx, gcs.ResumableSession{
			UploadID: uploadID, Bucket: bucket, Name: object, ContentType: contentType,
			Length: length, TmpPath: tmpPath, LastAccess: clock.RealNow(),
		})
	}

	if !complete {
		data := map[string]any{}
		if length > 0 {
			data[wire.RangeKey] = fmt.Sprintf("bytes=0-%d", length-1)
		}
		status := 308
		if no308, _ := nr.Params[wire.No308Key].(bool); no308 {
			// The SDK sets X-GUploader-No-308: yes; signal resume-incomplete
			// with 200 + X-Http-Status-Code-Override instead of a literal 308.
			data[wire.StatusOverrideKey] = "308"
			status = 200
		}
		return &model.ProviderResponse{HTTPStatus: status, Data: data}, nil
	}

	defer func() {
		if closeStream != nil {
			closeStream()
		}
	}()

	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	nr.Params[wire.StreamKey] = stream
	nr.Params[wire.ContentTypeKey] = contentType
	if metadata != nil {
		nr.Params[wire.MetaHeadersKey] = metadata
	}
	resp, err := p.ObjectsInsert(ctx, nr)
	if err != nil {
		return nil, err
	}
	// Record a bounded completion tombstone so a post-completion status query
	// returns 200 + the object (real GCS replays the completed resource).
	p.mu.Lock()
	if len(p.completed) < maxUploadSessions {
		p.completed[uploadID] = &completedSession{
			Bucket:     bucket,
			Object:     object,
			objectJSON: resp.Data,
			lastAccess: clock.RealNow(),
		}
	}
	p.mu.Unlock()
	return resp, nil
}

// appendChunk appends media to an in-progress session, spilling to a temp file
// once the in-memory buffer exceeds the threshold. The caller must hold p.mu.
func (p *Provider) appendChunk(sess *uploadSession, media []byte) error {
	if sess.tmpFile != nil {
		if _, err := sess.tmpFile.Write(media); err != nil {
			return fmt.Errorf("resumable upload: write spill file: %w", err)
		}
		return nil
	}
	if int64(len(sess.buf))+int64(len(media)) > resumableSpillThreshold {
		if err := spillSession(sess); err != nil {
			return err
		}
		if _, err := sess.tmpFile.Write(media); err != nil {
			return fmt.Errorf("resumable upload: write spill file: %w", err)
		}
		return nil
	}
	sess.buf = append(sess.buf, media...)
	return nil
}

// offsetGapError builds the plain-text 503 returned when a resumable chunk
// starts past the persisted byte count. The message text matches live GCS
// (double space after "request."), attested by the Java SDK's put task and SAP
// KB 2840945.
func offsetGapError(start, length int64) *model.ProviderError {
	msg := fmt.Sprintf("Invalid request.  According to the Content-Range header, the upload offset is %d byte(s), which exceeds already uploaded size of %d byte(s).", start, length)
	return (&model.ProviderError{Code: "InvalidRequest", Message: msg, HTTPStatus: 503, Status: gcperr.Unavailable}).WithData(map[string]any{"errorFormat": "plain"})
}

// spillSession flushes the in-memory buffer to a temp file and switches the
// session to file-backed accumulation. The caller must hold p.mu.
func spillSession(sess *uploadSession) error {
	f, err := os.CreateTemp("", "jaiscloud-gcp-resumable-*")
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

// parseContentRange parses a "bytes <start>-<end>/<total>" header, or a status
// query "bytes */<total>". total may be "*" (unknown) in which case hasTotal is
// false. isStatus is true for the "bytes *" status query. endUnknown is true
// when the end position is "*" (bytes <start>-*/<total>), the terminal
// single-request form emitted by Google's Node SDK.
func parseContentRange(cr string) (start, end, total int64, hasTotal, isStatus, endUnknown bool) {
	rest := strings.TrimPrefix(cr, "bytes ")
	rangePart, totalPart, _ := strings.Cut(rest, "/")
	if rangePart == "*" {
		if totalPart != "" && totalPart != "*" {
			total, _ = strconv.ParseInt(totalPart, 10, 64)
			hasTotal = true
		}
		return 0, 0, total, hasTotal, true, false
	}
	se := strings.SplitN(rangePart, "-", 2)
	if len(se) == 2 {
		start, _ = strconv.ParseInt(se[0], 10, 64)
		if se[1] == "*" {
			endUnknown = true
		} else {
			end, _ = strconv.ParseInt(se[1], 10, 64)
		}
	}
	if totalPart != "" && totalPart != "*" {
		total, _ = strconv.ParseInt(totalPart, 10, 64)
		hasTotal = true
	}
	return
}
