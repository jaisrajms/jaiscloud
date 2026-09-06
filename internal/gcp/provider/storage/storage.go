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
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/store/gcs"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// Resource types used in the generic ResourceStore (IAM + ACL; buckets and
// objects live in the dedicated gcs.ObjectStore).
const (
	rtBucketIAM = "gcs_bucket_iam"
	rtObjectIAM = "gcs_object_iam"
	rtACL       = "gcs_acl"
)

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

	genMu sync.Mutex
	gen   int64 // monotonically-increasing object generation counter
}

// uploadSession holds the state of an in-progress resumable upload.
type uploadSession struct {
	Bucket      string
	Object      string
	ContentType string
	buf         []byte    // in-memory bytes up to resumableSpillThreshold
	tmpPath     string    // spill file path once threshold is exceeded
	tmpFile     *os.File  // open handle for appending spilled bytes
	length      int64     // total accumulated bytes across chunks
	lastAccess  time.Time // last chunk/status-query time, for the TTL sweep
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

// nextGen returns a unique, monotonically-increasing object generation.
func (p *Provider) nextGen() string {
	p.genMu.Lock()
	p.gen++
	g := p.gen
	p.genMu.Unlock()
	return strconv.FormatInt(g, 10)
}

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
	p.mu.Unlock()
}

// sweepSessions removes stale in-memory resumable sessions, closing and
// deleting any spill files. The caller must hold p.mu.
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

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Storage.BucketsList":                 p.BucketsList,
		"Storage.BucketsInsert":               p.BucketsInsert,
		"Storage.BucketsGet":                  p.BucketsGet,
		"Storage.BucketsUpdate":               p.BucketsUpdate,
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
	Versioning      map[string]any `json:"versioning,omitempty"`
	RetentionPolicy map[string]any `json:"retentionPolicy,omitempty"`
	Lifecycle       map[string]any `json:"lifecycle,omitempty"`
	Encryption      map[string]any `json:"encryption,omitempty"`
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

// fromStoreObject converts a stored ObjectMeta back into the wire objectMeta,
// re-deriving the derived fields (kind/id/etag/selfLink/mediaLink).
func fromStoreObject(m gcs.ObjectMeta) objectMeta {
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
		TemporaryHold:  m.TemporaryHold,
		EventBasedHold: m.EventBasedHold,
	}
	if !m.TimeCreated.IsZero() {
		o.TimeCreated = m.TimeCreated.Format(time.RFC3339Nano)
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
	o.SelfLink = "https://www.googleapis.com/storage/v1/b/" + m.Bucket + "/o/" + url.PathEscape(m.Name)
	o.MediaLink = "https://www.googleapis.com/download/storage/v1/b/" + m.Bucket + "/o/" + url.PathEscape(m.Name) + "?alt=media"
	return o
}

// blobKey returns the blob store key for an object generation. Every object
// (versioned or not) is stored under a generation-specific key so overwriting a
// versioned object never clobbers a prior generation's bytes.
func blobKey(bucket, object, generation string) string {
	return bucket + "/" + object + "/" + generation
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
	buckets, err := p.objects.ListBuckets(ctx, nr.AccountID)
	if err != nil {
		return nil, err
	}
	page, nextToken := paginateBuckets(buckets, nr.Params)
	items := make([]any, 0, len(page))
	for _, m := range page {
		items = append(items, toBucketMap(mapToBucket(m)))
	}
	resp := map[string]any{"kind": "storage#buckets", "items": items}
	if nextToken != "" {
		resp["nextPageToken"] = nextToken
	}
	return provider.OK(resp), nil
}

func (p *Provider) BucketsInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
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
	b.TimeCreated = clock.Now().Format(time.RFC3339Nano)
	b.Updated = b.TimeCreated

	if err := p.objects.CreateBucket(ctx, nr.AccountID, name, bucketToMap(b)); err != nil {
		if errors.Is(err, gcs.ErrAlreadyExists) {
			return nil, model.NewProviderError("Conflict", "bucket already exists", 409)
		}
		return nil, err
	}
	return provider.OK(toBucketMap(b)), nil
}

func (p *Provider) BucketsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["bucket"].(string)
	meta, err := p.objects.GetBucket(ctx, name)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	return provider.OK(toBucketMap(mapToBucket(meta))), nil
}

func (p *Provider) BucketsUpdate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["bucket"].(string)
	meta, err := p.objects.GetBucket(ctx, name)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	b := mapToBucket(meta)
	// Preserve timeCreated; update only fields present in the request body.
	body, _ := nr.Params["body"].(map[string]any)
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
		b.RetentionPolicy = bodyMap(body, "retentionPolicy")
	}
	if _, ok := body["lifecycle"]; ok {
		b.Lifecycle = bodyMap(body, "lifecycle")
	}
	if _, ok := body["encryption"]; ok {
		b.Encryption = bodyMap(body, "encryption")
	}
	b.Updated = clock.Now().Format(time.RFC3339Nano)
	if err := p.objects.UpdateBucketMeta(ctx, name, bucketToMap(b)); err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		return nil, err
	}
	return provider.OK(toBucketMap(b)), nil
}

func (p *Provider) BucketsDelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["bucket"].(string)
	if err := p.objects.DeleteBucket(ctx, name); err != nil {
		if errors.Is(err, gcs.ErrNoSuchBucket) {
			return nil, model.NewProviderError("NotFound", "bucket not found", 404)
		}
		if errors.Is(err, gcs.ErrBucketNotEmpty) {
			return nil, model.NewProviderError("bucketNotEmpty", "bucket is not empty", 409)
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
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
	delim, _ := nr.Params["delimiter"].(string)
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
			rest := m.Name[len(pfx):]
			if i := strings.Index(rest, delim); i >= 0 {
				common := pfx + rest[:i+len(delim)]
				if !seen[common] {
					seen[common] = true
					all = append(all, listed{name: common})
				}
				continue
			}
			o := fromStoreObject(m)
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

	// No delimiter: prefix filter + pagination.
	var filtered []gcs.ObjectMeta
	for _, m := range objs {
		if pfx == "" || strings.HasPrefix(m.Name, pfx) {
			filtered = append(filtered, m)
		}
	}

	// Cursor pagination over object names.
	limit := maxResults(nr.Params)
	start := 0
	if tok, _ := nr.Params["pageToken"].(string); tok != "" {
		if cursor := decodeCursor(tok); cursor != "" {
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
		next = encodeCursor(page[len(page)-1].Name)
	}

	items := make([]any, 0, len(page))
	for _, m := range page {
		items = append(items, toMap(fromStoreObject(m)))
	}
	resp := map[string]any{"kind": "storage#objects", "items": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) ObjectsInsert(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
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
	id := blobKey(bucket, object, generation)

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
	if body != nil {
		if md, ok := body["metadata"].(map[string]any); ok {
			o.Metadata = make(map[string]string, len(md))
			for k, v := range md {
				if s, ok := v.(string); ok {
					o.Metadata[k] = s
				}
			}
		}
		if h, ok := body["temporaryHold"].(bool); ok {
			o.TemporaryHold = h
		}
		if h, ok := body["eventBasedHold"].(bool); ok {
			o.EventBasedHold = h
		}
	}
	o.ID = bucket + "/" + object + "/" + o.Generation
	o.Etag = "CAE="
	o.SelfLink = "https://www.googleapis.com/storage/v1/b/" + bucket + "/o/" + url.PathEscape(object)
	o.MediaLink = "https://www.googleapis.com/download/storage/v1/b/" + bucket + "/o/" + url.PathEscape(object) + "?alt=media"

	// Capture the prior live generation's blob key so a non-versioned overwrite
	// can clean it up after the new generation is durably stored.
	var priorBlobKey string
	if !versioned {
		if prev, err := p.objects.GetObjectMeta(ctx, bucket, object); err == nil && prev.Generation != generation {
			priorBlobKey = blobKey(bucket, object, prev.Generation)
		}
	}

	// Resolve the object encryption key: CSEK (header) wins, then CMEK
	// (per-object kmsKeyName query param, else the bucket's
	// encryption.defaultKmsKeyName), else the server DEK via Wrap.
	kmsKeyName, cseKey, cseKeySHA256, err := p.resolveWriteKey(nr, bucket, bmeta)
	if err != nil {
		return nil, err
	}

	// Read the raw object bytes. Envelope encryption is applied whole-object
	// with AES-GCM, so the body is buffered; TODO(streaming-AEAD): stream very
	// large objects through a streaming AEAD (e.g. Tink StreamingAead) rather
	// than buffering the full ciphertext in memory.
	var raw []byte
	if stream, ok := nr.Params[wire.StreamKey].(io.Reader); ok {
		raw, err = io.ReadAll(stream)
		if err != nil {
			return nil, err
		}
	} else {
		raw = media
	}

	// Plaintext checksums/size (GCS reports the logical object, not the
	// ciphertext).
	sum := md5.Sum(raw)
	o.Md5Hash = base64.StdEncoding.EncodeToString(sum[:])
	crc := crc32.Checksum(raw, crc32.MakeTable(crc32.Castagnoli))
	crcBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(crcBytes, crc)
	o.Crc32c = base64.StdEncoding.EncodeToString(crcBytes)
	o.Size = strconv.FormatInt(int64(len(raw)), 10)

	// Encrypt and store the ciphertext blob.
	var wrappedDEK []byte
	if cseKey != nil {
		ciphertext, err := kmsstore.EncryptData(cseKey, raw, nil)
		if err != nil {
			return nil, err
		}
		if err := p.blobs.Put(ctx, blobsNamespace, id, ciphertext); err != nil {
			return nil, err
		}
		o.CustomerEncryption = &customerEncryption{EncryptionAlgorithm: "AES256", KeySha256: cseKeySHA256}
	} else {
		rawDEK, wd, err := p.encryptor.Wrap(ctx, nr.AccountID, kmsKeyName)
		if err != nil {
			return nil, err
		}
		wrappedDEK = wd
		ciphertext, err := kmsstore.EncryptData(rawDEK, raw, nil)
		if err != nil {
			return nil, err
		}
		if err := p.blobs.Put(ctx, blobsNamespace, id, ciphertext); err != nil {
			return nil, err
		}
		o.KmsKeyName = kmsKeyName
	}

	storeMeta := toStoreObject(o)
	storeMeta.WrappedDEK = wrappedDEK
	if versioned {
		err = p.objects.PutObjectGeneration(ctx, bucket, object, storeMeta)
	} else {
		err = p.objects.PutObjectMeta(ctx, bucket, object, storeMeta)
	}
	if err != nil {
		// Roll back the just-written blob so a failed metadata write does not
		// leave an orphaned object (metadata absent, blob present).
		_ = p.blobs.Delete(ctx, blobsNamespace, id)
		return nil, err
	}
	if priorBlobKey != "" {
		_ = p.blobs.Delete(ctx, blobsNamespace, priorBlobKey)
	}
	return provider.OK(toMap(o)), nil
}

func (p *Provider) ObjectsGet(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.objectResponse(ctx, nr)
}

func (p *Provider) ObjectsGetMedia(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket, _ := nr.Params["bucket"].(string)
	object, _ := nr.Params["object"].(string)
	// Metadata first: metadata gone → 404; metadata present + blob absent → 500.
	meta, err := p.getObjectForRead(ctx, bucket, object, nr.Params)
	if err != nil {
		return nil, err
	}
	id := blobKey(bucket, object, meta.Generation)
	rc, err := p.blobs.GetStream(ctx, blobsNamespace, id, 0, -1)
	if err != nil {
		return nil, model.NewProviderError("InternalError", "object metadata present but blob missing: "+err.Error(), 500)
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
	return &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"_stream":           io.NopCloser(bytes.NewReader(plain)),
			wire.ContentTypeKey: meta.ContentType,
		},
	}, nil
}

// resolveWriteKey determines the DEK for an object write: CSEK (header) wins,
// then CMEK (per-object kmsKeyName query param, else the bucket's
// encryption.defaultKmsKeyName), else empty (server DEK via Wrap).
func (p *Provider) resolveWriteKey(nr *model.NormalizedRequest, bucket string, bmeta map[string]any) (kmsKeyName string, cseKey []byte, cseKeySHA256 string, err error) {
	if keyB64, _ := nr.Params[wire.CSEKKey].(string); keyB64 != "" {
		key, err := base64.StdEncoding.DecodeString(keyB64)
		if err != nil || len(key) != 32 {
			return "", nil, "", model.NewProviderError("InvalidArgument", "invalid customer-supplied encryption key", 400)
		}
		sum := sha256.Sum256(key)
		expected := base64.StdEncoding.EncodeToString(sum[:])
		gotSHA, _ := nr.Params[wire.CSEKKeySHA256].(string)
		if gotSHA != expected {
			return "", nil, "", model.NewProviderError("InvalidArgument", "customer-supplied encryption key hash mismatch", 400)
		}
		return "", key, expected, nil
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

// decryptObject returns the plaintext for a stored ciphertext, using the CSEK
// key (when the object is CSEK-encrypted) or the envelope DEK otherwise.
func (p *Provider) decryptObject(ctx context.Context, nr *model.NormalizedRequest, meta gcs.ObjectMeta, ciphertext []byte) ([]byte, error) {
	if meta.CSEKeySHA256 != "" {
		keyB64, _ := nr.Params[wire.CSEKKey].(string)
		if keyB64 == "" {
			return nil, model.NewProviderError("InvalidArgument", "missing customer-supplied encryption key", 400)
		}
		key, err := base64.StdEncoding.DecodeString(keyB64)
		if err != nil || len(key) != 32 {
			return nil, model.NewProviderError("InvalidArgument", "invalid customer-supplied encryption key", 400)
		}
		sum := sha256.Sum256(key)
		if base64.StdEncoding.EncodeToString(sum[:]) != meta.CSEKeySHA256 {
			return nil, model.NewProviderError("InvalidArgument", "customer-supplied encryption key mismatch", 400)
		}
		return kmsstore.DecryptData(key, ciphertext, nil)
	}
	rawDEK, err := p.encryptor.Unwrap(ctx, nr.AccountID, meta.KmsKeyName, meta.WrappedDEK)
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
	meta, err := p.objects.GetObjectMeta(ctx, bucket, object)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return nil, model.NewProviderError("NotFound", "object not found", 404)
		}
		return nil, err
	}
	o := fromStoreObject(meta)

	// Strict replace: writable metadata is taken verbatim from the request —
	// omitted fields are cleared.
	o.ContentType, _ = bodyString(nr.Params, "contentType")
	o.Metadata = bodyMetadata(nr.Params)
	o.StorageClass, _ = bodyString(nr.Params, "storageClass")
	if o.StorageClass == "" {
		o.StorageClass = "STANDARD"
	}

	o.Metageneration = bumpMeta(o.Metageneration)
	o.Updated = clock.Now().Format(time.RFC3339Nano)
	if err := p.objects.PutObjectMeta(ctx, bucket, object, toStoreObject(o)); err != nil {
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
	meta, err := p.objects.GetObjectMeta(ctx, bucket, object)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return nil, model.NewProviderError("NotFound", "object not found", 404)
		}
		return nil, err
	}
	// Preserve size/md5Hash/generation/storageClass/timeCreated; update only
	// the fields present in the request and bump metageneration.
	o := fromStoreObject(meta)
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
	}
	o.Metageneration = bumpMeta(o.Metageneration)
	o.Updated = clock.Now().Format(time.RFC3339Nano)
	if err := p.objects.PutObjectMeta(ctx, bucket, object, toStoreObject(o)); err != nil {
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

	// Retention check first: a held or retention-active object cannot be
	// deleted (GCS returns PERMISSION_DENIED).
	meta, err := p.objects.GetObjectMeta(ctx, bucket, object)
	if err != nil {
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return nil, model.NewProviderError("NotFound", "object not found", 404)
		}
		return nil, err
	}
	if objectProtected(meta) {
		return nil, model.NewProviderError("PermissionDenied", "Object is under hold or retention and cannot be deleted", 403)
	}

	if p.bucketVersioned(ctx, bucket) {
		// Versioned bucket: delete only the live generation, leaving a
		// non-live tombstone that appears in ?versions=true listings.
		if _, err := p.objects.TombstoneObjectMeta(ctx, bucket, object); err != nil {
			if errors.Is(err, gcs.ErrNoSuchObject) {
				return nil, model.NewProviderError("NotFound", "object not found", 404)
			}
			return nil, err
		}
		_ = p.blobs.Delete(ctx, blobsNamespace, blobKey(bucket, object, meta.Generation))
		return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
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
	if err := p.objects.DeleteObjectMeta(ctx, bucket, object); err != nil {
		if errors.Is(err, gcs.ErrNoSuchObject) {
			return nil, model.NewProviderError("NotFound", "object not found", 404)
		}
		return nil, err
	}
	for _, id := range blobKeys {
		_ = p.blobs.Delete(ctx, blobsNamespace, id)
	}
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// bucketVersioned reports whether versioning is enabled on the bucket.
func (p *Provider) bucketVersioned(ctx context.Context, bucket string) bool {
	bmeta, err := p.objects.GetBucket(ctx, bucket)
	if err != nil {
		return false
	}
	if v, ok := bmeta["versioning"].(map[string]any); ok {
		if en, _ := v["enabled"].(bool); en {
			return true
		}
	}
	return false
}

// objectProtected reports whether an object's holds or active retention block
// deletion. While temporaryHold/eventBasedHold is set, or the object's
// retention expiration is still in the future, the object cannot be deleted.
func objectProtected(m gcs.ObjectMeta) bool {
	if m.TemporaryHold || m.EventBasedHold {
		return true
	}
	if m.Retention != nil && !m.Retention.RetainUntilTime.IsZero() {
		return clock.Now().Before(m.Retention.RetainUntilTime)
	}
	return false
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

// toBucketMap converts a bucketMeta struct into a map for JSON-encoding.
func toBucketMap(b bucketMeta) map[string]any {
	versioning := b.Versioning
	if versioning == nil {
		versioning = map[string]any{"enabled": false}
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
		"metageneration": "1",
		"projectNumber":  "0",
		"selfLink":       "https://www.googleapis.com/storage/v1/b/" + b.Name,
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
	meta, err := p.getObjectForRead(ctx, bucket, object, nr.Params)
	if err != nil {
		return nil, err
	}
	o := fromStoreObject(meta)
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

func maxResults(params map[string]any) int {
	if v, ok := params["maxResults"].(string); ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
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
	ct, _ := nr.Params[wire.ContentTypeKey].(string)

	id := p.nextGen() // atomic + monotonic, collision-free under concurrency
	now := clock.RealNow()
	p.mu.Lock()
	p.sweepSessions()
	if len(p.uploads) >= maxUploadSessions {
		p.mu.Unlock()
		return nil, model.NewProviderError("InvalidRequest", "too many active resumable uploads", 429)
	}
	p.uploads[id] = &uploadSession{Bucket: bucket, Object: object, ContentType: ct, lastAccess: now}
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
		p.mu.Unlock()
		return nil, model.NewProviderError("NotFound", "unknown upload_id", 404)
	}
	sess.lastAccess = clock.RealNow()
	if ct, _ := nr.Params[wire.ContentTypeKey].(string); ct != "" {
		sess.ContentType = ct
	}
	start, end, total, hasTotal, isStatus := parseContentRange(cr)
	complete := false
	if isStatus {
		// Status query (bytes */N): finalize once all N bytes are received, so a
		// client whose final chunk was sent without a known total can discover
		// completion and receive the object resource.
		complete = hasTotal && sess.length >= total
	} else {
		// Offset repair: only append a chunk that begins exactly where the
		// accumulated bytes end; out-of-order/duplicate chunks are ignored.
		if sess.length == start {
			if sess.tmpFile != nil {
				if _, err := sess.tmpFile.Write(media); err != nil {
					p.mu.Unlock()
					return nil, fmt.Errorf("resumable upload: write spill file: %w", err)
				}
			} else if int64(len(sess.buf))+int64(len(media)) > resumableSpillThreshold {
				if err := spillSession(sess); err != nil {
					p.mu.Unlock()
					return nil, err
				}
				if _, err := sess.tmpFile.Write(media); err != nil {
					p.mu.Unlock()
					return nil, fmt.Errorf("resumable upload: write spill file: %w", err)
				}
			} else {
				sess.buf = append(sess.buf, media...)
			}
			sess.length += int64(len(media))
		}
		complete = hasTotal && end+1 >= total && sess.length >= total
	}
	if complete {
		delete(p.uploads, uploadID)
	}
	bucket := sess.Bucket
	object := sess.Object
	contentType := sess.ContentType
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
		rng := fmt.Sprintf("bytes=0-%d", length-1)
		if length == 0 {
			rng = "bytes=0-0"
		}
		data := map[string]any{wire.RangeKey: rng}
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
	return p.ObjectsInsert(ctx, nr)
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
// false. isStatus is true for the "bytes *" status query.
func parseContentRange(cr string) (start, end, total int64, hasTotal, isStatus bool) {
	rest := strings.TrimPrefix(cr, "bytes ")
	rangePart, totalPart, _ := strings.Cut(rest, "/")
	if rangePart == "*" {
		if totalPart != "" && totalPart != "*" {
			total, _ = strconv.ParseInt(totalPart, 10, 64)
			hasTotal = true
		}
		return 0, 0, total, hasTotal, true
	}
	se := strings.SplitN(rangePart, "-", 2)
	if len(se) == 2 {
		start, _ = strconv.ParseInt(se[0], 10, 64)
		end, _ = strconv.ParseInt(se[1], 10, 64)
	}
	if totalPart != "" && totalPart != "*" {
		total, _ = strconv.ParseInt(totalPart, 10, 64)
		hasTotal = true
	}
	return
}
