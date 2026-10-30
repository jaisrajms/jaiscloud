// Package object implements the S3 provider (ObjectProvider).
package object

import (
	"bufio"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/aws/store/object"
	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// ObjectProvider handles all S3 operations.
type ObjectProvider struct {
	meta       objectstore.ObjectMetaStore
	blobs      blobfs.BlobStore
	resources  store.ResourceStore
	notifStore sync.Map
	bus        *events.EventBus
	fanout     S3FanoutConfig
}

func New(meta objectstore.ObjectMetaStore, blobs blobfs.BlobStore) *ObjectProvider {
	return &ObjectProvider{meta: meta, blobs: blobs}
}

// NewWithBus constructs an ObjectProvider with an event bus for S3 notification dispatch.
func NewWithBus(meta objectstore.ObjectMetaStore, blobs blobfs.BlobStore, bus *events.EventBus) *ObjectProvider {
	return &ObjectProvider{meta: meta, blobs: blobs, bus: bus}
}

// WithResourceStore attaches a generic ResourceStore for access points and other metadata.
func (p *ObjectProvider) WithResourceStore(res store.ResourceStore) *ObjectProvider {
	p.resources = res
	return p
}

func (p *ObjectProvider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		// Bucket operations
		"Object.CreateBucket":      p.CreateBucket,
		"Object.DeleteBucket":      p.DeleteBucket,
		"Object.ListBuckets":       p.ListBuckets,
		"Object.GetBucketLocation": p.GetBucketLocation,
		"Object.HeadBucket":        p.HeadBucket,
		// Object operations
		"Object.PutObject":    p.PutObject,
		"Object.GetObject":    p.GetObject,
		"Object.HeadObject":   p.HeadObject,
		"Object.DeleteObject": p.DeleteObject,
		"Object.CopyObject":   p.CopyObject,
		// Listing
		"Object.ListObjectsV1": p.ListObjectsV1,
		"Object.ListObjectsV2": p.ListObjectsV2,
		"Object.DeleteObjects": p.DeleteObjects,
		// Multipart
		"Object.CreateMultipartUpload":   p.CreateMultipartUpload,
		"Object.UploadPart":              p.UploadPart,
		"Object.UploadPartCopy":          p.UploadPartCopy,
		"Object.CompleteMultipartUpload": p.CompleteMultipartUpload,
		"Object.AbortMultipartUpload":    p.AbortMultipartUpload,
		"Object.ListMultipartUploads":    p.ListMultipartUploads,
		"Object.ListParts":               p.ListParts,
		// Tagging (P2-7)
		"Object.PutObjectTagging":    p.PutObjectTagging,
		"Object.GetObjectTagging":    p.GetObjectTagging,
		"Object.DeleteObjectTagging": p.DeleteObjectTagging,
		"Object.PutBucketTagging":    p.PutBucketTagging,
		"Object.GetBucketTagging":    p.GetBucketTagging,
		"Object.DeleteBucketTagging": p.DeleteBucketTagging,
		// Encryption (P2-1)
		"Object.PutBucketEncryption":    p.PutBucketEncryption,
		"Object.GetBucketEncryption":    p.GetBucketEncryption,
		"Object.DeleteBucketEncryption": p.DeleteBucketEncryption,
		// Versioning (P2-2)
		"Object.PutBucketVersioning": p.PutBucketVersioning,
		"Object.GetBucketVersioning": p.GetBucketVersioning,
		"Object.ListObjectVersions":  p.ListObjectVersions,
		// Object Lock (P2-3)
		"Object.PutObjectLockConfiguration": p.PutObjectLockConfiguration,
		"Object.GetObjectLockConfiguration": p.GetObjectLockConfiguration,
		"Object.PutObjectRetention":         p.PutObjectRetention,
		"Object.GetObjectRetention":         p.GetObjectRetention,
		"Object.PutObjectLegalHold":         p.PutObjectLegalHold,
		"Object.GetObjectLegalHold":         p.GetObjectLegalHold,
		// ACLs (P2-4)
		"Object.GetBucketAcl": p.GetBucketAcl,
		"Object.PutBucketAcl": p.PutBucketAcl,
		"Object.GetObjectAcl": p.GetObjectAcl,
		"Object.PutObjectAcl": p.PutObjectAcl,
		// Ownership Controls (P4.3)
		"Object.PutBucketOwnershipControls":    p.PutBucketOwnershipControls,
		"Object.GetBucketOwnershipControls":    p.GetBucketOwnershipControls,
		"Object.DeleteBucketOwnershipControls": p.DeleteBucketOwnershipControls,
		// GetObjectAttributes (P4.12)
		"Object.GetObjectAttributes": p.GetObjectAttributes,
		// Lifecycle (P2-5)
		"Object.PutBucketLifecycleConfiguration": p.PutBucketLifecycleConfiguration,
		"Object.GetBucketLifecycleConfiguration": p.GetBucketLifecycleConfiguration,
		"Object.DeleteBucketLifecycle":           p.DeleteBucketLifecycle,
		// CORS (P2-6)
		"Object.PutBucketCors":    p.PutBucketCors,
		"Object.GetBucketCors":    p.GetBucketCors,
		"Object.DeleteBucketCors": p.DeleteBucketCors,
		// Notifications (P5.1)
		"Object.PutBucketNotificationConfiguration": p.PutBucketNotificationConfiguration,
		"Object.GetBucketNotificationConfiguration": p.GetBucketNotificationConfiguration,
		// S3 Select (P15.9)
		"Object.SelectObjectContent": p.SelectObjectContent,
		// S3 Access Points (P15.10)
		"Object.CreateAccessPoint":          p.CreateAccessPoint,
		"Object.GetAccessPoint":             p.GetAccessPoint,
		"Object.ListAccessPoints":           p.ListAccessPoints,
		"Object.DeleteAccessPoint":          p.DeleteAccessPoint,
		"Object.PutAccessPointPolicy":       p.PutAccessPointPolicy,
		"Object.GetAccessPointPolicy":       p.GetAccessPointPolicy,
		"Object.DeleteAccessPointPolicy":    p.DeleteAccessPointPolicy,
		"Object.GetAccessPointPolicyStatus": p.GetAccessPointPolicyStatus,
		// Policy / Website / Logging / Replication (P1.16-1.19)
		"Object.PutBucketPolicy":         p.PutBucketPolicy,
		"Object.GetBucketPolicy":         p.GetBucketPolicy,
		"Object.DeleteBucketPolicy":      p.DeleteBucketPolicy,
		"Object.PutBucketWebsite":        p.PutBucketWebsite,
		"Object.GetBucketWebsite":        p.GetBucketWebsite,
		"Object.DeleteBucketWebsite":     p.DeleteBucketWebsite,
		"Object.PutBucketLogging":        p.PutBucketLogging,
		"Object.GetBucketLogging":        p.GetBucketLogging,
		"Object.PutBucketReplication":    p.PutBucketReplication,
		"Object.GetBucketReplication":    p.GetBucketReplication,
		"Object.DeleteBucketReplication": p.DeleteBucketReplication,
		// S3 Batch Operations — not yet implemented
		"Object.CreateJob":         p.notImplemented("CreateJob (S3 Batch Operations)"),
		"Object.ListJobs":          p.notImplemented("ListJobs (S3 Batch Operations)"),
		"Object.DescribeJob":       p.notImplemented("DescribeJob (S3 Batch Operations)"),
		"Object.UpdateJobPriority": p.notImplemented("UpdateJobPriority (S3 Batch Operations)"),
		"Object.UpdateJobStatus":   p.notImplemented("UpdateJobStatus (S3 Batch Operations)"),
	}
}

func (p *ObjectProvider) stub(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return provider.OK(map[string]any{}), nil
}

// notImplemented returns a HandlerFunc that always responds with HTTP 501 and a
// clear "planned for future release" message.
func (p *ObjectProvider) notImplemented(op string) provider.HandlerFunc {
	return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return nil, model.NewProviderError(
			"UnsupportedOperation",
			op+" is not yet implemented in JaisCloud. This feature is planned for a future release. "+
				"Please open an issue at https://github.com/jaisrajms/jaiscloud/issues to track progress or request prioritization.",
			501,
		)
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func strParam(params map[string]any, key string) string {
	if v, ok := params[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func intParam(params map[string]any, key string, def int) int {
	if v, ok := params[key]; ok {
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		case int64:
			return int(n)
		case string:
			if i, err := strconv.Atoi(n); err == nil {
				return i
			}
		}
	}
	return def
}

func newUploadID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func etag(data []byte) string {
	return fmt.Sprintf(`"%x"`, md5.Sum(data))
}

// crc32Base64 returns the base64-encoded IEEE CRC32 of data, matching the
// x-amz-checksum-crc32 format expected by AWS SDK v2.
func crc32Base64(data []byte) string {
	sum := crc32.ChecksumIEEE(data)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, sum)
	return base64.StdEncoding.EncodeToString(b)
}

// crc32Base64FromHash encodes the running CRC32 hash state as base64.
func crc32Base64FromHash(h *crc32State) string {
	sum := h.Sum32()
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, sum)
	return base64.StdEncoding.EncodeToString(b)
}

// crc32State wraps hash/crc32 as an io.Writer so it can feed a TeeReader.
type crc32State struct{ h hash.Hash32 }

func newCRC32() *crc32State                       { return &crc32State{h: crc32.NewIEEE()} }
func (c *crc32State) Write(p []byte) (int, error) { return c.h.Write(p) }
func (c *crc32State) Sum32() uint32               { return c.h.Sum32() }

// computeChecksumValue computes and base64-encodes a checksum for data using algo.
// algo is one of: "CRC32", "CRC32C", "SHA1", "SHA256".
func computeChecksumValue(data []byte, algo string) string {
	switch algo {
	case "CRC32":
		sum := crc32.ChecksumIEEE(data)
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, sum)
		return base64.StdEncoding.EncodeToString(b)
	case "CRC32C":
		sum := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
		b := make([]byte, 4)
		binary.BigEndian.PutUint32(b, sum)
		return base64.StdEncoding.EncodeToString(b)
	case "SHA1":
		h := sha1.Sum(data)
		return base64.StdEncoding.EncodeToString(h[:])
	case "SHA256":
		h := sha256.Sum256(data)
		return base64.StdEncoding.EncodeToString(h[:])
	}
	return ""
}

// writeChecksums writes from r to the BlobStore and returns the MD5 ETag,
// CRC32 checksum (base64), and byte count — all computed in a single pass.
// If extraAlgo is non-empty, also computes and returns that checksum.
func (p *ObjectProvider) writeChecksums(ctx context.Context, bucket, key string, r io.Reader, extraAlgo string) (etagVal, crc32Val, extraChecksum string, n int64, err error) {
	md5h := md5.New()
	crc32h := newCRC32()
	writers := []io.Writer{md5h, crc32h}

	var extraH hash.Hash
	var crc32cTable = crc32.MakeTable(crc32.Castagnoli)
	switch extraAlgo {
	case "CRC32C":
		extraH = crc32.New(crc32cTable)
	case "SHA1":
		extraH = sha1.New()
	case "SHA256":
		extraH = sha256.New()
	}
	if extraH != nil {
		writers = append(writers, extraH)
	}

	tee := io.TeeReader(r, io.MultiWriter(writers...))
	n, err = p.blobs.PutStream(ctx, bucket, key, tee)
	if err != nil {
		return "", "", "", 0, err
	}
	etagVal = fmt.Sprintf(`"%x"`, md5h.Sum(nil))
	crc32Val = crc32Base64FromHash(crc32h)
	if extraH != nil {
		switch extraAlgo {
		case "CRC32C":
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, extraH.(hash.Hash32).Sum32())
			extraChecksum = base64.StdEncoding.EncodeToString(b)
		default:
			extraChecksum = base64.StdEncoding.EncodeToString(extraH.Sum(nil))
		}
	}
	return etagVal, crc32Val, extraChecksum, n, nil
}

// bodyReader returns an io.Reader for the request body, decoding aws-chunked
// framing when present. Used by the streaming upload path.
func bodyReader(nr *model.NormalizedRequest) io.Reader {
	r := nr.Raw.Body
	if strings.Contains(strings.ToLower(nr.Raw.Header.Get("Content-Encoding")), "aws-chunked") ||
		strings.HasPrefix(nr.Raw.Header.Get("x-amz-content-sha256"), "STREAMING-") ||
		nr.Raw.Header.Get("x-amz-decoded-content-length") != "" {
		return newAWSChunkedReader(r)
	}
	return r
}

// ─── aws-chunked streaming decoder ───────────────────────────────────────────
//
// AWS SDK v2 sends large uploads with Content-Encoding: aws-chunked.
// Format: "<hex-size>[;chunk-signature=...]\r\n<data>\r\n" … "0[;...]\r\n\r\n"

type awsChunkedReader struct {
	r         *bufio.Reader
	chunkLeft int64
	done      bool
}

func newAWSChunkedReader(r io.Reader) *awsChunkedReader {
	return &awsChunkedReader{r: bufio.NewReaderSize(r, 32*1024)}
}

func (a *awsChunkedReader) Read(p []byte) (int, error) {
	if a.done {
		return 0, io.EOF
	}
	for a.chunkLeft == 0 {
		line, err := a.r.ReadString('\n')
		if err != nil {
			return 0, err
		}
		line = strings.TrimRight(line, "\r\n")
		if semi := strings.IndexByte(line, ';'); semi >= 0 {
			line = line[:semi]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		size, err := strconv.ParseInt(line, 16, 64)
		if err != nil {
			return 0, fmt.Errorf("aws-chunked: invalid chunk size %q: %w", line, err)
		}
		if size == 0 {
			a.done = true
			return 0, io.EOF
		}
		a.chunkLeft = size
	}
	if int64(len(p)) > a.chunkLeft {
		p = p[:a.chunkLeft]
	}
	n, err := a.r.Read(p)
	a.chunkLeft -= int64(n)
	if a.chunkLeft == 0 && err == nil {
		// Consume the trailing \r\n after the chunk data.
		a.r.ReadString('\n')
	}
	return n, err
}

// ─── seqPartReader ────────────────────────────────────────────────────────────
//
// Reads multipart upload parts sequentially, opening one at a time to keep
// the number of concurrent open file descriptors to O(1).

type seqPartReader struct {
	ctx      context.Context
	blobs    blobfs.BlobStore
	bucket   string
	uploadID string
	parts    []objectstore.PartMeta
	idx      int
	current  io.ReadCloser
}

func (s *seqPartReader) Read(p []byte) (int, error) {
	for {
		if s.current == nil {
			if s.idx >= len(s.parts) {
				return 0, io.EOF
			}
			part := s.parts[s.idx]
			s.idx++
			partKey := fmt.Sprintf("%s/part%d", s.uploadID, part.PartNumber)
			rc, err := s.blobs.GetStream(s.ctx, s.bucket+"/__parts__", partKey, 0, -1)
			if err != nil {
				return 0, err
			}
			s.current = rc
		}
		n, err := s.current.Read(p)
		if err == io.EOF {
			s.current.Close()
			s.current = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (s *seqPartReader) Close() error {
	if s.current != nil {
		return s.current.Close()
	}
	return nil
}

// ─── shared helpers used across files ────────────────────────────────────────

// computeMultipartETag computes the S3 multipart ETag: hex(md5(concat(binary_etags))) + "-N".
func computeMultipartETag(etags []string) string {
	h := md5.New()
	for _, e := range etags {
		raw := strings.Trim(e, `"`)
		if b, err := hex.DecodeString(raw); err == nil {
			h.Write(b)
		} else {
			h.Write([]byte(raw))
		}
	}
	return fmt.Sprintf(`"%x-%d"`, h.Sum(nil), len(etags))
}

// extractUploadMeta recovers ContentType and user metadata from an InitMultipart meta map.
func extractUploadMeta(m map[string]any) (contentType string, metadata map[string]string) {
	contentType = "application/octet-stream"
	if m == nil {
		return
	}
	if ct, ok := m["content-type"].(string); ok && ct != "" {
		contentType = ct
	}
	switch um := m["user-metadata"].(type) {
	case map[string]string:
		metadata = um
	case map[string]interface{}:
		metadata = make(map[string]string, len(um))
		for k, v := range um {
			if s, ok := v.(string); ok {
				metadata[k] = s
			}
		}
	}
	return
}

// objectCondMeta holds the fields needed for conditional-request evaluation.
// Using a local type keeps checkConditions decoupled from the store package.
type objectCondMeta struct {
	ETag         string
	LastModified time.Time
	ContentType  string
}

// checkConditions evaluates conditional request headers per RFC 7232 §6 ordering:
// If-Match → If-Unmodified-Since → If-None-Match → If-Modified-Since.
// Returns (304 response, nil) for Not Modified, (nil, 412 error) for Precondition Failed,
// or (nil, nil) when all conditions are satisfied.
func checkConditions(nr *model.NormalizedRequest, m objectCondMeta) (*model.ProviderResponse, error) {
	etagRaw := strings.Trim(m.ETag, `"`)
	notModifiedResp := func() *model.ProviderResponse {
		return &model.ProviderResponse{HTTPStatus: 304, Data: map[string]any{
			"ETag":         m.ETag,
			"ContentType":  m.ContentType,
			"LastModified": m.LastModified.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"),
		}}
	}
	if v := strParam(nr.Params, "_cond_if_match"); v != "" {
		if v != "*" && strings.Trim(v, `"`) != etagRaw {
			return nil, model.NewProviderError("PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412)
		}
	}
	if v := strParam(nr.Params, "_cond_if_unmodified_since"); v != "" {
		if t, err := parseHTTPDate(v); err == nil && m.LastModified.After(t) {
			return nil, model.NewProviderError("PreconditionFailed", "At least one of the pre-conditions you specified did not hold", 412)
		}
	}
	if v := strParam(nr.Params, "_cond_if_none_match"); v != "" {
		if v == "*" || strings.Trim(v, `"`) == etagRaw {
			return notModifiedResp(), nil
		}
	}
	if v := strParam(nr.Params, "_cond_if_modified_since"); v != "" {
		if t, err := parseHTTPDate(v); err == nil && !m.LastModified.After(t) {
			return notModifiedResp(), nil
		}
	}
	return nil, nil
}

// checkCopySourceConditions evaluates x-amz-copy-source-if-* params (set by S3Codec).
// Returns (nil, PreconditionFailed 412) when a condition fails, (nil, nil) when all pass.
func checkCopySourceConditions(nr *model.NormalizedRequest, m objectstore.ObjectMeta) (*model.ProviderResponse, error) {
	etagRaw := strings.Trim(m.ETag, `"`)
	if v := strParam(nr.Params, "_copy_source_if_match"); v != "" {
		if v != "*" && strings.Trim(v, `"`) != etagRaw {
			return nil, model.NewProviderError("PreconditionFailed", "At least one of the copy-source pre-conditions did not hold", 412)
		}
	}
	if v := strParam(nr.Params, "_copy_source_if_unmodified_since"); v != "" {
		if t, err := parseHTTPDate(v); err == nil && m.LastModified.After(t) {
			return nil, model.NewProviderError("PreconditionFailed", "At least one of the copy-source pre-conditions did not hold", 412)
		}
	}
	if v := strParam(nr.Params, "_copy_source_if_none_match"); v != "" {
		if v == "*" || strings.Trim(v, `"`) == etagRaw {
			return nil, model.NewProviderError("PreconditionFailed", "At least one of the copy-source pre-conditions did not hold", 412)
		}
	}
	if v := strParam(nr.Params, "_copy_source_if_modified_since"); v != "" {
		if t, err := parseHTTPDate(v); err == nil && !m.LastModified.After(t) {
			return nil, model.NewProviderError("PreconditionFailed", "At least one of the copy-source pre-conditions did not hold", 412)
		}
	}
	return nil, nil
}

func parseHTTPDate(s string) (time.Time, error) {
	for _, layout := range []string{time.RFC1123, "Mon, 02-Jan-2006 15:04:05 MST", time.ANSIC} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid HTTP date: %q", s)
}

func urlEncode(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

// newVersionID generates a random hex version identifier.
func newVersionID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// blobKeyForVersion returns the blob store key for a versioned object.
// Versioned blobs are stored under the __jc_ver__ prefix so they never
// collide with unversioned blob keys (which use the S3 key directly).
// The null/empty versionID case maps to the same path as the unversioned key
// (Suspended-mode behaviour: only one "null" version slot exists per key).
func blobKeyForVersion(key, versionID string) string {
	if versionID == "" || versionID == "null" {
		return key
	}
	return "__jc_ver__/" + versionID + "/" + key
}

// ─── updateBucketConfig ───────────────────────────────────────────────────────

func (p *ObjectProvider) updateBucketConfig(ctx context.Context, bucket string, mutate func(meta map[string]any)) error {
	meta, err := p.meta.GetBucket(ctx, bucket)
	if err != nil {
		return err
	}
	mutate(meta)
	return p.meta.UpdateBucketMeta(ctx, bucket, meta)
}

// ─── Internal helpers (cross-provider) ───────────────────────────────────────

// InternalPutObject stores body into bucket/key directly, bypassing the HTTP
// codec.  Used by other providers (EMR log upload, etc.) to write to S3 without
// going through the full request pipeline.  Creates the bucket if it does not
// already exist (idempotent).
func (p *ObjectProvider) InternalPutObject(ctx context.Context, bucket, key, contentType string, body []byte) error {
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	// Ensure the bucket exists — create it silently if absent.
	if _, err := p.meta.GetBucket(ctx, bucket); err != nil {
		if createErr := p.meta.CreateBucket(ctx, bucket, map[string]any{"name": bucket}); createErr != nil {
			// Ignore "already exists" race; any other error is fatal.
			if _, checkErr := p.meta.GetBucket(ctx, bucket); checkErr != nil {
				return fmt.Errorf("InternalPutObject: create bucket %s: %w", bucket, createErr)
			}
		}
	}
	if err := p.blobs.Put(ctx, bucket, key, body); err != nil {
		return fmt.Errorf("InternalPutObject: put blob %s/%s: %w", bucket, key, err)
	}
	etagVal := etag(body)
	crc32Val := crc32Base64(body)
	meta := objectstore.ObjectMeta{
		Key:          key,
		ETag:         etagVal,
		CRC32:        crc32Val,
		Size:         int64(len(body)),
		ContentType:  contentType,
		LastModified: clock.Now(),
		StorageClass: "STANDARD",
	}
	return p.meta.PutObjectMeta(ctx, bucket, key, meta)
}

// InternalListObjects returns all object keys in bucket with the given prefix.
// Used by Glue crawlers for schema inference.
func (p *ObjectProvider) InternalListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	objs, _, _, _, err := p.meta.ListObjectMeta(ctx, bucket, prefix, "", "", 10000)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(objs))
	for _, o := range objs {
		keys = append(keys, o.Key)
	}
	return keys, nil
}

// InternalGetObject fetches the body of an object.
// Used by Glue crawlers for schema inference.
func (p *ObjectProvider) InternalGetObject(ctx context.Context, bucket, key string) ([]byte, error) {
	return p.blobs.Get(ctx, bucket, key)
}

// putObjectInternal serves PutObject for origin=internal callers (EMR and EMR on
// EKS log upload, Firehose delivery): the bucket is created on demand and no
// bucket notification fires, exactly like InternalPutObject. The body is
// "_body" ([]byte), the same key a wire PutObject uses.
func (p *ObjectProvider) putObjectInternal(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body, _ := nr.Params["_body"].([]byte)
	if err := p.InternalPutObject(ctx, strParam(nr.Params, "_bucket"), strParam(nr.Params, "_key"),
		strParam(nr.Params, "_content_type"), body); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// getObjectInternal serves GetObject for origin=internal callers (Glue crawlers).
// Unlike the wire response, which streams, the body comes back whole as "_body"
// ([]byte) so a plugin can read or replace it.
func (p *ObjectProvider) getObjectInternal(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body, err := p.InternalGetObject(ctx, strParam(nr.Params, "_bucket"), strParam(nr.Params, "_key"))
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
	}
	return provider.OK(map[string]any{"_body": body}), nil
}
