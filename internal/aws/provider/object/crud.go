package object

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	objectstore "jaiscloud/internal/aws/store/object"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// ─── Objects ──────────────────────────────────────────────────────────────────

func (p *ObjectProvider) PutObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if nr.Origin == model.OriginInternal {
		return p.putObjectInternal(ctx, nr)
	}
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	contentType := strParam(nr.Params, "_content_type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// If-None-Match: * — fail if object already exists.
	if strParam(nr.Params, "_cond_if_none_match") == "*" {
		if _, err := p.meta.GetObjectMeta(ctx, bucket, key); err == nil {
			return nil, model.NewProviderError("PreconditionFailed",
				"At least one of the pre-conditions you specified did not hold", 412)
		}
	}

	// P2-2: Determine blob storage key before writing so versioned blobs land at
	// the right path and GetObject can find them via blobKeyForVersion.
	vStatus, _ := p.meta.GetBucketVersioning(ctx, bucket)
	var preVersionID string
	blobKey := key
	switch vStatus {
	case objectstore.VersioningEnabled:
		preVersionID = newVersionID()
		blobKey = blobKeyForVersion(key, preVersionID)
	case objectstore.VersioningSuspended:
		preVersionID = "null"
		blobKey = blobKeyForVersion(key, "null")
	}

	var etagVal, crc32Val string
	var size int64

	// Determine client-supplied checksum algo (if any) for P4.4 validation.
	checksumAlgo := ""
	if checksumHdr := strParam(nr.Params, "_checksum_header"); checksumHdr != "" {
		checksumAlgo = strings.ToUpper(strings.TrimPrefix(checksumHdr, "x-amz-checksum-"))
		checksumAlgo = strings.ReplaceAll(checksumAlgo, "-", "")
	}
	expectedChecksum := strParam(nr.Params, "_checksum_value")

	var computedChecksum string
	if _, streaming := nr.Params["_streaming"]; streaming {
		// Streaming path: body arrives via nr.Raw.Body (gateway skipped io.ReadAll).
		var err error
		etagVal, crc32Val, computedChecksum, size, err = p.writeChecksums(ctx, bucket, blobKey, bodyReader(nr), checksumAlgo)
		if err != nil {
			return nil, err
		}
		// writeChecksums returns extraChecksum only for CRC32C/SHA1/SHA256.
		// CRC32 is always computed as crc32Val; use it here to match the non-streaming path.
		if checksumAlgo == "CRC32" {
			computedChecksum = crc32Val
		}
	} else {
		body, _ := nr.Params["_body"].([]byte)
		if err := p.blobs.Put(ctx, bucket, blobKey, body); err != nil {
			return nil, err
		}
		etagVal = etag(body)
		crc32Val = crc32Base64(body)
		size = int64(len(body))
		if checksumAlgo != "" {
			if checksumAlgo == "CRC32" {
				computedChecksum = crc32Val
			} else {
				computedChecksum = computeChecksumValue(body, checksumAlgo)
			}
		}
	}

	// P4.4: Validate client-supplied checksum; BadDigest on mismatch.
	if checksumAlgo != "" && expectedChecksum != "" && computedChecksum != expectedChecksum {
		_ = p.blobs.Delete(ctx, bucket, blobKey)
		return nil, model.NewProviderError("BadDigest",
			"The Content-MD5 or checksum you specified did not match what we received.", 400)
	}

	meta := objectstore.ObjectMeta{
		Key:          key,
		ETag:         etagVal,
		CRC32:        crc32Val,
		Size:         size,
		ContentType:  contentType,
		LastModified: clock.Now(),
		StorageClass: "STANDARD",
		Metadata:     extractUserMetadata(nr.Params),
	}
	if checksumAlgo != "" {
		meta.ChecksumAlgorithm = checksumAlgo
		if computedChecksum != "" {
			meta.ChecksumValue = computedChecksum
		} else {
			meta.ChecksumValue = expectedChecksum
		}
	}
	// P2-7: Tagging
	if tagging := strParam(nr.Params, "_tagging"); tagging != "" {
		if tags, err := parseTaggingHeader(tagging); err == nil {
			if err := validateTags(tags, 10); err != nil {
				return nil, model.NewProviderError("InvalidTag", err.Error(), 400)
			}
			meta.Tags = tags
		}
	}
	// P2-1: SSE
	enc, kmsKey, ssecMD5, sseErr := p.resolveSSE(ctx, nr, bucket)
	if sseErr != nil {
		return nil, sseErr
	}
	meta.Encryption = enc
	meta.KMSKeyID = kmsKey
	meta.SSECKeyMD5 = ssecMD5
	// P2-4: ACL
	meta.ACL = resolveACL(strParam(nr.Params, "_acl"), nr.AccountID)

	// P4.1: Object lock headers
	lockMode := strParam(nr.Params, "_lock_mode")
	lockUntilStr := strParam(nr.Params, "_lock_retain_until_date")
	legalHold := strParam(nr.Params, "_lock_legal_hold")
	if lockMode != "" && lockUntilStr == "" {
		return nil, model.NewProviderError("InvalidArgument",
			"x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied", 400)
	}
	if lockMode == "" && lockUntilStr != "" {
		return nil, model.NewProviderError("InvalidArgument",
			"x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied", 400)
	}
	if lockMode != "" && lockMode != "GOVERNANCE" && lockMode != "COMPLIANCE" {
		return nil, model.NewProviderError("InvalidArgument", "Unknown wormMode directive.", 400)
	}
	var lockRetainUntil *time.Time
	if lockUntilStr != "" {
		t, _ := time.Parse(time.RFC3339, lockUntilStr)
		lockRetainUntil = &t
	}
	if lockMode == "" {
		if bucketMeta, err := p.meta.GetBucket(ctx, bucket); err == nil {
			if lockCfg, ok := bucketMeta["object_lock_config"].(map[string]any); ok {
				if defaultMode, _ := lockCfg["DefaultMode"].(string); defaultMode != "" {
					lockMode = defaultMode
					days := 0
					switch d := lockCfg["DefaultDays"].(type) {
					case float64:
						days = int(d)
					case int:
						days = d
					}
					t := clock.Now().Add(time.Duration(days) * 24 * time.Hour)
					lockRetainUntil = &t
				}
			}
		}
	}
	meta.LockMode = lockMode
	meta.LockRetainUntil = lockRetainUntil
	meta.LegalHoldStatus = legalHold

	// P2-2: Versioning — use pre-generated versionID so metadata and blob agree.
	var versionID string
	if vStatus == objectstore.VersioningEnabled {
		meta.VersionID = preVersionID
		var verr error
		versionID, verr = p.meta.PutObjectVersion(ctx, bucket, key, meta)
		if verr != nil {
			return nil, model.NewProviderError("InternalError", verr.Error(), 500)
		}
		if err := p.meta.PutObjectMeta(ctx, bucket, key, meta); err != nil {
			slog.Warn("versioned PutObject: failed to update current-object pointer", "bucket", bucket, "key", key, "err", err)
		}
	} else if vStatus == objectstore.VersioningSuspended {
		meta.VersionID = "null"
		versionID, _ = p.meta.PutObjectVersion(ctx, bucket, key, meta)
		if err := p.meta.PutObjectMeta(ctx, bucket, key, meta); err != nil {
			slog.Warn("suspended PutObject: failed to update current-object pointer", "bucket", bucket, "key", key, "err", err)
		}
	} else {
		if err := p.meta.PutObjectMeta(ctx, bucket, key, meta); err != nil {
			if strings.Contains(err.Error(), "NoSuchBucket") {
				return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
			}
			return nil, model.NewProviderError("InternalError", err.Error(), 500)
		}
	}
	respData := map[string]any{
		"ETag":          etagVal,
		"_server_crc32": crc32Val,
	}
	if versionID != "" {
		respData["_version_id"] = versionID
	}
	sseResponseData(respData, enc, kmsKey, ssecMD5)
	p.dispatchNotification(ctx, bucket, key, "s3:ObjectCreated:Put")
	return &model.ProviderResponse{HTTPStatus: 200, Data: respData}, nil
}

// extractUserMetadata collects x-amz-meta-* headers stored under "_meta_*" params.
func extractUserMetadata(params map[string]any) map[string]string {
	var m map[string]string
	for k, v := range params {
		if strings.HasPrefix(k, "_meta_") {
			if s, ok := v.(string); ok {
				if m == nil {
					m = make(map[string]string)
				}
				m[strings.TrimPrefix(k, "_meta_")] = s
			}
		}
	}
	return m
}

func (p *ObjectProvider) GetObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if nr.Origin == model.OriginInternal {
		return p.getObjectInternal(ctx, nr)
	}
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")

	vStatus, _ := p.meta.GetBucketVersioning(ctx, bucket)
	requestedVersionID := strParam(nr.Params, "versionId")
	blobKey := key

	var m objectstore.ObjectMeta
	if vStatus == objectstore.VersioningEnabled || vStatus == objectstore.VersioningSuspended {
		if requestedVersionID != "" {
			var err error
			m, err = p.meta.GetObjectVersion(ctx, bucket, key, requestedVersionID)
			if err != nil {
				return nil, model.NewProviderError("NoSuchVersion", "The specified version does not exist", 404)
			}
			if m.IsDeleteMarker {
				return nil, model.NewProviderError("MethodNotAllowed", "The specified method is not allowed against this resource", 405)
			}
			blobKey = blobKeyForVersion(key, m.VersionID)
		} else {
			versions, _, _ := p.meta.ListObjectVersions(ctx, bucket, key, "", "", 100)
			var found *objectstore.ObjectMeta
			for i := range versions {
				if versions[i].Key == key {
					found = &versions[i]
					break
				}
			}
			if found == nil {
				return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
			}
			if found.IsDeleteMarker {
				return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404).
					WithData(map[string]any{"_delete_marker": true, "_version_id": found.VersionID})
			}
			m = *found
			blobKey = blobKeyForVersion(key, m.VersionID)
		}
	} else {
		var err error
		m, err = p.meta.GetObjectMeta(ctx, bucket, key)
		if err != nil {
			return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
		}
	}
	// P4.2: SSE-C re-validation — the caller must supply the matching key
	if m.SSECKeyMD5 != "" {
		requestedKeyMD5 := strParam(nr.Params, "_server_side_encryption_customer_key_md5")
		if requestedKeyMD5 == "" {
			return nil, model.NewProviderError("InvalidRequest",
				"The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object.", 400)
		}
		if requestedKeyMD5 != m.SSECKeyMD5 {
			return nil, model.NewProviderError("AccessDenied",
				"Requests specifying Server Side Encryption with Customer provided keys must provide the correct secret key.", 403)
		}
	}

	if resp304, pe := checkConditions(nr, objectCondMeta{ETag: m.ETag, LastModified: m.LastModified, ContentType: m.ContentType}); pe != nil {
		return nil, pe
	} else if resp304 != nil {
		return resp304, nil
	}

	status := 200
	var offset, length int64 = 0, -1
	contentLength := m.Size

	rangeHdr := strParam(nr.Params, "_range")
	if rangeHdr != "" {
		start, end, ok := parseByteRange(rangeHdr, m.Size)
		if !ok {
			return nil, model.NewProviderError("InvalidRange", "The requested range is not satisfiable", 416)
		}
		offset = start
		length = end - start + 1
		contentLength = length
		status = 206
	}

	rc, err := p.blobs.GetStream(ctx, bucket, blobKey, offset, length)
	if err != nil {
		// Blob miss — may be a concurrent delete racing with our metadata read.
		// Recheck metadata: if it's also gone, this is a clean concurrent delete → 404.
		// If metadata is still present the blob is missing without a delete → 500.
		if _, recheckErr := p.meta.GetObjectMeta(ctx, bucket, key); recheckErr != nil {
			return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
		}
		slog.Error("object: blob missing but metadata present — possible storage corruption",
			"bucket", bucket, "key", key, "err", err)
		return nil, model.NewProviderError("InternalError", "Internal server error", 500)
	}

	ct := m.ContentType
	if rct := strParam(nr.Params, "response-content-type"); rct != "" {
		ct = rct
	}
	data := map[string]any{
		"_stream":       rc,
		"_passthrough":  true,
		"_content_type": ct,
		"ETag":          m.ETag,
		"_crc32":        m.CRC32,
		"ContentLength": contentLength,
		"LastModified":  m.LastModified.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"),
	}
	// P4.4: expose stored checksum algo+value for codec to emit correct header
	if m.ChecksumAlgorithm != "" {
		data["_checksum_algo"] = m.ChecksumAlgorithm
		data["_checksum_value"] = m.ChecksumValue
	}
	if status == 206 {
		data["_status"] = status
		data["_range_start"] = offset
		data["_range_end"] = offset + length - 1
		data["_range_total"] = m.Size
	}
	if len(m.Metadata) > 0 {
		data["_metadata"] = m.Metadata
	}
	// P2-7: tagging count
	if len(m.Tags) > 0 {
		data["_tagging_count"] = len(m.Tags)
	}
	// P2-1: SSE headers
	sseResponseData(data, m.Encryption, m.KMSKeyID, m.SSECKeyMD5)
	// P2-2: version-id
	if m.VersionID != "" {
		data["_version_id"] = m.VersionID
	}
	// P2-5: lifecycle expiration
	if bucketMeta, err := p.meta.GetBucket(ctx, bucket); err == nil {
		if exp := computeLifecycleExpiration(bucketMeta, key, m.LastModified); exp != "" {
			data["_expiration"] = exp
		}
	}
	overrides := map[string]string{}
	for _, pair := range [][2]string{
		{"response-content-disposition", "Content-Disposition"},
		{"response-content-language", "Content-Language"},
		{"response-content-encoding", "Content-Encoding"},
		{"response-cache-control", "Cache-Control"},
		{"response-expires", "Expires"},
	} {
		if v := strParam(nr.Params, pair[0]); v != "" {
			overrides[pair[1]] = v
		}
	}
	if len(overrides) > 0 {
		data["_response_overrides"] = overrides
	}
	return &model.ProviderResponse{HTTPStatus: status, Data: data}, nil
}

// parseByteRange parses a "bytes=<start>-<end>" Range header.
// Returns inclusive [start, end] indices and true on success.
func parseByteRange(hdr string, size int64) (int64, int64, bool) {
	hdr = strings.TrimSpace(hdr)
	if !strings.HasPrefix(hdr, "bytes=") {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(hdr, "bytes=")
	if strings.Contains(spec, ",") {
		return 0, 0, false
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	startStr, endStr := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	var start, end int64
	if startStr == "" {
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		start = size - n
		if start < 0 {
			start = 0
		}
		end = size - 1
	} else {
		var err error
		start, err = strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			return 0, 0, false
		}
		if endStr == "" {
			end = size - 1
		} else {
			end, err = strconv.ParseInt(endStr, 10, 64)
			if err != nil {
				return 0, 0, false
			}
		}
	}
	if start < 0 || end >= size || start > end {
		return 0, 0, false
	}
	return start, end, true
}

func (p *ObjectProvider) HeadObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	requestedVersionID := strParam(nr.Params, "versionId")

	vStatus, _ := p.meta.GetBucketVersioning(ctx, bucket)

	var m objectstore.ObjectMeta
	if vStatus == objectstore.VersioningEnabled || vStatus == objectstore.VersioningSuspended {
		if requestedVersionID != "" {
			vm, err := p.meta.GetObjectVersion(ctx, bucket, key, requestedVersionID)
			if err != nil {
				return nil, model.NewProviderError("NoSuchVersion", "The specified version does not exist", 404)
			}
			if vm.IsDeleteMarker {
				return nil, model.NewProviderError("MethodNotAllowed", "The specified method is not allowed against this resource", 405).
					WithData(map[string]any{"_delete_marker": true, "_version_id": requestedVersionID})
			}
			m = vm
		} else {
			versions, _, _ := p.meta.ListObjectVersions(ctx, bucket, key, "", "", 1)
			if len(versions) == 0 {
				return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
			}
			if versions[0].IsDeleteMarker {
				return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
			}
			m = versions[0]
		}
	} else {
		var err error
		m, err = p.meta.GetObjectMeta(ctx, bucket, key)
		if err != nil {
			return nil, model.NewProviderError("NoSuchKey", "The specified key does not exist", 404)
		}
	}

	if resp304, pe := checkConditions(nr, objectCondMeta{ETag: m.ETag, LastModified: m.LastModified, ContentType: m.ContentType}); pe != nil {
		return nil, pe
	} else if resp304 != nil {
		return resp304, nil
	}
	data := map[string]any{
		"ETag":          m.ETag,
		"ContentLength": m.Size,
		"ContentType":   m.ContentType,
		"LastModified":  m.LastModified.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"),
	}
	if len(m.Metadata) > 0 {
		data["_metadata"] = m.Metadata
	}
	if len(m.Tags) > 0 {
		data["_tagging_count"] = len(m.Tags)
	}
	sseResponseData(data, m.Encryption, m.KMSKeyID, m.SSECKeyMD5)
	if m.VersionID != "" {
		data["_version_id"] = m.VersionID
	}
	if bucketMeta, err := p.meta.GetBucket(ctx, bucket); err == nil {
		if exp := computeLifecycleExpiration(bucketMeta, key, m.LastModified); exp != "" {
			data["_expiration"] = exp
		}
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: data}, nil
}

func (p *ObjectProvider) DeleteObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	key := strParam(nr.Params, "_key")
	requestedVersionID := strParam(nr.Params, "versionId")

	// P2-2: Versioning path
	vStatus, _ := p.meta.GetBucketVersioning(ctx, bucket)
	if vStatus == objectstore.VersioningEnabled {
		if requestedVersionID != "" {
			// Delete a specific version — check lock before deleting.
			m, err := p.meta.GetObjectVersion(ctx, bucket, key, requestedVersionID)
			if err == nil {
				if lockErr := checkObjectLock(nr, m); lockErr != nil {
					return nil, lockErr
				}
			}
			_ = p.meta.DeleteObjectVersion(ctx, bucket, key, requestedVersionID)
			_ = p.blobs.Delete(ctx, bucket, blobKeyForVersion(key, requestedVersionID))
			return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{
				"_version_id": requestedVersionID,
			}}, nil
		}
		// No versionId: insert a delete marker.
		marker := objectstore.ObjectMeta{
			Key:            key,
			IsDeleteMarker: true,
			LastModified:   clock.Now(),
			StorageClass:   "STANDARD",
		}
		markerID, _ := p.meta.PutObjectVersion(ctx, bucket, key, marker)
		return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{
			"_delete_marker": true,
			"_version_id":    markerID,
		}}, nil
	}

	// Non-versioned path — check lock first.
	if m, err := p.meta.GetObjectMeta(ctx, bucket, key); err == nil {
		if lockErr := checkObjectLock(nr, m); lockErr != nil {
			return nil, lockErr
		}
	}

	// Metadata-first: after this succeeds, GetObject returns 404 immediately so
	// no caller ever sees metadata present + blob absent (torn state).
	if err := p.meta.DeleteObjectMeta(ctx, bucket, key); err != nil {
		// Only log; don't delete the blob — that would create the reverse torn state
		// (metadata present, blob gone). Real S3 returns 204 for missing keys so we
		// swallow the error and return success regardless.
		slog.Warn("object: metadata delete failed", "bucket", bucket, "key", key, "err", err)
		return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
	}
	// Best-effort blob delete. Any orphaned blob is invisible via GetObject.
	if err := p.blobs.Delete(ctx, bucket, key); err != nil {
		slog.Warn("object: blob delete failed after metadata delete", "bucket", bucket, "key", key, "err", err)
	}
	p.dispatchNotification(ctx, bucket, key, "s3:ObjectRemoved:Delete")
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, nil
}

// checkObjectLock returns an error if the object is protected by a lock.
func checkObjectLock(nr *model.NormalizedRequest, m objectstore.ObjectMeta) error {
	if m.LegalHoldStatus == "ON" {
		return model.NewProviderError("AccessDenied", "Object protected by legal hold", 403)
	}
	if m.LockRetainUntil != nil && clock.Now().Before(*m.LockRetainUntil) {
		if m.LockMode == "COMPLIANCE" {
			return model.NewProviderError("AccessDenied", "Object locked in COMPLIANCE mode", 403)
		}
		if m.LockMode == "GOVERNANCE" && strParam(nr.Params, "_bypass_governance_retention") != "true" {
			return model.NewProviderError("AccessDenied", "Object locked in GOVERNANCE mode", 403)
		}
	}
	return nil
}

func (p *ObjectProvider) CopyObject(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	dstBucket := strParam(nr.Params, "_bucket")
	dstKey := strParam(nr.Params, "_key")
	src := strings.TrimPrefix(strParam(nr.Params, "_copy_source"), "/")
	parts := strings.SplitN(src, "/", 2)
	if len(parts) != 2 {
		return nil, model.NewProviderError("InvalidArgument", "Invalid copy source", 400)
	}
	srcBucket, srcKey := parts[0], parts[1]

	srcMeta, err := p.meta.GetObjectMeta(ctx, srcBucket, srcKey)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "Source key does not exist", 404)
	}

	// Copy-source conditional checks (params set by S3Codec).
	if _, pe := checkCopySourceConditions(nr, srcMeta); pe != nil {
		return nil, pe
	}

	data, err := p.blobs.Get(ctx, srcBucket, srcKey)
	if err != nil {
		return nil, model.NewProviderError("NoSuchKey", "Source key does not exist", 404)
	}

	// P2-2: Determine dest blob key before writing.
	dstVStatus, _ := p.meta.GetBucketVersioning(ctx, dstBucket)
	var preDstVersionID string
	dstBlobKey := dstKey
	switch dstVStatus {
	case objectstore.VersioningEnabled:
		preDstVersionID = newVersionID()
		dstBlobKey = blobKeyForVersion(dstKey, preDstVersionID)
	case objectstore.VersioningSuspended:
		preDstVersionID = "null"
		dstBlobKey = blobKeyForVersion(dstKey, "null")
	}

	_ = p.blobs.Put(ctx, dstBucket, dstBlobKey, data)
	etagVal := etag(data)
	now := clock.Now()
	dstMeta := objectstore.ObjectMeta{
		Key: dstKey, ETag: etagVal, CRC32: crc32Base64(data), Size: srcMeta.Size,
		LastModified: now, StorageClass: "STANDARD",
	}
	if strParam(nr.Params, "_metadata_directive") == "REPLACE" {
		dstMeta.ContentType = strParam(nr.Params, "_content_type")
		if dstMeta.ContentType == "" {
			dstMeta.ContentType = "application/octet-stream"
		}
		dstMeta.Metadata = extractUserMetadata(nr.Params)
	} else {
		dstMeta.ContentType = srcMeta.ContentType
		dstMeta.Metadata = srcMeta.Metadata
	}
	// P2-7: Tagging directive
	tagDirective := strParam(nr.Params, "_tagging_directive")
	if strings.ToUpper(tagDirective) == "REPLACE" {
		if tagging := strParam(nr.Params, "_tagging"); tagging != "" {
			tags, _ := parseTaggingHeader(tagging)
			dstMeta.Tags = tags
		}
	} else {
		dstMeta.Tags = srcMeta.Tags
	}
	// P2-1: SSE on destination
	enc, kmsKey, ssecMD5, sseErr := p.resolveSSE(ctx, nr, dstBucket)
	if sseErr != nil {
		return nil, sseErr
	}
	dstMeta.Encryption = enc
	dstMeta.KMSKeyID = kmsKey
	dstMeta.SSECKeyMD5 = ssecMD5
	// P2-4: ACL
	dstMeta.ACL = resolveACL(strParam(nr.Params, "_acl"), nr.AccountID)

	// P2-2: Versioning — use pre-generated versionID.
	var versionID string
	if dstVStatus == objectstore.VersioningEnabled {
		dstMeta.VersionID = preDstVersionID
		versionID, _ = p.meta.PutObjectVersion(ctx, dstBucket, dstKey, dstMeta)
		if err := p.meta.PutObjectMeta(ctx, dstBucket, dstKey, dstMeta); err != nil {
			slog.Warn("versioned CopyObject: failed to update current-object pointer", "bucket", dstBucket, "key", dstKey, "err", err)
		}
	} else if dstVStatus == objectstore.VersioningSuspended {
		dstMeta.VersionID = "null"
		versionID, _ = p.meta.PutObjectVersion(ctx, dstBucket, dstKey, dstMeta)
		if err := p.meta.PutObjectMeta(ctx, dstBucket, dstKey, dstMeta); err != nil {
			slog.Warn("suspended CopyObject: failed to update current-object pointer", "bucket", dstBucket, "key", dstKey, "err", err)
		}
	} else {
		if err := p.meta.PutObjectMeta(ctx, dstBucket, dstKey, dstMeta); err != nil {
			slog.Warn("CopyObject: failed to put object meta", "bucket", dstBucket, "key", dstKey, "err", err)
		}
	}
	respData := map[string]any{
		"CopyObjectResult": map[string]any{
			"ETag":         etagVal,
			"LastModified": now.UTC().Format(time.RFC3339),
		},
	}
	if versionID != "" {
		respData["_version_id"] = versionID
	}
	sseResponseData(respData, enc, kmsKey, ssecMD5)
	return provider.OK(respData), nil
}

// ─── List ─────────────────────────────────────────────────────────────────────

func (p *ObjectProvider) ListObjectsV1(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.listObjects(ctx, nr, false)
}

func (p *ObjectProvider) ListObjectsV2(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.listObjects(ctx, nr, true)
}

func (p *ObjectProvider) listObjects(ctx context.Context, nr *model.NormalizedRequest, v2 bool) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	prefix := strParam(nr.Params, "prefix")
	if prefix == "" {
		prefix = strParam(nr.Params, "Prefix")
	}
	delimiter := strParam(nr.Params, "delimiter")
	if delimiter == "" {
		delimiter = strParam(nr.Params, "Delimiter")
	}
	marker := strParam(nr.Params, "marker") // ListObjectsV1
	if marker == "" {
		marker = strParam(nr.Params, "continuation-token") // ListObjectsV2 subsequent pages
	}
	if marker == "" {
		marker = strParam(nr.Params, "start-after") // ListObjectsV2 first page with StartAfter
	}
	maxKeys := intParam(nr.Params, "max-keys", 1000)
	encodingType := strParam(nr.Params, "encoding-type")

	objects, commonPrefixes, truncated, nextMarker, err := p.meta.ListObjectMeta(ctx, bucket, prefix, delimiter, marker, maxKeys)
	if err != nil {
		return nil, model.NewProviderError("NoSuchBucket", "The specified bucket does not exist", 404)
	}

	var contents []map[string]any
	for _, obj := range objects {
		k := obj.Key
		if encodingType == "url" {
			k = urlEncode(k)
		}
		contents = append(contents, map[string]any{
			"Key":          k,
			"ETag":         obj.ETag,
			"Size":         obj.Size,
			"LastModified": obj.LastModified.UTC().Format(time.RFC3339),
			"StorageClass": obj.StorageClass,
		})
	}
	if contents == nil {
		contents = []map[string]any{}
	}

	result := map[string]any{
		"Name":        bucket,
		"Prefix":      prefix,
		"Delimiter":   delimiter,
		"MaxKeys":     maxKeys,
		"IsTruncated": truncated,
		"Contents":    contents,
		"Marker":      marker,
	}
	if encodingType == "url" {
		result["EncodingType"] = "url"
		result["Prefix"] = urlEncode(prefix)
		result["Delimiter"] = urlEncode(delimiter)
		for i, cp := range commonPrefixes {
			commonPrefixes[i] = urlEncode(cp)
		}
	}
	result["CommonPrefixes"] = commonPrefixes
	if v2 {
		result["KeyCount"] = len(contents)
	}
	// Pass the opaque next-page token to the codec using a cloud-neutral key.
	// The codec translates it to the cloud-specific field name (e.g. AWS
	// NextContinuationToken for V2, NextMarker for V1).
	if truncated && nextMarker != "" {
		result["_nextPageToken"] = nextMarker
	}
	return provider.OK(result), nil
}

func (p *ObjectProvider) DeleteObjects(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	bucket := strParam(nr.Params, "_bucket")
	objects, _ := nr.Params["Delete"].(map[string]any)
	if objects == nil {
		return provider.OK(map[string]any{"Deleted": []any{}}), nil
	}
	keys, _ := objects["Object"].([]any)
	vStatus, _ := p.meta.GetBucketVersioning(ctx, bucket)

	var deleted []map[string]any
	var errored []map[string]any

	for _, k := range keys {
		km, _ := k.(map[string]any)
		key, _ := km["Key"].(string)
		versionID, _ := km["VersionId"].(string)

		if vStatus == objectstore.VersioningEnabled || vStatus == objectstore.VersioningSuspended {
			if versionID != "" {
				// Delete a specific version.
				m, err := p.meta.GetObjectVersion(ctx, bucket, key, versionID)
				if err != nil {
					errored = append(errored, map[string]any{
						"Key": key, "VersionId": versionID,
						"Code": "NoSuchVersion", "Message": "The specified version does not exist",
					})
					continue
				}
				if lockErr := checkObjectLock(nr, m); lockErr != nil {
					errored = append(errored, map[string]any{
						"Key": key, "VersionId": versionID,
						"Code": "AccessDenied", "Message": lockErr.Error(),
					})
					continue
				}
				wasMarker := m.IsDeleteMarker
				if err := p.meta.DeleteObjectVersion(ctx, bucket, key, versionID); err != nil {
					errored = append(errored, map[string]any{
						"Key": key, "VersionId": versionID,
						"Code": "InternalError", "Message": err.Error(),
					})
					continue
				}
				if !wasMarker {
					_ = p.blobs.Delete(ctx, bucket, key)
				}
				entry := map[string]any{"Key": key, "VersionId": versionID}
				if wasMarker {
					entry["DeleteMarker"] = true
					entry["DeleteMarkerVersionId"] = versionID
				}
				deleted = append(deleted, entry)
			} else {
				// No versionId: create a delete marker.
				marker := objectstore.ObjectMeta{
					Key:            key,
					IsDeleteMarker: true,
				}
				markerID, err := p.meta.PutObjectVersion(ctx, bucket, key, marker)
				if err != nil {
					errored = append(errored, map[string]any{
						"Key": key, "Code": "InternalError", "Message": err.Error(),
					})
					continue
				}
				deleted = append(deleted, map[string]any{
					"Key":                   key,
					"DeleteMarker":          true,
					"DeleteMarkerVersionId": markerID,
				})
			}
		} else {
			// Non-versioned path.
			if err := p.meta.DeleteObjectMeta(ctx, bucket, key); err != nil {
				slog.Warn("object: meta delete failed in DeleteObjects", "bucket", bucket, "key", key, "err", err)
				continue
			}
			if err := p.blobs.Delete(ctx, bucket, key); err != nil {
				slog.Warn("object: blob delete failed in DeleteObjects", "bucket", bucket, "key", key, "err", err)
			}
			deleted = append(deleted, map[string]any{"Key": key})
		}
	}
	if deleted == nil {
		deleted = []map[string]any{}
	}
	result := map[string]any{"Deleted": deleted}
	if len(errored) > 0 {
		result["Errors"] = errored
	}
	return provider.OK(result), nil
}
