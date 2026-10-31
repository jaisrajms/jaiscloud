package services

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
)

// decodeAWSChunked strips AWS chunked-upload framing.
// Each chunk: <hex-size>[;chunk-signature=...]\r\n<data>\r\n
// Final chunk: 0[;...]\r\n
// Returns body unchanged if it does not look like aws-chunked format.
func decodeAWSChunked(body []byte) []byte {
	var out []byte
	// parsedChunked tracks whether we have successfully recognised at least one
	// valid chunk header (including the terminal 0-length chunk). If true, we
	// know the body was valid aws-chunked and it is safe to return an empty
	// slice when no data chunks were present (i.e. an empty object upload).
	parsedChunked := false
	remaining := body
	for len(remaining) > 0 {
		idx := strings.Index(string(remaining), "\r\n")
		if idx < 0 {
			break
		}
		sizeLine := string(remaining[:idx])
		if semi := strings.IndexByte(sizeLine, ';'); semi >= 0 {
			sizeLine = sizeLine[:semi]
		}
		sizeLine = strings.TrimSpace(sizeLine)
		if sizeLine == "" {
			return body
		}
		chunkLen, err := strconv.ParseInt(sizeLine, 16, 64)
		if err != nil {
			return body // not aws-chunked, pass through unchanged
		}
		parsedChunked = true
		remaining = remaining[idx+2:]
		if chunkLen == 0 {
			break // final chunk — end of valid chunked data
		}
		if int64(len(remaining)) < chunkLen {
			return body // truncated, pass through
		}
		out = append(out, remaining[:chunkLen]...)
		remaining = remaining[chunkLen:]
		if len(remaining) >= 2 && remaining[0] == '\r' && remaining[1] == '\n' {
			remaining = remaining[2:]
		}
	}
	// Only fall back to the raw body if we never saw a valid chunk header.
	// An empty aws-chunked stream (terminal 0-chunk only) correctly decodes
	// to empty bytes — do not return the raw framing bytes in that case.
	if len(out) == 0 && len(body) > 0 && !parsedChunked {
		return body
	}
	return out
}

// s3ChecksumCRC32 returns the base64-encoded IEEE CRC32 of body.
// The AWS SDK v2 validates this on PutObject / CopyObject / CompleteMultipartUpload responses.
func s3ChecksumCRC32(body []byte) string {
	sum := crc32.ChecksumIEEE(body)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, sum)
	return base64.StdEncoding.EncodeToString(b)
}

// S3Codec handles S3 REST wire format (path-style and virtual-hosted URLs).
// VirtualHostBases lists host suffixes (e.g. jaiscloud.devbox.svc.cluster.local)
// that the codec treats as virtual-hosted: "bucket.<base>" -> bucket extracted
// from the hostname, key from the URL path.
type S3Codec struct {
	VirtualHostBases []string
}

func (c *S3Codec) ServiceName() string { return "s3" }

// extractVirtualHostedBucket returns the bucket name when the request host
// matches one of the configured VirtualHostBases (strips port first).
// Returns "" if no base matches.
func (c *S3Codec) extractVirtualHostedBucket(host string) string {
	// Strip port suffix if present.
	h := host
	if idx := strings.LastIndexByte(h, ':'); idx >= 0 {
		h = h[:idx]
	}
	for _, base := range c.VirtualHostBases {
		suffix := "." + base
		if strings.HasSuffix(h, suffix) {
			return h[:len(h)-len(suffix)]
		}
	}
	return ""
}

// ─── Decode ───────────────────────────────────────────────────────────────────

func (c *S3Codec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	// Decode AWS chunked body so ETag/checksum are computed over actual content.
	if strings.Contains(strings.ToLower(r.Header.Get("Content-Encoding")), "aws-chunked") ||
		strings.HasPrefix(r.Header.Get("x-amz-content-sha256"), "STREAMING-") ||
		r.Header.Get("x-amz-decoded-content-length") != "" {
		body = decodeAWSChunked(body)
	}

	var bucket, key string
	if host := r.Host; strings.Contains(host, ".s3.") {
		// Virtual-hosted: "mybucket.s3.us-east-1.amazonaws.com"
		bucket = host[:strings.Index(host, ".s3.")]
		key = strings.TrimPrefix(r.URL.Path, "/")
	} else if b := c.extractVirtualHostedBucket(r.Host); b != "" {
		// Virtual-hosted with custom base: "mybucket.jaiscloud.devbox.svc.cluster.local[:port]"
		bucket = b
		key = strings.TrimPrefix(r.URL.Path, "/")
	} else {
		// Path-style: /{bucket}/{key...}
		path := strings.TrimPrefix(r.URL.Path, "/")
		if idx := strings.IndexByte(path, '/'); idx >= 0 {
			bucket = path[:idx]
			key = path[idx+1:]
		} else {
			bucket = path
		}
	}

	query := r.URL.Query()

	// P2-8: Presigned URL expiration check.
	if query.Has("X-Amz-Algorithm") || query.Has("X-Amz-Date") || query.Has("Expires") {
		if err := checkPresignedExpiration(query); err != nil {
			return nil, model.NewProviderError("AccessDenied", "Request has expired", 403)
		}
	}

	action := s3DetectAction(r.Method, bucket, key, query, r.Header)

	// The gateway skips io.ReadAll for streaming uploads (PutObject/UploadPart) so
	// that large bodies can be streamed directly to the blob store. However, some
	// operations (e.g. PutObjectLegalHold, PutObjectRetention) also trigger the
	// streaming-upload headers (aws-chunked, STREAMING-AWS4-HMAC-SHA256-PAYLOAD) even
	// though they carry small XML bodies. For those we read the body here.
	if body == nil && !s3ActionIsStreaming(action) && r.Body != nil {
		body, _ = io.ReadAll(r.Body)
	}

	params := map[string]any{
		"_bucket": bucket,
		"_key":    key,
		"_body":   body,
	}
	// body == nil only when the gateway determined this is a streaming upload
	// (PutObject / UploadPart) and intentionally skipped io.ReadAll.
	if body == nil {
		params["_streaming"] = true
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		params["_content_type"] = ct
	}
	if cs := r.Header.Get("X-Amz-Copy-Source"); cs != "" {
		params["_copy_source"] = cs
	}
	if rng := r.Header.Get("Range"); rng != "" {
		params["_range"] = rng
	}
	if md := r.Header.Get("X-Amz-Metadata-Directive"); md != "" {
		params["_metadata_directive"] = md
	}
	if csr := r.Header.Get("X-Amz-Copy-Source-Range"); csr != "" {
		params["_copy_source_range"] = csr
	}
	if v := r.Header.Get("If-Match"); v != "" {
		params["_cond_if_match"] = v
	}
	if v := r.Header.Get("If-None-Match"); v != "" {
		params["_cond_if_none_match"] = v
	}
	if v := r.Header.Get("If-Modified-Since"); v != "" {
		params["_cond_if_modified_since"] = v
	}
	if v := r.Header.Get("If-Unmodified-Since"); v != "" {
		params["_cond_if_unmodified_since"] = v
	}
	// CopyObject source conditionals — header names stay in codec only.
	if v := r.Header.Get("x-amz-copy-source-if-match"); v != "" {
		params["_copy_source_if_match"] = v
	}
	if v := r.Header.Get("x-amz-copy-source-if-none-match"); v != "" {
		params["_copy_source_if_none_match"] = v
	}
	if v := r.Header.Get("x-amz-copy-source-if-modified-since"); v != "" {
		params["_copy_source_if_modified_since"] = v
	}
	if v := r.Header.Get("x-amz-copy-source-if-unmodified-since"); v != "" {
		params["_copy_source_if_unmodified_since"] = v
	}
	// P2-7: Tagging
	if v := r.Header.Get("x-amz-tagging"); v != "" {
		params["_tagging"] = v
	}
	if v := r.Header.Get("x-amz-tagging-directive"); v != "" {
		params["_tagging_directive"] = v
	}
	// P2-4: ACL
	if v := r.Header.Get("x-amz-acl"); v != "" {
		params["_acl"] = v
	}
	// P2-3: Object Lock bypass
	if v := r.Header.Get("x-amz-bypass-governance-retention"); v != "" {
		params["_bypass_governance_retention"] = v
	}
	// P2-1: SSE headers
	for _, hdr := range []string{
		"x-amz-server-side-encryption",
		"x-amz-server-side-encryption-aws-kms-key-id",
		"x-amz-server-side-encryption-bucket-key-enabled",
		"x-amz-server-side-encryption-customer-algorithm",
		"x-amz-server-side-encryption-customer-key",
		"x-amz-server-side-encryption-customer-key-MD5",
	} {
		if v := r.Header.Get(hdr); v != "" {
			paramKey := "_" + strings.ToLower(strings.ReplaceAll(strings.TrimPrefix(hdr, "x-amz-"), "-", "_"))
			params[paramKey] = v
		}
	}
	// Capture x-amz-meta-* user metadata headers.
	for k, vs := range r.Header {
		lower := strings.ToLower(k)
		if strings.HasPrefix(lower, "x-amz-meta-") && len(vs) > 0 {
			params["_meta_"+strings.TrimPrefix(lower, "x-amz-meta-")] = vs[0]
		}
	}
	// P4.12: GetObjectAttributes — SDK sends each attribute as a separate header value.
	if vals := r.Header.Values("x-amz-object-attributes"); len(vals) > 0 {
		params["_object_attributes"] = strings.Join(vals, ",")
	}
	// P4.1: Object lock headers on PutObject
	if v := r.Header.Get("x-amz-object-lock-mode"); v != "" {
		params["_lock_mode"] = v
	}
	if v := r.Header.Get("x-amz-object-lock-retain-until-date"); v != "" {
		params["_lock_retain_until_date"] = v
	}
	if v := r.Header.Get("x-amz-object-lock-legal-hold"); v != "" {
		params["_lock_legal_hold"] = v
	}
	// S3 Control: capture account ID header.
	if v := r.Header.Get("x-amz-account-id"); v != "" {
		params["_account_id"] = v
	}
	// Capture inbound flexible checksum to echo back in response.
	for _, algo := range []string{"crc32", "crc32c", "sha256", "sha1"} {
		if v := r.Header.Get("x-amz-checksum-" + algo); v != "" {
			params["_checksum_header"] = "x-amz-checksum-" + algo
			params["_checksum_value"] = v
			break
		}
	}
	// Propagate all URL query params (prefix, delimiter, marker, max-keys, etc.)
	for k, vs := range query {
		if len(vs) > 0 {
			params[k] = vs[0]
		}
	}

	// CompleteMultipartUpload: parse XML part list so provider can validate order/sizes.
	if action == "CompleteMultipartUpload" && len(body) > 0 {
		var completeReq struct {
			Parts []struct {
				PartNumber int    `xml:"PartNumber"`
				ETag       string `xml:"ETag"`
			} `xml:"Part"`
		}
		if xml.Unmarshal(body, &completeReq) == nil {
			parts := make([]map[string]any, len(completeReq.Parts))
			for i, p := range completeReq.Parts {
				parts[i] = map[string]any{"PartNumber": p.PartNumber, "ETag": p.ETag}
			}
			params["_requested_parts"] = parts
		}
	}

	// DeleteObjects: parse XML body into params["Delete"]
	if action == "DeleteObjects" && len(body) > 0 {
		var deleteReq struct {
			Objects []struct {
				Key string `xml:"Key"`
			} `xml:"Object"`
		}
		if xml.Unmarshal(body, &deleteReq) == nil {
			objs := make([]any, 0, len(deleteReq.Objects))
			for _, o := range deleteReq.Objects {
				objs = append(objs, map[string]any{"Key": o.Key})
			}
			params["Delete"] = map[string]any{"Object": objs}
		}
	}

	// PutBucketNotificationConfiguration: parse XML into _notification_config param.
	if action == "PutBucketNotificationConfiguration" && len(body) > 0 {
		var notifReq struct {
			QueueConfigurations []struct {
				Id     string   `xml:"Id"`
				Queue  string   `xml:"Queue"`
				Events []string `xml:"Event"`
				Filter struct {
					S3Key struct {
						FilterRules []struct {
							Name  string `xml:"Name"`
							Value string `xml:"Value"`
						} `xml:"FilterRule"`
					} `xml:"S3Key"`
				} `xml:"Filter"`
			} `xml:"QueueConfiguration"`
			TopicConfigurations []struct {
				Id     string   `xml:"Id"`
				Topic  string   `xml:"Topic"`
				Events []string `xml:"Event"`
				Filter struct {
					S3Key struct {
						FilterRules []struct {
							Name  string `xml:"Name"`
							Value string `xml:"Value"`
						} `xml:"FilterRule"`
					} `xml:"S3Key"`
				} `xml:"Filter"`
			} `xml:"TopicConfiguration"`
			LambdaConfigurations []struct {
				Id            string   `xml:"Id"`
				CloudFunction string   `xml:"CloudFunction"`
				Events        []string `xml:"Event"`
				Filter        struct {
					S3Key struct {
						FilterRules []struct {
							Name  string `xml:"Name"`
							Value string `xml:"Value"`
						} `xml:"FilterRule"`
					} `xml:"S3Key"`
				} `xml:"Filter"`
			} `xml:"CloudFunctionConfiguration"`
			// <EventBridgeConfiguration/> is an empty marker element: presence alone
			// turns EventBridge delivery on for the bucket.
			EventBridge *struct{} `xml:"EventBridgeConfiguration"`
		}
		if xml.Unmarshal(body, &notifReq) == nil {
			cfg := map[string]any{}
			if notifReq.EventBridge != nil {
				cfg["EventBridgeConfiguration"] = map[string]any{}
			}
			// Queue configs
			qcs := make([]any, 0, len(notifReq.QueueConfigurations))
			for _, q := range notifReq.QueueConfigurations {
				rules := make([]any, 0, len(q.Filter.S3Key.FilterRules))
				for _, r := range q.Filter.S3Key.FilterRules {
					rules = append(rules, map[string]any{"Name": r.Name, "Value": r.Value})
				}
				qcs = append(qcs, map[string]any{
					"Id":       q.Id,
					"QueueArn": q.Queue,
					"Events":   q.Events,
					"Filter":   map[string]any{"S3Key": map[string]any{"FilterRules": rules}},
				})
			}
			cfg["QueueConfigurations"] = qcs
			// Topic configs
			tcs := make([]any, 0, len(notifReq.TopicConfigurations))
			for _, t := range notifReq.TopicConfigurations {
				rules := make([]any, 0, len(t.Filter.S3Key.FilterRules))
				for _, r := range t.Filter.S3Key.FilterRules {
					rules = append(rules, map[string]any{"Name": r.Name, "Value": r.Value})
				}
				tcs = append(tcs, map[string]any{
					"Id":       t.Id,
					"TopicArn": t.Topic,
					"Events":   t.Events,
					"Filter":   map[string]any{"S3Key": map[string]any{"FilterRules": rules}},
				})
			}
			cfg["TopicConfigurations"] = tcs
			// Lambda configs
			lcs := make([]any, 0, len(notifReq.LambdaConfigurations))
			for _, l := range notifReq.LambdaConfigurations {
				rules := make([]any, 0, len(l.Filter.S3Key.FilterRules))
				for _, r := range l.Filter.S3Key.FilterRules {
					rules = append(rules, map[string]any{"Name": r.Name, "Value": r.Value})
				}
				lcs = append(lcs, map[string]any{
					"Id":                l.Id,
					"LambdaFunctionArn": l.CloudFunction,
					"Events":            l.Events,
					"Filter":            map[string]any{"S3Key": map[string]any{"FilterRules": rules}},
				})
			}
			cfg["LambdaConfigurations"] = lcs
			params["_notification_config"] = cfg
		}
	}

	if action == "CreateBucket" && !isValidBucketName(bucket) {
		return nil, model.NewProviderError("InvalidBucketName", "The specified bucket is not valid", 400)
	}

	return &model.NormalizedRequest{
		Service: "s3",
		Action:  action,
		Params:  params,
		Raw:     r,
	}, nil
}

// isValidBucketName enforces AWS S3 bucket naming rules:
// 3–63 chars, lowercase alphanumeric/hyphens/dots, no leading/trailing hyphens or dots, no "..".
func isValidBucketName(name string) bool {
	n := len(name)
	if n < 3 || n > 63 {
		return false
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.') {
			return false
		}
	}
	if name[0] == '-' || name[0] == '.' || name[n-1] == '-' || name[n-1] == '.' {
		return false
	}
	return !strings.Contains(name, "..")
}

// s3ActionIsStreaming reports whether an action legitimately has its body
// consumed as a raw stream by the provider (i.e. the blob bytes themselves).
// All other actions that carry a body use small XML payloads that must be
// fully buffered before parsing.
func s3ActionIsStreaming(action string) bool {
	return action == "PutObject" || action == "UploadPart"
}

func s3DetectAction(method, bucket, key string, query url.Values, headers http.Header) string {
	hasCopySource := headers.Get("X-Amz-Copy-Source") != ""

	if bucket == "" {
		return "ListBuckets"
	}

	// S3 Control API: /v20180820/accesspoint/...
	if bucket == "v20180820" {
		parts := strings.SplitN(key, "/", 3)
		if len(parts) >= 1 && parts[0] == "accesspoint" {
			switch {
			case len(parts) == 1 && method == http.MethodGet:
				return "ListAccessPoints"
			case len(parts) == 2:
				switch method {
				case http.MethodPut:
					return "CreateAccessPoint"
				case http.MethodGet:
					return "GetAccessPoint"
				case http.MethodDelete:
					return "DeleteAccessPoint"
				}
			case len(parts) == 3 && parts[2] == "policy":
				switch method {
				case http.MethodPut:
					return "PutAccessPointPolicy"
				case http.MethodGet:
					return "GetAccessPointPolicy"
				case http.MethodDelete:
					return "DeleteAccessPointPolicy"
				}
			case len(parts) == 3 && parts[2] == "policyStatus":
				return "GetAccessPointPolicyStatus"
			}
		}
		return "ListBuckets"
	}

	if key == "" {
		// Bucket-level operations
		switch method {
		case http.MethodHead:
			return "HeadBucket"
		case http.MethodDelete:
			switch {
			case query.Has("tagging"):
				return "DeleteBucketTagging"
			case query.Has("encryption"):
				return "DeleteBucketEncryption"
			case query.Has("lifecycle"):
				return "DeleteBucketLifecycle"
			case query.Has("cors"):
				return "DeleteBucketCors"
			case query.Has("ownershipControls"):
				return "DeleteBucketOwnershipControls"
			case query.Has("policy"):
				return "DeleteBucketPolicy"
			case query.Has("website"):
				return "DeleteBucketWebsite"
			case query.Has("replication"):
				return "DeleteBucketReplication"
			default:
				return "DeleteBucket"
			}
		case http.MethodPut:
			switch {
			case query.Has("acl"):
				return "PutBucketAcl"
			case query.Has("tagging"):
				return "PutBucketTagging"
			case query.Has("versioning"):
				return "PutBucketVersioning"
			case query.Has("encryption"):
				return "PutBucketEncryption"
			case query.Has("object-lock"):
				return "PutObjectLockConfiguration"
			case query.Has("lifecycle"):
				return "PutBucketLifecycleConfiguration"
			case query.Has("cors"):
				return "PutBucketCors"
			case query.Has("ownershipControls"):
				return "PutBucketOwnershipControls"
			case query.Has("notification"):
				return "PutBucketNotificationConfiguration"
			case query.Has("policy"):
				return "PutBucketPolicy"
			case query.Has("website"):
				return "PutBucketWebsite"
			case query.Has("logging"):
				return "PutBucketLogging"
			case query.Has("replication"):
				return "PutBucketReplication"
			default:
				return "CreateBucket"
			}
		case http.MethodGet:
			switch {
			case query.Has("location"):
				return "GetBucketLocation"
			case query.Has("uploads"):
				return "ListMultipartUploads"
			case query.Get("list-type") == "2":
				return "ListObjectsV2"
			case query.Has("acl"):
				return "GetBucketAcl"
			case query.Has("tagging"):
				return "GetBucketTagging"
			case query.Has("versioning"):
				return "GetBucketVersioning"
			case query.Has("versions"):
				return "ListObjectVersions"
			case query.Has("encryption"):
				return "GetBucketEncryption"
			case query.Has("object-lock"):
				return "GetObjectLockConfiguration"
			case query.Has("lifecycle"):
				return "GetBucketLifecycleConfiguration"
			case query.Has("cors"):
				return "GetBucketCors"
			case query.Has("ownershipControls"):
				return "GetBucketOwnershipControls"
			case query.Has("notification"):
				return "GetBucketNotificationConfiguration"
			case query.Has("policy"):
				return "GetBucketPolicy"
			case query.Has("website"):
				return "GetBucketWebsite"
			case query.Has("logging"):
				return "GetBucketLogging"
			case query.Has("replication"):
				return "GetBucketReplication"
			default:
				return "ListObjectsV1"
			}
		case http.MethodPost:
			if query.Has("delete") {
				return "DeleteObjects"
			}
		}
		return "ListObjectsV1"
	}

	// Object-level operations
	switch method {
	case http.MethodGet:
		switch {
		case query.Has("uploadId") && !query.Has("partNumber"):
			return "ListParts"
		case query.Has("tagging"):
			return "GetObjectTagging"
		case query.Has("acl"):
			return "GetObjectAcl"
		case query.Has("retention"):
			return "GetObjectRetention"
		case query.Has("legal-hold"):
			return "GetObjectLegalHold"
		case query.Has("attributes"):
			return "GetObjectAttributes"
		default:
			return "GetObject"
		}
	case http.MethodHead:
		return "HeadObject"
	case http.MethodPut:
		switch {
		case hasCopySource && query.Has("partNumber"):
			return "UploadPartCopy"
		case hasCopySource:
			return "CopyObject"
		case query.Has("partNumber"):
			return "UploadPart"
		case query.Has("tagging"):
			return "PutObjectTagging"
		case query.Has("acl"):
			return "PutObjectAcl"
		case query.Has("retention"):
			return "PutObjectRetention"
		case query.Has("legal-hold"):
			return "PutObjectLegalHold"
		default:
			return "PutObject"
		}
	case http.MethodDelete:
		switch {
		case query.Has("uploadId"):
			return "AbortMultipartUpload"
		case query.Has("tagging"):
			return "DeleteObjectTagging"
		default:
			return "DeleteObject"
		}
	case http.MethodPost:
		switch {
		case query.Has("uploads"):
			return "CreateMultipartUpload"
		case query.Has("uploadId"):
			return "CompleteMultipartUpload"
		case query.Has("select") && query.Get("select-type") == "2":
			return "SelectObjectContent"
		}
	}
	return "GetObject"
}

// ─── Encode ───────────────────────────────────────────────────────────────────

func (c *S3Codec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	// Raw body passthrough (GetObject / range reads)
	if pass, _ := resp.Data["_passthrough"].(bool); pass {
		h := http.Header{}
		if ct, ok := resp.Data["_content_type"].(string); ok && ct != "" {
			h.Set("Content-Type", ct)
		} else {
			h.Set("Content-Type", "application/octet-stream")
		}
		if etag, ok := resp.Data["ETag"].(string); ok {
			h.Set("ETag", etag)
		}
		if lm, ok := resp.Data["LastModified"].(string); ok {
			h.Set("Last-Modified", lm)
		}
		if cl, ok := resp.Data["ContentLength"]; ok {
			h.Set("Content-Length", fmt.Sprintf("%v", cl))
		}
		// Emit x-amz-meta-* user metadata headers.
		if md, ok := resp.Data["_metadata"].(map[string]string); ok {
			for k, v := range md {
				h.Set("x-amz-meta-"+k, v)
			}
		}
		// Response header overrides (response-content-disposition etc.)
		if overrides, ok := resp.Data["_response_overrides"].(map[string]string); ok {
			for k, v := range overrides {
				h.Set(k, v)
			}
		}
		// Content-Range for partial content responses.
		if start, ok := resp.Data["_range_start"].(int64); ok {
			end, _ := resp.Data["_range_end"].(int64)
			total, _ := resp.Data["_range_total"].(int64)
			h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
		}
		// Checksum: only emit when client requests ChecksumMode=ENABLED and not a range read.
		// AWS omits checksums on partial content (206) because the stored checksum covers
		// the full object, not the byte range.
		isRangeRead := resp.Data["_range_start"] != nil
		checksumRequested := strings.EqualFold(nr.Raw.Header.Get("x-amz-checksum-mode"), "ENABLED")
		if checksumRequested && !isRangeRead {
			if algo, ok := resp.Data["_checksum_algo"].(string); ok && algo != "" {
				// P4.4: emit the algorithm the object was stored with
				hdrName := "x-amz-checksum-" + strings.ToLower(algo)
				if val, ok := resp.Data["_checksum_value"].(string); ok && val != "" {
					h.Set(hdrName, val)
				}
			} else if crc32v, ok := resp.Data["_crc32"].(string); ok && crc32v != "" {
				h.Set("x-amz-checksum-crc32", crc32v)
			}
		}
		status := resp.HTTPStatus
		if s, ok := resp.Data["_status"].(int); ok {
			status = s
		}
		s3EmitVersionSSEHeaders(h, resp.Data)
		// Streaming response: headers are set; gateway will io.Copy the stream.
		if _, ok := resp.Data["_stream"].(io.ReadCloser); ok {
			return status, h, nil
		}
		body, _ := resp.Data["_raw_body"].([]byte)
		if h.Get("x-amz-checksum-crc32") == "" {
			h.Set("x-amz-checksum-crc32", s3ChecksumCRC32(body))
		}
		return status, h, body
	}

	// SelectObjectContent — AWS event-stream binary encoding.
	if nr.Action == "SelectObjectContent" {
		payload, _ := resp.Data["_select_payload"].([]byte)
		h := http.Header{}
		h.Set("Content-Type", "application/vnd.amazon.eventstream")
		body := s3BuildSelectEventStream(payload)
		return 200, h, body
	}

	// 304 Not Modified — ETag/Last-Modified headers, no body.
	if resp.HTTPStatus == 304 {
		return 304, s3MetaHeaders(resp.Data), nil
	}

	// HeadObject / HeadBucket — headers only, no body
	if nr.Action == "HeadObject" || nr.Action == "HeadBucket" {
		h := s3MetaHeaders(resp.Data)
		return resp.HTTPStatus, h, nil
	}

	// No-body responses (DeleteObject/DeleteObjects/etc.)
	if resp.HTTPStatus == 204 || resp.HTTPStatus == 0 {
		h := http.Header{}
		if etag, ok := resp.Data["ETag"].(string); ok {
			h.Set("ETag", etag)
		}
		if vid, ok := resp.Data["_version_id"].(string); ok && vid != "" {
			h.Set("x-amz-version-id", vid)
		}
		if dm, ok := resp.Data["_delete_marker"].(bool); ok && dm {
			h.Set("x-amz-delete-marker", "true")
		}
		return 204, h, nil
	}

	// PutObject / UploadPart — ETag header, empty body
	if nr.Action == "PutObject" || nr.Action == "UploadPart" {
		h := http.Header{}
		if etag, ok := resp.Data["ETag"].(string); ok {
			h.Set("ETag", etag)
		}
		if nr.Action == "PutObject" {
			if hdr, ok := nr.Params["_checksum_header"].(string); ok {
				// Client supplied a checksum algorithm — echo the value back.
				if val, ok := nr.Params["_checksum_value"].(string); ok {
					h.Set(hdr, val)
				}
			} else if crc32v, ok := resp.Data["_server_crc32"].(string); ok && crc32v != "" {
				// Provider computed CRC32 during streaming write.
				h.Set("x-amz-checksum-crc32", crc32v)
			} else {
				// Buffered path: compute from the in-memory body.
				uploadedBody, _ := nr.Params["_body"].([]byte)
				h.Set("x-amz-checksum-crc32", s3ChecksumCRC32(uploadedBody))
			}
		}
		s3EmitVersionSSEHeaders(h, resp.Data)
		return resp.HTTPStatus, h, nil
	}

	// CreateBucket — Location header
	if nr.Action == "CreateBucket" {
		h := http.Header{}
		if loc, ok := resp.Data["Location"].(string); ok {
			h.Set("Location", loc)
		}
		return resp.HTTPStatus, h, nil
	}

	// Raw JSON passthrough (GetBucketPolicy)
	if raw, ok := resp.Data["_raw_json"].(string); ok {
		h := http.Header{}
		h.Set("Content-Type", "application/json")
		return resp.HTTPStatus, h, []byte(raw)
	}

	// Raw XML passthrough (GetBucketWebsite, GetBucketLogging, GetBucketReplication)
	if raw, ok := resp.Data["_raw_xml"].(string); ok {
		h := http.Header{}
		h.Set("Content-Type", "application/xml")
		return resp.HTTPStatus, h, []byte(raw)
	}

	// XML body for all other operations
	body := s3BuildXML(nr.Action, resp.Data)
	h := http.Header{}
	if len(body) > 0 {
		h.Set("Content-Type", "application/xml")
	}
	// AWS SDK v2 validates CRC32 on these write operations
	if nr.Action == "CopyObject" || nr.Action == "CompleteMultipartUpload" {
		h.Set("x-amz-checksum-crc32", s3ChecksumCRC32(body))
	}
	return resp.HTTPStatus, h, body
}

func s3MetaHeaders(data map[string]any) http.Header {
	h := http.Header{}
	if ct, ok := data["ContentType"].(string); ok {
		h.Set("Content-Type", ct)
	}
	if etag, ok := data["ETag"].(string); ok {
		h.Set("ETag", etag)
	}
	if lm, ok := data["LastModified"].(string); ok {
		h.Set("Last-Modified", lm)
	}
	if cl, ok := data["ContentLength"]; ok {
		h.Set("Content-Length", fmt.Sprintf("%v", cl))
	}
	if md, ok := data["_metadata"].(map[string]string); ok {
		for k, v := range md {
			h.Set("x-amz-meta-"+k, v)
		}
	}
	if region, ok := data["_region"].(string); ok && region != "" {
		h.Set("x-amz-bucket-region", region)
	}
	s3EmitVersionSSEHeaders(h, data)
	return h
}

// s3EmitVersionSSEHeaders emits x-amz-version-id, x-amz-delete-marker, SSE headers,
// x-amz-tagging-count, and x-amz-expiration from resp.Data into h.
func s3EmitVersionSSEHeaders(h http.Header, data map[string]any) {
	if vid, ok := data["_version_id"].(string); ok && vid != "" {
		h.Set("x-amz-version-id", vid)
	}
	if dm, ok := data["_delete_marker"].(bool); ok && dm {
		h.Set("x-amz-delete-marker", "true")
	}
	if enc, ok := data["_sse"].(string); ok && enc != "" {
		h.Set("x-amz-server-side-encryption", enc)
	}
	if kmsKey, ok := data["_sse_kms_key_id"].(string); ok && kmsKey != "" {
		h.Set("x-amz-server-side-encryption-aws-kms-key-id", kmsKey)
	}
	if algo, ok := data["_ssec_algo"].(string); ok && algo != "" {
		h.Set("x-amz-server-side-encryption-customer-algorithm", algo)
	}
	if ssecMD5, ok := data["_ssec_key_md5"].(string); ok && ssecMD5 != "" {
		h.Set("x-amz-server-side-encryption-customer-key-MD5", ssecMD5)
	}
	if tc, ok := data["_tagging_count"].(int); ok && tc > 0 {
		h.Set("x-amz-tagging-count", strconv.Itoa(tc))
	}
	if exp, ok := data["_expiration"].(string); ok && exp != "" {
		h.Set("x-amz-expiration", exp)
	}
}

// ─── EncodeError ──────────────────────────────────────────────────────────────

func (c *S3Codec) EncodeError(_ *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	h := http.Header{}
	h.Set("Content-Type", "application/xml")
	// Help SDK error parsers (especially S3 Control) identify the error code.
	h.Set("x-amzn-errortype", perr.Code)
	// Consume special header-emitting keys (e.g. delete marker on 404).
	if dm, ok := perr.Data["_delete_marker"].(bool); ok && dm {
		h.Set("x-amz-delete-marker", "true")
		delete(perr.Data, "_delete_marker")
	}
	if vid, ok := perr.Data["_version_id"].(string); ok && vid != "" {
		h.Set("x-amz-version-id", vid)
		delete(perr.Data, "_version_id")
	}
	var extra strings.Builder
	for k, v := range perr.Data {
		extra.WriteString(fmt.Sprintf("<%s>%s</%s>", xmlEscape(k), xmlEscape(fmt.Sprint(v)), xmlEscape(k)))
	}
	body := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<Error><Code>%s</Code><Message>%s</Message>%s`+
			`<RequestId>00000000-0000-0000-0000-000000000000</RequestId></Error>`,
		xmlEscape(perr.Code), xmlEscape(perr.Message), extra.String(),
	)
	return perr.HTTPStatus, h, []byte(body)
}

// ─── XML builder ──────────────────────────────────────────────────────────────

func s3BuildXML(action string, data map[string]any) []byte {
	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)

	switch action {
	case "ListBuckets":
		sb.WriteString(`<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		if owner, ok := data["Owner"].(map[string]any); ok {
			sb.WriteString("<Owner>")
			sb.WriteString(xmlTag("ID", str(owner["ID"])))
			sb.WriteString(xmlTag("DisplayName", str(owner["DisplayName"])))
			sb.WriteString("</Owner>")
		}
		sb.WriteString("<Buckets>")
		if buckets, ok := data["Buckets"].([]map[string]any); ok {
			for _, b := range buckets {
				sb.WriteString("<Bucket>")
				sb.WriteString(xmlTag("Name", str(b["Name"])))
				sb.WriteString(xmlTag("CreationDate", str(b["CreationDate"])))
				sb.WriteString("</Bucket>")
			}
		}
		sb.WriteString("</Buckets>")
		sb.WriteString("</ListAllMyBucketsResult>")

	case "ListObjectsV1":
		sb.WriteString(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		sb.WriteString(xmlTag("Name", str(data["Name"])))
		sb.WriteString(xmlTag("Prefix", str(data["Prefix"])))
		sb.WriteString(xmlTag("MaxKeys", str(data["MaxKeys"])))
		if et := str(data["EncodingType"]); et != "" {
			sb.WriteString(xmlTag("EncodingType", et))
		}
		sb.WriteString(xmlTag("IsTruncated", str(data["IsTruncated"])))
		sb.WriteString(xmlTag("Marker", str(data["Marker"])))
		if nm := str(data["_nextPageToken"]); nm != "" && str(data["Delimiter"]) != "" {
			sb.WriteString(xmlTag("NextMarker", nm))
		}
		if contents, ok := data["Contents"].([]map[string]any); ok {
			for _, obj := range contents {
				sb.WriteString("<Contents>")
				sb.WriteString(xmlTag("Key", str(obj["Key"])))
				sb.WriteString(xmlTag("LastModified", str(obj["LastModified"])))
				sb.WriteString(xmlTag("ETag", str(obj["ETag"])))
				sb.WriteString(xmlTag("Size", str(obj["Size"])))
				sb.WriteString(xmlTag("StorageClass", str(obj["StorageClass"])))
				sb.WriteString("</Contents>")
			}
		}
		if cps, ok := data["CommonPrefixes"].([]string); ok {
			for _, cp := range cps {
				sb.WriteString("<CommonPrefixes>")
				sb.WriteString(xmlTag("Prefix", cp))
				sb.WriteString("</CommonPrefixes>")
			}
		}
		sb.WriteString("</ListBucketResult>")

	case "ListObjectsV2":
		sb.WriteString(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		sb.WriteString(xmlTag("Name", str(data["Name"])))
		sb.WriteString(xmlTag("Prefix", str(data["Prefix"])))
		sb.WriteString(xmlTag("MaxKeys", str(data["MaxKeys"])))
		sb.WriteString(xmlTag("KeyCount", str(data["KeyCount"])))
		if et := str(data["EncodingType"]); et != "" {
			sb.WriteString(xmlTag("EncodingType", et))
		}
		sb.WriteString(xmlTag("IsTruncated", str(data["IsTruncated"])))
		if nct := str(data["_nextPageToken"]); nct != "" {
			sb.WriteString(xmlTag("NextContinuationToken", nct))
		}
		if contents, ok := data["Contents"].([]map[string]any); ok {
			for _, obj := range contents {
				sb.WriteString("<Contents>")
				sb.WriteString(xmlTag("Key", str(obj["Key"])))
				sb.WriteString(xmlTag("LastModified", str(obj["LastModified"])))
				sb.WriteString(xmlTag("ETag", str(obj["ETag"])))
				sb.WriteString(xmlTag("Size", str(obj["Size"])))
				sb.WriteString(xmlTag("StorageClass", str(obj["StorageClass"])))
				sb.WriteString("</Contents>")
			}
		}
		if cps, ok := data["CommonPrefixes"].([]string); ok {
			for _, cp := range cps {
				sb.WriteString("<CommonPrefixes>")
				sb.WriteString(xmlTag("Prefix", cp))
				sb.WriteString("</CommonPrefixes>")
			}
		}
		sb.WriteString("</ListBucketResult>")

	case "GetBucketLocation":
		lc := str(data["LocationConstraint"])
		// S3 wire protocol represents us-east-1 as an empty LocationConstraint element.
		if lc == "us-east-1" {
			lc = ""
		}
		if lc == "" {
			sb.WriteString(`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`)
		} else {
			sb.WriteString(`<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
			sb.WriteString(xmlEscape(lc))
			sb.WriteString(`</LocationConstraint>`)
		}

	case "GetBucketVersioning":
		sb.WriteString(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		if status, ok := data["VersioningStatus"].(string); ok && status != "" {
			sb.WriteString(xmlTag("Status", status))
		}
		sb.WriteString(`</VersioningConfiguration>`)

	case "ListObjectVersions":
		sb.WriteString(`<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		sb.WriteString(xmlTag("Name", str(data["Name"])))
		sb.WriteString(xmlTag("Prefix", str(data["Prefix"])))
		sb.WriteString(xmlTag("KeyMarker", str(data["KeyMarker"])))
		sb.WriteString(xmlTag("VersionIdMarker", str(data["VersionIdMarker"])))
		sb.WriteString(xmlTag("MaxKeys", str(data["MaxKeys"])))
		sb.WriteString(xmlTag("IsTruncated", str(data["IsTruncated"])))
		if versions, ok := data["Versions"].([]map[string]any); ok {
			for _, v := range versions {
				isDM, _ := v["IsDeleteMarker"].(bool)
				if isDM {
					sb.WriteString("<DeleteMarker>")
				} else {
					sb.WriteString("<Version>")
				}
				sb.WriteString(xmlTag("Key", str(v["Key"])))
				sb.WriteString(xmlTag("VersionId", str(v["VersionId"])))
				sb.WriteString(xmlTag("IsLatest", str(v["IsLatest"])))
				sb.WriteString(xmlTag("LastModified", str(v["LastModified"])))
				if !isDM {
					sb.WriteString(xmlTag("ETag", str(v["ETag"])))
					sb.WriteString(xmlTag("Size", str(v["Size"])))
					sb.WriteString(xmlTag("StorageClass", str(v["StorageClass"])))
				}
				if isDM {
					sb.WriteString("</DeleteMarker>")
				} else {
					sb.WriteString("</Version>")
				}
			}
		}
		sb.WriteString("</ListVersionsResult>")

	case "GetBucketAcl", "GetObjectAcl":
		sb.WriteString(`<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		if owner, ok := data["Owner"].(map[string]any); ok {
			sb.WriteString("<Owner>")
			sb.WriteString(xmlTag("ID", str(owner["ID"])))
			sb.WriteString(xmlTag("DisplayName", str(owner["DisplayName"])))
			sb.WriteString("</Owner>")
		} else {
			sb.WriteString("<Owner><ID>owner</ID><DisplayName>owner</DisplayName></Owner>")
		}
		sb.WriteString("<AccessControlList>")
		if grants, ok := data["Grants"].([]map[string]any); ok {
			for _, g := range grants {
				sb.WriteString("<Grant>")
				granteeType := str(g["GranteeType"])
				sb.WriteString(fmt.Sprintf(`<Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="%s">`, xmlEscape(granteeType)))
				if granteeType == "CanonicalUser" {
					sb.WriteString(xmlTag("ID", str(g["GranteeID"])))
					sb.WriteString(xmlTag("DisplayName", str(g["GranteeID"])))
				} else if granteeType == "Group" {
					sb.WriteString(xmlTag("URI", str(g["GranteeURI"])))
				}
				sb.WriteString("</Grantee>")
				sb.WriteString(xmlTag("Permission", str(g["Permission"])))
				sb.WriteString("</Grant>")
			}
		} else {
			sb.WriteString("<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">")
			sb.WriteString("<ID>owner</ID><DisplayName>owner</DisplayName></Grantee>")
			sb.WriteString("<Permission>FULL_CONTROL</Permission></Grant>")
		}
		sb.WriteString("</AccessControlList>")
		sb.WriteString("</AccessControlPolicy>")

	case "GetObjectTagging", "GetBucketTagging":
		sb.WriteString(`<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet>`)
		if tags, ok := data["Tags"].(map[string]string); ok {
			for k, v := range tags {
				sb.WriteString("<Tag>")
				sb.WriteString(xmlTag("Key", k))
				sb.WriteString(xmlTag("Value", v))
				sb.WriteString("</Tag>")
			}
		}
		sb.WriteString("</TagSet></Tagging>")

	case "GetBucketEncryption":
		sb.WriteString(`<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		if rule, ok := data["EncryptionRule"].(map[string]any); ok {
			sb.WriteString("<Rule><ApplyServerSideEncryptionByDefault>")
			sb.WriteString(xmlTag("SSEAlgorithm", str(rule["Algorithm"])))
			if kms, ok := rule["KMSKeyID"].(string); ok && kms != "" {
				sb.WriteString(xmlTag("KMSMasterKeyID", kms))
			}
			sb.WriteString("</ApplyServerSideEncryptionByDefault>")
			if bke, _ := rule["BucketKeyEnabled"].(bool); bke {
				sb.WriteString(xmlTag("BucketKeyEnabled", "true"))
			}
			sb.WriteString("</Rule>")
		}
		sb.WriteString("</ServerSideEncryptionConfiguration>")

	case "GetObjectLockConfiguration":
		sb.WriteString(`<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		if cfg, ok := data["ObjectLockConfig"].(map[string]any); ok {
			sb.WriteString(xmlTag("ObjectLockEnabled", str(cfg["ObjectLockEnabled"])))
			if mode, ok := cfg["DefaultMode"].(string); ok && mode != "" {
				sb.WriteString("<Rule><DefaultRetention>")
				sb.WriteString(xmlTag("Mode", mode))
				if days, ok := cfg["DefaultDays"].(int); ok && days > 0 {
					sb.WriteString(xmlTag("Days", strconv.Itoa(days)))
				}
				sb.WriteString("</DefaultRetention></Rule>")
			}
		}
		sb.WriteString("</ObjectLockConfiguration>")

	case "GetObjectRetention":
		sb.WriteString(`<Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		sb.WriteString(xmlTag("Mode", str(data["LockMode"])))
		if rd, ok := data["RetainUntilDate"].(string); ok && rd != "" {
			sb.WriteString(xmlTag("RetainUntilDate", rd))
		}
		sb.WriteString("</Retention>")

	case "GetObjectLegalHold":
		sb.WriteString(`<LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		sb.WriteString(xmlTag("Status", str(data["LegalHoldStatus"])))
		sb.WriteString("</LegalHold>")

	case "GetBucketLifecycleConfiguration":
		sb.WriteString(`<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		if rules, ok := data["LifecycleRules"].([]any); ok {
			for _, r := range rules {
				rm, _ := r.(map[string]any)
				if rm == nil {
					continue
				}
				sb.WriteString("<Rule>")
				sb.WriteString(xmlTag("ID", str(rm["ID"])))
				sb.WriteString(xmlTag("Status", str(rm["Status"])))
				if prefix := str(rm["Prefix"]); prefix != "" {
					sb.WriteString("<Filter>")
					sb.WriteString(xmlTag("Prefix", prefix))
					sb.WriteString("</Filter>")
				}
				days := 0
				switch d := rm["ExpirationDays"].(type) {
				case int:
					days = d
				case float64:
					days = int(d)
				}
				if days > 0 {
					sb.WriteString("<Expiration>")
					sb.WriteString(xmlTag("Days", strconv.Itoa(days)))
					sb.WriteString("</Expiration>")
				}
				sb.WriteString("</Rule>")
			}
		}
		sb.WriteString("</LifecycleConfiguration>")

	case "GetBucketCors":
		sb.WriteString(`<CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		if rules, ok := data["CORSRules"].([]any); ok {
			for _, r := range rules {
				rm, _ := r.(map[string]any)
				if rm == nil {
					continue
				}
				sb.WriteString("<CORSRule>")
				for _, o := range toStringSlice(rm["AllowedOrigins"]) {
					sb.WriteString(xmlTag("AllowedOrigin", o))
				}
				for _, m := range toStringSlice(rm["AllowedMethods"]) {
					sb.WriteString(xmlTag("AllowedMethod", m))
				}
				for _, h := range toStringSlice(rm["AllowedHeaders"]) {
					sb.WriteString(xmlTag("AllowedHeader", h))
				}
				for _, e := range toStringSlice(rm["ExposeHeaders"]) {
					sb.WriteString(xmlTag("ExposeHeader", e))
				}
				if age, ok := rm["MaxAgeSeconds"].(int); ok && age > 0 {
					sb.WriteString(xmlTag("MaxAgeSeconds", strconv.Itoa(age)))
				} else if age, ok := rm["MaxAgeSeconds"].(float64); ok && age > 0 {
					sb.WriteString(xmlTag("MaxAgeSeconds", strconv.Itoa(int(age))))
				}
				sb.WriteString("</CORSRule>")
			}
		}
		sb.WriteString("</CORSConfiguration>")

	case "CreateMultipartUpload":
		sb.WriteString(`<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		sb.WriteString(xmlTag("Bucket", str(data["Bucket"])))
		sb.WriteString(xmlTag("Key", str(data["Key"])))
		sb.WriteString(xmlTag("UploadId", str(data["UploadId"])))
		sb.WriteString("</InitiateMultipartUploadResult>")

	case "CompleteMultipartUpload":
		sb.WriteString(`<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		sb.WriteString(xmlTag("Location", str(data["Location"])))
		sb.WriteString(xmlTag("Bucket", str(data["Bucket"])))
		sb.WriteString(xmlTag("Key", str(data["Key"])))
		sb.WriteString(xmlTag("ETag", str(data["ETag"])))
		sb.WriteString("</CompleteMultipartUploadResult>")

	case "ListMultipartUploads":
		sb.WriteString(`<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		sb.WriteString(xmlTag("Bucket", str(data["Bucket"])))
		if uploads, ok := data["Uploads"].([]map[string]any); ok {
			for _, u := range uploads {
				sb.WriteString("<Upload>")
				sb.WriteString(xmlTag("Key", str(u["Key"])))
				sb.WriteString(xmlTag("UploadId", str(u["UploadId"])))
				sb.WriteString(xmlTag("Initiated", str(u["Initiated"])))
				sb.WriteString("</Upload>")
			}
		}
		sb.WriteString("</ListMultipartUploadsResult>")

	case "ListParts":
		sb.WriteString(`<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		if parts, ok := data["Parts"].([]map[string]any); ok {
			for _, pt := range parts {
				sb.WriteString("<Part>")
				sb.WriteString(xmlTag("PartNumber", fmt.Sprintf("%v", pt["PartNumber"])))
				sb.WriteString(xmlTag("ETag", str(pt["ETag"])))
				sb.WriteString(xmlTag("Size", fmt.Sprintf("%v", pt["Size"])))
				sb.WriteString("</Part>")
			}
		}
		sb.WriteString("</ListPartsResult>")

	case "DeleteObjects":
		sb.WriteString(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		if deleted, ok := data["Deleted"].([]map[string]any); ok {
			for _, d := range deleted {
				sb.WriteString("<Deleted>")
				sb.WriteString(xmlTag("Key", str(d["Key"])))
				sb.WriteString("</Deleted>")
			}
		}
		sb.WriteString("</DeleteResult>")

	case "CopyObject":
		if result, ok := data["CopyObjectResult"].(map[string]any); ok {
			sb.WriteString(`<CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
			sb.WriteString(xmlTag("ETag", str(result["ETag"])))
			sb.WriteString(xmlTag("LastModified", str(result["LastModified"])))
			sb.WriteString("</CopyObjectResult>")
		}

	case "UploadPartCopy":
		if result, ok := data["CopyPartResult"].(map[string]any); ok {
			sb.WriteString(`<CopyPartResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
			sb.WriteString(xmlTag("ETag", str(result["ETag"])))
			sb.WriteString(xmlTag("LastModified", str(result["LastModified"])))
			sb.WriteString("</CopyPartResult>")
		}

	case "GetObjectAttributes":
		sb.WriteString(`<GetObjectAttributesResponse xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		sb.WriteString(xmlTag("LastModified", str(data["LastModified"])))
		if etag, ok := data["ETag"].(string); ok && etag != "" {
			sb.WriteString(xmlTag("ETag", etag))
		}
		if size, ok := data["ObjectSize"]; ok {
			sb.WriteString(xmlTag("ObjectSize", fmt.Sprintf("%v", size)))
		}
		if sc, ok := data["StorageClass"].(string); ok && sc != "" {
			sb.WriteString(xmlTag("StorageClass", sc))
		}
		if cksum, ok := data["Checksum"].(map[string]any); ok {
			sb.WriteString("<Checksum>")
			for k, v := range cksum {
				sb.WriteString(xmlTag(k, str(v)))
			}
			sb.WriteString("</Checksum>")
		}
		sb.WriteString("</GetObjectAttributesResponse>")

	case "GetBucketOwnershipControls":
		sb.WriteString(`<OwnershipControls xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		sb.WriteString("<Rule>")
		sb.WriteString(xmlTag("ObjectOwnership", str(data["ObjectOwnership"])))
		sb.WriteString("</Rule>")
		sb.WriteString("</OwnershipControls>")

	case "CreateAccessPoint":
		sb.WriteString(`<CreateAccessPointResult xmlns="http://awss3control.amazonaws.com/doc/2018-08-20/">`)
		sb.WriteString(xmlTag("AccessPointArn", str(data["AccessPointArn"])))
		sb.WriteString(xmlTag("Alias", str(data["Alias"])))
		sb.WriteString("</CreateAccessPointResult>")

	case "GetAccessPoint":
		sb.WriteString(`<GetAccessPointResult xmlns="http://awss3control.amazonaws.com/doc/2018-08-20/">`)
		sb.WriteString(xmlTag("Name", str(data["Name"])))
		sb.WriteString(xmlTag("Bucket", str(data["Bucket"])))
		sb.WriteString(xmlTag("AccessPointArn", str(data["AccessPointArn"])))
		sb.WriteString(xmlTag("Alias", str(data["Alias"])))
		sb.WriteString(xmlTag("NetworkOrigin", str(data["NetworkOrigin"])))
		if cd := str(data["CreationDate"]); cd != "" {
			sb.WriteString(xmlTag("CreationDate", cd))
		}
		sb.WriteString("</GetAccessPointResult>")

	case "ListAccessPoints":
		sb.WriteString(`<ListAccessPointsResult xmlns="http://awss3control.amazonaws.com/doc/2018-08-20/">`)
		sb.WriteString("<AccessPointList>")
		if aps, ok := data["AccessPointList"].([]map[string]any); ok {
			for _, ap := range aps {
				sb.WriteString("<AccessPoint>")
				sb.WriteString(xmlTag("Name", str(ap["Name"])))
				sb.WriteString(xmlTag("Bucket", str(ap["Bucket"])))
				sb.WriteString(xmlTag("AccessPointArn", str(ap["AccessPointArn"])))
				sb.WriteString(xmlTag("Alias", str(ap["Alias"])))
				sb.WriteString(xmlTag("NetworkOrigin", str(ap["NetworkOrigin"])))
				if cd := str(ap["CreationDate"]); cd != "" {
					sb.WriteString(xmlTag("CreationDate", cd))
				}
				sb.WriteString("</AccessPoint>")
			}
		}
		sb.WriteString("</AccessPointList>")
		if nt := str(data["NextToken"]); nt != "" {
			sb.WriteString(xmlTag("NextToken", nt))
		}
		sb.WriteString("</ListAccessPointsResult>")

	case "GetAccessPointPolicy":
		sb.WriteString(`<GetAccessPointPolicyResult xmlns="http://awss3control.amazonaws.com/doc/2018-08-20/">`)
		sb.WriteString(xmlTag("Policy", str(data["Policy"])))
		sb.WriteString("</GetAccessPointPolicyResult>")

	case "GetAccessPointPolicyStatus":
		sb.WriteString(`<GetAccessPointPolicyStatusResult xmlns="http://awss3control.amazonaws.com/doc/2018-08-20/">`)
		sb.WriteString("<PolicyStatus>")
		isPublic := false
		if ps, ok := data["PolicyStatus"].(map[string]any); ok {
			if v, ok := ps["IsPublic"].(bool); ok {
				isPublic = v
			}
		}
		sb.WriteString(xmlTag("IsPublic", strconv.FormatBool(isPublic)))
		sb.WriteString("</PolicyStatus>")
		sb.WriteString("</GetAccessPointPolicyStatusResult>")
	}

	return []byte(sb.String())
}

// toStringSlice converts []string or []any (CORS rules stored as JSON) to []string.
func toStringSlice(v any) []string {
	switch s := v.(type) {
	case []string:
		return s
	case []any:
		out := make([]string, 0, len(s))
		for _, item := range s {
			if str, ok := item.(string); ok {
				out = append(out, str)
			}
		}
		return out
	}
	return nil
}

// ─── P2-8: Presigned URL expiration ──────────────────────────────────────────

func checkPresignedExpiration(query url.Values) error {
	// SigV4 presigned URL.
	if dateStr := query.Get("X-Amz-Date"); dateStr != "" {
		t, err := time.Parse("20060102T150405Z", dateStr)
		if err != nil {
			return nil // skip on parse error
		}
		expiresStr := query.Get("X-Amz-Expires")
		expires, err := strconv.Atoi(expiresStr)
		if err != nil {
			return nil
		}
		if clock.Now().After(t.Add(time.Duration(expires) * time.Second)) {
			return fmt.Errorf("expired")
		}
		return nil
	}
	// SigV2 presigned URL.
	if expiresStr := query.Get("Expires"); expiresStr != "" {
		expiresUnix, err := strconv.ParseInt(expiresStr, 10, 64)
		if err != nil {
			return nil
		}
		if clock.Now().Unix() > expiresUnix {
			return fmt.Errorf("expired")
		}
	}
	return nil
}

// ─── S3 Select event-stream ───────────────────────────────────────────────────

// s3BuildSelectEventStream encodes payload as AWS event-stream binary frames:
// a Records event containing the raw bytes followed by an End event.
// Frame layout: [4B total][4B headers-len][4B prelude-crc32][headers][payload][4B msg-crc32]
// Header layout: [1B name-len][name][1B type=7 (string)][2B value-len][value]
func s3BuildSelectEventStream(payload []byte) []byte {
	var out []byte
	out = append(out, s3SelectFrame(
		[][2]string{
			{":event-type", "Records"},
			{":content-type", "application/octet-stream"},
			{":message-type", "event"},
		},
		payload,
	)...)
	out = append(out, s3SelectFrame(
		[][2]string{
			{":event-type", "End"},
			{":message-type", "event"},
		},
		nil,
	)...)
	return out
}

func s3SelectFrame(hdrs [][2]string, payload []byte) []byte {
	// Encode headers.
	var hdrBuf []byte
	for _, kv := range hdrs {
		name, val := kv[0], kv[1]
		hdrBuf = append(hdrBuf, byte(len(name)))
		hdrBuf = append(hdrBuf, []byte(name)...)
		hdrBuf = append(hdrBuf, 7) // type: string
		hdrBuf = binary.BigEndian.AppendUint16(hdrBuf, uint16(len(val)))
		hdrBuf = append(hdrBuf, []byte(val)...)
	}

	totalLen := uint32(4 + 4 + 4 + len(hdrBuf) + len(payload) + 4)
	hdrLen := uint32(len(hdrBuf))

	prelude := make([]byte, 8)
	binary.BigEndian.PutUint32(prelude[0:], totalLen)
	binary.BigEndian.PutUint32(prelude[4:], hdrLen)
	preludeCRC := crc32.ChecksumIEEE(prelude)

	msg := make([]byte, 0, totalLen)
	msg = append(msg, prelude...)
	msg = binary.BigEndian.AppendUint32(msg, preludeCRC)
	msg = append(msg, hdrBuf...)
	msg = append(msg, payload...)
	msgCRC := crc32.ChecksumIEEE(msg)
	msg = binary.BigEndian.AppendUint32(msg, msgCRC)
	return msg
}
