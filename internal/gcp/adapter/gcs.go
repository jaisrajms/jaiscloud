package gcp

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"

	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// GCSCodec decodes/encodes the Google Cloud Storage JSON + media wire API.
// Implements adapter.Codec.
type GCSCodec struct{}

func (c *GCSCodec) ServiceName() string { return "storage" }

// Decode routes the request to the correct storage handler based on path prefix.
func (c *GCSCodec) Decode(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	path := r.URL.EscapedPath()
	switch {
	case strings.HasPrefix(path, "/upload/storage/v1/"):
		return c.decodeUpload(r, body, strings.TrimPrefix(path, "/upload/storage/v1/"))
	case strings.HasPrefix(path, "/resumable/upload/storage/v1/"):
		// Discovery-documented resumable initiation path
		// (mediaUpload.protocols.resumable.path) used by `gcloud storage cp`.
		return c.decodeUpload(r, body, strings.TrimPrefix(path, "/resumable/upload/storage/v1/"))
	case strings.HasPrefix(path, "/download/storage/v1/"):
		return c.decodeDownload(r, body, strings.TrimPrefix(path, "/download/storage/v1/"))
	case strings.HasPrefix(path, "/storage/v1/"):
		return c.decodeStorage(r, body, strings.TrimPrefix(path, "/storage/v1/"))
	default:
		// Raw media path: /{bucket}/{object...} (GET/HEAD) → media download.
		return c.decodeRawMedia(r, body)
	}
}

// decodeRawMedia handles the raw XML API URL /{bucket}/{object...}: object
// downloads (GET/HEAD, used by the GCS storage client) and object uploads
// (PUT, signed-URL or plain XML API). The object name's slashes are literal
// path separators here (unlike the JSON API, where they are percent-encoded).
func (c *GCSCodec) decodeRawMedia(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	seg := splitEscaped(strings.TrimPrefix(r.URL.EscapedPath(), "/"))
	if len(seg) < 2 || seg[0] == "" {
		return nil, model.NewProviderError("InvalidRequest", "unsupported storage path", 404)
	}
	nr := &model.NormalizedRequest{Service: "storage", Params: map[string]any{}, Raw: r}
	queryToParams(r, nr.Params)
	csekFromHeaders(r, nr.Params)
	metadataFromHeaders(r, nr.Params)
	nr.Params["bucket"] = seg[0]
	nr.Params["object"] = strings.Join(seg[1:], "/")
	// This request arrived on the XML API raw path (not the JSON API), so the
	// provider/encoder answer it in the XML wire shape: media downloads carry a
	// quoted hex-MD5 ETag and an XML PUT Object returns 200 + an empty body.
	nr.Params[wire.XMLAPIKey] = true
	signed := hasSignedSignature(r)
	if signed {
		nr.Params[wire.SignedURLKey] = true
	}
	switch {
	case r.Method == http.MethodGet || r.Method == http.MethodHead:
		nr.Action = "ObjectsGetMedia"
	case r.Method == http.MethodPut:
		// XML API object upload: PUT /{bucket}/{object}. This covers V4
		// signed-URL uploads and plain (unsigned) XML PUTs — the emulator does
		// not enforce authentication, so an unsigned PUT stores the object like
		// any other write. CSEK/CMEK material and x-goog-meta-* metadata are
		// picked up by the header extractors above.
		//
		// Other XML API PUT variants (copy with x-goog-copy-source, compose,
		// set ACL, set retention/encryption, multipart parts) share the PUT
		// method but are not implemented. Rejecting them here keeps them a loud
		// 404 rather than silently storing the request body as the object.
		if r.Header.Get("x-goog-copy-source") != "" || hasUnsupportedXMLPutSubResource(r.URL.Query()) {
			return nil, model.NewProviderError("InvalidRequest", "unsupported storage path", 404)
		}
		nr.Action = "ObjectsInsert"
		if body == nil {
			nr.Params[wire.StreamKey] = r.Body
		} else {
			nr.Params[wire.MediaKey] = body
		}
		if ct := r.Header.Get("Content-Type"); ct != "" {
			nr.Params[wire.ContentTypeKey] = ct
		}
		// The XML API PUT's Content-Encoding is the object's own stored
		// encoding (e.g. gzip), captured as Object.contentEncoding (J69).
		if ce := r.Header.Get("Content-Encoding"); ce != "" {
			nr.Params["contentEncoding"] = ce
		}
	default:
		return nil, model.NewProviderError("InvalidRequest", "unsupported storage path", 404)
	}
	return nr, nil
}

// hasSignedSignature reports whether the request carries a V4 signed-URL
// signature query parameter (case-preserving key). Presence marks the request
// as a signed URL; format/expiry validation happens in the storage provider,
// which deliberately does not verify the cryptographic signature.
func hasSignedSignature(r *http.Request) bool {
	_, ok := r.URL.Query()["X-Goog-Signature"]
	return ok
}

// unsupportedXMLPutSubResources lists the XML API object sub-resource query
// parameters that address an operation other than a plain object upload. The
// emulator does not implement them, so a PUT carrying one must not be decoded
// as an ObjectsInsert (which would store the request body as the object).
var unsupportedXMLPutSubResources = []string{
	"acl", "compose", "retention", "encryption", "tagging",
	"uploads", "uploadId", "partNumber",
}

// hasUnsupportedXMLPutSubResource reports whether q carries an XML API object
// sub-resource parameter that is not a plain object upload.
func hasUnsupportedXMLPutSubResource(q url.Values) bool {
	for _, k := range unsupportedXMLPutSubResources {
		if q.Has(k) {
			return true
		}
	}
	return false
}

// decodeDownload handles /download/storage/v1/... — always a media download, so
// object GETs are forced to ObjectsGetMedia regardless of the alt= param.
func (c *GCSCodec) decodeDownload(r *http.Request, body []byte, rest string) (*model.NormalizedRequest, error) {
	nr, err := c.decodeStorage(r, body, rest)
	if err != nil {
		return nil, err
	}
	if nr.Action == "ObjectsGet" {
		nr.Action = "ObjectsGetMedia"
	}
	return nr, nil
}

// decodeStorage handles the metadata/JSON API under /storage/v1/ and /download/storage/v1/.
func (c *GCSCodec) decodeStorage(r *http.Request, body []byte, rest string) (*model.NormalizedRequest, error) {
	seg := splitEscaped(rest)
	nr := &model.NormalizedRequest{Service: "storage", Params: map[string]any{}, Raw: r}
	queryToParams(r, nr.Params)
	csekFromHeaders(r, nr.Params)
	metadataFromHeaders(r, nr.Params)
	// The request's absolute base URL, used to build emulator-relative
	// selfLink/mediaLink fields (the SDKs follow mediaLink, so it must point
	// back at this emulator, not real GCS).
	nr.Params[wire.BaseURLKey] = baseURLFromRequest(r)

	// Resumable-session requests can arrive on the JSON path when a client
	// rewrites the session URI's /upload/storage/v1/ prefix to /storage/v1/
	// (emulator accommodation; real GCS serves sessions only at
	// /upload/storage/v1/). Route before the generic switch so the object
	// collection branch below does not swallow them.
	if uploadType, _ := nr.Params["uploadType"].(string); uploadType == "resumable" {
		return c.decodeStorageResumable(r, body, seg, nr)
	}
	if id, _ := nr.Params["upload_id"].(string); id != "" {
		return c.decodeStorageResumable(r, body, seg, nr)
	}

	switch {
	case len(seg) == 1 && seg[0] == "b":
		// /b — bucket list (GET) or insert (POST)
		if r.Method == http.MethodPost {
			nr.Action = "BucketsInsert"
		} else {
			nr.Action = "BucketsList"
		}
		m, err := parseJSON(body)
		if err != nil {
			return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
		}
		nr.Params["body"] = m
	case len(seg) == 2 && seg[0] == "b":
		// /b/{bucket}
		nr.Params["bucket"] = seg[1]
		switch r.Method {
		case http.MethodPost, http.MethodPut, http.MethodPatch:
			nr.Action = "BucketsUpdate"
			m, err := parseJSON(body)
			if err != nil {
				return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
			}
			nr.Params["body"] = m
		case http.MethodDelete:
			nr.Action = "BucketsDelete"
		default:
			nr.Action = "BucketsGet"
		}
	case len(seg) == 3 && seg[0] == "b" && seg[2] == "lockRetentionPolicy":
		// /b/{bucket}/lockRetentionPolicy — buckets.lockRetentionPolicy. The
		// ifMetagenerationMatch query param is captured by queryToParams.
		nr.Params["bucket"] = seg[1]
		nr.Action = "BucketsLockRetentionPolicy"
	case len(seg) == 3 && seg[0] == "b" && seg[2] == "storageLayout":
		// /b/{bucket}/storageLayout — buckets.getStorageLayout. No request
		// body; the optional `prefix` permission-check query param is captured
		// by queryToParams.
		nr.Params["bucket"] = seg[1]
		nr.Action = "BucketsGetStorageLayout"
	case len(seg) >= 3 && seg[0] == "b" && seg[2] == "notificationConfigs":
		// /b/{bucket}/notificationConfigs[/{notification}] —
		// storage.notifications.{insert,list,get,delete}.
		nr.Params["bucket"] = seg[1]
		if len(seg) == 3 {
			if r.Method == http.MethodPost {
				nr.Action = "NotificationsInsert"
				m, err := parseJSON(body)
				if err != nil {
					return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
				}
				nr.Params["body"] = m
			} else {
				nr.Action = "NotificationsList"
			}
		} else {
			nr.Params["notification"] = seg[3]
			if r.Method == http.MethodDelete {
				nr.Action = "NotificationsDelete"
			} else {
				nr.Action = "NotificationsGet"
			}
		}
	case len(seg) >= 3 && seg[0] == "b" && seg[2] == "iam":
		// /b/{bucket}/iam
		nr.Params["bucket"] = seg[1]
		if r.Method == http.MethodPut {
			nr.Action = "BucketsSetIamPolicy"
			m, err := parseJSON(body)
			if err != nil {
				return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
			}
			nr.Params["body"] = m
		} else {
			nr.Action = "BucketsGetIamPolicy"
		}
	case len(seg) == 3 && seg[0] == "b" && seg[2] == "acl":
		// /b/{bucket}/acl
		nr.Params["bucket"] = seg[1]
		if r.Method == http.MethodPost {
			nr.Action = "BucketACLInsert"
			m, err := parseJSON(body)
			if err != nil {
				return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
			}
			nr.Params["body"] = m
		} else {
			nr.Action = "BucketACLList"
		}
	case len(seg) >= 5 && seg[0] == "b" && seg[2] == "o" && seg[len(seg)-1] == "acl":
		// /b/{bucket}/o/{object...}/acl
		nr.Params["bucket"] = seg[1]
		nr.Params["object"] = strings.Join(seg[3:len(seg)-1], "/")
		if r.Method == http.MethodPost {
			nr.Action = "ObjectACLInsert"
			m, err := parseJSON(body)
			if err != nil {
				return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
			}
			nr.Params["body"] = m
		} else {
			nr.Action = "ObjectACLList"
		}
	case len(seg) >= 5 && seg[0] == "b" && seg[2] == "o" && seg[len(seg)-1] == "iam":
		// /b/{bucket}/o/{object...}/iam — object-level IAM policy.
		nr.Params["bucket"] = seg[1]
		nr.Params["object"] = strings.Join(seg[3:len(seg)-1], "/")
		if r.Method == http.MethodPut {
			nr.Action = "ObjectsSetIamPolicy"
			m, err := parseJSON(body)
			if err != nil {
				return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
			}
			nr.Params["body"] = m
		} else {
			nr.Action = "ObjectsGetIamPolicy"
		}
	case len(seg) >= 3 && seg[0] == "b" && seg[2] == "o" && segmentIndex(seg, "copyTo") >= 0:
		// /b/{srcBucket}/o/{srcObject...}/copyTo/b/{dstBucket}/o/{dstObject...}
		// objects.copy returns the destination Object directly (unlike
		// objects.rewrite's rewriteResponse envelope).
		ci := segmentIndex(seg, "copyTo")
		nr.Params["sourceBucket"] = seg[1]
		nr.Params["sourceObject"] = strings.Join(seg[3:ci], "/")
		if ci+3 < len(seg) && seg[ci+1] == "b" && seg[ci+3] == "o" {
			nr.Params["destinationBucket"] = seg[ci+2]
			nr.Params["destinationObject"] = strings.Join(seg[ci+4:], "/")
		}
		nr.Action = "ObjectsCopy"
		m, err := parseJSON(body)
		if err != nil {
			return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
		}
		if m != nil {
			nr.Params["body"] = m
		}
	case len(seg) >= 3 && seg[0] == "b" && seg[2] == "o" && segmentIndex(seg, "rewriteTo") >= 0:
		// /b/{srcBucket}/o/{srcObject...}/rewriteTo/b/{dstBucket}/o/{dstObject...}
		ri := segmentIndex(seg, "rewriteTo")
		nr.Params["sourceBucket"] = seg[1]
		nr.Params["sourceObject"] = strings.Join(seg[3:ri], "/")
		if ri+3 < len(seg) && seg[ri+1] == "b" && seg[ri+3] == "o" {
			nr.Params["destinationBucket"] = seg[ri+2]
			nr.Params["destinationObject"] = strings.Join(seg[ri+4:], "/")
		}
		nr.Action = "ObjectsRewrite"
		if r.Method == http.MethodPost {
			m, err := parseJSON(body)
			if err != nil {
				return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
			}
			nr.Params["body"] = m
		}
	case len(seg) >= 3 && seg[0] == "b" && seg[2] == "o" && segmentIndex(seg, "moveTo") >= 0:
		// /b/{srcBucket}/o/{srcObject...}/moveTo/o/{dstObject...} (same bucket)
		// or /b/{srcBucket}/o/{srcObject...}/moveTo/b/{dstBucket}/o/{dstObject...}
		// (cross-bucket; the discovery doc models the same-bucket form, but real
		// GCS clients use the /moveTo/b/.../o/... form too, so both are accepted).
		mi := segmentIndex(seg, "moveTo")
		nr.Params["sourceBucket"] = seg[1]
		nr.Params["sourceObject"] = strings.Join(seg[3:mi], "/")
		if mi+3 < len(seg) && seg[mi+1] == "b" && seg[mi+3] == "o" {
			nr.Params["destinationBucket"] = seg[mi+2]
			nr.Params["destinationObject"] = strings.Join(seg[mi+4:], "/")
		} else {
			nr.Params["destinationBucket"] = seg[1]
			nr.Params["destinationObject"] = strings.Join(seg[mi+2:], "/")
		}
		nr.Action = "ObjectsMove"
	case len(seg) >= 5 && seg[0] == "b" && seg[2] == "o" && seg[len(seg)-1] == "restore":
		// /b/{bucket}/o/{object...}/restore — objects.restore (soft-delete restore).
		// The generation to restore is carried by the ?generation= query param.
		nr.Params["bucket"] = seg[1]
		nr.Params["object"] = strings.Join(seg[3:len(seg)-1], "/")
		nr.Action = "ObjectsRestore"
	case len(seg) >= 4 && seg[0] == "b" && seg[2] == "o" && seg[len(seg)-1] == "compose":
		// /b/{bucket}/o/{destination...}/compose
		nr.Params["bucket"] = seg[1]
		nr.Params["object"] = strings.Join(seg[3:len(seg)-1], "/")
		nr.Action = "ObjectsCompose"
		m, err := parseJSON(body)
		if err != nil {
			return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
		}
		nr.Params["body"] = m
	case len(seg) >= 3 && seg[0] == "b" && seg[2] == "o":
		// /b/{bucket}/o[/{object}]
		nr.Params["bucket"] = seg[1]
		switch {
		case len(seg) == 3:
			// /b/{bucket}/o — object list (GET) or JSON-metadata insert (POST)
			if r.Method == http.MethodPost {
				nr.Action = "ObjectsInsert"
				m, err := parseJSON(body)
				if err != nil {
					return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
				}
				nr.Params["body"] = m
			} else {
				nr.Action = "ObjectsList"
			}
		default:
			// /b/{bucket}/o/{object...}
			nr.Params["object"] = strings.Join(seg[3:], "/")
			switch r.Method {
			case http.MethodDelete:
				nr.Action = "ObjectsDelete"
			case http.MethodPatch:
				// objects.patch (PATCH) — merge semantics.
				nr.Action = "ObjectsPatch"
				m, err := parseJSON(body)
				if err != nil {
					return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
				}
				nr.Params["body"] = m
			case http.MethodPut:
				// objects.update (PUT) — strict replacement semantics.
				nr.Action = "ObjectsUpdate"
				m, err := parseJSON(body)
				if err != nil {
					return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
				}
				nr.Params["body"] = m
			case http.MethodPost:
				// No JSON-API POST method targets an object resource path; keep
				// the defensive mapping so stray POSTs behave like PUT.
				nr.Action = "ObjectsUpdate"
				m, err := parseJSON(body)
				if err != nil {
					return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
				}
				nr.Params["body"] = m
			default:
				// alt=media → raw bytes; otherwise JSON metadata
				if nr.Params["alt"] == "media" {
					nr.Action = "ObjectsGetMedia"
				} else {
					nr.Action = "ObjectsGet"
				}
			}
		}
	default:
		return nil, model.NewProviderError("InvalidRequest", "unsupported storage path", 404)
	}
	return nr, nil
}

// decodeStorageResumable decodes a resumable-session request that arrived on
// the JSON /storage/v1/ path (a client-rewritten session URI). Mirrors the
// /upload/storage/v1/ resumable branch in decodeUpload: upload_id present →
// chunk/status, absent → session start.
func (c *GCSCodec) decodeStorageResumable(r *http.Request, body []byte, seg []string, nr *model.NormalizedRequest) (*model.NormalizedRequest, error) {
	if !(len(seg) >= 3 && seg[0] == "b" && seg[2] == "o") {
		return nil, model.NewProviderError("InvalidRequest", "unsupported storage path", 404)
	}
	nr.Params["bucket"] = seg[1]
	if len(seg) > 3 {
		nr.Params["object"] = strings.Join(seg[3:], "/")
	}
	if n, _ := nr.Params["name"].(string); n != "" {
		nr.Params["object"] = n
	}
	if id, _ := nr.Params["upload_id"].(string); id != "" {
		nr.Action = "ObjectsInsertResumable"
		nr.Params[wire.MediaKey] = body
		if cr := r.Header.Get("Content-Range"); cr != "" {
			nr.Params["contentRange"] = cr
		}
		if r.Header.Get("X-GUploader-No-308") == "yes" {
			nr.Params[wire.No308Key] = true
		}
		if ct := r.Header.Get("Content-Type"); ct != "" {
			nr.Params[wire.ContentTypeKey] = ct
		}
		return nr, nil
	}
	nr.Action = "ObjectsInsertStartResumable"
	m, err := parseJSON(body)
	if err != nil {
		return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
	}
	nr.Params["body"] = m
	if ct := r.Header.Get("X-Upload-Content-Type"); ct != "" {
		nr.Params[wire.ContentTypeKey] = ct
	}
	return nr, nil
}

// decodeUpload handles the media API under /upload/storage/v1/.
func (c *GCSCodec) decodeUpload(r *http.Request, body []byte, rest string) (*model.NormalizedRequest, error) {
	seg := splitEscaped(rest)
	if !(len(seg) >= 3 && seg[0] == "b" && seg[2] == "o") {
		return nil, model.NewProviderError("InvalidRequest", "unsupported upload path", 404)
	}
	nr := &model.NormalizedRequest{Service: "storage", Params: map[string]any{}, Raw: r}
	queryToParams(r, nr.Params)
	csekFromHeaders(r, nr.Params)
	metadataFromHeaders(r, nr.Params)
	// Emulator-relative self/media links for the created object.
	nr.Params[wire.BaseURLKey] = baseURLFromRequest(r)
	nr.Params["bucket"] = seg[1]

	if len(seg) > 3 {
		nr.Params["object"] = strings.Join(seg[3:], "/")
	}
	if n := nr.Params["name"]; n != nil && n != "" {
		nr.Params["object"] = n
	}

	uploadType, _ := nr.Params["uploadType"].(string)
	switch uploadType {
	case "media":
		nr.Action = "ObjectsInsert"
		if body == nil {
			// Streaming upload — the gateway left r.Body unread.
			nr.Params[wire.StreamKey] = r.Body
		} else {
			nr.Params[wire.MediaKey] = body
		}
		if ct := r.Header.Get("Content-Type"); ct != "" {
			nr.Params[wire.ContentTypeKey] = ct
		}
	case "multipart":
		nr.Action = "ObjectsInsert"
		if err := parseMultipart(r, body, nr.Params); err != nil {
			return nil, err
		}
	case "resumable":
		// The resumable session is keyed by upload_id, not by method: the Go
		// storage SDK uploads chunks with POST (not PUT) and queries status with
		// a "bytes */N" Content-Range. A request carrying upload_id is a chunk
		// upload or status query; one without is the session start.
		if id, _ := nr.Params["upload_id"].(string); id != "" {
			nr.Action = "ObjectsInsertResumable"
			nr.Params[wire.MediaKey] = body
			if cr := r.Header.Get("Content-Range"); cr != "" {
				nr.Params["contentRange"] = cr
			}
			if r.Header.Get("X-GUploader-No-308") == "yes" {
				nr.Params[wire.No308Key] = true
			}
		} else {
			nr.Action = "ObjectsInsertStartResumable"
			m, err := parseJSON(body)
			if err != nil {
				return nil, model.NewProviderError("InvalidRequest", "malformed JSON body", 400)
			}
			nr.Params["body"] = m
			// The object content type is declared once at start via the
			// X-Upload-Content-Type header (the chunk's Content-Type on PUT is
			// the media type, not the object content type).
			if ct := r.Header.Get("X-Upload-Content-Type"); ct != "" {
				nr.Params[wire.ContentTypeKey] = ct
			}
			// The SDK expects an absolute Location header back, so pass the
			// request base URL down for the provider to build it.
		}
	default:
		// No uploadType: treat POST body as raw media (defensive default).
		nr.Action = "ObjectsInsert"
		nr.Params[wire.MediaKey] = body
		if ct := r.Header.Get("Content-Type"); ct != "" {
			nr.Params[wire.ContentTypeKey] = ct
		}
	}
	return nr, nil
}

// Encode serialises a provider response. Media responses (Data carries the
// wire.MediaKey) are written as raw bytes with the stored content type; everything
// else is JSON-encoded.
func (c *GCSCodec) Encode(nr *model.NormalizedRequest, resp *model.ProviderResponse) (int, http.Header, []byte) {
	status := resp.HTTPStatus
	if status == 0 {
		status = http.StatusOK
	}
	headers := http.Header{}
	headers.Set("Content-Type", "application/json; charset=UTF-8")

	// Forward any extra response headers the provider surfaced (media-download
	// x-goog-* headers, etc.). Applied before every branch below so they are
	// emitted for streaming and buffered responses alike.
	if extra, ok := resp.Data[wire.HeadersKey].(map[string]string); ok {
		for k, v := range extra {
			headers.Set(k, v)
		}
	}

	if loc, ok := resp.Data[wire.LocationKey].(string); ok && loc != "" {
		headers.Set("Location", loc)
		return status, headers, nil
	}

	if rng, ok := resp.Data[wire.RangeKey].(string); ok && rng != "" {
		headers.Set("Range", rng)
		if so, ok := resp.Data[wire.StatusOverrideKey].(string); ok && so != "" {
			headers.Set("X-Http-Status-Code-Override", so)
		}
		return status, headers, nil
	}

	// A length-0 resume-incomplete response carries the status override but no
	// Range header. The override must be emitted independently of Range — the Go
	// SDK's only signal of resume-incomplete under X-GUploader-No-308 is this
	// header — so decouple the two and return an empty body.
	if so, ok := resp.Data[wire.StatusOverrideKey].(string); ok && so != "" {
		headers.Set("X-Http-Status-Code-Override", so)
		return status, headers, nil
	}

	// A 308 Resume Incomplete with no Range (empty session) must return an
	// empty body, not the "{}" the generic JSON marshal would emit.
	if status == http.StatusPermanentRedirect {
		return status, headers, nil
	}

	// Streaming download — the gateway will io.Copy the reader; return headers only.
	if _, ok := resp.Data["_stream"].(io.ReadCloser); ok {
		if ct, _ := resp.Data[wire.ContentTypeKey].(string); ct != "" {
			headers.Set("Content-Type", ct)
		}
		return status, headers, nil
	}

	if b, ok := resp.Data[wire.MediaKey].([]byte); ok {
		headers.Set("Content-Type", "application/octet-stream")
		if ct, _ := resp.Data[wire.ContentTypeKey].(string); ct != "" {
			headers.Set("Content-Type", ct)
		}
		return status, headers, b
	}

	// XML API object upload (PUT /{bucket}/{object}): real GCS replies 200 with an
	// empty body and the object's ETag/generation/hash as response headers, which
	// the provider attached under wire.HeadersKey. The JSON API upload paths
	// (/upload/storage/v1, /storage/v1) never set wire.XMLAPIKey, so they keep the
	// storage#object body produced below.
	if xmlAPI, _ := nr.Params[wire.XMLAPIKey].(bool); xmlAPI && nr.Action == "ObjectsInsert" {
		headers.Set("Content-Type", "application/xml; charset=UTF-8")
		return status, headers, nil
	}

	out, err := json.Marshal(resp.Data)
	if err != nil {
		return http.StatusInternalServerError, headers, []byte(`{"error":{"code":500,"message":"encode failure","status":"INTERNAL"}}`)
	}
	return status, headers, out
}

// EncodeError serialises a ProviderError as a GCS error envelope. GCS (unlike
// other GCP REST APIs) returns {"error":{"errors":[{"domain","reason","message"}],
// "code","message"}} — no "status" field.
func (c *GCSCodec) EncodeError(nr *model.NormalizedRequest, perr *model.ProviderError) (int, http.Header, []byte) {
	status := perr.HTTPStatus
	if status == 0 {
		status = http.StatusInternalServerError
	}
	headers := http.Header{}
	if perr.Data != nil {
		switch perr.Data["errorFormat"] {
		case "plain":
			// Offset-past-end 503 (resumable): a plain-text body, no envelope.
			headers.Set("Content-Type", "text/plain; charset=utf-8")
			return status, headers, []byte(perr.Message)
		case "xml":
			// Signed-URL errors use the XML API error document.
			headers.Set("Content-Type", "application/xml")
			var b strings.Builder
			b.WriteString("<?xml version='1.0' encoding='utf-8'?><Error><Code>")
			b.WriteString(perr.Code)
			b.WriteString("</Code><Message>")
			b.WriteString(perr.Message)
			b.WriteString("</Message>")
			if param, _ := perr.Data["parameterName"].(string); param != "" {
				b.WriteString("<ParameterName>")
				b.WriteString(param)
				b.WriteString("</ParameterName>")
			}
			b.WriteString("</Error>")
			return status, headers, []byte(b.String())
		}
	}
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	// J61: a request that arrived on the raw XML API path /{bucket}/{object}
	// answers errors with the XML error document and XML-API code names instead
	// of the JSON envelope. The signed-URL errors above already set
	// errorFormat "xml" and are handled there; this covers every other error on
	// the raw path (missing object, precondition, range, CSEK, ...).
	if xmlAPI, _ := nrBoolParam(nr, wire.XMLAPIKey); xmlAPI {
		return xmlAPIError(status, perr, isHeadRequest(nr))
	}
	// A provider may carry the documented GCS reason (e.g. the CSEK
	// customerEncryption* reasons); it wins over the canonical-code mapping.
	reason := gcpReason(perr.Code)
	if r, _ := perr.Data["reason"].(string); r != "" {
		reason = r
	}
	env := map[string]any{
		"error": map[string]any{
			"errors": []any{
				map[string]any{
					"domain":  "global",
					"reason":  reason,
					"message": perr.Message,
				},
			},
			"code":    status,
			"message": perr.Message,
		},
	}
	out, _ := json.Marshal(env)
	return status, headers, out
}

// nrBoolParam reads a bool request param, tolerating a nil NormalizedRequest
// (a codec can be asked to encode a decode-time error).
func nrBoolParam(nr *model.NormalizedRequest, key string) (bool, bool) {
	if nr == nil {
		return false, false
	}
	v, ok := nr.Params[key].(bool)
	return v, ok
}

// isHeadRequest reports whether the original HTTP request used HEAD.
func isHeadRequest(nr *model.NormalizedRequest) bool {
	return nr != nil && nr.Raw != nil && nr.Raw.Method == http.MethodHead
}

// xmlAPIErrorCode maps a canonical ProviderError code to the XML API <Code>
// element. The XML API uses different names than the JSON API's reason strings
// (e.g. NotFound is NoSuchKey/NoSuchBucket), and the CSEK customerEncryption*
// reasons are capitalized (CustomerEncryptionKeyIsIncorrect, ...). Unknown codes
// pass through unchanged (the signed-URL codes already are XML API names).
func xmlAPIErrorCode(perr *model.ProviderError) string {
	// An explicit xmlCode set by the provider wins (e.g. NoSuchBucket, which is
	// otherwise indistinguishable from NoSuchKey by the canonical NotFound code).
	if c, _ := perr.Data["xmlCode"].(string); c != "" {
		return c
	}
	if r, _ := perr.Data["reason"].(string); strings.HasPrefix(r, "customerEncryption") {
		return strings.ToUpper(r[:1]) + r[1:]
	}
	switch perr.Code {
	case "NotFound":
		return "NoSuchKey"
	case "InvalidRange":
		return "InvalidRange"
	case "PreconditionFailed":
		return "PreconditionFailed"
	case "InvalidArgument":
		return "InvalidArgument"
	case "AlreadyExists", "Conflict":
		return "Conflict"
	case "UnsupportedOperation":
		return "NotImplemented"
	}
	return perr.Code
}

// xmlAPIError renders the XML API error document for a request that arrived on
// the raw /{bucket}/{object} path (J61). A HEAD request gets the status and
// Content-Type but no body, matching real GCS.
func xmlAPIError(status int, perr *model.ProviderError, head bool) (int, http.Header, []byte) {
	headers := http.Header{}
	headers.Set("Content-Type", "application/xml; charset=UTF-8")
	if head {
		return status, headers, nil
	}
	var b strings.Builder
	b.WriteString("<?xml version='1.0' encoding='utf-8'?><Error><Code>")
	writeXMLEscaped(&b, xmlAPIErrorCode(perr))
	b.WriteString("</Code><Message>")
	writeXMLEscaped(&b, perr.Message)
	b.WriteString("</Message>")
	if param, _ := perr.Data["parameterName"].(string); param != "" {
		b.WriteString("<ParameterName>")
		writeXMLEscaped(&b, param)
		b.WriteString("</ParameterName>")
	}
	b.WriteString("</Error>")
	return status, headers, []byte(b.String())
}

// writeXMLEscaped writes s with XML metacharacters escaped.
func writeXMLEscaped(b *strings.Builder, s string) {
	_ = xml.EscapeText(b, []byte(s))
}

// gcpReason maps a canonical ProviderError code to a GCS error reason string.
func gcpReason(code string) string {
	switch code {
	case "NotFound":
		return "notFound"
	case "AlreadyExists":
		return "alreadyExists"
	case "bucketNotEmpty":
		return "bucketNotEmpty"
	case "Conflict":
		return "conflict"
	case "InvalidRequest":
		return "invalid"
	case "UnsupportedOperation":
		return "unsupported"
	default:
		return "internalError"
	}
}

// splitEscaped splits an escaped URL path on "/" and unescapes each segment, so
// %2F within a segment (a slash in an object name) survives as part of the name.
func splitEscaped(path string) []string {
	raw := strings.Split(strings.TrimPrefix(path, "/"), "/")
	out := make([]string, 0, len(raw))
	for _, s := range raw {
		if u, err := url.PathUnescape(s); err == nil {
			out = append(out, u)
		} else {
			out = append(out, s)
		}
	}
	return out
}

// baseURLFromRequest returns the request's "scheme://host" base, used to build
// the absolute Location header for resumable uploads (the SDK does not resolve
// relative Locations).
func baseURLFromRequest(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if fwd := r.Header.Get("X-Forwarded-Proto"); fwd != "" {
		scheme = fwd
	}
	return scheme + "://" + r.Host
}

// queryToParams copies single-valued query parameters into params as strings.
func queryToParams(r *http.Request, params map[string]any) {
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			params[k] = vs[0]
		}
	}
}

// csekFromHeaders copies the customer-supplied encryption key headers into the
// request params so the storage provider can validate and use them. The GCS
// CSEK contract carries the algorithm (AES256), the key (base64 AES-256), and
// its base64 SHA-256 digest in headers, distinct from CMEK's kmsKeyName query
// param. Copy/rewrite requests carry a second set of copy-source-encryption-*
// headers describing the source object's key.
func csekFromHeaders(r *http.Request, params map[string]any) {
	if v := r.Header.Get("x-goog-encryption-algorithm"); v != "" {
		params[wire.CSEKAlgorithm] = v
	}
	if v := r.Header.Get("x-goog-encryption-key"); v != "" {
		params[wire.CSEKKey] = v
	}
	if v := r.Header.Get("x-goog-encryption-key-sha256"); v != "" {
		params[wire.CSEKKeySHA256] = v
	}
	// Copy/rewrite source-object CSEK, carried in distinct headers that apply
	// to the source while x-goog-encryption-* (above) applies to the
	// destination.
	if v := r.Header.Get("x-goog-copy-source-encryption-algorithm"); v != "" {
		params[wire.CopySourceCSEKAlgorithm] = v
	}
	if v := r.Header.Get("x-goog-copy-source-encryption-key"); v != "" {
		params[wire.CopySourceCSEKKey] = v
	}
	if v := r.Header.Get("x-goog-copy-source-encryption-key-sha256"); v != "" {
		params[wire.CopySourceCSEKKeySHA256] = v
	}
}

// metadataFromHeaders copies x-goog-meta-* request headers into params as
// wire.MetaHeadersKey (map[string]string). Custom object metadata is carried in
// these headers by the simple-upload path (uploadType=media); the multipart and
// resumable paths carry it in the JSON body's "metadata" map instead, and the
// provider merges both sources.
func metadataFromHeaders(r *http.Request, params map[string]any) {
	md := map[string]string{}
	for k, vs := range r.Header {
		if len(vs) == 0 || !strings.HasPrefix(strings.ToLower(k), "x-goog-meta-") {
			continue
		}
		key := k[len("x-goog-meta-"):]
		md[strings.ToLower(key)] = vs[0]
	}
	if len(md) > 0 {
		params[wire.MetaHeadersKey] = md
	}
}

// segmentIndex returns the index of the first path segment equal to s, or -1.
func segmentIndex(seg []string, s string) int {
	for i, v := range seg {
		if v == s {
			return i
		}
	}
	return -1
}

// parseJSON decodes a JSON body into a map, or returns nil for empty/JSON bodies
// that are not objects (e.g. media). Never returns an error — decode failures
// are surfaced by the provider as validation errors.
func parseJSON(body []byte) (map[string]any, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// parseMultipart decodes a multipart/related upload body (JSON metadata part
// followed by a media part) into params. When body is nil the request body is
// streamed directly: the metadata part is read fully (it is small JSON) and the
// media part is passed through as wire.StreamKey so large uploads are never
// buffered in memory.
func parseMultipart(r *http.Request, body []byte, params map[string]any) error {
	ct := r.Header.Get("Content-Type")
	mediaType := strings.TrimSpace(ct)
	if i := strings.IndexByte(mediaType, ';'); i >= 0 {
		mediaType = strings.TrimSpace(mediaType[:i])
	}
	if !strings.HasPrefix(strings.ToLower(mediaType), "multipart/") {
		return model.NewProviderError("InvalidRequest", "expected multipart body", 400)
	}
	boundary := extractBoundary(ct)
	if boundary == "" {
		return model.NewProviderError("InvalidRequest", "expected multipart body", 400)
	}
	var src io.Reader = bytes.NewReader(body)
	if body == nil {
		src = r.Body
	}
	mr := multipart.NewReader(src, boundary)
	for partIdx := 0; ; partIdx++ {
		part, err := mr.NextPart()
		if err != nil {
			break // io.EOF ends the loop
		}
		if partIdx == 0 {
			var buf bytes.Buffer
			if _, err := buf.ReadFrom(part); err != nil {
				return model.NewProviderError("InvalidRequest", "malformed multipart body", 400)
			}
			m, err := parseJSON(buf.Bytes())
			if err != nil {
				return model.NewProviderError("InvalidRequest", "malformed JSON metadata", 400)
			}
			params["body"] = m
			if m != nil {
				if n, ok := m["name"].(string); ok && n != "" {
					params["object"] = n
				}
			}
		} else {
			// Media part — stream it through rather than buffering it.
			params[wire.StreamKey] = part
			if cth := part.Header.Get("Content-Type"); cth != "" {
				params[wire.ContentTypeKey] = cth
			}
			break
		}
	}
	if params[wire.MediaKey] == nil && params[wire.StreamKey] == nil {
		return model.NewProviderError("InvalidRequest", "multipart body missing media part", 400)
	}
	return nil
}

// extractBoundary extracts the multipart boundary from a Content-Type header,
// accepting bare, double-quoted, and single-quoted values. mime.ParseMediaType
// is deliberately avoided: it rejects single-quoted boundaries containing '='
// (an RFC-2045 tspecial), which gcloud/apitools emits (boundary='===...==').
func extractBoundary(ct string) string {
	idx := strings.Index(strings.ToLower(ct), "boundary=")
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(ct[idx+len("boundary="):])
	if rest == "" {
		return ""
	}
	if rest[0] == '"' || rest[0] == '\'' {
		q := rest[0]
		rest = rest[1:]
		if j := strings.IndexByte(rest, q); j >= 0 {
			return rest[:j]
		}
		return rest
	}
	if j := strings.IndexByte(rest, ';'); j >= 0 {
		rest = rest[:j]
	}
	return strings.TrimSpace(rest)
}
