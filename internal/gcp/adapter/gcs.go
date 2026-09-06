package gcp

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
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
	case strings.HasPrefix(path, "/download/storage/v1/"):
		return c.decodeDownload(r, body, strings.TrimPrefix(path, "/download/storage/v1/"))
	case strings.HasPrefix(path, "/storage/v1/"):
		return c.decodeStorage(r, body, strings.TrimPrefix(path, "/storage/v1/"))
	default:
		// Raw media path: /{bucket}/{object...} (GET/HEAD) → media download.
		return c.decodeRawMedia(r, body)
	}
}

// decodeRawMedia handles the raw media URL /{bucket}/{object...} used by the
// GCS storage client for downloads. The object name's slashes are literal path
// separators here (unlike the JSON API, where they are percent-encoded).
func (c *GCSCodec) decodeRawMedia(r *http.Request, body []byte) (*model.NormalizedRequest, error) {
	seg := splitEscaped(strings.TrimPrefix(r.URL.EscapedPath(), "/"))
	if len(seg) < 2 || seg[0] == "" {
		return nil, model.NewProviderError("InvalidRequest", "unsupported storage path", 404)
	}
	nr := &model.NormalizedRequest{Service: "storage", Params: map[string]any{}}
	queryToParams(r, nr.Params)
	csekFromHeaders(r, nr.Params)
	metadataFromHeaders(r, nr.Params)
	nr.Params["bucket"] = seg[0]
	nr.Params["object"] = strings.Join(seg[1:], "/")
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		nr.Action = "ObjectsGetMedia"
	} else {
		return nil, model.NewProviderError("InvalidRequest", "unsupported storage path", 404)
	}
	return nr, nil
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
	nr := &model.NormalizedRequest{Service: "storage", Params: map[string]any{}}
	queryToParams(r, nr.Params)
	csekFromHeaders(r, nr.Params)
	metadataFromHeaders(r, nr.Params)

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

// decodeUpload handles the media API under /upload/storage/v1/.
func (c *GCSCodec) decodeUpload(r *http.Request, body []byte, rest string) (*model.NormalizedRequest, error) {
	seg := splitEscaped(rest)
	if !(len(seg) >= 3 && seg[0] == "b" && seg[2] == "o") {
		return nil, model.NewProviderError("InvalidRequest", "unsupported upload path", 404)
	}
	nr := &model.NormalizedRequest{Service: "storage", Params: map[string]any{}}
	queryToParams(r, nr.Params)
	csekFromHeaders(r, nr.Params)
	metadataFromHeaders(r, nr.Params)
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
			nr.Params[wire.BaseURLKey] = baseURLFromRequest(r)
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
	headers.Set("Content-Type", "application/json; charset=UTF-8")
	env := map[string]any{
		"error": map[string]any{
			"errors": []any{
				map[string]any{
					"domain":  "global",
					"reason":  gcpReason(perr.Code),
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

// gcpStatusString maps an HTTP status to the google.rpc.Code status string.
func gcpStatusString(code int) string {
	switch code {
	case 400:
		return "INVALID_ARGUMENT"
	case 401:
		return "UNAUTHENTICATED"
	case 403:
		return "PERMISSION_DENIED"
	case 404:
		return "NOT_FOUND"
	case 409:
		return "ALREADY_EXISTS"
	case 412:
		return "FAILED_PRECONDITION"
	case 429:
		return "RESOURCE_EXHAUSTED"
	case 499:
		return "CANCELLED"
	case 500:
		return "INTERNAL"
	case 501:
		return "UNIMPLEMENTED"
	case 503:
		return "UNAVAILABLE"
	default:
		return "UNKNOWN"
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
// CSEK contract carries the key (base64 AES-256) and its base64 SHA-256 digest
// in these headers, distinct from CMEK's kmsKeyName query param.
func csekFromHeaders(r *http.Request, params map[string]any) {
	if v := r.Header.Get("x-goog-encryption-key"); v != "" {
		params[wire.CSEKKey] = v
	}
	if v := r.Header.Get("x-goog-encryption-key-sha256"); v != "" {
		params[wire.CSEKKeySHA256] = v
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
	mediaType, p, err := mime.ParseMediaType(ct)
	if err != nil || !strings.HasPrefix(mediaType, "multipart/") {
		return model.NewProviderError("InvalidRequest", "expected multipart body", 400)
	}
	var src io.Reader = bytes.NewReader(body)
	if body == nil {
		src = r.Body
	}
	mr := multipart.NewReader(src, p["boundary"])
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
