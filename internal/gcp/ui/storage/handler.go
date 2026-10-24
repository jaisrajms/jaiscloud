// Package storageui serves the Cloud Storage UI API. Handlers call the GCS
// provider directly (in-process) rather than over the wire.
package storageui

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/ui/uihelper"
	"jaiscloud/internal/gcp/wire"
)

// Handler serves Cloud Storage UI API requests by calling the GCS provider.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// account resolves the project for a request, falling back to the configured
// project when the inject-config middleware has not populated the context.
func (h *Handler) account(r *http.Request) string {
	if a := uihelper.AccountFrom(r); a != "" {
		return a
	}
	return h.cfg.AccountID
}

// bucketParam returns the decoded single-segment bucket name, or writes 400 and
// reports false. A decoded '/' is rejected: bucket names are one path segment.
func bucketParam(w http.ResponseWriter, r *http.Request) (string, bool) {
	name, ok := uihelper.Segment(r, "bucket")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid bucket name", http.StatusBadRequest)
		return "", false
	}
	return name, true
}

func str(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

func boolAt(m map[string]any, key string) bool {
	b, _ := m[key].(bool)
	return b
}

func bucketFromMap(m map[string]any) Bucket {
	versioning, _ := m["versioning"].(map[string]any)
	enabled, _ := versioning["enabled"].(bool)
	return Bucket{
		Name:         str(m, "name"),
		Location:     str(m, "location"),
		StorageClass: str(m, "storageClass"),
		TimeCreated:  str(m, "timeCreated"),
		Updated:      str(m, "updated"),
		Versioning:   enabled,
	}
}

func objectFromMap(m map[string]any) GCSObject {
	return GCSObject{
		Name:            str(m, "name"),
		Bucket:          str(m, "bucket"),
		Size:            str(m, "size"),
		ContentType:     str(m, "contentType"),
		StorageClass:    str(m, "storageClass"),
		Updated:         str(m, "updated"),
		TimeCreated:     str(m, "timeCreated"),
		TimeDeleted:     str(m, "timeDeleted"),
		MD5:             str(m, "md5Hash"),
		Generation:      str(m, "generation"),
		Metageneration:  str(m, "metageneration"),
		TemporaryHold:   boolAt(m, "temporaryHold"),
		EventBasedHold:  boolAt(m, "eventBasedHold"),
		RetentionExpiry: str(m, "retentionExpirationTime"),
	}
}

// ─── Buckets ─────────────────────────────────────────────────────────────────

// GET /buckets
func (h *Handler) ListBuckets(w http.ResponseWriter, r *http.Request) {
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.BucketsList", "global", account)

	resp, err := h.provider.BucketsList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	raw := uihelper.AsSlice(resp.Data["items"])
	buckets := make([]Bucket, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			buckets = append(buckets, bucketFromMap(m))
		}
	}
	uihelper.WriteJSON(w, ListBucketsResponse{Items: buckets, Total: len(buckets)})
}

// POST /buckets
func (h *Handler) CreateBucket(w http.ResponseWriter, r *http.Request) {
	var req CreateBucketRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}

	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.BucketsInsert", "global", account)
	body := map[string]any{"name": req.Name}
	if req.Location != "" {
		body["location"] = req.Location
	}
	if req.StorageClass != "" {
		body["storageClass"] = req.StorageClass
	}
	nr.Params["body"] = body

	resp, err := h.provider.BucketsInsert(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, bucketFromMap(resp.Data))
}

// GET /buckets/{bucket}
func (h *Handler) GetBucket(w http.ResponseWriter, r *http.Request) {
	bucket, ok := bucketParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.BucketsGet", "global", account)
	nr.Params["bucket"] = bucket

	resp, err := h.provider.BucketsGet(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /buckets/{bucket}
func (h *Handler) DeleteBucket(w http.ResponseWriter, r *http.Request) {
	bucket, ok := bucketParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.BucketsDelete", "global", account)
	nr.Params["bucket"] = bucket

	if _, err := h.provider.BucketsDelete(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /buckets/{bucket}/versioning
func (h *Handler) GetBucketVersioning(w http.ResponseWriter, r *http.Request) {
	h.writeBucketField(w, r, "versioning")
}

// PUT /buckets/{bucket}/versioning  body: { "enabled": true|false }
func (h *Handler) PutBucketVersioning(w http.ResponseWriter, r *http.Request) {
	var req VersioningRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	enabled := false
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	h.updateBucket(w, r, map[string]any{"versioning": map[string]any{"enabled": enabled}})
}

// GET /buckets/{bucket}/lifecycle
func (h *Handler) GetBucketLifecycle(w http.ResponseWriter, r *http.Request) {
	h.writeBucketField(w, r, "lifecycle")
}

// PUT /buckets/{bucket}/lifecycle  body: { "lifecycle": { "rule": [...] } }
func (h *Handler) PutBucketLifecycle(w http.ResponseWriter, r *http.Request) {
	var req LifecycleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	h.updateBucket(w, r, map[string]any{"lifecycle": req.Lifecycle})
}

// GET /buckets/{bucket}/retention
func (h *Handler) GetBucketRetention(w http.ResponseWriter, r *http.Request) {
	h.writeBucketField(w, r, "retentionPolicy")
}

// PUT /buckets/{bucket}/retention  body: { "retentionPolicy": { "retentionPeriod": "..." } }
func (h *Handler) PutBucketRetention(w http.ResponseWriter, r *http.Request) {
	var req RetentionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	h.updateBucket(w, r, map[string]any{"retentionPolicy": req.RetentionPolicy})
}

// POST /buckets/{bucket}/retention/lock
func (h *Handler) LockBucketRetention(w http.ResponseWriter, r *http.Request) {
	bucket, ok := bucketParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.BucketsLockRetentionPolicy", "global", account)
	nr.Params["bucket"] = bucket

	resp, err := h.provider.BucketsLockRetentionPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// PUT /buckets/{bucket}/default-event-based-hold  body: { "defaultEventBasedHold": true|false }
func (h *Handler) PutDefaultEventBasedHold(w http.ResponseWriter, r *http.Request) {
	var req DefaultEventBasedHoldRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	hold := false
	if req.DefaultEventBasedHold != nil {
		hold = *req.DefaultEventBasedHold
	}
	h.updateBucket(w, r, map[string]any{"defaultEventBasedHold": hold})
}

// writeBucketField fetches the bucket and returns just the named field so the
// UI can read versioning/lifecycle/retentionPolicy without the full resource.
func (h *Handler) writeBucketField(w http.ResponseWriter, r *http.Request, field string) {
	bucket, ok := bucketParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.BucketsGet", "global", account)
	nr.Params["bucket"] = bucket

	resp, err := h.provider.BucketsGet(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, map[string]any{field: resp.Data[field]})
}

// updateBucket applies a partial bucket update via Storage.BucketsUpdate.
func (h *Handler) updateBucket(w http.ResponseWriter, r *http.Request, body map[string]any) {
	bucket, ok := bucketParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.BucketsUpdate", "global", account)
	nr.Params["bucket"] = bucket
	nr.Params["body"] = body

	resp, err := h.provider.BucketsUpdate(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, bucketFromMap(resp.Data))
}

// ─── Objects ─────────────────────────────────────────────────────────────────

// GET /buckets/{bucket}/objects[?versions=true&prefix=&delimiter=&pageToken=&maxResults=]
func (h *Handler) ListObjects(w http.ResponseWriter, r *http.Request) {
	bucket, ok := bucketParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.ObjectsList", "global", account)
	nr.Params["bucket"] = bucket
	if prefix := r.URL.Query().Get("prefix"); prefix != "" {
		nr.Params["prefix"] = prefix
	}
	versions := r.URL.Query().Get("versions")
	if versions != "" {
		nr.Params["versions"] = versions
	}
	if versions != "true" {
		delimiter := r.URL.Query().Get("delimiter")
		if delimiter == "" {
			delimiter = "/"
		}
		nr.Params["delimiter"] = delimiter
	}
	if max := r.URL.Query().Get("maxResults"); max != "" {
		nr.Params["maxResults"] = max
	}
	if token := r.URL.Query().Get("pageToken"); token != "" {
		nr.Params["pageToken"] = token
	}

	resp, err := h.provider.ObjectsList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	raw := uihelper.AsSlice(resp.Data["items"])
	objects := make([]GCSObject, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			objects = append(objects, objectFromMap(m))
		}
	}
	prefixes, _ := resp.Data["prefixes"].([]string)
	next, _ := resp.Data["nextPageToken"].(string)
	uihelper.WriteJSON(w, ListObjectsResponse{Items: objects, Prefixes: prefixes, NextPageToken: next})
}

// PUT /buckets/{bucket}/objects?name=<object>  (raw body = object bytes)
func (h *Handler) UploadObject(w http.ResponseWriter, r *http.Request) {
	bucket, ok := bucketParam(w, r)
	if !ok {
		return
	}
	object := r.URL.Query().Get("name")
	if object == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		uihelper.UIError(w, "BadRequest", "failed to read body", http.StatusBadRequest)
		return
	}
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.ObjectsInsert", "global", account)
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	nr.Params["body"] = map[string]any{"name": object, "contentType": contentType}
	nr.Params[wire.MediaKey] = body
	nr.Params[wire.ContentTypeKey] = contentType

	resp, err := h.provider.ObjectsInsert(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, objectFromMap(resp.Data))
}

// GET /buckets/{bucket}/objects/metadata?name=<object>&generation=<g>
func (h *Handler) GetObject(w http.ResponseWriter, r *http.Request) {
	bucket, ok := bucketParam(w, r)
	if !ok {
		return
	}
	object := r.URL.Query().Get("name")
	if object == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.ObjectsGet", "global", account)
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	if g := r.URL.Query().Get("generation"); g != "" {
		nr.Params["generation"] = g
	}

	resp, err := h.provider.ObjectsGet(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /buckets/{bucket}/objects/download?name=<object>&generation=<g>
func (h *Handler) DownloadObject(w http.ResponseWriter, r *http.Request) {
	bucket, ok := bucketParam(w, r)
	if !ok {
		return
	}
	object := r.URL.Query().Get("name")
	if object == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.ObjectsGetMedia", "global", account)
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	if g := r.URL.Query().Get("generation"); g != "" {
		nr.Params["generation"] = g
	}

	resp, err := h.provider.ObjectsGetMedia(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	contentType, _ := resp.Data[wire.ContentTypeKey].(string)
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, object))

	if stream, ok := resp.Data["_stream"].(io.ReadCloser); ok {
		defer stream.Close()
		io.Copy(w, stream) //nolint:errcheck
	} else if body, ok := resp.Data["_body"].([]byte); ok {
		w.Write(body) //nolint:errcheck
	}
}

// PATCH /buckets/{bucket}/objects?name=<object>&generation=<g>
func (h *Handler) PatchObject(w http.ResponseWriter, r *http.Request) {
	bucket, ok := bucketParam(w, r)
	if !ok {
		return
	}
	object := r.URL.Query().Get("name")
	if object == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}
	// Decode as a generic map so only fields present in the request are applied
	// (patch merge semantics; false booleans must be distinguishable from absent).
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	h.objectWrite(w, r, "Storage.ObjectsPatch", bucket, object, body)
}

// DELETE /buckets/{bucket}/objects?name=<object>&generation=<g>
func (h *Handler) DeleteObject(w http.ResponseWriter, r *http.Request) {
	bucket, ok := bucketParam(w, r)
	if !ok {
		return
	}
	object := r.URL.Query().Get("name")
	if object == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.ObjectsDelete", "global", account)
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	if g := r.URL.Query().Get("generation"); g != "" {
		nr.Params["generation"] = g
	}

	if _, err := h.provider.ObjectsDelete(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /buckets/{bucket}/objects/restore?name=<object>&generation=<g>
func (h *Handler) RestoreObject(w http.ResponseWriter, r *http.Request) {
	bucket, ok := bucketParam(w, r)
	if !ok {
		return
	}
	object := r.URL.Query().Get("name")
	generation := r.URL.Query().Get("generation")
	if object == "" || generation == "" {
		uihelper.UIError(w, "BadRequest", "name and generation are required", http.StatusBadRequest)
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.ObjectsRestore", "global", account)
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	nr.Params["generation"] = generation

	resp, err := h.provider.ObjectsRestore(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, objectFromMap(resp.Data))
}

// objectWrite runs a metadata write action (patch) against the provider.
func (h *Handler) objectWrite(w http.ResponseWriter, r *http.Request, action, bucket, object string, body map[string]any) {
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", action, "global", account)
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	if g := r.URL.Query().Get("generation"); g != "" {
		nr.Params["generation"] = g
	}
	nr.Params["body"] = body

	resp, err := h.provider.ObjectsPatch(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, objectFromMap(resp.Data))
}

// ─── IAM ─────────────────────────────────────────────────────────────────────

// GET /buckets/{bucket}/iam
func (h *Handler) GetBucketIam(w http.ResponseWriter, r *http.Request) {
	bucket, ok := bucketParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.BucketsGetIamPolicy", "global", account)
	nr.Params["bucket"] = bucket

	resp, err := h.provider.BucketsGetIamPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// PUT /buckets/{bucket}/iam
func (h *Handler) PutBucketIam(w http.ResponseWriter, r *http.Request) {
	bucket, ok := bucketParam(w, r)
	if !ok {
		return
	}
	body, ok := h.decodeIamBody(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.BucketsSetIamPolicy", "global", account)
	nr.Params["bucket"] = bucket
	nr.Params["body"] = body

	resp, err := h.provider.BucketsSetIamPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /buckets/{bucket}/objects/iam?name=<object>
func (h *Handler) GetObjectIam(w http.ResponseWriter, r *http.Request) {
	bucket, object, ok := h.bucketObject(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.ObjectsGetIamPolicy", "global", account)
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object

	resp, err := h.provider.ObjectsGetIamPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// PUT /buckets/{bucket}/objects/iam?name=<object>
func (h *Handler) PutObjectIam(w http.ResponseWriter, r *http.Request) {
	bucket, object, ok := h.bucketObject(w, r)
	if !ok {
		return
	}
	body, ok := h.decodeIamBody(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.ObjectsSetIamPolicy", "global", account)
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	nr.Params["body"] = body

	resp, err := h.provider.ObjectsSetIamPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// ─── ACL ─────────────────────────────────────────────────────────────────────

// GET /buckets/{bucket}/acl
func (h *Handler) ListBucketACL(w http.ResponseWriter, r *http.Request) {
	bucket, ok := bucketParam(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.BucketACLList", "global", account)
	nr.Params["bucket"] = bucket

	resp, err := h.provider.BucketACLList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// POST /buckets/{bucket}/acl
func (h *Handler) InsertBucketACL(w http.ResponseWriter, r *http.Request) {
	bucket, ok := bucketParam(w, r)
	if !ok {
		return
	}
	body, ok := h.decodeACLBody(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.BucketACLInsert", "global", account)
	nr.Params["bucket"] = bucket
	nr.Params["body"] = body

	resp, err := h.provider.BucketACLInsert(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /buckets/{bucket}/objects/acl?name=<object>
func (h *Handler) ListObjectACL(w http.ResponseWriter, r *http.Request) {
	bucket, object, ok := h.bucketObject(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.ObjectACLList", "global", account)
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object

	resp, err := h.provider.ObjectACLList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// POST /buckets/{bucket}/objects/acl?name=<object>
func (h *Handler) InsertObjectACL(w http.ResponseWriter, r *http.Request) {
	bucket, object, ok := h.bucketObject(w, r)
	if !ok {
		return
	}
	body, ok := h.decodeACLBody(w, r)
	if !ok {
		return
	}
	account := h.account(r)
	nr := uihelper.NR(r.Context(), h.cfg, "storage", "Storage.ObjectACLInsert", "global", account)
	nr.Params["bucket"] = bucket
	nr.Params["object"] = object
	nr.Params["body"] = body

	resp, err := h.provider.ObjectACLInsert(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// bucketObject reads the bucket path param and the object query param.
func (h *Handler) bucketObject(w http.ResponseWriter, r *http.Request) (bucket, object string, ok bool) {
	bucket, ok = bucketParam(w, r)
	if !ok {
		return bucket, "", false
	}
	object = r.URL.Query().Get("name")
	if object == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return bucket, "", false
	}
	return bucket, object, true
}

// decodeIamBody decodes a setIamPolicy body into the generic shape the provider
// expects (bindings as []any, etag, version).
func (h *Handler) decodeIamBody(w http.ResponseWriter, r *http.Request) (map[string]any, bool) {
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return nil, false
	}
	return body, true
}

// decodeACLBody decodes an {entity, role} ACL insert body.
func (h *Handler) decodeACLBody(w http.ResponseWriter, r *http.Request) (map[string]any, bool) {
	var req ACLInsertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return nil, false
	}
	if req.Entity == "" || req.Role == "" {
		uihelper.UIError(w, "BadRequest", "entity and role are required", http.StatusBadRequest)
		return nil, false
	}
	return map[string]any{"entity": req.Entity, "role": req.Role}, true
}
