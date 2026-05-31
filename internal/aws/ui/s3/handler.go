package s3ui

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves S3 UI API requests by calling the S3 provider directly.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /buckets
func (h *Handler) ListBuckets(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "s3", "ListBuckets", region, account)
	resp, err := h.provider.ListBuckets(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	raw, _ := resp.Data["Buckets"].([]map[string]any)
	buckets := make([]Bucket, 0, len(raw))
	for _, b := range raw {
		name, _ := b["Name"].(string)
		buckets = append(buckets, Bucket{
			Name:       name,
			Region:     region,
			Versioning: "Off",
		})
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

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	if req.Region != "" {
		region = req.Region
	}

	nr := uihelper.NR(r.Context(), h.cfg, "s3", "CreateBucket", region, account)
	nr.Params["_bucket"] = req.Name
	if region != "us-east-1" {
		nr.Params["LocationConstraint"] = region
	}

	if _, err := h.provider.CreateBucket(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, Bucket{Name: req.Name, Region: region, Versioning: "Off"})
}

// DELETE /buckets/{bucket}
func (h *Handler) DeleteBucket(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "s3", "DeleteBucket", region, account)
	nr.Params["_bucket"] = bucket

	if _, err := h.provider.DeleteBucket(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /buckets/{bucket}/objects
func (h *Handler) ListObjects(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	if delimiter == "" {
		delimiter = "/"
	}
	maxKeys := 100
	if s := r.URL.Query().Get("maxKeys"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			maxKeys = n
		}
	}
	continuationToken := r.URL.Query().Get("continuationToken")

	nr := uihelper.NR(r.Context(), h.cfg, "s3", "ListObjectsV2", region, account)
	nr.Params["_bucket"] = bucket
	nr.Params["prefix"] = prefix
	nr.Params["delimiter"] = delimiter
	nr.Params["max-keys"] = maxKeys
	if continuationToken != "" {
		nr.Params["continuation-token"] = continuationToken
	}

	resp, err := h.provider.ListObjectsV2(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawContents, _ := resp.Data["Contents"].([]map[string]any)
	objects := make([]S3Object, 0, len(rawContents))
	for _, obj := range rawContents {
		o := S3Object{
			Key:          strAny(obj, "Key"),
			ETag:         strAny(obj, "ETag"),
			LastModified: strAny(obj, "LastModified"),
			StorageClass: strAny(obj, "StorageClass"),
		}
		if sz, ok := obj["Size"]; ok {
			switch v := sz.(type) {
			case float64:
				o.Size = int64(v)
			case int64:
				o.Size = v
			case int:
				o.Size = int64(v)
			}
		}
		objects = append(objects, o)
	}

	rawCPs, _ := resp.Data["CommonPrefixes"].([]string)
	truncated, _ := resp.Data["IsTruncated"].(bool)
	nextToken, _ := resp.Data["_nextPageToken"].(string)
	keyCount, _ := resp.Data["KeyCount"].(int)

	uihelper.WriteJSON(w, ListObjectsResponse{
		Items:            objects,
		CommonPrefixes:   rawCPs,
		IsTruncated:      truncated,
		NextContinuation: nextToken,
		KeyCount:         keyCount,
	})
}

// GET /buckets/{bucket}/objects/head?key=<key>
func (h *Handler) HeadObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key := r.URL.Query().Get("key")
	if key == "" {
		uihelper.UIError(w, "BadRequest", "key is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "s3", "HeadObject", region, account)
	nr.Params["_bucket"] = bucket
	nr.Params["_key"] = key

	resp, err := h.provider.HeadObject(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /buckets/{bucket}/objects/download?key=<key>
func (h *Handler) DownloadObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key := r.URL.Query().Get("key")
	if key == "" {
		uihelper.UIError(w, "BadRequest", "key is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "s3", "GetObject", region, account)
	nr.Params["_bucket"] = bucket
	nr.Params["_key"] = key

	resp, err := h.provider.GetObject(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	contentType, _ := resp.Data["ContentType"].(string)
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, key))

	if body, ok := resp.Data["_body"].([]byte); ok {
		w.Write(body) //nolint:errcheck
	} else if rc, ok := resp.Data["_stream"].(io.ReadCloser); ok {
		defer rc.Close()
		io.Copy(w, rc) //nolint:errcheck
	}
}

// PUT /buckets/{bucket}/objects?key=<key>
func (h *Handler) PutObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key := r.URL.Query().Get("key")
	if key == "" {
		uihelper.UIError(w, "BadRequest", "key is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		uihelper.UIError(w, "BadRequest", "failed to read body", http.StatusBadRequest)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	nr := uihelper.NR(r.Context(), h.cfg, "s3", "PutObject", region, account)
	nr.Params["_bucket"] = bucket
	nr.Params["_key"] = key
	nr.Params["_body"] = body
	nr.Params["_content_type"] = contentType

	if _, err := h.provider.PutObject(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	uihelper.WriteJSON(w, map[string]string{"key": key})
}

// DELETE /buckets/{bucket}/objects?key=<key>
func (h *Handler) DeleteObject(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key := r.URL.Query().Get("key")
	if key == "" {
		uihelper.UIError(w, "BadRequest", "key is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "s3", "DeleteObject", region, account)
	nr.Params["_bucket"] = bucket
	nr.Params["_key"] = key

	if _, err := h.provider.DeleteObject(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /buckets/{bucket}/objects/delete-batch  body: { "keys": ["k1","k2"] }
func (h *Handler) DeleteObjects(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	var req struct {
		Keys []string `json:"keys"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	objs := make([]any, len(req.Keys))
	for i, k := range req.Keys {
		objs[i] = map[string]any{"Key": k}
	}
	nr := uihelper.NR(r.Context(), h.cfg, "s3", "DeleteObjects", region, account)
	nr.Params["_bucket"] = bucket
	nr.Params["Delete"] = map[string]any{"Object": objs}

	resp, err := h.provider.DeleteObjects(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// POST /buckets/{bucket}/objects/copy
func (h *Handler) CopyObject(w http.ResponseWriter, r *http.Request) {
	destBucket := chi.URLParam(r, "bucket")
	var req CopyObjectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.SourceBucket == "" || req.SourceKey == "" || req.DestKey == "" {
		uihelper.UIError(w, "BadRequest", "sourceBucket, sourceKey, and destKey are required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "s3", "CopyObject", region, account)
	nr.Params["_bucket"] = destBucket
	nr.Params["_key"] = req.DestKey
	nr.Params["_copy_source"] = req.SourceBucket + "/" + req.SourceKey

	resp, err := h.provider.CopyObject(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /buckets/{bucket}/versioning
func (h *Handler) GetBucketVersioning(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "s3", "GetBucketVersioning", region, account)
	nr.Params["_bucket"] = bucket

	resp, err := h.provider.GetBucketVersioning(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// PUT /buckets/{bucket}/versioning  body: { "status": "Enabled" | "Suspended" }
func (h *Handler) PutBucketVersioning(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	var req struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "s3", "PutBucketVersioning", region, account)
	nr.Params["_bucket"] = bucket
	nr.Params["Status"] = req.Status

	if _, err := h.provider.PutBucketVersioning(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /buckets/{bucket}/versions?prefix=...
func (h *Handler) ListObjectVersions(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	prefix := r.URL.Query().Get("prefix")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "s3", "ListObjectVersions", region, account)
	nr.Params["_bucket"] = bucket
	if prefix != "" {
		nr.Params["prefix"] = prefix
	}

	resp, err := h.provider.ListObjectVersions(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawVersions, _ := resp.Data["Versions"].([]map[string]any)
	rawMarkers, _ := resp.Data["DeleteMarkers"].([]map[string]any)
	items := make([]ObjectVersion, 0, len(rawVersions)+len(rawMarkers))

	for _, v := range rawVersions {
		ov := ObjectVersion{
			Key:          strAny(v, "Key"),
			VersionID:    strAny(v, "VersionId"),
			ETag:         strAny(v, "ETag"),
			LastModified: strAny(v, "LastModified"),
		}
		if il, ok := v["IsLatest"].(bool); ok {
			ov.IsLatest = il
		}
		if sz, ok := v["Size"]; ok {
			switch sv := sz.(type) {
			case float64:
				ov.Size = int64(sv)
			case int64:
				ov.Size = sv
			}
		}
		items = append(items, ov)
	}
	for _, m := range rawMarkers {
		items = append(items, ObjectVersion{
			Key:            strAny(m, "Key"),
			VersionID:      strAny(m, "VersionId"),
			LastModified:   strAny(m, "LastModified"),
			IsDeleteMarker: true,
		})
	}

	truncated, _ := resp.Data["IsTruncated"].(bool)
	uihelper.WriteJSON(w, ListVersionsResponse{Items: items, IsTruncated: truncated})
}

// GET /buckets/{bucket}/uploads
func (h *Handler) ListMultipartUploads(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "s3", "ListMultipartUploads", region, account)
	nr.Params["_bucket"] = bucket

	resp, err := h.provider.ListMultipartUploads(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawUploads, _ := resp.Data["Uploads"].([]map[string]any)
	items := make([]MultipartUpload, 0, len(rawUploads))
	for _, u := range rawUploads {
		items = append(items, MultipartUpload{
			UploadID:     strAny(u, "UploadId"),
			Key:          strAny(u, "Key"),
			Initiated:    strAny(u, "Initiated"),
			StorageClass: strAny(u, "StorageClass"),
		})
	}
	truncated, _ := resp.Data["IsTruncated"].(bool)
	uihelper.WriteJSON(w, ListMultipartUploadsResponse{Items: items, IsTruncated: truncated})
}

// DELETE /buckets/{bucket}/uploads?key=<key>&uploadId=<id>
func (h *Handler) AbortMultipartUpload(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	key := r.URL.Query().Get("key")
	uploadID := r.URL.Query().Get("uploadId")
	if key == "" || uploadID == "" {
		uihelper.UIError(w, "BadRequest", "key and uploadId are required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "s3", "AbortMultipartUpload", region, account)
	nr.Params["_bucket"] = bucket
	nr.Params["_key"] = key
	nr.Params["uploadId"] = uploadID

	if _, err := h.provider.AbortMultipartUpload(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /buckets/{bucket}/tags
func (h *Handler) GetBucketTags(w http.ResponseWriter, r *http.Request) {
	bucket := chi.URLParam(r, "bucket")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "s3", "GetBucketTagging", region, account)
	nr.Params["_bucket"] = bucket

	resp, err := h.provider.GetBucketTagging(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawTags, _ := resp.Data["TagSet"].([]map[string]any)
	tags := make(map[string]string, len(rawTags))
	for _, t := range rawTags {
		k, _ := t["Key"].(string)
		v, _ := t["Value"].(string)
		if k != "" {
			tags[k] = v
		}
	}
	uihelper.WriteJSON(w, TagsResponse{Tags: tags})
}

// helpers

func strAny(m map[string]any, key string) string {
	switch v := m[key].(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', 0, 64)
	case int64:
		return strconv.FormatInt(v, 10)
	}
	return ""
}
