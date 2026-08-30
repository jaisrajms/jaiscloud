package s3ui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the S3 UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/buckets", h.ListBuckets)
	r.Post("/buckets", h.CreateBucket)
	r.Delete("/buckets/{bucket}", h.DeleteBucket)

	r.Get("/buckets/{bucket}/objects", h.ListObjects)
	r.Get("/buckets/{bucket}/objects/head", h.HeadObject)
	r.Get("/buckets/{bucket}/objects/download", h.DownloadObject)
	r.Put("/buckets/{bucket}/objects", h.PutObject)
	r.Delete("/buckets/{bucket}/objects", h.DeleteObject)
	r.Post("/buckets/{bucket}/objects/delete-batch", h.DeleteObjects)
	r.Post("/buckets/{bucket}/objects/copy", h.CopyObject)

	r.Get("/buckets/{bucket}/versioning", h.GetBucketVersioning)
	r.Put("/buckets/{bucket}/versioning", h.PutBucketVersioning)
	r.Get("/buckets/{bucket}/versions", h.ListObjectVersions)

	r.Get("/buckets/{bucket}/uploads", h.ListMultipartUploads)
	r.Delete("/buckets/{bucket}/uploads", h.AbortMultipartUpload)

	r.Get("/buckets/{bucket}/tags", h.GetBucketTags)

	return r
}
