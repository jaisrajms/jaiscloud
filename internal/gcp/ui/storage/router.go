package storageui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Cloud Storage UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/buckets", h.ListBuckets)
	r.Post("/buckets", h.CreateBucket)
	r.Get("/buckets/{bucket}", h.GetBucket)
	r.Delete("/buckets/{bucket}", h.DeleteBucket)

	r.Get("/buckets/{bucket}/versioning", h.GetBucketVersioning)
	r.Put("/buckets/{bucket}/versioning", h.PutBucketVersioning)
	r.Get("/buckets/{bucket}/lifecycle", h.GetBucketLifecycle)
	r.Put("/buckets/{bucket}/lifecycle", h.PutBucketLifecycle)
	r.Get("/buckets/{bucket}/retention", h.GetBucketRetention)
	r.Put("/buckets/{bucket}/retention", h.PutBucketRetention)
	r.Post("/buckets/{bucket}/retention/lock", h.LockBucketRetention)
	r.Put("/buckets/{bucket}/default-event-based-hold", h.PutDefaultEventBasedHold)

	r.Get("/buckets/{bucket}/iam", h.GetBucketIam)
	r.Put("/buckets/{bucket}/iam", h.PutBucketIam)
	r.Get("/buckets/{bucket}/acl", h.ListBucketACL)
	r.Post("/buckets/{bucket}/acl", h.InsertBucketACL)

	r.Get("/buckets/{bucket}/objects", h.ListObjects)
	r.Put("/buckets/{bucket}/objects", h.UploadObject)
	r.Patch("/buckets/{bucket}/objects", h.PatchObject)
	r.Delete("/buckets/{bucket}/objects", h.DeleteObject)
	r.Get("/buckets/{bucket}/objects/metadata", h.GetObject)
	r.Get("/buckets/{bucket}/objects/download", h.DownloadObject)
	r.Post("/buckets/{bucket}/objects/restore", h.RestoreObject)
	r.Get("/buckets/{bucket}/objects/iam", h.GetObjectIam)
	r.Put("/buckets/{bucket}/objects/iam", h.PutObjectIam)
	r.Get("/buckets/{bucket}/objects/acl", h.ListObjectACL)
	r.Post("/buckets/{bucket}/objects/acl", h.InsertObjectACL)

	return r
}
