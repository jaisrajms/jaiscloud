// Package gcs provides the GCS metadata store implementations, mirroring the
// AWS S3 store (internal/aws/store/s3/). Buckets, objects, and resumable-upload
// sessions live in dedicated tables (jc_gcs_*) rather than the generic
// jc_resources store; object bytes remain in blobfs.
package gcs

import (
	"context"
	"errors"
	"time"

	"jaiscloud/internal/clock"
)

// Sentinel errors returned by the store, mapped to GCS HTTP responses by the
// provider (errors.Is-compatible, unlike AWS's string sentinels).
var (
	ErrNoSuchBucket   = errors.New("NoSuchBucket")
	ErrNoSuchObject   = errors.New("NoSuchObject")
	ErrNoSuchUpload   = errors.New("NoSuchUpload")
	ErrBucketNotEmpty = errors.New("BucketNotEmpty")
	ErrAlreadyExists  = errors.New("AlreadyExists")
)

// ObjectRetention is the GCS Object.retention object ({retainUntilTime, mode}).
// mode is "Unlocked" or "Locked".
type ObjectRetention struct {
	RetainUntilTime time.Time `json:"retainUntilTime,omitempty"`
	Mode            string    `json:"mode,omitempty"`
}

// ObjectMeta holds GCS object metadata. Generation/metageneration are the GCS
// analogues of S3's version_id; CRC32C is the GCS checksum (vs S3's IEEE CRC32).
type ObjectMeta struct {
	Bucket         string            `json:"bucket"`
	Name           string            `json:"name"`
	Generation     string            `json:"generation"`
	Metageneration string            `json:"metageneration"`
	ContentType    string            `json:"contentType,omitempty"`
	Size           int64             `json:"size"`
	MD5Hash        string            `json:"md5Hash,omitempty"`
	CRC32C         string            `json:"crc32c,omitempty"`
	StorageClass   string            `json:"storageClass"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	TimeCreated    time.Time         `json:"timeCreated"`
	Updated        time.Time         `json:"updated"`
	// Retention is the object-level retention policy (GCS Object.retention).
	Retention *ObjectRetention `json:"retention,omitempty"`
	// TemporaryHold / EventBasedHold mirror GCS Object.temporaryHold /
	// Object.eventBasedHold: while true, the object cannot be deleted.
	TemporaryHold  bool `json:"temporaryHold,omitempty"`
	EventBasedHold bool `json:"eventBasedHold,omitempty"`
	// TimeDeleted is set when this generation is no longer live (versioning).
	TimeDeleted *time.Time `json:"timeDeleted,omitempty"`
	// KmsKeyName is the CMEK key name (empty when server-DEK encrypted).
	KmsKeyName string `json:"kmsKeyName,omitempty"`
	// WrappedDEK is the DEK wrapped under KmsKeyName (nil for CSEK/server-DEK).
	WrappedDEK []byte `json:"wrappedDek,omitempty"`
	// CSEKeySHA256 is the base64 SHA-256 of the customer-supplied encryption
	// key when the object is CSEK-encrypted (empty otherwise).
	CSEKeySHA256 string `json:"cseKeySha256,omitempty"`
}

// ResumableSession tracks an in-progress GCS resumable upload (the analogue of
// S3 multipart uploads). The body spills to TmpPath once it exceeds the
// in-memory threshold.
type ResumableSession struct {
	UploadID    string
	Bucket      string
	Name        string
	ContentType string
	Length      int64
	TmpPath     string
	LastAccess  time.Time
}

// ObjectStore is the GCS metadata store. Object bytes are stored separately in
// blobfs.BlobStore.
type ObjectStore interface {
	// Buckets. meta is the bucket's JSON map (location, storageClass,
	// timeCreated, ...). Bucket names are globally unique; projectID is the
	// owning project used only for CreateBucket/ListBuckets scoping.
	CreateBucket(ctx context.Context, projectID, name string, meta map[string]any) error
	GetBucket(ctx context.Context, name string) (map[string]any, error)
	UpdateBucketMeta(ctx context.Context, name string, meta map[string]any) error
	DeleteBucket(ctx context.Context, name string) error // ErrBucketNotEmpty if objects exist
	ListBuckets(ctx context.Context, projectID string) ([]map[string]any, error)

	// Objects.
	// PutObjectMeta replaces the object's metadata with a single live
	// generation (prior generations for this name are removed). Used when
	// versioning is disabled.
	PutObjectMeta(ctx context.Context, bucket, name string, meta ObjectMeta) error
	// PutObjectGeneration appends a new live generation, marking any prior
	// live generation non-live (timeDeleted set). Used when versioning is
	// enabled so prior generations are retained.
	PutObjectGeneration(ctx context.Context, bucket, name string, meta ObjectMeta) error
	// GetObjectMeta returns the live generation of the object, or
	// ErrNoSuchObject.
	GetObjectMeta(ctx context.Context, bucket, name string) (ObjectMeta, error)
	// GetObjectGeneration returns the generation with the given decimal id, or
	// ErrNoSuchObject.
	GetObjectGeneration(ctx context.Context, bucket, name, generation string) (ObjectMeta, error)
	// DeleteObjectMeta removes every generation of the object.
	DeleteObjectMeta(ctx context.Context, bucket, name string) error
	// TombstoneObjectMeta marks the live generation non-live (timeDeleted set)
	// and returns it, leaving any prior non-live generations untouched. Used
	// for versioned-object deletion so a non-live tombstone is retained.
	TombstoneObjectMeta(ctx context.Context, bucket, name string) (ObjectMeta, error)
	// ListObjects returns the live generation of every object in the bucket,
	// sorted by name. Prefix, delimiter, and pageToken pagination are applied
	// by the provider.
	ListObjects(ctx context.Context, bucket string) ([]ObjectMeta, error)
	// ListObjectVersions returns every generation (live and non-live) of every
	// object, sorted by name then generation. Used by ?versions=true listings.
	ListObjectVersions(ctx context.Context, bucket string) ([]ObjectMeta, error)

	// MaxGeneration returns the highest object generation across all buckets
	// (as a decimal string), or "" when no objects exist. Used to keep the
	// generation counter monotonic across restarts.
	MaxGeneration(ctx context.Context) (string, error)

	// Resumable upload sessions.
	InitResumable(ctx context.Context, s ResumableSession) error
	GetResumable(ctx context.Context, uploadID string) (ResumableSession, error)
	UpdateResumable(ctx context.Context, s ResumableSession) error
	DeleteResumable(ctx context.Context, uploadID string) error
	// ListStaleResumable returns sessions with LastAccess older than cutoff.
	ListStaleResumable(ctx context.Context, cutoff time.Time) ([]ResumableSession, error)

	Reset(ctx context.Context)
}

// normalizeMeta fills default object fields before persistence (both backends).
func normalizeMeta(meta *ObjectMeta) {
	if meta.ContentType == "" {
		meta.ContentType = "application/octet-stream"
	}
	if meta.StorageClass == "" {
		meta.StorageClass = "STANDARD"
	}
	if meta.TimeCreated.IsZero() {
		meta.TimeCreated = clock.Now()
	}
	if meta.Updated.IsZero() {
		meta.Updated = meta.TimeCreated
	}
}
