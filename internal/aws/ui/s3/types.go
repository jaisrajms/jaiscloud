package s3ui

// Bucket is the UI representation of an S3 bucket.
type Bucket struct {
	Name         string `json:"name"`
	Region       string `json:"region"`
	CreationDate string `json:"creationDate,omitempty"`
	Versioning   string `json:"versioning"` // "Enabled" | "Suspended" | "Off"
	ObjectCount  int64  `json:"objectCount"`
}

// ListBucketsResponse is the response for GET /s3/buckets.
type ListBucketsResponse struct {
	Items []Bucket `json:"items"`
	Total int      `json:"total"`
}

// S3Object is the UI representation of an S3 object.
type S3Object struct {
	Key          string `json:"key"`
	Size         int64  `json:"size"`
	ETag         string `json:"etag,omitempty"`
	LastModified string `json:"lastModified,omitempty"`
	StorageClass string `json:"storageClass,omitempty"`
	ContentType  string `json:"contentType,omitempty"`
}

// ListObjectsResponse is the response for GET /s3/buckets/{bucket}/objects.
type ListObjectsResponse struct {
	Items             []S3Object `json:"items"`
	CommonPrefixes    []string   `json:"commonPrefixes"`
	IsTruncated       bool       `json:"isTruncated"`
	NextContinuation  string     `json:"nextContinuationToken,omitempty"`
	KeyCount          int        `json:"keyCount"`
}

// ObjectVersion is a versioned object entry.
type ObjectVersion struct {
	Key          string `json:"key"`
	VersionID    string `json:"versionId"`
	IsLatest     bool   `json:"isLatest"`
	Size         int64  `json:"size"`
	ETag         string `json:"etag,omitempty"`
	LastModified string `json:"lastModified,omitempty"`
	IsDeleteMarker bool  `json:"isDeleteMarker"`
}

// ListVersionsResponse is the response for GET /s3/buckets/{bucket}/versions.
type ListVersionsResponse struct {
	Items       []ObjectVersion `json:"items"`
	IsTruncated bool            `json:"isTruncated"`
}

// CreateBucketRequest is the body for POST /s3/buckets.
type CreateBucketRequest struct {
	Name   string `json:"name"`
	Region string `json:"region,omitempty"`
}

// CopyObjectRequest is the body for POST /s3/buckets/{bucket}/objects/copy.
type CopyObjectRequest struct {
	SourceBucket string `json:"sourceBucket"`
	SourceKey    string `json:"sourceKey"`
	DestKey      string `json:"destKey"`
}

// MultipartUpload represents an in-progress multipart upload.
type MultipartUpload struct {
	UploadID     string `json:"uploadId"`
	Key          string `json:"key"`
	Initiated    string `json:"initiated,omitempty"`
	StorageClass string `json:"storageClass,omitempty"`
}

// ListMultipartUploadsResponse is the response for GET /s3/buckets/{bucket}/uploads.
type ListMultipartUploadsResponse struct {
	Items       []MultipartUpload `json:"items"`
	IsTruncated bool              `json:"isTruncated"`
}

// TagsResponse wraps bucket/object tags.
type TagsResponse struct {
	Tags map[string]string `json:"tags"`
}
