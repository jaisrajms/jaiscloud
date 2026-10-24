package storageui

// Bucket is the UI representation of a Cloud Storage bucket.
type Bucket struct {
	Name         string `json:"name"`
	Location     string `json:"location,omitempty"`
	StorageClass string `json:"storageClass,omitempty"`
	TimeCreated  string `json:"timeCreated,omitempty"`
	Updated      string `json:"updated,omitempty"`
	Versioning   bool   `json:"versioning"`
}

// ListBucketsResponse is the response for GET /buckets.
type ListBucketsResponse struct {
	Items []Bucket `json:"items"`
	Total int      `json:"total"`
}

// GCSObject is the UI representation of a Cloud Storage object. Generation and
// Metageneration are populated for ?versions=true listings so the UI can act on
// a specific revision.
type GCSObject struct {
	Name            string `json:"name"`
	Bucket          string `json:"bucket,omitempty"`
	Size            string `json:"size,omitempty"`
	ContentType     string `json:"contentType,omitempty"`
	StorageClass    string `json:"storageClass,omitempty"`
	Updated         string `json:"updated,omitempty"`
	TimeCreated     string `json:"timeCreated,omitempty"`
	TimeDeleted     string `json:"timeDeleted,omitempty"`
	MD5             string `json:"md5Hash,omitempty"`
	Generation      string `json:"generation,omitempty"`
	Metageneration  string `json:"metageneration,omitempty"`
	TemporaryHold   bool   `json:"temporaryHold,omitempty"`
	EventBasedHold  bool   `json:"eventBasedHold,omitempty"`
	RetentionExpiry string `json:"retentionExpirationTime,omitempty"`
}

// ListObjectsResponse is the response for GET /buckets/{bucket}/objects.
type ListObjectsResponse struct {
	Items         []GCSObject `json:"items"`
	Prefixes      []string    `json:"prefixes,omitempty"`
	NextPageToken string      `json:"nextPageToken,omitempty"`
}

// CreateBucketRequest is the body for POST /buckets.
type CreateBucketRequest struct {
	Name         string `json:"name"`
	Location     string `json:"location,omitempty"`
	StorageClass string `json:"storageClass,omitempty"`
}

// VersioningRequest is the body for PUT /buckets/{bucket}/versioning.
type VersioningRequest struct {
	Enabled *bool `json:"enabled"`
}

// LifecycleRequest is the body for PUT /buckets/{bucket}/lifecycle. A null or
// empty lifecycle clears the bucket's lifecycle rules.
type LifecycleRequest struct {
	Lifecycle map[string]any `json:"lifecycle"`
}

// RetentionRequest is the body for PUT /buckets/{bucket}/retention. A null or
// empty policy clears the bucket's retention policy (unless locked).
type RetentionRequest struct {
	RetentionPolicy map[string]any `json:"retentionPolicy"`
}

// DefaultEventBasedHoldRequest is the body for PUT
// /buckets/{bucket}/default-event-based-hold.
type DefaultEventBasedHoldRequest struct {
	DefaultEventBasedHold *bool `json:"defaultEventBasedHold"`
}

// ACLInsertRequest is the body for POST .../acl.
type ACLInsertRequest struct {
	Entity string `json:"entity"`
	Role   string `json:"role"`
}
