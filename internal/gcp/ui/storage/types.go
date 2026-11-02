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

// Notification is a bucket notification config (storage#notification). Event
// types are the provider's documented set: OBJECT_FINALIZE, OBJECT_INITIALIZE,
// OBJECT_METADATA_UPDATE, OBJECT_DELETE, OBJECT_ARCHIVE.
type Notification struct {
	ID               string            `json:"id,omitempty"`
	Topic            string            `json:"topic"`
	PayloadFormat    string            `json:"payloadFormat,omitempty"`
	EventTypes       []string          `json:"eventTypes,omitempty"`
	ObjectNamePrefix string            `json:"objectNamePrefix,omitempty"`
	CustomAttributes map[string]string `json:"customAttributes,omitempty"`
}

// ListNotificationsResponse is the response for GET /buckets/{bucket}/notifications.
type ListNotificationsResponse struct {
	Items []Notification `json:"items"`
	Total int            `json:"total"`
}

// NotificationRequest is the body for POST /buckets/{bucket}/notifications.
type NotificationRequest struct {
	Topic            string            `json:"topic"`
	PayloadFormat    string            `json:"payloadFormat,omitempty"`
	EventTypes       []string          `json:"eventTypes,omitempty"`
	ObjectNamePrefix string            `json:"objectNamePrefix,omitempty"`
	CustomAttributes map[string]string `json:"customAttributes,omitempty"`
}

// TransferObjectRequest is the console body for copy, rewrite and move. The
// bucket in the path is the source bucket; the destination defaults to it.
// Body carries the writable object overrides (metadata, contentType,
// destinationPredefinedAcl, ...).
type TransferObjectRequest struct {
	SourceObject            string         `json:"sourceObject"`
	DestinationBucket       string         `json:"destinationBucket,omitempty"`
	DestinationObject       string         `json:"destinationObject"`
	IfGenerationMatch       string         `json:"ifGenerationMatch,omitempty"`
	IfSourceGenerationMatch string         `json:"ifSourceGenerationMatch,omitempty"`
	RewriteToken            string         `json:"rewriteToken,omitempty"`
	Body                    map[string]any `json:"body,omitempty"`
}

// RewriteResponse is the console rendering of storage#rewriteResponse. The
// provider completes a rewrite in one call (done=true); token is carried for
// wire compatibility.
type RewriteResponse struct {
	Done                bool      `json:"done"`
	TotalBytesRewritten string    `json:"totalBytesRewritten,omitempty"`
	ObjectSize          string    `json:"objectSize,omitempty"`
	RewriteToken        string    `json:"rewriteToken,omitempty"`
	Resource            GCSObject `json:"resource,omitempty"`
}

// ComposeSource is one component of a compose request.
type ComposeSource struct {
	Name       string `json:"name"`
	Generation string `json:"generation,omitempty"`
}

// ComposeObjectRequest is the body for POST /buckets/{bucket}/objects/compose.
// Destination defaults to the bucket in the path.
type ComposeObjectRequest struct {
	DestinationBucket string          `json:"destinationBucket,omitempty"`
	DestinationObject string          `json:"destinationObject"`
	SourceObjects     []ComposeSource `json:"sourceObjects"`
	Destination       map[string]any  `json:"destination,omitempty"`
	IfGenerationMatch string          `json:"ifGenerationMatch,omitempty"`
}
