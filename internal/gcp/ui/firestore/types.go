// Package firestoreui serves the Firestore UI API. Handlers call the Firestore
// provider directly (in-process) rather than over the wire.
//
// Documents are exchanged in Firestore's own typed "fields" encoding (the same
// JSON the wire API uses: stringValue, integerValue as a decimal string,
// doubleValue, booleanValue, nullValue, arrayValue, mapValue, timestampValue,
// bytesValue, referenceValue, geoPointValue). The console editor edits that
// object verbatim, so every value type round-trips losslessly — notably
// integerValue stays an exact int64 and doubleValue never collapses to an
// integer.
package firestoreui

// Collection is the UI representation of a top-level Firestore collection.
type Collection struct {
	ID string `json:"id"`
}

// ListCollectionsResponse is the response for GET /collections.
type ListCollectionsResponse struct {
	Collections   []Collection `json:"collections"`
	Total         int          `json:"total"`
	NextPageToken string       `json:"nextPageToken,omitempty"`
}

// Document is the UI representation of a Firestore document. ID is the short
// document id (the last path segment); Fields is the raw Firestore value
// encoding. Missing marks a document that has no fields of its own but has
// subcollections nested underneath it (Firestore showMissing): it is browsable
// but has no create/update time.
type Document struct {
	ID         string         `json:"id"`
	Name       string         `json:"name"`
	Collection string         `json:"collection"`
	Fields     map[string]any `json:"fields"`
	CreateTime string         `json:"createTime,omitempty"`
	UpdateTime string         `json:"updateTime,omitempty"`
	Missing    bool           `json:"missing,omitempty"`
}

// ListDocumentsResponse is the response for GET /collections/{collection}/documents.
type ListDocumentsResponse struct {
	Documents     []Document `json:"documents"`
	Total         int        `json:"total"`
	NextPageToken string     `json:"nextPageToken,omitempty"`
}

// CreateDocumentRequest is the body for POST /collections/{collection}/documents.
// DocumentID is optional; the server generates a 20-character auto-ID when empty.
type CreateDocumentRequest struct {
	DocumentID string         `json:"documentId,omitempty"`
	Fields     map[string]any `json:"fields"`
}

// UpdateDocumentRequest is the body for PATCH .../documents/{document}. Fields
// fully replaces the stored fields. UpdateTime, when set, is the document's
// observed updateTime and is enforced as an optimistic-concurrency precondition
// (a concurrent change fails the save instead of silently overwriting it).
type UpdateDocumentRequest struct {
	Fields     map[string]any `json:"fields"`
	UpdateTime string         `json:"updateTime,omitempty"`
}

// RunQueryRequest is the body for POST /query. StructuredQuery is a Firestore
// StructuredQuery passed through verbatim (the console assembles it from its
// structured builder or edits the raw JSON). Scope is the parent document path
// relative to the database documents root: empty for the whole database, or a
// document path (e.g. `cities/SF`) to scope a subcollection/collection-group
// query.
type RunQueryRequest struct {
	Scope           string         `json:"scope,omitempty"`
	StructuredQuery map[string]any `json:"structuredQuery"`
}

// RunQueryResponse is the response for POST /query. ReadTime is the server read
// timestamp shared by the returned documents; SkippedResults is set when the
// query's offset skipped documents.
type RunQueryResponse struct {
	Documents      []Document `json:"documents"`
	ReadTime       string     `json:"readTime,omitempty"`
	SkippedResults int        `json:"skippedResults,omitempty"`
}

// IndexField is one field of a composite index. Exactly one of Order
// (ASCENDING/DESCENDING) or ArrayConfig (CONTAINS) is set for a directional or
// array field respectively.
type IndexField struct {
	FieldPath   string `json:"fieldPath"`
	Order       string `json:"order,omitempty"`
	ArrayConfig string `json:"arrayConfig,omitempty"`
}

// Index is the UI representation of a Firestore composite index. ID and
// CollectionGroup are derived from the fully qualified Name
// (projects/{p}/databases/{db}/collectionGroups/{cg}/indexes/{id}).
type Index struct {
	Name            string       `json:"name"`
	ID              string       `json:"id"`
	CollectionGroup string       `json:"collectionGroup"`
	QueryScope      string       `json:"queryScope,omitempty"`
	State           string       `json:"state,omitempty"`
	Fields          []IndexField `json:"fields"`
}

// ListIndexesResponse is the response for GET /indexes.
type ListIndexesResponse struct {
	Indexes       []Index `json:"indexes"`
	Total         int     `json:"total"`
	NextPageToken string  `json:"nextPageToken,omitempty"`
}

// CreateIndexRequest is the body for POST /indexes. Fields must contain at least
// two entries; QueryScope defaults to COLLECTION when empty.
type CreateIndexRequest struct {
	CollectionGroup string       `json:"collectionGroup"`
	QueryScope      string       `json:"queryScope,omitempty"`
	Fields          []IndexField `json:"fields"`
}
