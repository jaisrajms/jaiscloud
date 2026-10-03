// Package firestore provides the Firestore document data-plane store. Documents
// live in the dedicated jc_firestore_documents table (Postgres, slice 2) or in
// memory; unlike DynamoDB there are no materialized index side-tables —
// collections are implicit path segments and single-field indexes are implicit.
package firestore

import (
	"context"
	"errors"
	"strings"
	"time"

	"jaiscloud/internal/clock"
)

// Sentinel errors returned by the store, mapped to Firestore HTTP responses by
// the provider (errors.Is-compatible, matching the gcs store conventions).
var (
	ErrDocumentNotFound   = errors.New("DocumentNotFound")
	ErrDocumentExists     = errors.New("DocumentExists")
	ErrInvalidArgument    = errors.New("InvalidArgument")
	ErrPreconditionFailed = errors.New("PreconditionFailed")
	ErrAborted            = errors.New("Aborted")
)

// DocumentMissingError reports a write whose currentDocument.exists=true
// precondition failed because the target document is absent. It carries the
// document name so the provider can render the canonical Firestore NOT_FOUND
// message ("No document to update: <name>"), and unwraps to
// ErrPreconditionFailed so errors.Is classification still works.
type DocumentMissingError struct{ Name string }

func (e *DocumentMissingError) Error() string { return "document does not exist: " + e.Name }

// Unwrap lets existing errors.Is(err, ErrPreconditionFailed) checks keep
// matching a missing-document existence failure.
func (e *DocumentMissingError) Unwrap() error { return ErrPreconditionFailed }

// DocumentExistsError reports a write whose currentDocument.exists=false
// precondition failed because the target document already exists. It unwraps to
// ErrDocumentExists.
type DocumentExistsError struct{ Name string }

func (e *DocumentExistsError) Error() string { return "document already exists: " + e.Name }

// Unwrap lets existing errors.Is(err, ErrDocumentExists) checks keep matching.
func (e *DocumentExistsError) Unwrap() error { return ErrDocumentExists }

// Document is a single Firestore document. Name is the full resource name
// ("projects/{p}/databases/{db}/documents/{path}"). CollectionID and ParentPath
// are derived from Name and persisted only for efficient collectionGroup and
// hierarchical queries; they are not part of the REST wire Document.
type Document struct {
	CollectionID string            `json:"-"`
	ParentPath   string            `json:"-"`
	Name         string            `json:"name,omitempty"`
	Fields       map[string]*Value `json:"fields,omitempty"`
	CreateTime   time.Time         `json:"createTime,omitempty"`
	UpdateTime   time.Time         `json:"updateTime,omitempty"`
}

// Precondition is an optimistic per-write guard on a document's current state.
// Exactly one of Exists / UpdateTime is meaningful: Exists is a *bool so the
// wire can distinguish "exists must be false" from "not set".
type Precondition struct {
	Exists     *bool
	UpdateTime *time.Time
}

// Write is a single document write applied atomically within a Commit. Document
// is nil for a delete.
type Write struct {
	Name         string
	Document     *Document
	Precondition *Precondition
}

// ReadRef records a document read within a transaction, for optimistic
// concurrency re-validation at commit time.
type ReadRef struct {
	Name       string
	Exists     bool
	UpdateTime time.Time
}

// FirestoreStore is the Firestore document data-plane store. Documents are keyed
// by their full resource name, which is globally unique (it embeds the owning
// project and database).
//
// TODO(slice-2): composite-index registry persistence. Commit (atomic batch
// with preconditions + read-set validation) is implemented on both backends.
type FirestoreStore interface {
	// GetDocument returns the document with the given full name, or
	// ErrDocumentNotFound.
	GetDocument(ctx context.Context, name string) (Document, error)
	// CreateDocument stores a new document, or ErrDocumentExists.
	CreateDocument(ctx context.Context, doc Document) error
	// UpdateDocument replaces an existing document, or ErrDocumentNotFound.
	UpdateDocument(ctx context.Context, doc Document) error
	// DeleteDocument removes a document. Idempotent: deleting a non-existent
	// document succeeds (real Firestore DeleteDocument does not error without a
	// precondition).
	DeleteDocument(ctx context.Context, name string) error
	// ListDocuments returns every document under the given project+database,
	// sorted by name. Used by listDocuments/runQuery (slice 2).
	ListDocuments(ctx context.Context, project, database string) ([]Document, error)
	// Commit applies a batch of writes atomically after (1) re-validating the
	// transaction read-set and (2) validating each write's precondition. It
	// returns ErrAborted when a read-set entry no longer matches; for a failed
	// precondition it returns *DocumentMissingError (exists=true, absent,
	// errors.Is ErrPreconditionFailed) or *DocumentExistsError (exists=false,
	// present, errors.Is ErrDocumentExists), or ErrPreconditionFailed for an
	// updateTime mismatch. In every case no writes are applied.
	Commit(ctx context.Context, reads []ReadRef, writes []Write) error
	Reset(ctx context.Context)
}

// ParseDocumentName splits a full document name
// "projects/{p}/databases/{db}/documents/{path}" into its project, database,
// and relative document path components. ok is false when name is malformed.
func ParseDocumentName(name string) (project, database, path string, ok bool) {
	segs := strings.Split(name, "/")
	if len(segs) < 5 || segs[0] != "projects" || segs[2] != "databases" || segs[4] != "documents" {
		return "", "", "", false
	}
	return segs[1], segs[3], strings.Join(segs[5:], "/"), true
}

// normalizeDocument fills default timestamps and derives CollectionID/ParentPath
// from Name before persistence (both backends).
func normalizeDocument(doc *Document) {
	if doc.Fields == nil {
		doc.Fields = map[string]*Value{}
	}
	if doc.CreateTime.IsZero() {
		doc.CreateTime = clock.Now()
	}
	if doc.UpdateTime.IsZero() {
		doc.UpdateTime = doc.CreateTime
	}
	project, database, path, ok := ParseDocumentName(doc.Name)
	if !ok {
		return
	}
	segs := strings.Split(path, "/")
	if len(segs) < 2 {
		return
	}
	doc.CollectionID = segs[len(segs)-2]
	prefix := "projects/" + project + "/databases/" + database + "/documents/"
	doc.ParentPath = prefix + strings.Join(segs[:len(segs)-1], "/")
}
