package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound           = errors.New("resource not found")
	ErrAlreadyExists      = errors.New("resource already exists")
	ErrStorageUnavailable = errors.New("storage unavailable")
)

// GlobalRegion is the sentinel region value for services that are not
// region-scoped (IAM, Route53). Use this instead of "" so that accidental
// empty-region writes are caught by Create/Upsert validation.
const GlobalRegion = "global"

// ResourceEntry is a single control-plane resource in the store.
type ResourceEntry struct {
	Type      string          // e.g. "sqs_queues"
	ID        string          // unique within Type (e.g. queue URL)
	Data      json.RawMessage // serialised resource state (JSONB in Postgres mode)
	Seeded    bool            // true if this entry was created by a provider seed (vs. user action)
	CreatedAt time.Time
	UpdatedAt time.Time
	// Account and Region are populated by List when doing a cross-scope scan
	// (account="" and region=""). Callers that need to Update after a cross-scope
	// List must use these values for the subsequent Update call.
	Account string
	Region  string
}

// ResourceStore manages control-plane resource metadata.
// Phase 0: MemoryResourceStore. Phase 1: PostgresResourceStore.
type ResourceStore interface {
	Create(ctx context.Context, account, region string, entry ResourceEntry) error
	Upsert(ctx context.Context, account, region string, entry ResourceEntry) error
	Get(ctx context.Context, account, region, resourceType, id string) (ResourceEntry, error)
	Update(ctx context.Context, account, region string, entry ResourceEntry) error
	Delete(ctx context.Context, account, region, resourceType, id string) error
	List(ctx context.Context, account, region, resourceType, prefix string) ([]ResourceEntry, error)
	Purge(ctx context.Context, account, region, resourceType string) error

	// UpsertAtomic performs a locked read-mutate-write cycle on a single entry:
	// mutate receives the current entry (zero value, exists=false if absent)
	// and returns the entry to persist, or an error to abort without writing.
	// Unlike a separate Get followed by Update/Upsert, the read and write are
	// atomic with respect to concurrent Create/Upsert/Update/UpsertAtomic calls
	// on the same key — callers doing read-modify-write with an in-value
	// precondition (e.g. etag/CAS checks) must use this instead of Get+Upsert
	// to avoid a lost-update race between two concurrent callers.
	UpsertAtomic(ctx context.Context, account, region, resourceType, id string, mutate func(current ResourceEntry, exists bool) (ResourceEntry, error)) (ResourceEntry, error)

	// Reset wipes all state — used by the admin reset endpoint.
	Reset(ctx context.Context)
}
