package datastore

import (
	"time"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// Key is a transport-neutral Datastore key path element. The emulator supports
// single-element keys only (no ancestors); completeness is expressed by HasID /
// HasName. A key with neither is incomplete (used by AllocateIds and by
// insert/upsert mutations that want a server-allocated numeric ID).
type Key struct {
	Kind    string
	ID      int64
	Name    string
	HasID   bool
	HasName bool
}

// Complete reports whether the key names a concrete entity (numeric ID or
// name). Ancestor paths are rejected before a Key reaches the core.
func (k Key) Complete() bool { return k.HasID || k.HasName }

// MutationOp identifies a single Datastore mutation.
type MutationOp int

const (
	MutationInsert MutationOp = iota
	MutationUpdate
	MutationUpsert
	MutationDelete
)

// Mutation is one transport-neutral mutation. Entity carries the properties for
// Insert/Update/Upsert (its Key is the canonical store key, or "" to request a
// server-allocated ID). DeleteKey is the entity to delete. Precondition is the
// optional conflict-detection condition (base_version/update_time).
type Mutation struct {
	Op           MutationOp
	Entity       dsstore.Entity
	DeleteKey    Key
	Precondition *dsstore.Precondition
}

// CommitMode mirrors Datastore's CommitRequest.Mode.
type CommitMode int

const (
	// CommitModeUnspecified means: transactional iff a transaction handle is
	// present.
	CommitModeUnspecified CommitMode = iota
	CommitModeTransactional
	CommitModeNonTransactional
)

// CommitRequest is a transport-neutral Commit call.
type CommitRequest struct {
	Mode        CommitMode
	Transaction []byte
	Mutations   []Mutation
}

// MutationResult is the transport-neutral result of one mutation.
type MutationResult struct {
	Version          int64
	Key              *Key // non-nil when an ID was allocated for the mutation
	ConflictDetected bool
}

// CommitResponse is the transport-neutral result of a Commit.
type CommitResponse struct {
	Results []MutationResult
	// CommitTime is zero for a non-transactional commit (the proto omits it).
	CommitTime time.Time
}

// EntityResult is a found entity plus its per-entity version.
type EntityResult struct {
	Entity  dsstore.Entity
	Version int64
}

// LookupResult is the transport-neutral result of a Lookup.
type LookupResult struct {
	Found   []EntityResult
	Missing []Key
}

// MoreResults mirrors Datastore's QueryResultBatch.MoreResults for the two
// states the emulator emits.
type MoreResults int

const (
	MoreResultsNoMoreResults MoreResults = iota
	MoreResultsAfterLimit
)

// Query is a transport-neutral Datastore query (structured form only; GQL is
// rejected by the transports before reaching the core).
type Query struct {
	Kind   string
	Filter *Filter
	Offset int
	Limit  *int // nil = unbounded
}

// QueryResult is the transport-neutral result of RunQuery.
type QueryResult struct {
	Entities    []EntityResult
	Skipped     int
	MoreResults MoreResults
}

// PropertyOp is a transport-neutral PropertyFilter operator. Unsupported and
// unspecified operators are represented explicitly so the core can fail closed.
type PropertyOp int

const (
	PropertyUnspecified PropertyOp = iota
	PropertyEqual
	PropertyNotEqual
	PropertyIn
	PropertyLessThan
	PropertyLessThanOrEqual
	PropertyGreaterThan
	PropertyGreaterThanOrEqual
)

// CompositeOp is a transport-neutral CompositeFilter operator.
type CompositeOp int

const (
	CompositeUnspecified CompositeOp = iota
	CompositeAnd
	CompositeOr
)

// PropertyFilter is a transport-neutral property filter.
type PropertyFilter struct {
	Property string
	Op       PropertyOp
	Value    dsstore.Value
}

// CompositeFilter is a transport-neutral composite filter.
type CompositeFilter struct {
	Op      CompositeOp
	Filters []*Filter
}

// Filter is either a property filter or a composite filter.
type Filter struct {
	Property  *PropertyFilter
	Composite *CompositeFilter
}

// AggOp is a transport-neutral aggregation operator.
type AggOp int

const (
	AggUnspecified AggOp = iota
	AggCount
	AggSum
	AggAvg
)

// Aggregation is a transport-neutral aggregation.
type Aggregation struct {
	Op       AggOp
	Alias    string
	Property string
	UpTo     *int64 // Count only
}

// AggregationQuery wraps a nested query and its aggregations.
type AggregationQuery struct {
	Nested       Query
	Aggregations []Aggregation
}

// AggregationResult carries one aggregate value per alias plus the read time.
type AggregationResult struct {
	Aggregates map[string]dsstore.Value
	ReadTime   time.Time
}
