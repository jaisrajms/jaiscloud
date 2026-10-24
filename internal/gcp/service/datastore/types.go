package datastore

import (
	"time"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// Key is a transport-neutral Datastore key. The final path element is held in
// the flat Kind/ID/Name fields, with completeness expressed by HasID/HasName; a
// key with neither is incomplete (used by AllocateIds and by insert/upsert
// mutations that want a server-allocated numeric ID). Ancestors holds the
// parent elements from root to immediate parent (empty for a root entity), and
// Namespace/Database are the key's partition dimensions ("" for the default
// namespace / default database).
type Key struct {
	Kind    string
	ID      int64
	Name    string
	HasID   bool
	HasName bool

	Ancestors []dsstore.PathElement
	Namespace string
	Database  string
}

// Complete reports whether the key's final element names a concrete entity
// (numeric ID or name).
func (k Key) Complete() bool { return k.HasID || k.HasName }

// PathElements returns the key's full path: its ancestors followed by the
// final element. It is exported so transports can rebuild their wire key.
func (k Key) PathElements() []dsstore.PathElement { return k.path() }

// ValidateKeyPath checks that a decoded key path is structurally well-formed:
// every element must have a non-empty kind and every ancestor element (all but
// the final one) must be complete. The final element may be incomplete (an
// insert/upsert key awaiting a server-allocated ID). An empty path means "no
// key" and is allowed. Transports call this after decoding, so a malformed key
// is rejected consistently rather than diverging between store backends.
func ValidateKeyPath(path []dsstore.PathElement) error {
	for i, e := range path {
		if e.Kind == "" {
			return invalidArgument("key path element has an empty kind")
		}
		if i < len(path)-1 && !e.HasID && !e.HasName {
			return invalidArgument("ancestor key is incomplete")
		}
	}
	return nil
}

// path returns the key's full path: its ancestors followed by the final
// element.
func (k Key) path() []dsstore.PathElement {
	final := dsstore.PathElement{Kind: k.Kind, ID: k.ID, Name: k.Name, HasID: k.HasID, HasName: k.HasName}
	if len(k.Ancestors) == 0 {
		return []dsstore.PathElement{final}
	}
	out := make([]dsstore.PathElement, 0, len(k.Ancestors)+1)
	out = append(out, k.Ancestors...)
	out = append(out, final)
	return out
}

// MutationOp identifies a single Datastore mutation.
type MutationOp int

const (
	MutationInsert MutationOp = iota
	MutationUpdate
	MutationUpsert
	MutationDelete
)

// Mutation is one transport-neutral mutation. Key is the target entity's key
// for Insert/Update/Upsert; it may be incomplete (no final id/name) for
// Insert/Upsert, in which case the core allocates one. Entity carries the
// properties for those operations (its Key is filled by the core from Key).
// DeleteKey is the entity to delete. Precondition is the optional
// conflict-detection condition (base_version/update_time).
type Mutation struct {
	Op           MutationOp
	Key          Key
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
	// CreateTime is the entity's creation time (create_time): set for an applied
	// insert/update/upsert and for a rejected mutation (the current entity's
	// stamp), and zero for a delete. Real Datastore is output-only here.
	CreateTime time.Time
	// UpdateTime is the server-stamped entity update time: the new stamp after
	// an applied insert/update/upsert, the current entity's stamp when a
	// mutation is rejected (conflict_detected — the mutation changed nothing),
	// and zero for a delete (which leaves update_time unset). It is one of the
	// optimistic-concurrency tokens a client can pass forward as a
	// Mutation.update_time precondition.
	UpdateTime time.Time
}

// CommitResponse is the transport-neutral result of a Commit.
type CommitResponse struct {
	Results []MutationResult
	// CommitTime is zero for a non-transactional commit (the proto omits it).
	CommitTime time.Time
	// IndexUpdates is the number of index entries updated during the commit
	// (real Datastore's output-only CommitResponse.index_updates). The emulator
	// approximates it with the number of mutations that changed the datastore.
	IndexUpdates int32
}

// EntityResult is a found entity plus its per-entity version and output-only
// timestamps. Cursor is the position marker after the entity, set only for a
// RunQuery result (real Datastore sets it only within a QueryResultBatch).
type EntityResult struct {
	Entity     dsstore.Entity
	Version    int64
	CreateTime time.Time
	Cursor     []byte
}

// LookupResult is the transport-neutral result of a Lookup.
type LookupResult struct {
	Found   []EntityResult
	Missing []Key
	// ReadTime is the timestamp at which the lookup was served. Both the REST
	// and gRPC responses carry it (real Datastore returns it on every lookup).
	ReadTime time.Time
}

// MoreResults mirrors Datastore's QueryResultBatch.MoreResults for the two
// states the emulator emits.
type MoreResults int

const (
	MoreResultsNoMoreResults MoreResults = iota
	MoreResultsAfterLimit
	// MoreResultsAfterCursor mirrors real Datastore's MORE_RESULTS_AFTER_CURSOR:
	// the request's end cursor bounded the batch.
	MoreResultsAfterCursor
)

// Query is a transport-neutral Datastore query. Both the structured form and
// the GQL form (parsed by ParseGQL) resolve to this type, so the two share one
// execution engine.
type Query struct {
	Kind   string
	Filter *Filter
	Offset int
	Limit  *int // nil = unbounded
	// StartCursor and EndCursor are opaque positions in the ordered result
	// stream: StartCursor resumes strictly after the position it names and
	// EndCursor stops at (and includes) the position it names. The emulator's
	// cursor is the canonical key of the entity the position follows, so an
	// end cursor round-trips a page's last result and a start cursor resumes
	// at the next one.
	StartCursor []byte
	EndCursor   []byte
	// Namespace and Database scope the query's partition ("" = default).
	Namespace string
	Database  string
	// Projection, when non-nil, restricts each result entity to the named
	// properties (the entity's key is always included). A non-nil projection
	// with no properties is a keys-only query: results carry only their key.
	// A nil Projection returns full entities.
	Projection *Projection
}

// Projection is a structured (or GQL) query's property projection. Properties
// are top-level property names; "__key__" is accepted as a no-op (the key is
// always present).
type Projection struct {
	Properties []string
}

// ResultType classifies what a query's result entities carry.
type ResultType int

const (
	// ResultFull means the query has no projection: entities carry all properties.
	ResultFull ResultType = iota
	// ResultProjection means each entity carries only the projected properties.
	ResultProjection
	// ResultKeysOnly means each entity carries only its key.
	ResultKeysOnly
)

// ResultType returns the result classification for the query. A projection
// that names only "__key__" (how the clients express a keys-only query) is
// keys-only.
func (q *Query) ResultType() ResultType {
	if q == nil || q.Projection == nil {
		return ResultFull
	}
	for _, p := range q.Projection.Properties {
		if p != "__key__" {
			return ResultProjection
		}
	}
	return ResultKeysOnly
}

// projectEntity returns a copy of e carrying only the projected properties
// (plus its key and metadata). named may include "__key__", which is a no-op.
func projectEntity(e dsstore.Entity, named []string) dsstore.Entity {
	if len(named) == 0 {
		e.Properties = map[string]dsstore.Value{}
		return e
	}
	keep := make(map[string]dsstore.Value, len(named))
	for _, name := range named {
		if name == "__key__" {
			continue
		}
		if v, ok := e.Properties[name]; ok {
			keep[name] = v
		}
	}
	e.Properties = keep
	return e
}

// QueryResult is the transport-neutral result of RunQuery.
type QueryResult struct {
	Entities    []EntityResult
	Skipped     int
	MoreResults MoreResults
	// ResultType classifies what the entities carry (full / projected / keys-only).
	ResultType ResultType
	// ReadTime is the timestamp the batch was read from, and SnapshotVersion a
	// monotonically increasing version of the snapshot it came from. EndCursor
	// points to the position after the last result, and SkippedCursor to the
	// position after the last skipped result (set only when Skipped > 0). All
	// are output-only QueryResultBatch fields real Datastore sets.
	ReadTime        time.Time
	SnapshotVersion int64
	EndCursor       []byte
	SkippedCursor   []byte
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
	// PropertyHasAncestor is the special "__key__ HAS ANCESTOR" operator. Its
	// property name must be "__key__" and its value a key; it matches entities
	// whose key path has that key as a proper prefix.
	PropertyHasAncestor
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
