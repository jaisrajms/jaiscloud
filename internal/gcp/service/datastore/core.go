// Package datastore is the transport-neutral core of the Cloud Datastore v1
// service. It owns all Datastore business logic — entity/mutation application,
// the read-set optimistic-concurrency transaction protocol, query filtering and
// aggregation, and ID allocation — over the shared datastore store.
//
// It deliberately has no dependency on protobuf or on NormalizedRequest: the
// gRPC transport (internal/gcp/transport/grpc/datastore) and the REST transport
// (internal/gcp/transport/rest/datastore) both transcode their wire format into
// this package's typed API and then call the SAME Service instance. That is the
// dual-protocol invariant: one core, one piece of state (including the
// transaction read-set registry), so the transports cannot drift.
//
// Transactions follow real Datastore's read-set optimistic-concurrency model.
// BeginTransaction returns an opaque token and registers an empty read-set;
// Lookup/RunQuery calls carrying that token record the entities they observe;
// and a TRANSACTIONAL Commit re-validates every observed entity's version and
// then applies all mutations atomically (see the Service.Commit logic and the
// store's Commit method). A conflict aborts the commit with ABORTED and applies
// nothing; a per-mutation Precondition mismatch aborts it with
// FAILED_PRECONDITION.
//
// Two internal approximations are documented and never surface as new RPCs or
// fields:
//
//   - RunQuery read validation: real Datastore validates a query's read
//     *range* at commit. The emulator records the version of every entity the
//     query returned and re-validates those entities, which catches a
//     concurrent modification to any returned entity but not the appearance or
//     disappearance of a would-be match outside the result set.
//   - Transaction TTL: real read-write transactions expire after ~270s. The
//     emulator lazily evicts a read-set once it is older than txnTTL, returning
//     InvalidArgument for any later use (the same as an unknown token).
package datastore

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"jaiscloud/internal/clock"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// txnTTL bounds a transaction's lifetime, mirroring real Datastore's ~270s
// read-write transaction timeout. Expiry is enforced lazily on each access
// rather than by a background sweeper.
const txnTTL = 270 * time.Second

// maxAggregations is the proto cap on aggregations per AggregationQuery.
const maxAggregations = 5

// Service is the transport-neutral Datastore service over the shared store.
type Service struct {
	store       dsstore.Store
	defaultProj string

	// txnMu guards readSets: the in-memory transaction read-set registry
	// (transactions are ephemeral and never persisted).
	txnMu    sync.Mutex
	readSets map[string]*readSet
}

// readSet is an open transaction's observed entities (canonical key → observed
// state) plus its start time for TTL expiry.
type readSet struct {
	reads map[string]dsstore.ReadRef
	start time.Time
}

// NewService returns a Datastore core backed by the shared store. defaultProj
// is the config-default project, used by transports when a request carries
// none.
func NewService(store dsstore.Store, defaultProj string) *Service {
	return &Service{
		store:       store,
		defaultProj: defaultProj,
		readSets:    make(map[string]*readSet),
	}
}

// DefaultProject returns the configured default project.
func (s *Service) DefaultProject() string { return s.defaultProj }

// Reset clears the in-memory transaction read-set registry. The store's own
// state is reset separately (it is registered as a Resetter by main.go).
func (s *Service) Reset(context.Context) {
	s.txnMu.Lock()
	s.readSets = make(map[string]*readSet)
	s.txnMu.Unlock()
}

// ─── transaction registry ─────────────────────────────────────────────────────

// txnLocked returns the read-set for an *active* transaction, evicting it if
// its TTL has elapsed, or nil when it is unknown/expired. The caller must hold
// txnMu.
func (s *Service) txnLocked(txn string) *readSet {
	rs := s.readSets[txn]
	if rs == nil {
		return nil
	}
	if clock.Now().Sub(rs.start) > txnTTL {
		delete(s.readSets, txn)
		return nil
	}
	return rs
}

// recordRead registers an entity observation in a transaction's read-set.
// Non-transactional calls (empty token) and unknown/expired transactions are
// ignored. Missing entities are recorded with Exists=false (Version 0) so a
// concurrent create aborts the eventual commit.
func (s *Service) recordRead(txn []byte, key string, exists bool, version int64) {
	if len(txn) == 0 {
		return
	}
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	rs := s.txnLocked(string(txn))
	if rs == nil {
		return
	}
	if rs.reads == nil {
		rs.reads = make(map[string]dsstore.ReadRef)
	}
	rs.reads[key] = dsstore.ReadRef{Key: key, Exists: exists, Version: version}
}

// readSetFor returns the transaction's observations as a slice sorted by key,
// or nil when the token is empty/unknown/expired/there were no reads.
func (s *Service) readSetFor(txn []byte) []dsstore.ReadRef {
	if len(txn) == 0 {
		return nil
	}
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	rs := s.txnLocked(string(txn))
	if rs == nil || len(rs.reads) == 0 {
		return nil
	}
	out := make([]dsstore.ReadRef, 0, len(rs.reads))
	for _, r := range rs.reads {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// clearReadSet discards a transaction's read-set after commit/rollback.
func (s *Service) clearReadSet(txn []byte) {
	if len(txn) == 0 {
		return
	}
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	delete(s.readSets, string(txn))
}

// requireActive returns an InvalidArgument error when transaction is non-empty
// but not currently active (rolled back, already committed, expired, or never
// begun). An empty transaction denotes a non-transactional operation and always
// passes, so it still works for unknown-token detection in Commit.
func (s *Service) requireActive(transaction []byte) error {
	if len(transaction) == 0 {
		return nil
	}
	s.txnMu.Lock()
	defer s.txnMu.Unlock()
	if s.txnLocked(string(transaction)) == nil {
		return invalidArgument("transaction is no longer active (rolled back, committed, expired, or never begun)")
	}
	return nil
}

// txnSeq mints the opaque transaction tokens returned by BeginTransaction.
// Real handles are opaque bytes; a process-unique token is all the registry
// needs to key a transaction's read-set.
var txnSeq int64

// BeginTransaction opens a transaction and returns its opaque handle.
func (s *Service) BeginTransaction(context.Context) ([]byte, error) {
	txn := []byte("txn-" + strconv.FormatInt(atomic.AddInt64(&txnSeq, 1), 10))
	s.txnMu.Lock()
	if s.readSets == nil {
		s.readSets = make(map[string]*readSet)
	}
	s.readSets[string(txn)] = &readSet{reads: make(map[string]dsstore.ReadRef), start: clock.Now()}
	s.txnMu.Unlock()
	return txn, nil
}

// Rollback discards a transaction's read-set. Idempotent for an unknown/expired
// transaction, matching real Datastore.
func (s *Service) Rollback(_ context.Context, transaction []byte) {
	s.clearReadSet(transaction)
}

// ─── Commit ───────────────────────────────────────────────────────────────────

// Commit applies a batch of mutations. Dispatch on the requested mode: a
// TRANSACTIONAL commit (or an unspecified mode carrying a transaction handle)
// re-validates the read-set and applies atomically; otherwise each mutation is
// applied independently, reporting per-mutation conflicts in the result.
func (s *Service) Commit(ctx context.Context, project string, req *CommitRequest) (*CommitResponse, error) {
	txn := req.Transaction

	// MODE_UNSPECIFIED (the zero value) is treated as non-transactional unless
	// a transaction selector is present — the behavior non-transactional SDK
	// commits rely on; a TRANSACTIONAL commit always requires a handle.
	transactional := false
	switch req.Mode {
	case CommitModeTransactional:
		if len(txn) == 0 {
			return nil, invalidArgument("TRANSACTIONAL commit requires a transaction")
		}
		transactional = true
	case CommitModeNonTransactional:
		transactional = false
	default:
		transactional = len(txn) > 0
	}
	if transactional {
		return s.commitTransactional(ctx, project, txn, req.Mutations)
	}

	results := make([]MutationResult, 0, len(req.Mutations))
	for _, m := range req.Mutations {
		mr := MutationResult{Version: 1}
		switch m.Op {
		case MutationInsert, MutationUpsert:
			storeKind := dsstore.MutationInsert
			if m.Op == MutationUpsert {
				storeKind = dsstore.MutationUpsert
			}
			e, allocated, err := s.resolveEntity(ctx, project, m.Entity)
			if err != nil {
				return nil, err
			}
			applied, err := s.store.ApplyMutation(ctx, project, storeKind, e, m.Precondition)
			switch {
			case errors.Is(err, dsstore.ErrConflict):
				mr.ConflictDetected = true
				mr.Version = applied.Version
			case errors.Is(err, dsstore.ErrEntityExists):
				return nil, alreadyExists("entity already exists")
			case err != nil:
				return nil, mapStoreError(err)
			default:
				mr.Version = applied.Version
				if allocated {
					mr.Key = keyFromCanonical(e.Key)
				}
			}
		case MutationUpdate:
			e := m.Entity
			if e.Key == "" {
				return nil, invalidArgument("update key is incomplete")
			}
			applied, err := s.store.ApplyMutation(ctx, project, dsstore.MutationUpdate, e, m.Precondition)
			switch {
			case errors.Is(err, dsstore.ErrConflict):
				mr.ConflictDetected = true
				mr.Version = applied.Version
			case errors.Is(err, dsstore.ErrEntityNotFound):
				return nil, failedPrecondition("entity not found", 404)
			case err != nil:
				return nil, mapStoreError(err)
			default:
				mr.Version = applied.Version
			}
		case MutationDelete:
			key, err := deleteKey(m.DeleteKey)
			if err != nil {
				return nil, err
			}
			if err := s.store.DeleteConflictChecked(ctx, project, key, m.Precondition); err != nil {
				if errors.Is(err, dsstore.ErrConflict) {
					mr.ConflictDetected = true
				} else {
					return nil, mapStoreError(err)
				}
			}
		default:
			return nil, invalidArgument("mutation has no operation")
		}
		results = append(results, mr)
	}
	return &CommitResponse{Results: results}, nil
}

// commitTransactional implements the Datastore transaction commit protocol. It
// re-validates the transaction's read-set and applies every mutation
// atomically: any read-set conflict aborts the whole commit with ABORTED and
// applies nothing, while a per-mutation Precondition mismatch aborts it with
// FAILED_PRECONDITION. The transaction is terminated on every attempt that
// reaches the store (real Datastore requires a fresh BeginTransaction to retry
// an aborted one).
//
// Entity resolution (key parsing and auto-ID allocation) happens before the
// store commit. ID allocation is not rolled back if the commit aborts — an
// internal approximation matching real Datastore, where allocated IDs are
// monotonic and may be skipped.
func (s *Service) commitTransactional(ctx context.Context, project string, txn []byte, mutations []Mutation) (*CommitResponse, error) {
	if err := s.requireActive(txn); err != nil {
		return nil, err
	}
	reads := s.readSetFor(txn)

	writes := make([]dsstore.Write, 0, len(mutations))
	allocatedKeys := make([]*Key, len(mutations))
	for i, m := range mutations {
		switch m.Op {
		case MutationInsert, MutationUpsert:
			writeOp := dsstore.WriteInsert
			if m.Op == MutationUpsert {
				writeOp = dsstore.WriteUpsert
			}
			e, allocated, err := s.resolveEntity(ctx, project, m.Entity)
			if err != nil {
				return nil, err
			}
			writes = append(writes, dsstore.Write{Op: writeOp, Key: e.Key, Entity: e, Precondition: m.Precondition})
			if allocated {
				allocatedKeys[i] = keyFromCanonical(e.Key)
			}
		case MutationUpdate:
			e := m.Entity
			if e.Key == "" {
				return nil, invalidArgument("update key is incomplete")
			}
			writes = append(writes, dsstore.Write{Op: dsstore.WriteUpdate, Key: e.Key, Entity: e, Precondition: m.Precondition})
		case MutationDelete:
			key, err := deleteKey(m.DeleteKey)
			if err != nil {
				return nil, err
			}
			writes = append(writes, dsstore.Write{Op: dsstore.WriteDelete, Key: key, Precondition: m.Precondition})
		default:
			return nil, invalidArgument("mutation has no operation")
		}
	}

	commitTime := clock.Now()
	applied, err := s.store.Commit(ctx, project, reads, writes)
	s.clearReadSet(txn)
	if err != nil {
		return nil, mapStoreError(err)
	}

	results := make([]MutationResult, 0, len(applied))
	for i := range applied {
		mr := MutationResult{Version: applied[i].Version}
		if allocatedKeys[i] != nil {
			mr.Key = allocatedKeys[i]
		}
		results = append(results, mr)
	}
	return &CommitResponse{Results: results, CommitTime: commitTime}, nil
}

// ─── Lookup / RunQuery ────────────────────────────────────────────────────────

// Lookup returns the entities at the requested complete keys, partitioning the
// result into found and missing (the miss is recorded in the transaction
// read-set too).
func (s *Service) Lookup(ctx context.Context, project string, keys []Key, txn []byte) (*LookupResult, error) {
	if err := s.requireActive(txn); err != nil {
		return nil, err
	}
	resp := &LookupResult{}
	for _, k := range keys {
		if !k.Complete() {
			return nil, invalidArgument("lookup key is incomplete")
		}
		key := canonicalKey(k)
		e, err := s.store.Get(ctx, project, key)
		switch {
		case errors.Is(err, dsstore.ErrEntityNotFound):
			// Record the miss (version 0) so a concurrent create aborts the
			// eventual commit — real Datastore records absent keys in the
			// read-set too.
			s.recordRead(txn, key, false, 0)
			resp.Missing = append(resp.Missing, k)
		case err != nil:
			return nil, mapStoreError(err)
		default:
			s.recordRead(txn, key, true, e.Version)
			resp.Found = append(resp.Found, EntityResult{Entity: e, Version: e.Version})
		}
	}
	return resp, nil
}

// RunQuery runs a structured query with the emulator's ListKind + filter engine.
func (s *Service) RunQuery(ctx context.Context, project string, q *Query, txn []byte) (*QueryResult, error) {
	if err := s.requireActive(txn); err != nil {
		return nil, err
	}
	if q == nil {
		return nil, invalidArgument("run query request has no query")
	}
	offset, limit, err := queryWindow(q)
	if err != nil {
		return nil, err
	}
	entities, err := s.store.ListKind(ctx, project, q.Kind)
	if err != nil {
		return nil, mapStoreError(err)
	}
	out := &QueryResult{MoreResults: MoreResultsNoMoreResults}
	skipped := 0
	for _, e := range entities {
		match, err := matchesFilter(e, q.Filter)
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}
		// Offset is applied after filtering: the skipped entities are counted
		// so a cursor-based client can reconcile them.
		if skipped < offset {
			skipped++
			continue
		}
		// Limit caps the returned entities. The scan keeps going only far
		// enough to learn whether a further match exists, so MoreResults can
		// report MORE_RESULTS_AFTER_LIMIT (real Datastore's signal that a
		// limit, not the data set, ended the batch).
		if limit >= 0 && len(out.Entities) == limit {
			out.MoreResults = MoreResultsAfterLimit
			break
		}
		// Internal approximation: the emulator records the version of every
		// entity the query returned and re-validates exactly those entities.
		s.recordRead(txn, e.Key, true, e.Version)
		out.Entities = append(out.Entities, EntityResult{Entity: e, Version: e.Version})
	}
	if skipped > 0 {
		out.Skipped = skipped
	}
	return out, nil
}

// queryWindow extracts a query's non-negative offset and limit. limit is -1
// when the query carries no limit (unbounded). A negative offset or limit is
// rejected with InvalidArgument, matching real Datastore rather than silently
// treating it as unbounded.
func queryWindow(q *Query) (offset, limit int, err error) {
	limit = -1
	if q == nil {
		return 0, -1, nil
	}
	offset = q.Offset
	if offset < 0 {
		return 0, 0, invalidArgument("query offset must be non-negative")
	}
	if q.Limit != nil {
		limit = *q.Limit
		if limit < 0 {
			return 0, 0, invalidArgument("query limit must be non-negative")
		}
	}
	return offset, limit, nil
}

// RunAggregationQuery implements the Datastore aggregation-query RPC over the
// structured AggregationQuery form. It runs the wrapped nested query with the
// same engine RunQuery uses and reduces the matching entities to one result per
// aggregation (count/sum/avg), keyed by alias.
//
// Transaction-aware: a transaction selector must name an *active* transaction,
// and every entity the nested query returns is recorded in the transaction's
// read-set — exactly as RunQuery does — so a transactional commit re-validates
// them.
func (s *Service) RunAggregationQuery(ctx context.Context, project string, aq *AggregationQuery, txn []byte) (*AggregationResult, error) {
	if err := s.requireActive(txn); err != nil {
		return nil, err
	}
	if aq == nil {
		return nil, invalidArgument("aggregation query is missing")
	}
	aggs := aq.Aggregations
	if len(aggs) == 0 {
		return nil, invalidArgument("aggregation query must contain at least one aggregation")
	}
	if len(aggs) > maxAggregations {
		return nil, invalidArgument("aggregation query supports at most five aggregations")
	}

	entities, err := s.store.ListKind(ctx, project, aq.Nested.Kind)
	if err != nil {
		return nil, mapStoreError(err)
	}

	// Reduce the nested query once, recording every matching entity in the
	// transaction read-set (the same approximation RunQuery makes).
	matching := make([]dsstore.Entity, 0, len(entities))
	for _, e := range entities {
		match, err := matchesFilter(e, aq.Nested.Filter)
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}
		s.recordRead(txn, e.Key, true, e.Version)
		matching = append(matching, e)
	}

	// A non-grouped aggregation query returns one result whose aggregate
	// properties map holds one entry per aggregation.
	result := &AggregationResult{Aggregates: make(map[string]dsstore.Value, len(aggs)), ReadTime: clock.Now()}
	unnamed := 0
	for _, agg := range aggs {
		alias := agg.Alias
		if alias == "" {
			// Real Datastore auto-names an unaliased aggregation
			// "property_<incremental_id>", sharing one counter across the
			// whole query.
			unnamed++
			alias = "property_" + strconv.Itoa(unnamed)
		}
		if _, dup := result.Aggregates[alias]; dup {
			return nil, invalidArgument("duplicate aggregation alias: " + alias)
		}
		val, err := aggregate(agg, matching)
		if err != nil {
			return nil, err
		}
		result.Aggregates[alias] = val
	}
	return result, nil
}

// ─── ID allocation ────────────────────────────────────────────────────────────

// AllocateIDs allocates a numeric ID for each incomplete key and returns the
// completed keys. A complete key is rejected.
func (s *Service) AllocateIDs(ctx context.Context, project string, keys []Key) ([]Key, error) {
	for _, k := range keys {
		if k.Complete() {
			return nil, invalidArgument("allocate ids requires incomplete keys")
		}
	}
	ids, err := s.store.AllocateIDs(ctx, project, len(keys))
	if err != nil {
		return nil, mapStoreError(err)
	}
	out := make([]Key, len(keys))
	for i, k := range keys {
		out[i] = Key{Kind: k.Kind, ID: ids[i], HasID: true}
	}
	return out, nil
}

// ReserveIDs advances the project's ID allocator past every supplied complete
// numeric key so a later AllocateIds never reissues those IDs. Name keys are a
// no-op (names never collide with the numeric-ID space). An incomplete key is
// rejected.
func (s *Service) ReserveIDs(ctx context.Context, project string, keys []Key) error {
	for _, k := range keys {
		if !k.Complete() {
			return invalidArgument("reserve ids requires complete keys")
		}
		if err := s.advanceAllocator(ctx, project, canonicalKey(k)); err != nil {
			return err
		}
	}
	return nil
}

// resolveEntity allocates a numeric ID when the entity's key path is
// incomplete. The bool reports whether an ID was allocated (so Commit can echo
// the resolved key back in MutationResult.Key). For an explicitly-keyed entity,
// the per-project ID allocator is advanced past the explicit numeric ID so
// AllocateIds never reissues an ID already in use.
func (s *Service) resolveEntity(ctx context.Context, project string, e dsstore.Entity) (dsstore.Entity, bool, error) {
	if e.Key == "" {
		ids, err := s.store.AllocateIDs(ctx, project, 1)
		if err != nil {
			return e, false, mapStoreError(err)
		}
		e.Key = dsstore.KeyOfID(e.Kind, ids[0])
		return e, true, nil
	}
	if err := s.advanceAllocator(ctx, project, e.Key); err != nil {
		return e, false, err
	}
	return e, false, nil
}

// advanceAllocator advances the project's ID allocator past an explicitly-used
// numeric ID (name keys are ignored).
func (s *Service) advanceAllocator(ctx context.Context, project, key string) error {
	_, idOrName, ok := dsstore.SplitKey(key)
	if !ok {
		return nil
	}
	id, _, isID := dsstore.ParseIDOrName(idOrName)
	if !isID {
		return nil
	}
	return mapStoreError(s.store.AdvanceIDs(ctx, project, id))
}

// deleteKey requires a complete key and returns its canonical store key.
func deleteKey(k Key) (string, error) {
	if !k.Complete() {
		return "", invalidArgument("delete key is incomplete")
	}
	return canonicalKey(k), nil
}

// canonicalKey converts a complete neutral key to the store's canonical key
// string.
func canonicalKey(k Key) string {
	if k.HasID {
		return dsstore.KeyOfID(k.Kind, k.ID)
	}
	if k.HasName {
		return dsstore.KeyOfName(k.Kind, k.Name)
	}
	return ""
}

// CanonicalKey returns the store's canonical key string for a neutral key. It
// is exported so transports can build the store.Entity.Key of an explicitly
// keyed entity.
func CanonicalKey(k Key) string { return canonicalKey(k) }

// keyFromCanonical reconstructs a neutral Key from the store's canonical key
// string, or nil when the key is malformed.
func keyFromCanonical(key string) *Key {
	kind, idOrName, ok := dsstore.SplitKey(key)
	if !ok {
		return nil
	}
	id, name, isID := dsstore.ParseIDOrName(idOrName)
	if isID {
		return &Key{Kind: kind, ID: id, HasID: true}
	}
	return &Key{Kind: kind, Name: name, HasName: true}
}

// KeyFromCanonical reconstructs a neutral Key from a canonical store key
// string. ok is false when the key is malformed. It is exported so transports
// can rebuild their wire key from a stored entity's canonical key.
func KeyFromCanonical(key string) (Key, bool) {
	k := keyFromCanonical(key)
	if k == nil {
		return Key{}, false
	}
	return *k, true
}
