// Package datastore provides the Cloud Datastore entity data-plane store.
// Entities live in the dedicated jc_datastore_entities table (Postgres) or in
// memory. Datastore is the DynamoDB analogue: a key-value/document store of
// kind + key + properties, with a per-project numeric-ID allocator.
package datastore

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
)

// Sentinel errors returned by the store, mapped to gRPC status codes by the
// service (errors.Is-compatible, matching the firestore/logging conventions).
var (
	ErrEntityNotFound = errors.New("EntityNotFound")
	ErrEntityExists   = errors.New("EntityExists")
	ErrInvalidKey     = errors.New("InvalidKey")
	// ErrConflict is returned by ApplyMutation/DeleteConflictChecked when a
	// caller-supplied Precondition doesn't match the entity's current state.
	// The mutation was NOT applied. This maps to a real Datastore Mutation's
	// per-mutation conflict_detection_strategy outcome (MutationResult.
	// conflict_detected=true) rather than aborting the whole Commit — see
	// ApplyMutation's doc comment.
	ErrConflict = errors.New("Conflict")
	// ErrAborted is returned by Commit when a transaction read-set entry no
	// longer matches the entity's current state. No write in the batch was
	// applied; the caller must retry the whole transaction. Mirrors real
	// Datastore's ABORTED (transaction contention).
	ErrAborted = errors.New("Aborted")
)

// GeoPoint is a Datastore geo_point_value ({latitude, longitude}).
type GeoPoint struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// ArrayValue is the Datastore array_value container.
type ArrayValue struct {
	Values []Value `json:"values,omitempty"`
}

// Value is a Datastore value: exactly one variant field is set. It mirrors the
// Datastore wire Value one-of (null, boolean, integer, double, timestamp, key,
// string, blob, geo point, entity, array). Integers and doubles are split
// (unlike DynamoDB's arbitrary-precision number), and key_value stores a key as
// its canonical string form (see KeyOfPath/ParseKey).
type Value struct {
	NullValue      *string     `json:"nullValue,omitempty"`
	BooleanValue   *bool       `json:"booleanValue,omitempty"`
	IntegerValue   *int64      `json:"integerValue,omitempty"`
	DoubleValue    *float64    `json:"doubleValue,omitempty"`
	TimestampValue *string     `json:"timestampValue,omitempty"` // RFC 3339
	KeyValue       *string     `json:"keyValue,omitempty"`       // canonical key
	StringValue    *string     `json:"stringValue,omitempty"`
	BlobValue      []byte      `json:"blobValue,omitempty"` // base64 on the wire
	GeoPointValue  *GeoPoint   `json:"geoPointValue,omitempty"`
	EntityValue    *Entity     `json:"entityValue,omitempty"`
	ArrayValue     *ArrayValue `json:"arrayValue,omitempty"`
}

// Entity is a stored Datastore entity. Kind is the entity kind (denormalized
// for kind-scoped queries). Key is the stable canonical key string
// (see KeyOfPath/ParseKey), encoding the key's partition (database +
// namespace) and full ancestor path; the store's primary key is (project,
// Key). Properties is the property map keyed by property name.
// Version and UpdateTime are server-managed bookkeeping used only by
// ApplyMutation/DeleteConflictChecked's optimistic-concurrency check — a
// caller constructing an Entity for Insert/Update/Upsert/ApplyMutation does
// not set these; the store stamps them on write.
type Entity struct {
	Kind       string           `json:"kind"`
	Key        string           `json:"key"`
	Properties map[string]Value `json:"properties"`
	Version    int64            `json:"version,omitempty"`
	UpdateTime time.Time        `json:"updateTime,omitempty"`
}

// MutationKind identifies which Datastore mutation ApplyMutation applies.
type MutationKind int

const (
	MutationInsert MutationKind = iota
	MutationUpdate
	MutationUpsert
)

// WriteOp identifies the operation a Write applies within Store.Commit. It is
// the multi-mutation analogue of MutationKind, extended with a delete (which
// ApplyMutation does not handle — a delete has no stored entity to return).
type WriteOp int

const (
	WriteInsert WriteOp = iota
	WriteUpdate
	WriteUpsert
	WriteDelete
)

// ReadRef records an entity observed within a transaction, for optimistic
// concurrency re-validation at commit time. Key is the store's canonical key
// string; Exists is false when the key was read as absent (Version is then 0).
type ReadRef struct {
	Key     string
	Exists  bool
	Version int64
}

// Write is a single mutation applied atomically within Store.Commit. Key is
// the canonical store key holding the target (for WriteDelete, the entity
// being deleted; for the others it must equal Entity.Key). Entity is ignored
// for WriteDelete.
type Write struct {
	Op           WriteOp
	Key          string
	Entity       Entity
	Precondition *Precondition
}

// Precondition is the optional per-mutation conflict-detection condition from
// Datastore's real Mutation.conflict_detection_strategy oneof. At most one of
// BaseVersion/UpdateTime should be set; nil (no Precondition at all) always
// matches.
type Precondition struct {
	BaseVersion *int64
	UpdateTime  *time.Time
}

// Store is the Cloud Datastore entity data-plane store. Entities are
// project-scoped and keyed by their canonical key string.
type Store interface {
	// Get returns the entity at key, or ErrEntityNotFound.
	Get(ctx context.Context, project, key string) (Entity, error)
	// Insert stores a new entity, or ErrEntityExists if one exists at key.
	Insert(ctx context.Context, project string, e Entity) error
	// Upsert stores an entity, overwriting any existing one at key.
	Upsert(ctx context.Context, project string, e Entity) error
	// Update replaces an existing entity, or ErrEntityNotFound.
	Update(ctx context.Context, project string, e Entity) error
	// Delete removes the entity at key. Idempotent.
	Delete(ctx context.Context, project, key string) error

	// ApplyMutation atomically checks precondition (if non-nil) against the
	// entity currently stored at e.Key and, if it matches (or precondition is
	// nil), applies the insert/update/upsert — all under one lock/transaction,
	// so no concurrent mutation on the same entity can be observed or applied
	// in between the check and the apply. Version and UpdateTime on e are
	// ignored (server-managed) and stamped on the returned entity: 1 and
	// clock.Now() for a successful Insert, current.Version+1 and clock.Now()
	// for Update/Upsert.
	//
	// Returns ErrConflict — without applying the mutation — if precondition
	// doesn't match. This must NOT be treated as fatal by the caller the way
	// ErrEntityExists/ErrEntityNotFound are: real Datastore's
	// conflict_detection_strategy is a per-mutation outcome (marked in that
	// mutation's MutationResult.conflict_detected), not a reason to abort the
	// rest of the Commit's other mutations.
	//
	// Returns ErrEntityExists for an Insert whose key already exists, or
	// ErrEntityNotFound for an Update whose key doesn't — exactly as Insert/
	// Update do — checked precondition-first, so a conflict takes priority
	// over an existence mismatch when both would apply.
	ApplyMutation(ctx context.Context, project string, kind MutationKind, e Entity, precondition *Precondition) (Entity, error)

	// DeleteConflictChecked atomically checks precondition (if non-nil)
	// against the entity currently stored at key and, if it matches (or
	// precondition is nil), deletes it. Deleting an already-absent entity
	// with a nil precondition is idempotent (matching Delete); with a
	// non-nil precondition against an absent entity, only a
	// Precondition{BaseVersion: &0} conceptually "matches" (real Datastore
	// treats a missing entity as version 0) — anything else returns
	// ErrConflict.
	DeleteConflictChecked(ctx context.Context, project, key string, precondition *Precondition) error

	// Commit applies a batch of writes atomically (all-or-nothing) after
	// (1) re-validating the transaction read-set and (2) validating each
	// write's Precondition and existence requirement. No check-then-apply gap
	// exists between any step: in memory the whole sequence runs under one
	// lock, and in Postgres inside one Serializable transaction with each
	// touched row locked (SELECT ... FOR UPDATE).
	//
	// Returns ErrAborted when a read-set entry no longer matches, ErrConflict
	// when a write precondition fails, ErrEntityExists for an Insert whose key
	// already exists, or ErrEntityNotFound for an Update whose key is absent.
	// In every error case NO write is applied.
	//
	// On success the returned slice is aligned with writes and carries the
	// server-stamped Version for each applied entity (a delete yields
	// Entity{Key: <key>}).
	Commit(ctx context.Context, project string, reads []ReadRef, writes []Write) ([]Entity, error)
	// ListKind returns every entity of the given kind in the project, sorted by
	// key. An empty kind returns every entity in the project.
	ListKind(ctx context.Context, project, kind string) ([]Entity, error)
	// AllocateIDs reserves n positive, monotonic numeric IDs for the project.
	AllocateIDs(ctx context.Context, project string, n int) ([]int64, error)
	// AdvanceIDs advances the project's numeric-ID allocator so the next
	// allocated ID is strictly greater than max. Used to ensure AllocateIDs
	// never reissues an ID that was already assigned to an explicitly-keyed
	// (numeric) entity.
	AdvanceIDs(ctx context.Context, project string, max int64) error
	Reset(ctx context.Context)
}

// preconditionMatches reports whether p is satisfied by the entity's current
// state (current, exists). A nil p always matches. Shared by every Store
// implementation's ApplyMutation/DeleteConflictChecked.
func preconditionMatches(current Entity, exists bool, p *Precondition) bool {
	if p == nil {
		return true
	}
	if p.BaseVersion != nil {
		if !exists {
			// Real Datastore treats a nonexistent entity as version 0.
			return *p.BaseVersion == 0
		}
		return current.Version == *p.BaseVersion
	}
	if p.UpdateTime != nil {
		if !exists {
			return false
		}
		return current.UpdateTime.Equal(*p.UpdateTime)
	}
	return true
}

// resolveWrite validates one Write against the entity currently stored at its
// key (current, exists) and returns the entity to persist with its
// server-stamped Version/UpdateTime, or an error. For WriteDelete the returned
// entity is Entity{Key: w.Key} (the caller performs the delete). Shared by the
// memory and Postgres Commit implementations so both enforce identical
// semantics: a precondition mismatch (ErrConflict) takes priority over an
// existence mismatch (ErrEntityExists / ErrEntityNotFound), exactly as
// ApplyMutation does.
func resolveWrite(current Entity, exists bool, w Write) (Entity, error) {
	if !preconditionMatches(current, exists, w.Precondition) {
		return Entity{}, ErrConflict
	}
	// Derive the stamp from the stored value so it is strictly monotonic per
	// entity even under a frozen clock (the optimistic-concurrency token a
	// Mutation.update_time precondition compares against) — see nextUpdateTime.
	// For an insert the current entity is absent (zero UpdateTime), so the
	// stamp is plain clock.Now().
	stamp := nextUpdateTime(current.UpdateTime, clock.Now())
	switch w.Op {
	case WriteInsert:
		if exists {
			return Entity{}, ErrEntityExists
		}
		e := w.Entity
		e.Version = 1
		e.UpdateTime = stamp
		return e, nil
	case WriteUpdate:
		if !exists {
			return Entity{}, ErrEntityNotFound
		}
		e := w.Entity
		e.Version = current.Version + 1
		e.UpdateTime = stamp
		return e, nil
	case WriteUpsert:
		e := w.Entity
		e.Version = current.Version + 1
		e.UpdateTime = stamp
		return e, nil
	case WriteDelete:
		return Entity{Key: w.Key}, nil
	default:
		return Entity{}, ErrInvalidKey
	}
}

// nextUpdateTime returns the update timestamp to stamp on a write, guaranteed
// to be strictly greater than the entity's current update time.
//
// Datastore uses update_time as one of its optimistic-concurrency tokens: a
// mutation only succeeds if the stored update_time still equals the one the
// writer observed (a Mutation.update_time precondition). The emulator derives
// update_time from clock.Now(), which a frozen clock (time control,
// deterministic tests) can return unchanged for every write. Without a guard,
// two concurrent writers would both observe T, both compute T, and both pass
// the check — a silent lost update.
//
// Both stamps are truncated to microseconds (the granularity of the Postgres
// TIMESTAMPTZ column, so the token survives a round-trip through the persistent
// backend and matches the value a later read returns) and a microsecond is
// added whenever the clock does not advance. That keeps update_time strictly
// monotonic per entity, so the second writer's observed value no longer matches
// and its mutation is flagged conflict_detected instead of overwriting. Under a
// live clock now is almost always after prev, making the conditional advance a
// no-op.
func nextUpdateTime(prev, now time.Time) time.Time {
	prev = prev.Truncate(time.Microsecond)
	now = now.Truncate(time.Microsecond)
	if prev.IsZero() || now.After(prev) {
		return now
	}
	return prev.Add(time.Microsecond)
}

// keyVersionPrefix marks the canonical key encoding. Every canonical key is
// "<prefix>" + the length-prefixed partition (database, namespace) + one
// length-prefixed segment per path element. The prefix exists so a malformed
// or pre-ancestor key is rejected outright rather than misparsed.
const keyVersionPrefix = "v1|"

// PathElement is one element of a Datastore key path: a kind plus either a
// numeric ID or a string name (exactly one, unless the element is the
// incomplete final element of an auto-ID key). It mirrors the protobuf
// Key.PathElement one-of.
type PathElement struct {
	Kind    string
	ID      int64
	Name    string
	HasID   bool
	HasName bool
}

// KeyOfPath returns the canonical store key for a fully-qualified Datastore
// key: a partition (database + namespace) and a path of one or more elements.
// The encoding is self-delimiting — every field is length-prefixed — so a kind
// or name containing "/", ":", or any other character round-trips exactly.
// This mirrors real Datastore/Firestore, where a Key is a protobuf message
// with explicit length-delimited fields, never a delimiter-joined string (see
// https://cloud.google.com/php/docs/reference/cloud-datastore/latest/V1.Key).
//
// The final path element may be incomplete (neither HasID nor HasName): it is
// encoded with a "?" tag and round-trips through ParseKey. Callers that only
// store complete entities never build such a key; it exists so a core can
// canonicalize an incomplete key while allocating an ID.
func KeyOfPath(database, namespace string, path []PathElement) string {
	var b strings.Builder
	b.WriteString(keyVersionPrefix)
	writeLenPrefixed(&b, database)
	writeLenPrefixed(&b, namespace)
	for _, e := range path {
		writeLenPrefixed(&b, e.Kind)
		switch {
		case e.HasID:
			b.WriteByte('i')
			writeLenPrefixed(&b, strconv.FormatInt(e.ID, 10))
		case e.HasName:
			b.WriteByte('n')
			writeLenPrefixed(&b, e.Name)
		default:
			b.WriteByte('?')
		}
	}
	return b.String()
}

// KeyOfID returns the canonical store key for a root, default-partition
// numeric-ID key (a convenience wrapper over KeyOfPath).
func KeyOfID(kind string, id int64) string {
	return KeyOfPath("", "", []PathElement{{Kind: kind, ID: id, HasID: true}})
}

// KeyOfName returns the canonical store key for a root, default-partition name
// key (a convenience wrapper over KeyOfPath).
func KeyOfName(kind, name string) string {
	return KeyOfPath("", "", []PathElement{{Kind: kind, Name: name, HasName: true}})
}

// ParseKey parses a canonical store key into its partition (database,
// namespace) and path. ok is false when the key is malformed or was produced
// by an incompatible older encoding.
func ParseKey(key string) (database, namespace string, path []PathElement, ok bool) {
	rest, found := strings.CutPrefix(key, keyVersionPrefix)
	if !found {
		return "", "", nil, false
	}
	database, rest, ok = readLenPrefixed(rest)
	if !ok {
		return "", "", nil, false
	}
	namespace, rest, ok = readLenPrefixed(rest)
	if !ok {
		return "", "", nil, false
	}
	for rest != "" {
		kind, r, kok := readLenPrefixed(rest)
		if !kok || kind == "" || r == "" {
			return "", "", nil, false
		}
		tag := r[0]
		r = r[1:]
		switch tag {
		case 'i':
			val, r2, vok := readLenPrefixed(r)
			if !vok {
				return "", "", nil, false
			}
			id, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return "", "", nil, false
			}
			path = append(path, PathElement{Kind: kind, ID: id, HasID: true})
			rest = r2
		case 'n':
			val, r2, vok := readLenPrefixed(r)
			if !vok {
				return "", "", nil, false
			}
			path = append(path, PathElement{Kind: kind, Name: val, HasName: true})
			rest = r2
		case '?':
			path = append(path, PathElement{Kind: kind})
			rest = r
		default:
			return "", "", nil, false
		}
	}
	if len(path) == 0 {
		return "", "", nil, false
	}
	return database, namespace, path, true
}

// KeyKind returns the kind of a canonical key's final path element, or "" when
// the key is malformed. It is the denormalized value the store keeps in its
// kind column for kind-scoped listing.
func KeyKind(key string) string {
	_, _, path, ok := ParseKey(key)
	if !ok || len(path) == 0 {
		return ""
	}
	return path[len(path)-1].Kind
}

// writeLenPrefixed writes "<len>:<s>" — a decimal byte length, a colon, then
// the raw bytes — to b.
func writeLenPrefixed(b *strings.Builder, s string) {
	b.WriteString(strconv.Itoa(len(s)))
	b.WriteByte(':')
	b.WriteString(s)
}

// readLenPrefixed consumes one "<len>:<value>" field from s and returns the
// value and the unconsumed remainder.
func readLenPrefixed(s string) (value, rest string, ok bool) {
	i := strings.IndexByte(s, ':')
	if i < 0 {
		return "", "", false
	}
	n, err := strconv.Atoi(s[:i])
	if err != nil || n < 0 || len(s) < i+1+n {
		return "", "", false
	}
	return s[i+1 : i+1+n], s[i+1+n:], true
}
