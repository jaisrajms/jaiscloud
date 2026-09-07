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
// its canonical string form (see KeyOfID/KeyOfName).
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
// "kind/id-or-name" (see KeyOfID/KeyOfName); the store's primary key is
// (project, Key). Properties is the property map keyed by property name.
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

// KeyOfID returns the canonical key string for a numeric-ID key:
// "<len(kind)>:<kind>/id:<id>". The kind is length-prefixed — rather than
// relying on "/" as an unambiguous separator — so a kind containing "/" (or
// any other character) still round-trips correctly through SplitKey. This
// mirrors the structural principle real Datastore/Firestore key encoding
// uses: a Key is a protobuf message with explicit length-delimited fields,
// never a delimiter-joined string, so a kind or name can never corrupt the
// parse regardless of its contents. See
// https://cloud.google.com/php/docs/reference/cloud-datastore/latest/V1.Key
// (Key.PathElement: kind + (id xor name), not a joined string).
func KeyOfID(kind string, id int64) string {
	return encodeKind(kind) + "id:" + strconv.FormatInt(id, 10)
}

// KeyOfName returns the canonical key string for a name key:
// "<len(kind)>:<kind>/name:<name>". See KeyOfID for why kind is
// length-prefixed. name is not: ParseIDOrName splits it off by the fixed
// "id:"/"name:" tag prefix (a single Cut on the first ":"), so name may
// itself contain "/" or ":" without ambiguity.
func KeyOfName(kind, name string) string {
	return encodeKind(kind) + "name:" + name
}

func encodeKind(kind string) string {
	return strconv.Itoa(len(kind)) + ":" + kind + "/"
}

// SplitKey parses "<len(kind)>:<kind>/id-or-name" into its kind and tagged
// id-or-name. ok is false when the key is malformed.
func SplitKey(key string) (kind, idOrName string, ok bool) {
	lenStr, rest, found := strings.Cut(key, ":")
	if !found {
		return "", "", false
	}
	n, err := strconv.Atoi(lenStr)
	if err != nil || n < 0 || n > len(rest) {
		return "", "", false
	}
	kind = rest[:n]
	if kind == "" || len(rest) == n || rest[n] != '/' {
		return "", "", false
	}
	idOrName = rest[n+1:]
	if idOrName == "" {
		return "", "", false
	}
	return kind, idOrName, true
}

// ParseIDOrName parses a tagged id-or-name ("id:<n>" or "name:<s>") into its
// numeric id (isID=true) or name (isID=false).
func ParseIDOrName(s string) (id int64, name string, isID bool) {
	tag, val, ok := strings.Cut(s, ":")
	if !ok {
		return 0, "", false
	}
	switch tag {
	case "id":
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, "", false
		}
		return n, "", true
	case "name":
		return 0, val, false
	default:
		return 0, "", false
	}
}
