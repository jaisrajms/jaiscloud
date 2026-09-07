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
)

// Sentinel errors returned by the store, mapped to gRPC status codes by the
// service (errors.Is-compatible, matching the firestore/logging conventions).
var (
	ErrEntityNotFound = errors.New("EntityNotFound")
	ErrEntityExists   = errors.New("EntityExists")
	ErrInvalidKey     = errors.New("InvalidKey")
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
type Entity struct {
	Kind       string           `json:"kind"`
	Key        string           `json:"key"`
	Properties map[string]Value `json:"properties"`
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
