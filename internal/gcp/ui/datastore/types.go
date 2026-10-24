// Package datastoreui serves the Datastore console UI API. Handlers call the
// transport-neutral Datastore core directly (in-process) rather than over the
// wire, so the console and the REST/gRPC transports share one store and one
// query engine.
//
// Entities are exchanged in a wire-shaped JSON encoding: keys are ancestor
// paths plus a partition, and property values use the Datastore value oneof
// (stringValue, integerValue as a decimal string, doubleValue, booleanValue,
// nullValue, timestampValue, keyValue, blobValue base64, geoPointValue,
// entityValue, arrayValue). The console edits that object verbatim, so every
// value type round-trips losslessly — notably integerValue stays an exact int64
// and keyValue stays a structured key path rather than an internal token.
package datastoreui

import (
	"encoding/base64"
	"encoding/json"
	"strconv"

	core "jaiscloud/internal/gcp/service/datastore"
	dsstore "jaiscloud/internal/gcp/store/datastore"
	"jaiscloud/internal/model"
)

// Reserved Datastore metadata kinds and their property names (wire names).
const (
	metadataKindKind       = "__kind__"
	metadataKindProperty   = "__property__"
	propertyRepresentation = "property_representation"
	keyProperty            = "__key__"
)

// invalidArgument returns an InvalidArgument provider error.
func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// KeyElement is one element of a Datastore key path: a kind plus at most one of
// a numeric id (a decimal string, preserving int64 range) or a string name. An
// element with neither is the incomplete final element of an auto-ID key.
type KeyElement struct {
	Kind string `json:"kind"`
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}

// KeyRef is a full Datastore key: the ancestor path (root first, the entity's
// own element last) plus the partition (namespace/database).
type KeyRef struct {
	Path      []KeyElement `json:"path"`
	Namespace string       `json:"namespace,omitempty"`
	Database  string       `json:"database,omitempty"`
}

// Entity is the UI representation of a Datastore entity.
type Entity struct {
	Key        KeyRef         `json:"key"`
	Kind       string         `json:"kind"`
	Properties map[string]any `json:"properties"`
	Version    string         `json:"version,omitempty"`
	UpdateTime string         `json:"updateTime,omitempty"`
}

// Kind is one entry in the kinds list (a __kind__ metadata result name).
type Kind struct {
	Name string `json:"name"`
}

// ListKindsResponse is the response for GET /kinds.
type ListKindsResponse struct {
	Kinds []Kind `json:"kinds"`
	Total int    `json:"total"`
}

// ListEntitiesResponse is the response for GET /kinds/{kind}/entities.
type ListEntitiesResponse struct {
	Entities      []Entity `json:"entities"`
	Total         int      `json:"total"`
	NextPageToken string   `json:"nextPageToken,omitempty"`
}

// Property is one (kind, property) pair's metadata: the property name and the
// value type representations seen for it.
type Property struct {
	Name            string   `json:"name"`
	Representations []string `json:"representations"`
}

// ListPropertiesResponse is the response for GET /kinds/{kind}/properties.
type ListPropertiesResponse struct {
	Properties []Property `json:"properties"`
	Total      int        `json:"total"`
}

// UpsertEntityRequest is the body for PUT /entity. The key's final element may
// be incomplete, in which case the core allocates a numeric ID.
type UpsertEntityRequest struct {
	Key        KeyRef         `json:"key"`
	Properties map[string]any `json:"properties"`
}

// GQLQueryRequest is the body for POST /query.
type GQLQueryRequest struct {
	QueryString   string `json:"queryString"`
	Namespace     string `json:"namespace,omitempty"`
	Database      string `json:"database,omitempty"`
	AllowLiterals bool   `json:"allowLiterals,omitempty"`
}

// QueryResponse is the response for POST /query.
type QueryResponse struct {
	Entities       []Entity `json:"entities"`
	Total          int      `json:"total"`
	SkippedResults int      `json:"skippedResults,omitempty"`
	MoreResults    bool     `json:"moreResults,omitempty"`
}

// ─── core → UI ────────────────────────────────────────────────────────────────

// entityFromCore converts a stored entity into the UI shape.
func entityFromCore(e dsstore.Entity) Entity {
	out := Entity{Properties: make(map[string]any, len(e.Properties))}
	if k, ok := core.KeyFromCanonical(e.Key); ok {
		out.Key = keyRefFromCore(k)
		out.Kind = k.Kind
	}
	for name, v := range e.Properties {
		out.Properties[name] = valueToUI(v)
	}
	if e.Version != 0 {
		out.Version = strconv.FormatInt(e.Version, 10)
	}
	if !e.UpdateTime.IsZero() {
		out.UpdateTime = core.CanonicalTimestamp(e.UpdateTime)
	}
	return out
}

// keyRefFromCore maps a neutral core key to the UI key shape.
func keyRefFromCore(k core.Key) KeyRef {
	els := k.PathElements()
	path := make([]KeyElement, 0, len(els))
	for _, e := range els {
		ke := KeyElement{Kind: e.Kind}
		switch {
		case e.HasID:
			ke.ID = strconv.FormatInt(e.ID, 10)
		case e.HasName:
			ke.Name = e.Name
		}
		path = append(path, ke)
	}
	return KeyRef{Path: path, Namespace: k.Namespace, Database: k.Database}
}

// valueToUI maps a stored value to the wire-shaped JSON the console edits.
func valueToUI(v dsstore.Value) map[string]any {
	switch {
	case v.NullValue != nil:
		return map[string]any{"nullValue": "NULL_VALUE"}
	case v.BooleanValue != nil:
		return map[string]any{"booleanValue": *v.BooleanValue}
	case v.IntegerValue != nil:
		return map[string]any{"integerValue": strconv.FormatInt(*v.IntegerValue, 10)}
	case v.DoubleValue != nil:
		return map[string]any{"doubleValue": *v.DoubleValue}
	case v.TimestampValue != nil:
		return map[string]any{"timestampValue": *v.TimestampValue}
	case v.KeyValue != nil:
		if k, ok := core.KeyFromCanonical(*v.KeyValue); ok {
			return map[string]any{"keyValue": keyRefFromCore(k)}
		}
		return map[string]any{}
	case v.StringValue != nil:
		return map[string]any{"stringValue": *v.StringValue}
	case v.BlobValue != nil:
		return map[string]any{"blobValue": base64.StdEncoding.EncodeToString(v.BlobValue)}
	case v.GeoPointValue != nil:
		return map[string]any{"geoPointValue": map[string]any{
			"latitude":  v.GeoPointValue.Latitude,
			"longitude": v.GeoPointValue.Longitude,
		}}
	case v.EntityValue != nil:
		return map[string]any{"entityValue": entityFromCore(*v.EntityValue)}
	case v.ArrayValue != nil:
		vals := make([]any, 0, len(v.ArrayValue.Values))
		for _, x := range v.ArrayValue.Values {
			vals = append(vals, valueToUI(x))
		}
		return map[string]any{"arrayValue": map[string]any{"values": vals}}
	}
	return map[string]any{}
}

// ─── UI → core ────────────────────────────────────────────────────────────────

// keyRefToCore maps a UI key to the neutral core key. The final element may be
// incomplete (auto-ID); ancestors must be complete.
func keyRefToCore(ref KeyRef) (core.Key, error) {
	if len(ref.Path) == 0 {
		return core.Key{}, invalidArgument("key path is empty")
	}
	els := make([]dsstore.PathElement, 0, len(ref.Path))
	for _, ke := range ref.Path {
		if ke.Kind == "" {
			return core.Key{}, invalidArgument("key path element kind is required")
		}
		pe := dsstore.PathElement{Kind: ke.Kind}
		switch {
		case ke.ID != "":
			id, err := strconv.ParseInt(ke.ID, 10, 64)
			if err != nil {
				return core.Key{}, invalidArgument("invalid key id")
			}
			pe.ID, pe.HasID = id, true
		case ke.Name != "":
			pe.Name, pe.HasName = ke.Name, true
		}
		els = append(els, pe)
	}
	if err := core.ValidateKeyPath(els); err != nil {
		return core.Key{}, err
	}
	final := els[len(els)-1]
	out := core.Key{
		Kind:      final.Kind,
		ID:        final.ID,
		Name:      final.Name,
		HasID:     final.HasID,
		HasName:   final.HasName,
		Namespace: ref.Namespace,
		Database:  ref.Database,
	}
	if len(els) > 1 {
		out.Ancestors = els[:len(els)-1]
	}
	return out, nil
}

// entityToCore converts a UI entity (key + wire-encoded properties) into a
// stored entity. The Key is only set when the UI key is complete.
func entityToCore(e Entity) (dsstore.Entity, error) {
	out := dsstore.Entity{Properties: make(map[string]dsstore.Value, len(e.Properties))}
	for name, raw := range e.Properties {
		v, err := valueFromUI(raw)
		if err != nil {
			return dsstore.Entity{}, err
		}
		out.Properties[name] = v
	}
	if len(e.Key.Path) > 0 {
		k, err := keyRefToCore(e.Key)
		if err != nil {
			return dsstore.Entity{}, err
		}
		if k.Complete() {
			out.Key = core.CanonicalKey(k)
			out.Kind = k.Kind
		}
	}
	return out, nil
}

// valueFromUI decodes a wire-shaped value object into a stored value.
func valueFromUI(v any) (dsstore.Value, error) {
	m, ok := v.(map[string]any)
	if !ok || m == nil {
		return dsstore.Value{}, invalidArgument("malformed value")
	}
	switch {
	case hasKey(m, "nullValue"):
		s := "NULL_VALUE"
		return dsstore.Value{NullValue: &s}, nil
	case hasKey(m, "booleanValue"):
		b, _ := m["booleanValue"].(bool)
		return dsstore.Value{BooleanValue: &b}, nil
	case hasKey(m, "integerValue"):
		n, err := int64FromAny(m["integerValue"])
		if err != nil {
			return dsstore.Value{}, invalidArgument("invalid integerValue")
		}
		return dsstore.Value{IntegerValue: &n}, nil
	case hasKey(m, "doubleValue"):
		f, ok := float64FromAny(m["doubleValue"])
		if !ok {
			return dsstore.Value{}, invalidArgument("invalid doubleValue")
		}
		return dsstore.Value{DoubleValue: &f}, nil
	case hasKey(m, "timestampValue"):
		// Route through the same canonicalizer the transports use so the shared
		// store holds one form (UTC, microsecond precision) no matter which path
		// wrote it. (TD42)
		raw, _ := m["timestampValue"].(string)
		s, err := core.NormalizeTimestamp(raw)
		if err != nil {
			return dsstore.Value{}, invalidArgument("invalid timestampValue")
		}
		return dsstore.Value{TimestampValue: &s}, nil
	case hasKey(m, "keyValue"):
		ref, err := keyRefFromAny(m["keyValue"])
		if err != nil {
			return dsstore.Value{}, err
		}
		k, err := keyRefToCore(ref)
		if err != nil {
			return dsstore.Value{}, err
		}
		if !k.Complete() {
			return dsstore.Value{}, invalidArgument("key value is incomplete")
		}
		ck := core.CanonicalKey(k)
		return dsstore.Value{KeyValue: &ck}, nil
	case hasKey(m, "stringValue"):
		s, _ := m["stringValue"].(string)
		return dsstore.Value{StringValue: &s}, nil
	case hasKey(m, "blobValue"):
		s, _ := m["blobValue"].(string)
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return dsstore.Value{}, invalidArgument("invalid blobValue")
		}
		return dsstore.Value{BlobValue: b}, nil
	case hasKey(m, "geoPointValue"):
		gm, _ := m["geoPointValue"].(map[string]any)
		lat, _ := float64FromAny(gm["latitude"])
		lon, _ := float64FromAny(gm["longitude"])
		return dsstore.Value{GeoPointValue: &dsstore.GeoPoint{Latitude: lat, Longitude: lon}}, nil
	case hasKey(m, "entityValue"):
		e, err := entityFromAny(m["entityValue"])
		if err != nil {
			return dsstore.Value{}, err
		}
		return dsstore.Value{EntityValue: &e}, nil
	case hasKey(m, "arrayValue"):
		am, _ := m["arrayValue"].(map[string]any)
		raws, _ := am["values"].([]any)
		arr := dsstore.ArrayValue{Values: make([]dsstore.Value, 0, len(raws))}
		for _, x := range raws {
			xv, err := valueFromUI(x)
			if err != nil {
				return dsstore.Value{}, err
			}
			arr.Values = append(arr.Values, xv)
		}
		return dsstore.Value{ArrayValue: &arr}, nil
	}
	return dsstore.Value{}, invalidArgument("value has no variant")
}

// keyRefFromAny decodes a KeyRef from a decoded-JSON value (e.g. a value's
// keyValue or an entity's key) by round-tripping through JSON.
func keyRefFromAny(v any) (KeyRef, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return KeyRef{}, invalidArgument("malformed key")
	}
	var ref KeyRef
	if err := json.Unmarshal(b, &ref); err != nil {
		return KeyRef{}, invalidArgument("malformed key")
	}
	return ref, nil
}

// entityFromAny decodes a nested entityValue into a stored entity.
func entityFromAny(v any) (dsstore.Entity, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return dsstore.Entity{}, invalidArgument("malformed entity value")
	}
	var e Entity
	if err := json.Unmarshal(b, &e); err != nil {
		return dsstore.Entity{}, invalidArgument("malformed entity value")
	}
	return entityToCore(e)
}

// ─── scalar helpers ───────────────────────────────────────────────────────────

func hasKey(m map[string]any, k string) bool { _, ok := m[k]; return ok }

// int64FromAny parses a JSON int64, which the UI encodes as a decimal string
// but which may also arrive as a plain number.
func int64FromAny(v any) (int64, error) {
	switch x := v.(type) {
	case string:
		return strconv.ParseInt(x, 10, 64)
	case float64:
		return int64(x), nil
	case json.Number:
		return x.Int64()
	default:
		return 0, invalidArgument("not an int64")
	}
}

func float64FromAny(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	}
	return 0, false
}
