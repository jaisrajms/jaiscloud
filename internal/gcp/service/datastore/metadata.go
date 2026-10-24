package datastore

import (
	"context"
	"sort"
	"strings"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// Datastore metadata kinds. Datastore reserves every kind name beginning with
// two underscores; these three are the documented metadata query kinds
// (https://cloud.google.com/datastore/docs/concepts/metadataqueries). The
// emulator synthesizes __kind__ and __property__ query results from the entity
// store; __namespace__ is reserved (writes are rejected) but not yet
// synthesized because no console surface consumes it.
const (
	metadataKindKind     = "__kind__"
	metadataKindProperty = "__property__"
	metadataKindNS       = "__namespace__"

	// propertyRepresentation is the __property__ entity's only property: the
	// list of value type names seen for the kind/property pair.
	propertyRepresentation = "property_representation"
)

// isReservedKind reports whether a kind is reserved by Datastore. Every kind
// name beginning with two underscores (`__`) is reserved and may not be used by
// user data.
func isReservedKind(kind string) bool { return strings.HasPrefix(kind, "__") }

// queryCandidates returns the entity candidates a query over kind runs against:
// synthesized metadata entities for a metadata kind, otherwise the store's
// entities of that kind. The caller applies partition scoping, filtering,
// offset and limit uniformly, so the metadata path shares the query engine.
func (s *Service) queryCandidates(ctx context.Context, project, kind, namespace, database string) ([]dsstore.Entity, error) {
	switch kind {
	case metadataKindKind:
		return s.kindMetadata(ctx, project, namespace, database)
	case metadataKindProperty:
		return s.propertyMetadata(ctx, project, namespace, database)
	default:
		entities, err := s.store.ListKind(ctx, project, kind)
		if err != nil {
			return nil, mapStoreError(err)
		}
		return entities, nil
	}
}

// kindMetadata synthesizes the __kind__ query results: one entity per user kind
// present in the query's partition, keyed by the kind name. Results are sorted
// ascending by name (the __key__ order real Datastore guarantees). Reserved
// kinds are excluded because user data cannot use them.
func (s *Service) kindMetadata(ctx context.Context, project, namespace, database string) ([]dsstore.Entity, error) {
	all, err := s.store.ListKind(ctx, project, "")
	if err != nil {
		return nil, mapStoreError(err)
	}
	seen := map[string]struct{}{}
	for _, e := range all {
		if !entityInScope(e, namespace, database) || e.Kind == "" || isReservedKind(e.Kind) {
			continue
		}
		seen[e.Kind] = struct{}{}
	}
	kinds := make([]string, 0, len(seen))
	for k := range seen {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)

	out := make([]dsstore.Entity, 0, len(kinds))
	for _, k := range kinds {
		path := []dsstore.PathElement{{Kind: metadataKindKind, Name: k, HasName: true}}
		out = append(out, dsstore.Entity{
			Kind:    metadataKindKind,
			Key:     dsstore.KeyOfPath(database, namespace, path),
			Version: 1,
		})
	}
	return out, nil
}

// propertyMetadata synthesizes the __property__ query results: one entity per
// (kind, property) pair in the query's partition. Each entity's key path is
// [__kind__:<kind>, __property__:<property>] and its property_representation
// array lists every value type seen for that pair. Results are sorted by kind
// then property.
//
// Real Datastore lists only *indexed* properties; the emulator has no index
// model, so unindexed properties are included too (documented approximation).
func (s *Service) propertyMetadata(ctx context.Context, project, namespace, database string) ([]dsstore.Entity, error) {
	all, err := s.store.ListKind(ctx, project, "")
	if err != nil {
		return nil, mapStoreError(err)
	}

	// kind -> property -> set of representation names.
	byKind := map[string]map[string]map[string]struct{}{}
	for _, e := range all {
		if !entityInScope(e, namespace, database) || e.Kind == "" || isReservedKind(e.Kind) {
			continue
		}
		props := byKind[e.Kind]
		if props == nil {
			props = map[string]map[string]struct{}{}
			byKind[e.Kind] = props
		}
		for name, v := range e.Properties {
			// Reserved property names (including __key__) are not real,
			// user-visible properties.
			if strings.HasPrefix(name, "__") {
				continue
			}
			reps := props[name]
			if reps == nil {
				reps = map[string]struct{}{}
				props[name] = reps
			}
			for _, r := range representationsOf(v) {
				reps[r] = struct{}{}
			}
		}
	}

	type kindProp struct{ kind, prop string }
	pairs := make([]kindProp, 0)
	for k, props := range byKind {
		for p := range props {
			pairs = append(pairs, kindProp{k, p})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].kind != pairs[j].kind {
			return pairs[i].kind < pairs[j].kind
		}
		return pairs[i].prop < pairs[j].prop
	})

	out := make([]dsstore.Entity, 0, len(pairs))
	for _, kp := range pairs {
		reps := make([]string, 0, len(byKind[kp.kind][kp.prop]))
		for r := range byKind[kp.kind][kp.prop] {
			reps = append(reps, r)
		}
		sort.Strings(reps)
		values := make([]dsstore.Value, 0, len(reps))
		for _, r := range reps {
			r := r
			values = append(values, dsstore.Value{StringValue: &r})
		}
		path := []dsstore.PathElement{
			{Kind: metadataKindKind, Name: kp.kind, HasName: true},
			{Kind: metadataKindProperty, Name: kp.prop, HasName: true},
		}
		out = append(out, dsstore.Entity{
			Kind: metadataKindProperty,
			Key:  dsstore.KeyOfPath(database, namespace, path),
			Properties: map[string]dsstore.Value{
				propertyRepresentation: {ArrayValue: &dsstore.ArrayValue{Values: values}},
			},
			Version: 1,
		})
	}
	return out, nil
}

// representationsOf returns the Datastore property representations for a value.
// An array value contributes its elements' representations (repeated properties
// are represented by their element type, not a distinct ARRAY type, matching
// real Datastore).
func representationsOf(v dsstore.Value) []string {
	if v.ArrayValue != nil {
		out := make([]string, 0, len(v.ArrayValue.Values))
		for _, el := range v.ArrayValue.Values {
			out = append(out, representationsOf(el)...)
		}
		return out
	}
	if r := representationOf(v); r != "" {
		return []string{r}
	}
	return nil
}

// representationOf maps a single value variant to its Datastore representation
// name. The empty string means the value has no variant.
func representationOf(v dsstore.Value) string {
	switch {
	case v.NullValue != nil:
		return "NULL"
	case v.BooleanValue != nil:
		return "BOOLEAN"
	case v.IntegerValue != nil:
		return "INT64"
	case v.DoubleValue != nil:
		return "DOUBLE"
	case v.TimestampValue != nil:
		return "TIMESTAMP"
	case v.KeyValue != nil:
		return "KEY"
	case v.StringValue != nil:
		return "STRING"
	case v.BlobValue != nil:
		return "BLOB"
	case v.GeoPointValue != nil:
		return "GEO_POINT"
	case v.EntityValue != nil:
		return "ENTITY"
	}
	return ""
}

// validateReservedMutations rejects a commit whose mutations target a reserved
// key or carry a reserved property name. Datastore forbids user writes to key
// kinds/names and property names matching `__.*__` (the metadata namespaces);
// without the guard a user could shadow a synthesized metadata entity.
func validateReservedMutations(mutations []Mutation) error {
	for _, m := range mutations {
		k := m.Key
		if m.Op == MutationDelete {
			k = m.DeleteKey
		}
		if err := validateReservedKey(k); err != nil {
			return err
		}
		if m.Op == MutationInsert || m.Op == MutationUpdate || m.Op == MutationUpsert {
			for name := range m.Entity.Properties {
				if strings.HasPrefix(name, "__") {
					return invalidArgument("property name is reserved: " + name)
				}
			}
		}
	}
	return nil
}

// validateReservedKey rejects a key whose kind or name is reserved. Every
// element of the path is checked, not just the final one, because an ancestor
// with a reserved kind makes the whole key reserved.
func validateReservedKey(k Key) error {
	if isReservedKind(k.Kind) {
		return invalidArgument("kind name is reserved: " + k.Kind)
	}
	if k.HasName && strings.HasPrefix(k.Name, "__") {
		return invalidArgument("key name is reserved: " + k.Name)
	}
	for _, e := range k.Ancestors {
		if isReservedKind(e.Kind) {
			return invalidArgument("kind name is reserved: " + e.Kind)
		}
		if e.HasName && strings.HasPrefix(e.Name, "__") {
			return invalidArgument("key name is reserved: " + e.Name)
		}
	}
	return nil
}
