package datastore

import (
	"context"
	"errors"
	"strings"
	"testing"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

func strProp(name, s string) dsstore.Value { return dsstore.Value{StringValue: &s} }

// kindNames extracts the __kind__ metadata result names in order.
func kindNames(res *QueryResult) []string {
	out := make([]string, 0, len(res.Entities))
	for _, r := range res.Entities {
		if k, ok := KeyFromCanonical(r.Entity.Key); ok {
			out = append(out, k.Name)
		}
	}
	return out
}

// TestKindMetadataListsUserKinds verifies SELECT __key__ FROM __kind__ returns
// one entity per user kind, sorted ascending by kind name, with the metadata
// key shape (kind __kind__, name = kind).
func TestKindMetadataListsUserKinds(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	upsert(t, s, "p", nameKey("Task", "a"), intProp(1))
	upsert(t, s, "p", nameKey("User", "u"), intProp(2))
	upsert(t, s, "p", nameKey("Task", "b"), intProp(3))

	res, err := s.RunQuery(context.Background(), "p", &Query{Kind: metadataKindKind}, nil)
	if err != nil {
		t.Fatalf("RunQuery __kind__: %v", err)
	}
	got := kindNames(res)
	want := []string{"Task", "User"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("kinds = %v, want %v", got, want)
	}
	// The result entity's own kind is __kind__ and it has no properties.
	if res.Entities[0].Entity.Kind != metadataKindKind {
		t.Fatalf("metadata entity kind = %q", res.Entities[0].Entity.Kind)
	}
	if len(res.Entities[0].Entity.Properties) != 0 {
		t.Fatalf("__kind__ entity should have no properties: %+v", res.Entities[0].Entity.Properties)
	}
}

// TestKindMetadataRespectsNamespace verifies metadata queries are implicitly
// restricted to the query's namespace.
func TestKindMetadataRespectsNamespace(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	nsOnly := nameKey("TenantThing", "x")
	nsOnly.Namespace = "tenant-a"
	upsert(t, s, "p", nameKey("DefaultThing", "y"), intProp(1))
	upsert(t, s, "p", nsOnly, intProp(2))

	ctx := context.Background()
	def, err := s.RunQuery(ctx, "p", &Query{Kind: metadataKindKind}, nil)
	if err != nil {
		t.Fatalf("default namespace __kind__: %v", err)
	}
	if got := kindNames(def); len(got) != 1 || got[0] != "DefaultThing" {
		t.Fatalf("default kinds = %v, want [DefaultThing]", got)
	}

	ns, err := s.RunQuery(ctx, "p", &Query{Kind: metadataKindKind, Namespace: "tenant-a"}, nil)
	if err != nil {
		t.Fatalf("tenant-a __kind__: %v", err)
	}
	if got := kindNames(ns); len(got) != 1 || got[0] != "TenantThing" {
		t.Fatalf("tenant-a kinds = %v, want [TenantThing]", got)
	}
}

// TestPropertyMetadataAncestorFilter verifies a property query returns one
// entity per property of the ancestor kind, keyed [__kind__:k, __property__:p],
// with property_representation listing every value type seen (arrays flatten to
// their element types, NULL recorded when a null appears).
func TestPropertyMetadataAncestorFilter(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()

	taskA := dsstore.Entity{Properties: map[string]dsstore.Value{
		"name": strProp("name", "a"),
		"done": {BooleanValue: ptrBool(true)},
		"tags": {ArrayValue: &dsstore.ArrayValue{Values: []dsstore.Value{
			strProp("t", "x"), strProp("t", "y"),
		}}},
	}}
	nullStr := "NULL_VALUE"
	taskB := dsstore.Entity{Properties: map[string]dsstore.Value{
		"done": {NullValue: &nullStr},
	}}
	// A different kind's property must not leak into the ancestor query.
	upsert(t, s, "p", nameKey("Task", "a"), taskA)
	upsert(t, s, "p", nameKey("Task", "b"), taskB)
	upsert(t, s, "p", nameKey("User", "u"), dsstore.Entity{Properties: map[string]dsstore.Value{
		"email": strProp("email", "u@example.com"),
	}})

	ancestor := CanonicalKey(nameKey(metadataKindKind, "Task"))
	q := &Query{Kind: metadataKindProperty, Filter: &Filter{Property: &PropertyFilter{
		Property: keyPropertyName, Op: PropertyHasAncestor,
		Value: dsstore.Value{KeyValue: &ancestor},
	}}}
	res, err := s.RunQuery(ctx, "p", q, nil)
	if err != nil {
		t.Fatalf("RunQuery __property__: %v", err)
	}

	type propRow struct {
		name string
		reps []string
	}
	rows := map[string][]string{}
	for _, r := range res.Entities {
		k, ok := KeyFromCanonical(r.Entity.Key)
		if !ok {
			t.Fatalf("bad canonical key: %q", r.Entity.Key)
		}
		if k.Kind != metadataKindProperty || len(k.Ancestors) != 1 ||
			k.Ancestors[0].Kind != metadataKindKind || k.Ancestors[0].Name != "Task" {
			t.Fatalf("property key path = %+v", k)
		}
		arr := r.Entity.Properties[propertyRepresentation].ArrayValue
		if arr == nil {
			t.Fatalf("property %q missing representation", k.Name)
		}
		reps := make([]string, 0, len(arr.Values))
		for _, v := range arr.Values {
			reps = append(reps, *v.StringValue)
		}
		rows[k.Name] = reps
	}

	if len(rows) != 3 {
		t.Fatalf("properties = %v, want name/done/tags", rows)
	}
	if got := rows["name"]; len(got) != 1 || got[0] != "STRING" {
		t.Fatalf("name reps = %v", got)
	}
	if got := rows["tags"]; len(got) != 1 || got[0] != "STRING" {
		t.Fatalf("tags reps = %v (array should flatten to element type)", got)
	}
	if got := rows["done"]; len(got) != 2 || got[0] != "BOOLEAN" || got[1] != "NULL" {
		t.Fatalf("done reps = %v, want [BOOLEAN NULL]", got)
	}
}

func ptrBool(b bool) *bool { return &b }

// TestReservedWritesRejected verifies Datastore reserved names cannot be used by
// user writes: kinds beginning with "__", reserved key names, and reserved
// property names are all rejected.
func TestReservedWritesRejected(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()

	cases := []struct {
		name string
		mut  Mutation
	}{
		{"reserved kind", Mutation{Op: MutationUpsert, Key: nameKey("__secret__", "x"), Entity: intProp(1)}},
		{"reserved name", Mutation{Op: MutationUpsert, Key: nameKey("Task", "__id__"), Entity: intProp(1)}},
		{"reserved ancestor kind", Mutation{
			Op:     MutationUpsert,
			Key:    Key{Kind: "Child", Name: "c", HasName: true, Ancestors: []dsstore.PathElement{{Kind: "__parent__", ID: 1, HasID: true}}},
			Entity: intProp(1),
		}},
		{"reserved property", Mutation{Op: MutationUpsert, Key: nameKey("Task", "t"), Entity: dsstore.Entity{Properties: map[string]dsstore.Value{"__key__": {IntegerValue: ptrInt64(1)}}}}},
	}
	for _, tc := range cases {
		_, err := s.Commit(ctx, "p", &CommitRequest{Mutations: []Mutation{tc.mut}})
		if err == nil {
			t.Fatalf("%s: expected rejection", tc.name)
		}
		var perr interface{ Error() string }
		if !errors.As(err, &perr) {
			t.Fatalf("%s: unexpected error type %T", tc.name, err)
		}
	}

	// The metadata kinds themselves are reserved too.
	if _, err := s.Commit(ctx, "p", &CommitRequest{Mutations: []Mutation{
		{Op: MutationInsert, Key: nameKey("__kind__", "Task"), Entity: intProp(1)},
	}}); err == nil {
		t.Fatal("writing kind __kind__ should be rejected")
	}
}

// ─── synthesizer + reserved-name arms (TD19) ──────────────────────────────────
//
// The TD13 mutation spike scored the metadata synthesizers second only to the
// GQL parser for surviving mutants: the value→representation mapping, the
// reserved/empty/out-of-scope exclusions and the sort order were never asserted,
// so an emptied arm (reserved kinds leaking into __kind__, an unsorted result,
// a dropped representation case) survived. These tables name each arm.

// rawUpsert stores an entity directly on the backing store, bypassing the
// service's reserved-name guard — the only way to plant the reserved kind and
// reserved property the synthesizers must exclude (a user write could never
// create them).
func rawUpsert(t *testing.T, s *Service, project string, e dsstore.Entity) {
	t.Helper()
	if err := s.store.Upsert(context.Background(), project, e); err != nil {
		t.Fatalf("raw upsert %q: %v", e.Key, err)
	}
}

// TestRepresentationOfVariants pins the value→representation mapping for every
// Datastore value variant plus the empty value, and the array flattening.
func TestRepresentationOfVariants(t *testing.T) {
	cases := []struct {
		name string
		v    dsstore.Value
		want string
	}{
		{"null", valNull(), "NULL"},
		{"bool", valBool(true), "BOOLEAN"},
		{"int", valInt(1), "INT64"},
		{"double", valDbl(1.5), "DOUBLE"},
		{"timestamp", valTS("2020-01-01T00:00:00Z"), "TIMESTAMP"},
		{"key", valKey("k"), "KEY"},
		{"string", valStr("s"), "STRING"},
		{"blob", valBlob([]byte("b")), "BLOB"},
		{"geopoint", valGeo(1, 2), "GEO_POINT"},
		{"entity", valEntity(dsstore.Entity{}), "ENTITY"},
		{"empty", dsstore.Value{}, ""},
	}
	for _, tc := range cases {
		if got := representationOf(tc.v); got != tc.want {
			t.Errorf("representationOf(%s) = %q, want %q", tc.name, got, tc.want)
		}
	}

	// An array contributes its elements' representations (there is no distinct
	// ARRAY type), skipping an untyped element, and a nested array flattens.
	got := representationsOf(valArray(valStr("a"), valInt(1), dsstore.Value{}))
	if len(got) != 2 || got[0] != "STRING" || got[1] != "INT64" {
		t.Fatalf("representationsOf(array) = %v, want [STRING INT64]", got)
	}
	if got := representationsOf(valArray(valArray(valDbl(1)))); len(got) != 1 || got[0] != "DOUBLE" {
		t.Fatalf("representationsOf(nested array) = %v, want [DOUBLE]", got)
	}
	if got := representationsOf(dsstore.Value{}); got != nil {
		t.Fatalf("representationsOf(untyped) = %v, want nil", got)
	}
	if got := representationsOf(valArray()); len(got) != 0 {
		t.Fatalf("representationsOf(empty array) = %v, want empty", got)
	}
}

// TestKindMetadataSynthesizerArms asserts the __kind__ synthesizer deduplicates
// kinds, sorts them ascending, and excludes the empty kind, reserved kinds and
// entities outside the query's partition.
func TestKindMetadataSynthesizerArms(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()

	rawUpsert(t, s, "p", dsstore.Entity{Kind: "Zeta", Key: CanonicalKey(nameKey("Zeta", "z"))})
	rawUpsert(t, s, "p", dsstore.Entity{Kind: "Alpha", Key: CanonicalKey(nameKey("Alpha", "a"))})
	rawUpsert(t, s, "p", dsstore.Entity{Kind: "Alpha", Key: CanonicalKey(nameKey("Alpha", "b"))}) // duplicate kind
	rawUpsert(t, s, "p", dsstore.Entity{Kind: "", Key: CanonicalKey(Key{Kind: "Empty", Name: "e", HasName: true})})
	rawUpsert(t, s, "p", dsstore.Entity{
		Kind: "__secret__",
		Key:  dsstore.KeyOfPath("", "", []dsstore.PathElement{{Kind: "__secret__", Name: "s", HasName: true}}),
	})
	rawUpsert(t, s, "p", dsstore.Entity{
		Kind: "Other",
		Key:  CanonicalKey(Key{Kind: "Other", Name: "o", HasName: true, Namespace: "tenant"}),
	})

	ents, err := s.kindMetadata(ctx, "p", "", "")
	if err != nil {
		t.Fatalf("kindMetadata: %v", err)
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		k, ok := KeyFromCanonical(e.Key)
		if !ok {
			t.Fatalf("malformed metadata key %q", e.Key)
		}
		if e.Kind != metadataKindKind || k.Kind != metadataKindKind || !k.HasName {
			t.Fatalf("kind entity = %+v (key %+v), want a named __kind__", e, k)
		}
		if len(e.Properties) != 0 {
			t.Fatalf("__kind__ entity should have no properties: %+v", e.Properties)
		}
		names = append(names, k.Name)
	}
	want := []string{"Alpha", "Zeta"}
	if len(names) != len(want) || names[0] != want[0] || names[1] != want[1] {
		t.Fatalf("kindMetadata names = %v, want %v", names, want)
	}
}

// TestPropertyMetadataSynthesizerArms asserts the __property__ synthesizer emits
// one entity per (kind, property) pair sorted by kind then property, flattens
// arrays, sorts the representation list, excludes reserved properties/kinds and
// out-of-partition entities, and keys each entity [__kind__:k, __property__:p].
func TestPropertyMetadataSynthesizerArms(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()

	rawUpsert(t, s, "p", dsstore.Entity{Kind: "Beta", Key: CanonicalKey(nameKey("Beta", "b")), Properties: map[string]dsstore.Value{
		"int": valInt(1),
	}})
	rawUpsert(t, s, "p", dsstore.Entity{Kind: "Alpha", Key: CanonicalKey(nameKey("Alpha", "a")), Properties: map[string]dsstore.Value{
		"multi":   valInt(2),
		"z":       valStr("z"),
		"arr":     valArray(valDbl(1.5), valBool(true)),
		"__key__": valKey("k"),
	}})
	rawUpsert(t, s, "p", dsstore.Entity{Kind: "Alpha", Key: CanonicalKey(nameKey("Alpha", "b")), Properties: map[string]dsstore.Value{
		"multi": valStr("s"),
	}})
	rawUpsert(t, s, "p", dsstore.Entity{
		Kind:       "__reserved__",
		Key:        dsstore.KeyOfPath("", "", []dsstore.PathElement{{Kind: "__reserved__", Name: "r", HasName: true}}),
		Properties: map[string]dsstore.Value{"x": valInt(1)},
	})
	rawUpsert(t, s, "p", dsstore.Entity{
		Kind:       "Other",
		Key:        CanonicalKey(Key{Kind: "Other", Name: "o", HasName: true, Namespace: "tenant"}),
		Properties: map[string]dsstore.Value{"y": valInt(1)},
	})

	ents, err := s.propertyMetadata(ctx, "p", "", "")
	if err != nil {
		t.Fatalf("propertyMetadata: %v", err)
	}
	type row struct {
		kind string
		prop string
		reps string
	}
	rows := make([]row, 0, len(ents))
	for _, e := range ents {
		k, ok := KeyFromCanonical(e.Key)
		if !ok || len(k.Ancestors) != 1 || k.Ancestors[0].Kind != metadataKindKind || !k.Ancestors[0].HasName {
			t.Fatalf("property entity key = %+v (ok=%v)", k, ok)
		}
		arr := e.Properties[propertyRepresentation].ArrayValue
		if arr == nil {
			t.Fatalf("property %q is missing %s", k.Name, propertyRepresentation)
		}
		reps := make([]string, 0, len(arr.Values))
		for _, v := range arr.Values {
			reps = append(reps, *v.StringValue)
		}
		rows = append(rows, row{k.Ancestors[0].Name, k.Name, strings.Join(reps, ",")})
	}
	want := []row{
		{"Alpha", "arr", "BOOLEAN,DOUBLE"},
		{"Alpha", "multi", "INT64,STRING"},
		{"Alpha", "z", "STRING"},
		{"Beta", "int", "INT64"},
	}
	if len(rows) != len(want) {
		t.Fatalf("property rows = %+v, want %+v", rows, want)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Fatalf("property row %d = %+v, want %+v", i, rows[i], want[i])
		}
	}
}

// TestValidateReservedKeyTable covers every arm of the key reserve check: the
// reserved-kind and reserved-name checks on the final element and on each
// ancestor, and the "an incomplete element cannot reserve a name" boundary.
func TestValidateReservedKeyTable(t *testing.T) {
	ok := []Key{
		nameKey("Task", "a"),
		{Kind: "Task"}, // no name
		{Kind: "Task", Name: "__x", HasName: false}, // named-looking but incomplete
		{Kind: "Child", Name: "c", HasName: true, Ancestors: []dsstore.PathElement{{Kind: "Parent", Name: "p", HasName: true}}},
		{Kind: "Child", Name: "c", HasName: true, Ancestors: []dsstore.PathElement{{Kind: "Parent", Name: "__p", HasName: false}}},
	}
	for _, k := range ok {
		if err := validateReservedKey(k); err != nil {
			t.Errorf("validateReservedKey(%+v) = %v, want nil", k, err)
		}
	}
	bad := []Key{
		nameKey("__x__", "a"),
		nameKey("Task", "__a"),
		{Kind: "Child", Name: "c", HasName: true, Ancestors: []dsstore.PathElement{{Kind: "__p__", Name: "p", HasName: true}}},
		{Kind: "Child", Name: "c", HasName: true, Ancestors: []dsstore.PathElement{{Kind: "Parent", Name: "__p", HasName: true}}},
	}
	for _, k := range bad {
		if err := validateReservedKey(k); err == nil {
			t.Errorf("validateReservedKey(%+v) should fail", k)
		}
	}
}

// TestValidateReservedMutationsTable covers each mutation op: a reserved key, a
// reserved property name, and the delete path that resolves the key from
// DeleteKey and skips the property check.
func TestValidateReservedMutationsTable(t *testing.T) {
	if err := validateReservedMutations(nil); err != nil {
		t.Fatalf("validateReservedMutations(nil) = %v, want nil", err)
	}
	valid := []Mutation{
		{Op: MutationInsert, Key: nameKey("Task", "a"), Entity: dsstore.Entity{Properties: map[string]dsstore.Value{"ok": valInt(1)}}},
		{Op: MutationUpdate, Key: nameKey("Task", "b"), Entity: dsstore.Entity{Properties: map[string]dsstore.Value{"ok": valInt(1)}}},
		{Op: MutationUpsert, Key: nameKey("Task", "c"), Entity: dsstore.Entity{Properties: map[string]dsstore.Value{"ok": valInt(1)}}},
		// A delete checks DeleteKey, not Key, and never inspects properties.
		{Op: MutationDelete, Key: nameKey("Task", "d"), DeleteKey: nameKey("Task", "d"), Entity: dsstore.Entity{Properties: map[string]dsstore.Value{"__x": valInt(1)}}},
	}
	if err := validateReservedMutations(valid); err != nil {
		t.Fatalf("validateReservedMutations(valid) = %v, want nil", err)
	}
	bad := []struct {
		name string
		m    Mutation
	}{
		{"insert reserved kind", Mutation{Op: MutationInsert, Key: nameKey("__x__", "a"), Entity: intProp(1)}},
		{"insert reserved property", Mutation{Op: MutationInsert, Key: nameKey("Task", "a"), Entity: dsstore.Entity{Properties: map[string]dsstore.Value{"__key__": valInt(1)}}}},
		{"update reserved property", Mutation{Op: MutationUpdate, Key: nameKey("Task", "a"), Entity: dsstore.Entity{Properties: map[string]dsstore.Value{"__key__": valInt(1)}}}},
		{"upsert reserved property", Mutation{Op: MutationUpsert, Key: nameKey("Task", "a"), Entity: dsstore.Entity{Properties: map[string]dsstore.Value{"__key__": valInt(1)}}}},
		{"delete reserved key", Mutation{Op: MutationDelete, Key: nameKey("Task", "a"), DeleteKey: nameKey("__x__", "a")}},
	}
	for _, tc := range bad {
		if err := validateReservedMutations([]Mutation{tc.m}); err == nil {
			t.Errorf("%s: expected rejection", tc.name)
		}
	}
}

// failingListStore fails ListKind with a configured store error; the metadata
// synthesizers and the query candidate path must map it (not swallow it). The
// embedded interface's other methods are never reached by these tests.
type failingListStore struct {
	dsstore.Store
	err error
}

func (f failingListStore) ListKind(context.Context, string, string) ([]dsstore.Entity, error) {
	return nil, f.err
}

// TestQueryCandidatesErrorMapping asserts the store error ListKind returns is
// translated into the transport-neutral ProviderError on both the metadata and
// the ordinary-kind path. The sentinel is only a fixture (a listing cannot
// actually miss, so ErrInvalidKey stands in for "ListKind failed") and the
// assertion is that every path goes through mapStoreError.
func TestQueryCandidatesErrorMapping(t *testing.T) {
	s := NewService(failingListStore{err: dsstore.ErrInvalidKey}, "p")
	for _, kind := range []string{metadataKindKind, metadataKindProperty, "Task"} {
		_, err := s.queryCandidates(context.Background(), "p", kind, "", "")
		if err == nil {
			t.Fatalf("queryCandidates(%q) should fail", kind)
		}
		pe := asProviderError(t, err)
		if pe.Code != "InvalidArgument" {
			t.Fatalf("queryCandidates(%q) error = %q, want InvalidArgument", kind, pe.Code)
		}
	}
}

// TestQueryCandidatesNamespaceKind asserts the reserved __namespace__ kind is not
// a synthesized metadata kind: it falls through to the ordinary store listing
// (empty here) rather than the __kind__/__property__ synthesizers.
func TestQueryCandidatesNamespaceKind(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ents, err := s.queryCandidates(context.Background(), "p", metadataKindNS, "", "")
	if err != nil {
		t.Fatalf("queryCandidates(__namespace__): %v", err)
	}
	if len(ents) != 0 {
		t.Fatalf("queryCandidates(__namespace__) = %d entities, want 0", len(ents))
	}
}
