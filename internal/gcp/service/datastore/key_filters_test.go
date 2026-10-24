package datastore

import (
	"context"
	"sort"
	"testing"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// namedPath builds a canonical key whose path is a single kind/name element in
// the default partition.
func namedPath(kind, name string) string { return dsstore.KeyOfName(kind, name) }

// TestKeyCompare pins the documented key ordering: ancestor path, then entity
// kind, then identifier (numeric IDs numerically and before key names; kinds
// and names by byte value). Comparing the canonical strings directly would get
// the kind and negative-ID cases wrong, which is why keyCompare parses.
// https://cloud.google.com/datastore/docs/concepts/queries#key_filters
func TestKeyCompare(t *testing.T) {
	root := dsstore.KeyOfID("Parent", 1)
	nested := dsstore.KeyOfPath("", "", []dsstore.PathElement{
		{Kind: "Parent", ID: 1, HasID: true},
		{Kind: "Child", Name: "c", HasName: true},
	})
	kindA := dsstore.KeyOfPath("", "", []dsstore.PathElement{
		{Kind: "A", ID: 1, HasID: true},
		{Kind: "Child", Name: "c", HasName: true},
	})

	cases := []struct {
		name string
		a, b string
		want int
	}{
		// Kind byte value, not the encoding's length prefix ("b" after "aa").
		{"kind byte order", namedPath("aa", "x"), namedPath("b", "x"), -1},
		{"kind byte order reversed", namedPath("b", "x"), namedPath("aa", "x"), 1},
		// Numeric IDs are integers, not digit strings (negative IDs break the
		// encoding's digit-first order: canonical "-3" < "-5" lexically).
		{"negative ids", dsstore.KeyOfID("K", -5), dsstore.KeyOfID("K", -3), -1},
		{"id digit count", dsstore.KeyOfID("K", 9), dsstore.KeyOfID("K", 10), -1},
		// Numeric IDs precede key names for the same kind.
		{"id before name", dsstore.KeyOfID("K", 5), namedPath("K", "a"), -1},
		{"name after id", namedPath("K", "a"), dsstore.KeyOfID("K", 5), 1},
		// Key names order by byte value.
		{"name byte order", namedPath("K", "Z"), namedPath("K", "a"), -1},
		// A shorter path that prefixes a longer one sorts first.
		{"shorter path first", root, nested, -1},
		// The ancestor path is compared element by element.
		{"ancestor kind", kindA, nested, -1},
		{"same signature equal", nested, nested, 0},
		// The partition is not part of the documented criteria.
		{"partition ignored", dsstore.KeyOfPath("db", "ns", []dsstore.PathElement{{Kind: "K", ID: 1, HasID: true}}), namedPathID("K", 1), 0},
		// An incomplete element (unreachable on a stored key) sorts after every
		// numeric ID and orders as an empty name.
		{"incomplete after id", dsstore.KeyOfPath("", "", []dsstore.PathElement{{Kind: "K"}}), namedPathID("K", 1), 1},
		{"incomplete is empty name", dsstore.KeyOfPath("", "", []dsstore.PathElement{{Kind: "K"}}), namedPath("K", ""), 0},
		// A non-canonical value (unreachable from a transport) falls back to
		// byte order rather than becoming incomparable.
		{"non-canonical fallback", "a", "b", -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := keyCompare(tc.a, tc.b); got != tc.want {
				t.Errorf("keyCompare(%q, %q) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
			if got := keyCompare(tc.b, tc.a); got != -tc.want {
				t.Errorf("keyCompare(%q, %q) = %d, want %d (antisymmetric)", tc.b, tc.a, got, -tc.want)
			}
		})
	}
}

// namedPathID is a tiny alias used only to keep a long literal readable.
func namedPathID(kind string, id int64) string { return dsstore.KeyOfID(kind, id) }

// TestValueKeyComparisonUsesDocumentedOrder confirms the value-level key paths
// (valueEqual + valueCompare) treat a key *value* as partition-sensitive while
// ordering by the documented path order within a partition, so the encoding
// artifact ("aa" after "b") is gone.
func TestValueKeyComparisonUsesDocumentedOrder(t *testing.T) {
	// Kind byte order: the encoding would sort "aa" after "b".
	if c, ok := valueCompare(valKey(namedPath("aa", "x")), valKey(namedPath("b", "x"))); !ok || c >= 0 {
		t.Errorf("valueCompare(kind aa, kind b) = %d, %v; want < 0", c, ok)
	}
	// A key value carries its partition: the same path in different partitions
	// is neither equal nor unordered.
	part := dsstore.KeyOfPath("db", "ns", []dsstore.PathElement{{Kind: "K", ID: 1, HasID: true}})
	if valueEqual(valKey(part), valKey(namedPathID("K", 1))) {
		t.Error("key values differing in partition must not be equal")
	}
	if c, ok := valueCompare(valKey(part), valKey(namedPathID("K", 1))); !ok || c <= 0 {
		t.Errorf("valueCompare(partitioned key, default key) = %d, %v; want > 0", c, ok)
	}
	if valueEqual(valKey(namedPathID("K", 1)), valKey(namedPathID("K", 2))) {
		t.Error("different identifiers must not be equal")
	}
}

// keyFilter runs a kind-scoped query with a __key__ property filter and returns
// the matched canonical keys, sorted for stable assertions.
func keyFilter(t *testing.T, s *Service, kind string, op PropertyOp, value dsstore.Value) []string {
	t.Helper()
	res, err := s.RunQuery(context.Background(), "p", &Query{Kind: kind, Filter: &Filter{
		Property: &PropertyFilter{Property: keyPropertyName, Op: op, Value: value},
	}}, nil)
	if err != nil {
		t.Fatalf("RunQuery(__key__ %s): %v", propertyOpName(op), err)
	}
	out := make([]string, 0, len(res.Entities))
	for _, e := range res.Entities {
		out = append(out, e.Entity.Key)
	}
	sort.Strings(out)
	return out
}

func sortedKeys(keys ...string) []string {
	out := append([]string(nil), keys...)
	sort.Strings(out)
	return out
}

// TestKeyPropertyFilters exercises the documented key filters end to end: an
// entity has no stored "__key__" property, so these only match if the filter
// engine synthesizes it from the entity's own key.
func TestKeyPropertyFilters(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")

	// Same kind, mixed identifier types, so the numeric-before-name rule is
	// observable.
	nA, nB := nameKey("Task", "a"), nameKey("Task", "b")
	i2, i10 := idKey("Task", 2), idKey("Task", 10)
	for _, k := range []Key{nA, nB, i2, i10} {
		upsert(t, s, "p", k, intProp(1))
	}
	cA, cB := canonicalKey(nA), canonicalKey(nB)
	c2, c10 := canonicalKey(i2), canonicalKey(i10)
	keyVal := func(k string) dsstore.Value { return dsstore.Value{KeyValue: &k} }

	cases := []struct {
		name string
		op   PropertyOp
		v    dsstore.Value
		want []string
	}{
		{"equal name", PropertyEqual, keyVal(cA), []string{cA}},
		{"equal id", PropertyEqual, keyVal(c10), []string{c10}},
		{"equal miss", PropertyEqual, keyVal(namedPath("Task", "zzz")), nil},
		{"not equal", PropertyNotEqual, keyVal(cA), sortedKeys(cB, c2, c10)},
		// IN is a disjunction over the operand array.
		{"in", PropertyIn, dsstore.Value{ArrayValue: &dsstore.ArrayValue{Values: []dsstore.Value{keyVal(cA), keyVal(c10)}}}, sortedKeys(cA, c10)},
		// Ordering: numeric IDs sort before key names, so a bound on id 2 also
		// lets the name-keyed entities through on the greater-than side.
		{"greater than id", PropertyGreaterThan, keyVal(c2), sortedKeys(cA, cB, c10)},
		{"less than name", PropertyLessThan, keyVal(cB), sortedKeys(c2, c10, cA)},
		{"greater or equal self", PropertyGreaterThanOrEqual, keyVal(c2), sortedKeys(c2, cA, cB, c10)},
		{"less or equal id", PropertyLessThanOrEqual, keyVal(c2), []string{c2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := keyFilter(t, s, "Task", tc.op, tc.v)
			if len(got) != len(tc.want) {
				t.Fatalf("matched = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("matched = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// TestKeyFilterAncestorPath confirms a key filter distinguishes entities by
// their ancestor path (the first ordering criterion) and that a KEY(...)
// operand with an empty partition still matches within the query's partition.
func TestKeyFilterAncestorPath(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()

	c1 := Key{Kind: "Child", Name: "x", HasName: true, Ancestors: []dsstore.PathElement{ancestorElem("Parent", 1)}}
	c2 := Key{Kind: "Child", Name: "x", HasName: true, Ancestors: []dsstore.PathElement{ancestorElem("Parent", 2)}}
	upsert(t, s, "p", c1, intProp(1))
	upsert(t, s, "p", c2, intProp(2))
	ck1 := canonicalKey(c1)

	res, err := s.RunQuery(ctx, "p", &Query{Kind: "Child", Filter: &Filter{Property: &PropertyFilter{
		Property: keyPropertyName, Op: PropertyEqual, Value: dsstore.Value{KeyValue: &ck1},
	}}}, nil)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Entities) != 1 || *res.Entities[0].Entity.Properties["n"].IntegerValue != 1 {
		t.Fatalf("equality on a descendant key = %+v, want only c1", res.Entities)
	}

	res, err = s.RunQuery(ctx, "p", &Query{Kind: "Child", Filter: &Filter{Property: &PropertyFilter{
		Property: keyPropertyName, Op: PropertyGreaterThan, Value: dsstore.Value{KeyValue: &ck1},
	}}}, nil)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Entities) != 1 || *res.Entities[0].Entity.Properties["n"].IntegerValue != 2 {
		t.Fatalf("ordering on a descendant key = %+v, want only c2", res.Entities)
	}
}

// TestGQLKeyFilterEndToEnd drives the GQL KEY(...) literal through the same
// engine, in a non-default namespace: the literal carries no partition of its
// own, and the query's partition scopes the result set.
func TestGQLKeyFilterEndToEnd(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")

	task := nameKey("Task", "abc")
	task.Namespace = "ns"
	upsert(t, s, "p", task, intProp(1))

	res, err := s.RunQueryGQL(context.Background(), "p", GQLQuery{
		QueryString:   "SELECT * FROM Task WHERE __key__ = KEY('Task', 'abc')",
		AllowLiterals: true,
	}, nil, "ns", "")
	if err != nil {
		t.Fatalf("GQL key equality: %v", err)
	}
	if len(res.Entities) != 1 {
		t.Fatalf("GQL key equality results = %+v, want 1", res.Entities)
	}

	res, err = s.RunQueryGQL(context.Background(), "p", GQLQuery{
		QueryString:   "SELECT * FROM Task WHERE __key__ > KEY('Task', 'abc')",
		AllowLiterals: true,
	}, nil, "ns", "")
	if err != nil {
		t.Fatalf("GQL key ordering: %v", err)
	}
	if len(res.Entities) != 0 {
		t.Fatalf("GQL key ordering results = %+v, want none", res.Entities)
	}
}
