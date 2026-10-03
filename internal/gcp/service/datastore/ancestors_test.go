package datastore

import (
	"context"
	"testing"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

func nameKey(kind, name string) Key {
	return Key{Kind: kind, Name: name, HasName: true}
}

func idKey(kind string, id int64) Key {
	return Key{Kind: kind, ID: id, HasID: true}
}

func ancestorElem(kind string, id int64) dsstore.PathElement {
	return dsstore.PathElement{Kind: kind, ID: id, HasID: true}
}

func intProp(n int64) dsstore.Entity {
	return dsstore.Entity{Properties: map[string]dsstore.Value{"n": {IntegerValue: ptrInt64(n)}}}
}

func upsert(t *testing.T, s *Service, project string, k Key, e dsstore.Entity) {
	t.Helper()
	if _, err := s.Commit(context.Background(), project, &CommitRequest{
		Mutations: []Mutation{{Op: MutationUpsert, Key: k, Entity: e}},
	}); err != nil {
		t.Fatalf("upsert %+v: %v", k, err)
	}
}

// TestAncestorKeyRoundTrip stores a descendant key (ancestor Parent/1 + Child/c)
// and confirms the full path round-trips and that a sibling under a different
// ancestor is a distinct entity.
func TestAncestorKeyRoundTrip(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()

	child1 := Key{Kind: "Child", Name: "c", HasName: true, Ancestors: []dsstore.PathElement{ancestorElem("Parent", 1)}}
	child2 := Key{Kind: "Child", Name: "c", HasName: true, Ancestors: []dsstore.PathElement{ancestorElem("Parent", 2)}}
	upsert(t, s, "p", child1, intProp(1))

	got, err := s.Lookup(ctx, "p", []Key{child1}, nil)
	if err != nil {
		t.Fatalf("lookup child1: %v", err)
	}
	if len(got.Found) != 1 {
		t.Fatalf("child1 found = %d, want 1", len(got.Found))
	}
	canon := got.Found[0].Entity.Key
	if dsstore.KeyKind(canon) != "Child" {
		t.Fatalf("canonical kind = %q", dsstore.KeyKind(canon))
	}
	back, ok := KeyFromCanonical(canon)
	if !ok || len(back.Ancestors) != 1 || back.Ancestors[0] != ancestorElem("Parent", 1) ||
		back.Kind != "Child" || back.Name != "c" {
		t.Fatalf("round-tripped key = %+v ok=%v", back, ok)
	}

	// A sibling with the same kind+name under a different parent does not exist.
	miss, err := s.Lookup(ctx, "p", []Key{child2}, nil)
	if err != nil {
		t.Fatalf("lookup child2: %v", err)
	}
	if len(miss.Found) != 0 || len(miss.Missing) != 1 {
		t.Fatalf("child2 found=%d missing=%d, want 0/1", len(miss.Found), len(miss.Missing))
	}
}

// TestPartitionIsolation confirms the namespace and database dimensions scope
// entities: the same kind+name in different partitions are distinct, and a
// query only returns its own partition.
func TestPartitionIsolation(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()

	def := nameKey("Task", "a")
	nsA := nameKey("Task", "a")
	nsA.Namespace = "tenant-a"
	dbD := nameKey("Task", "a")
	dbD.Database = "db-d"

	upsert(t, s, "p", def, intProp(1))
	upsert(t, s, "p", nsA, intProp(2))
	upsert(t, s, "p", dbD, intProp(3))

	for _, tc := range []struct {
		name string
		k    Key
		want int64
	}{
		{"default", def, 1},
		{"namespace", nsA, 2},
		{"database", dbD, 3},
	} {
		got, err := s.Lookup(ctx, "p", []Key{tc.k}, nil)
		if err != nil {
			t.Fatalf("%s lookup: %v", tc.name, err)
		}
		if len(got.Found) != 1 || got.Found[0].Entity.Properties["n"].IntegerValue == nil || *got.Found[0].Entity.Properties["n"].IntegerValue != tc.want {
			t.Fatalf("%s lookup = %+v, want n=%d", tc.name, got, tc.want)
		}
	}

	// A query scoped to the tenant namespace returns only that partition.
	res, err := s.RunQuery(ctx, "p", &Query{Kind: "Task", Namespace: "tenant-a"}, nil)
	if err != nil {
		t.Fatalf("namespaced query: %v", err)
	}
	if len(res.Entities) != 1 || *res.Entities[0].Entity.Properties["n"].IntegerValue != 2 {
		t.Fatalf("namespaced query = %+v, want only n=2", res.Entities)
	}

	// The default-partition query sees only the default entity.
	res, err = s.RunQuery(ctx, "p", &Query{Kind: "Task"}, nil)
	if err != nil {
		t.Fatalf("default query: %v", err)
	}
	if len(res.Entities) != 1 || *res.Entities[0].Entity.Properties["n"].IntegerValue != 1 {
		t.Fatalf("default query = %+v, want only n=1", res.Entities)
	}
}

// TestHASAncestorQuery exercises the special __key__ HAS_ANCESTOR filter used by
// ancestor-scoped queries.
func TestHASAncestorQuery(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()

	child1 := Key{Kind: "Child", Name: "c1", HasName: true, Ancestors: []dsstore.PathElement{ancestorElem("Parent", 1)}}
	child2 := Key{Kind: "Child", Name: "c2", HasName: true, Ancestors: []dsstore.PathElement{ancestorElem("Parent", 2)}}
	upsert(t, s, "p", child1, intProp(1))
	upsert(t, s, "p", child2, intProp(2))

	ancestor := idKey("Parent", 1)
	ck := canonicalKey(ancestor)
	q := &Query{Kind: "Child", Filter: &Filter{Property: &PropertyFilter{
		Property: keyPropertyName,
		Op:       PropertyHasAncestor,
		Value:    dsstore.Value{KeyValue: &ck},
	}}}

	res, err := s.RunQuery(ctx, "p", q, nil)
	if err != nil {
		t.Fatalf("HAS_ANCESTOR query: %v", err)
	}
	if len(res.Entities) != 1 || res.Entities[0].Entity.Properties["n"].IntegerValue == nil || *res.Entities[0].Entity.Properties["n"].IntegerValue != 1 {
		t.Fatalf("HAS_ANCESTOR results = %+v, want only child1", res.Entities)
	}

	// The ancestor itself is not its own descendant.
	q = &Query{Kind: "Parent", Filter: &Filter{Property: &PropertyFilter{
		Property: keyPropertyName,
		Op:       PropertyHasAncestor,
		Value:    dsstore.Value{KeyValue: &ck},
	}}}
	res, err = s.RunQuery(ctx, "p", q, nil)
	if err != nil {
		t.Fatalf("HAS_ANCESTOR self query: %v", err)
	}
	if len(res.Entities) != 0 {
		t.Fatalf("HAS_ANCESTOR on the ancestor itself = %+v, want none", res.Entities)
	}

	// HAS_ANCESTOR on a non-__key__ property is rejected.
	bad := &Query{Kind: "Child", Filter: &Filter{Property: &PropertyFilter{
		Property: "n",
		Op:       PropertyHasAncestor,
		Value:    dsstore.Value{KeyValue: &ck},
	}}}
	if _, err := s.RunQuery(ctx, "p", bad, nil); err == nil {
		t.Fatal("HAS_ANCESTOR on a non-__key__ property should be rejected")
	}
}

// TestHASAncestorCrossPartitionKeyLiteral confirms an ancestor key whose own
// partition is empty still matches within the query's partition (the GQL
// KEY(...) literal / SDK ancestor-key case, where the ancestor carries no
// namespace of its own).
func TestHASAncestorCrossPartitionKeyLiteral(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()

	child := Key{Kind: "Child", Name: "c", HasName: true, Ancestors: []dsstore.PathElement{ancestorElem("Parent", 1)}, Namespace: "ns"}
	upsert(t, s, "p", child, intProp(1))

	ancestor := idKey("Parent", 1) // empty partition, as a GQL KEY(...) produces
	ck := canonicalKey(ancestor)
	q := &Query{Kind: "Child", Namespace: "ns", Filter: &Filter{Property: &PropertyFilter{
		Property: keyPropertyName,
		Op:       PropertyHasAncestor,
		Value:    dsstore.Value{KeyValue: &ck},
	}}}
	res, err := s.RunQuery(ctx, "p", q, nil)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Entities) != 1 {
		t.Fatalf("cross-partition ancestor literal results = %+v, want 1", res.Entities)
	}
}

// TestAllocateIDsPreservesAncestorsAndPartition confirms auto-ID allocation keeps
// the key's ancestor path and partition.
func TestAllocateIDsPreservesAncestorsAndPartition(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()

	incomplete := Key{Kind: "Child", Ancestors: []dsstore.PathElement{ancestorElem("Parent", 7)}, Namespace: "ns", Database: "db"}
	out, err := s.AllocateIDs(ctx, "p", []Key{incomplete})
	if err != nil {
		t.Fatalf("AllocateIDs: %v", err)
	}
	if len(out) != 1 || !out[0].Complete() || out[0].ID <= 0 {
		t.Fatalf("allocated = %+v", out)
	}
	if out[0].Namespace != "ns" || out[0].Database != "db" || len(out[0].Ancestors) != 1 || out[0].Ancestors[0] != ancestorElem("Parent", 7) {
		t.Fatalf("allocated key lost ancestors/partition: %+v", out[0])
	}

	// A mutation with an incomplete ancestor key allocates and echoes the ID.
	child := Key{Kind: "Child", Ancestors: []dsstore.PathElement{ancestorElem("Parent", 7)}}
	resp, err := s.Commit(ctx, "p", &CommitRequest{Mutations: []Mutation{{Op: MutationInsert, Key: child, Entity: dsstore.Entity{Properties: map[string]dsstore.Value{}}}}})
	if err != nil {
		t.Fatalf("insert incomplete child: %v", err)
	}
	if resp.Results[0].Key == nil || !resp.Results[0].Key.Complete() || len(resp.Results[0].Key.Ancestors) != 1 {
		t.Fatalf("insert result key = %+v", resp.Results[0].Key)
	}
}

// TestGQLHasAncestorEndToEnd confirms the GQL "HAS ANCESTOR" form reaches the
// same filter engine.
func TestGQLHasAncestorEndToEnd(t *testing.T) {
	s := NewService(dsstore.NewMemoryStore(), "p")
	ctx := context.Background()

	child1 := Key{Kind: "Child", Name: "c1", HasName: true, Ancestors: []dsstore.PathElement{ancestorElem("Parent", 1)}}
	child2 := Key{Kind: "Child", Name: "c2", HasName: true, Ancestors: []dsstore.PathElement{ancestorElem("Parent", 2)}}
	upsert(t, s, "p", child1, intProp(1))
	upsert(t, s, "p", child2, intProp(2))

	res, err := s.RunQueryGQL(ctx, "p", GQLQuery{
		QueryString:   "SELECT * FROM Child WHERE __key__ HAS ANCESTOR KEY('Parent', 1)",
		AllowLiterals: true,
	}, nil, "", "")
	if err != nil {
		t.Fatalf("GQL HAS ANCESTOR: %v", err)
	}
	if len(res.Entities) != 1 || *res.Entities[0].Entity.Properties["n"].IntegerValue != 1 {
		t.Fatalf("GQL HAS ANCESTOR results = %+v, want only child1", res.Entities)
	}
}
