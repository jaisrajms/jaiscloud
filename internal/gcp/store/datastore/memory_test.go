package datastore

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func str(v string) Value { s := v; return Value{StringValue: &s} }
func bl(v bool) Value    { b := v; return Value{BooleanValue: &b} }
func num(v int64) Value  { n := v; return Value{IntegerValue: &n} }

func testEntity(kind, name string, props map[string]Value) Entity {
	return Entity{Kind: kind, Key: KeyOfName(kind, name), Properties: props}
}

func TestMemoryStoreCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	e := testEntity("Task", "a", map[string]Value{"Description": str("hi"), "Done": bl(false)})

	if err := s.Insert(ctx, "p1", e); err != nil {
		t.Fatal(err)
	}
	// duplicate insert fails.
	if err := s.Insert(ctx, "p1", e); !errors.Is(err, ErrEntityExists) {
		t.Fatalf("duplicate insert = %v, want ErrEntityExists", err)
	}

	got, err := s.Get(ctx, "p1", e.Key)
	if err != nil {
		t.Fatal(err)
	}
	if got.Properties["Description"].StringValue == nil || *got.Properties["Description"].StringValue != "hi" {
		t.Fatalf("get = %+v", got)
	}

	// upsert overwrites.
	e.Properties["Done"] = bl(true)
	if err := s.Upsert(ctx, "p1", e); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get(ctx, "p1", e.Key)
	if got.Properties["Done"].BooleanValue == nil || !*got.Properties["Done"].BooleanValue {
		t.Fatalf("after upsert = %+v", got)
	}

	// update on missing fails.
	missing := testEntity("Task", "missing", map[string]Value{})
	if err := s.Update(ctx, "p1", missing); !errors.Is(err, ErrEntityNotFound) {
		t.Fatalf("update missing = %v, want ErrEntityNotFound", err)
	}

	// list by kind.
	e2 := testEntity("Task", "b", map[string]Value{"Done": bl(false)})
	if err := s.Upsert(ctx, "p1", e2); err != nil {
		t.Fatal(err)
	}
	other := testEntity("Other", "c", map[string]Value{})
	if err := s.Upsert(ctx, "p1", other); err != nil {
		t.Fatal(err)
	}
	list, err := s.ListKind(ctx, "p1", "Task")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].Key != e.Key || list[1].Key != e2.Key {
		t.Fatalf("list = %+v", list)
	}

	// delete + get missing.
	if err := s.Delete(ctx, "p1", e.Key); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "p1", e.Key); !errors.Is(err, ErrEntityNotFound) {
		t.Fatalf("get after delete = %v, want ErrEntityNotFound", err)
	}
}

func TestMemoryStoreApplyMutation(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	e := testEntity("Task", "a", map[string]Value{"n": num(1)})

	// Insert: no precondition, version stamped to 1.
	inserted, err := s.ApplyMutation(ctx, "p1", MutationInsert, e, nil)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if inserted.Version != 1 {
		t.Fatalf("inserted version = %d, want 1", inserted.Version)
	}

	// Insert again: ErrEntityExists regardless of precondition.
	if _, err := s.ApplyMutation(ctx, "p1", MutationInsert, e, nil); !errors.Is(err, ErrEntityExists) {
		t.Fatalf("duplicate insert = %v, want ErrEntityExists", err)
	}

	// Update with a stale base_version: ErrConflict, not applied. A fresh
	// Entity (its own Properties map, not aliased with the stored one) —
	// mutating e.Properties in place would corrupt the already-stored
	// entity's data via the shared map reference, since Go doesn't copy maps
	// on struct assignment.
	stale := int64(0)
	staleUpdate := testEntity("Task", "a", map[string]Value{"n": num(999)})
	got, err := s.ApplyMutation(ctx, "p1", MutationUpdate, staleUpdate, &Precondition{BaseVersion: &stale})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("stale update = %v, want ErrConflict", err)
	}
	if got.Version != 1 {
		t.Fatalf("conflict result version = %d, want 1 (the current, unchanged version)", got.Version)
	}
	current, _ := s.Get(ctx, "p1", e.Key)
	n := current.Properties["n"].IntegerValue
	if n == nil || *n != 1 {
		t.Fatalf("entity must be unchanged after a rejected conditional update, got %+v", current.Properties["n"])
	}

	// Update with the correct base_version: applies, version advances.
	correct := int64(1)
	goodUpdate := testEntity("Task", "a", map[string]Value{"n": num(2)})
	updated, err := s.ApplyMutation(ctx, "p1", MutationUpdate, goodUpdate, &Precondition{BaseVersion: &correct})
	if err != nil {
		t.Fatalf("correct update: %v", err)
	}
	if updated.Version != 2 {
		t.Fatalf("updated version = %d, want 2", updated.Version)
	}

	// Update on a missing entity: ErrEntityNotFound.
	missing := testEntity("Task", "missing", nil)
	if _, err := s.ApplyMutation(ctx, "p1", MutationUpdate, missing, nil); !errors.Is(err, ErrEntityNotFound) {
		t.Fatalf("update missing = %v, want ErrEntityNotFound", err)
	}
}

func TestMemoryStoreDeleteConflictChecked(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	e := testEntity("Task", "a", map[string]Value{"n": num(1)})
	if _, err := s.ApplyMutation(ctx, "p1", MutationInsert, e, nil); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Stale precondition: ErrConflict, entity not deleted.
	stale := int64(0)
	if err := s.DeleteConflictChecked(ctx, "p1", e.Key, &Precondition{BaseVersion: &stale}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale delete = %v, want ErrConflict", err)
	}
	if _, err := s.Get(ctx, "p1", e.Key); err != nil {
		t.Fatalf("entity must still exist after a rejected conditional delete, got %v", err)
	}

	// Correct precondition: deletes.
	correct := int64(1)
	if err := s.DeleteConflictChecked(ctx, "p1", e.Key, &Precondition{BaseVersion: &correct}); err != nil {
		t.Fatalf("correct delete: %v", err)
	}
	if _, err := s.Get(ctx, "p1", e.Key); !errors.Is(err, ErrEntityNotFound) {
		t.Fatalf("get after delete = %v, want ErrEntityNotFound", err)
	}
}

func TestMemoryStoreAllocateIDs(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	ids, err := s.AllocateIDs(ctx, "p1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 3 || ids[0] != 1 || ids[1] != 2 || ids[2] != 3 {
		t.Fatalf("first batch = %v", ids)
	}
	ids, _ = s.AllocateIDs(ctx, "p1", 2)
	if ids[0] != 4 || ids[1] != 5 {
		t.Fatalf("second batch = %v", ids)
	}
	// per-project counters are independent.
	ids, _ = s.AllocateIDs(ctx, "p2", 1)
	if ids[0] != 1 {
		t.Fatalf("p2 first id = %v", ids)
	}
}

func TestMemoryStoreSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.Upsert(ctx, "p1", testEntity("Task", "a", map[string]Value{"Done": bl(false)}))
	_ = s.Upsert(ctx, "p2", testEntity("Task", "b", map[string]Value{"Done": bl(true)}))
	_, _ = s.AllocateIDs(ctx, "p1", 5)

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatal(err)
	}

	s2 := NewMemoryStore()
	if err := s2.Restore(ctx, &buf); err != nil {
		t.Fatal(err)
	}
	list, _ := s2.ListKind(ctx, "p1", "Task")
	if len(list) != 1 || list[0].Key != KeyOfName("Task", "a") {
		t.Fatalf("restored p1 = %+v", list)
	}
	list, _ = s2.ListKind(ctx, "p2", "Task")
	if len(list) != 1 {
		t.Fatalf("restored p2 = %+v", list)
	}
	// allocator counter must be restored so IDs stay monotonic.
	ids, _ := s2.AllocateIDs(ctx, "p1", 1)
	if ids[0] != 6 {
		t.Fatalf("restored allocator = %v, want 6", ids)
	}
}

func TestMemoryStoreReset(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.Upsert(ctx, "p1", testEntity("Task", "a", nil))
	s.Reset(ctx)
	if list, _ := s.ListKind(ctx, "p1", "Task"); len(list) != 0 {
		t.Fatalf("after reset = %+v", list)
	}
	if ids, _ := s.AllocateIDs(ctx, "p1", 1); ids[0] != 1 {
		t.Fatalf("allocator after reset = %v", ids)
	}
}

func TestMemoryStoreAdvanceIDs(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	// Advancing past an explicit ID means the next allocation skips it.
	if err := s.AdvanceIDs(ctx, "p1", 100); err != nil {
		t.Fatal(err)
	}
	ids, _ := s.AllocateIDs(ctx, "p1", 1)
	if ids[0] != 101 {
		t.Fatalf("after advance(100), allocated = %v, want 101", ids)
	}

	// Advancing below the current cursor is a no-op.
	if err := s.AdvanceIDs(ctx, "p1", 50); err != nil {
		t.Fatal(err)
	}
	ids, _ = s.AllocateIDs(ctx, "p1", 1)
	if ids[0] != 102 {
		t.Fatalf("after no-op advance, allocated = %v, want 102", ids)
	}

	// Per-project counters are independent.
	ids, _ = s.AllocateIDs(ctx, "p2", 1)
	if ids[0] != 1 {
		t.Fatalf("p2 first id = %v, want 1", ids)
	}
}

func TestKeyHelpers(t *testing.T) {
	if got := KeyOfID("Task", 42); got != "v1|0:0:4:Taski2:42" {
		t.Fatalf("KeyOfID = %q", got)
	}
	if got := KeyOfName("Task", "foo"); got != "v1|0:0:4:Taskn3:foo" {
		t.Fatalf("KeyOfName = %q", got)
	}
	db, ns, path, ok := ParseKey(KeyOfName("Task", "foo"))
	if !ok || db != "" || ns != "" || len(path) != 1 {
		t.Fatalf("ParseKey = %q %q %+v %v", db, ns, path, ok)
	}
	if path[0].Kind != "Task" || !path[0].HasName || path[0].HasID || path[0].Name != "foo" {
		t.Fatalf("parsed element = %+v", path[0])
	}
	if got := KeyKind(KeyOfID("Task", 42)); got != "Task" {
		t.Fatalf("KeyKind = %q", got)
	}
}

// TestKeyOfPath verifies the full path + partition round-trip: ancestors,
// namespace, database, and a numeric ID, all of which must survive ParseKey.
func TestKeyOfPath(t *testing.T) {
	path := []PathElement{
		{Kind: "Parent", ID: 1, HasID: true},
		{Kind: "Child", Name: "x", HasName: true},
	}
	key := KeyOfPath("db1", "ns1", path)
	db, ns, got, ok := ParseKey(key)
	if !ok || db != "db1" || ns != "ns1" {
		t.Fatalf("ParseKey(%q) partition = %q %q %v", key, db, ns, ok)
	}
	if len(got) != 2 || got[0] != path[0] || got[1] != path[1] {
		t.Fatalf("ParseKey(%q) path = %+v", key, got)
	}
	if KeyKind(key) != "Child" {
		t.Fatalf("KeyKind = %q", KeyKind(key))
	}
}

// TestKeyHelpers_KindContainingSlash verifies the bug the length-prefixed
// encoding fixes: a kind containing "/" (or a name containing "/" and ":")
// must round-trip without corrupting the parse. Length-prefixing every field
// makes the boundaries explicit regardless of the contents.
func TestKeyHelpers_KindContainingSlash(t *testing.T) {
	key := KeyOfID("my/kind", 7)
	_, _, path, ok := ParseKey(key)
	if !ok || len(path) != 1 || path[0].Kind != "my/kind" || !path[0].HasID || path[0].ID != 7 {
		t.Fatalf("ParseKey(%q) = %+v %v, want kind \"my/kind\" id 7", key, path, ok)
	}

	// A name containing both "/" and ":" must also still round-trip.
	key = KeyOfName("k/i:nd", "a/b:c")
	_, _, path, ok = ParseKey(key)
	if !ok || len(path) != 1 || path[0].Kind != "k/i:nd" || !path[0].HasName || path[0].Name != "a/b:c" {
		t.Fatalf("ParseKey(%q) = %+v %v", key, path, ok)
	}
}

func TestParseKey_Malformed(t *testing.T) {
	for _, bad := range []string{
		"",
		"no-length-prefix",
		"4:Task/id:42",      // pre-ancestors encoding, no version prefix
		"v1|abc",            // non-numeric length
		"v1|0:0:",           // empty path
		"v1|0:0:0:",         // empty kind
		"v1|0:0:4:Taskx1:1", // unknown tag
		"v1|0:0:4:Taski",    // missing id value
		"v1|0:0:4:Taski9:1", // value length longer than remaining string
	} {
		if _, _, _, ok := ParseKey(bad); ok {
			t.Errorf("ParseKey(%q): expected ok=false", bad)
		}
	}
}
