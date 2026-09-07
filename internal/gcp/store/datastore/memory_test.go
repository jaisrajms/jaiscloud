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
	if KeyOfID("Task", 42) != "4:Task/id:42" {
		t.Fatalf("KeyOfID = %q", KeyOfID("Task", 42))
	}
	if KeyOfName("Task", "foo") != "4:Task/name:foo" {
		t.Fatalf("KeyOfName = %q", KeyOfName("Task", "foo"))
	}
	kind, idOrName, ok := SplitKey("4:Task/name:foo")
	if !ok || kind != "Task" || idOrName != "name:foo" {
		t.Fatalf("SplitKey = %q %q %v", kind, idOrName, ok)
	}
	id, name, isID := ParseIDOrName("id:42")
	if !isID || id != 42 || name != "" {
		t.Fatalf("ParseIDOrName id = %d %q %v", id, name, isID)
	}
	id, name, isID = ParseIDOrName("name:foo")
	if isID || id != 0 || name != "foo" {
		t.Fatalf("ParseIDOrName name = %d %q %v", id, name, isID)
	}
}

// TestKeyHelpers_KindContainingSlash verifies the bug the length-prefixed
// encoding fixes: a kind containing "/" used to corrupt SplitKey's parse
// (it split on the first "/", which could land inside the kind instead of
// at the kind/id-or-name boundary). Length-prefixing the kind makes the
// boundary explicit regardless of what characters the kind contains.
func TestKeyHelpers_KindContainingSlash(t *testing.T) {
	key := KeyOfID("my/kind", 7)
	kind, idOrName, ok := SplitKey(key)
	if !ok || kind != "my/kind" || idOrName != "id:7" {
		t.Fatalf("SplitKey(%q) = %q %q %v, want \"my/kind\" \"id:7\" true", key, kind, idOrName, ok)
	}
	id, _, isID := ParseIDOrName(idOrName)
	if !isID || id != 7 {
		t.Fatalf("ParseIDOrName(%q) = %d _ %v", idOrName, id, isID)
	}

	// A name containing both "/" and ":" must also still round-trip.
	key = KeyOfName("k/i:nd", "a/b:c")
	kind, idOrName, ok = SplitKey(key)
	if !ok || kind != "k/i:nd" || idOrName != "name:a/b:c" {
		t.Fatalf("SplitKey(%q) = %q %q %v", key, kind, idOrName, ok)
	}
	_, name, isID := ParseIDOrName(idOrName)
	if isID || name != "a/b:c" {
		t.Fatalf("ParseIDOrName(%q) = _ %q %v", idOrName, name, isID)
	}
}

func TestSplitKey_Malformed(t *testing.T) {
	for _, bad := range []string{
		"",
		"no-length-prefix",
		"abc:Task/id:1", // non-numeric length
		"-1:Task/id:1",  // negative length
		"100:Task/id:1", // length longer than remaining string
		"4:Task|id:1",   // missing "/" at the length boundary
		"0:/id:1",       // empty kind
		"4:Task/",       // empty id-or-name
	} {
		if _, _, ok := SplitKey(bad); ok {
			t.Errorf("SplitKey(%q): expected ok=false", bad)
		}
	}
}
