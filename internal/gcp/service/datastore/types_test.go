package datastore

import (
	"testing"

	dsstore "jaiscloud/internal/gcp/store/datastore"
	"jaiscloud/internal/model"
)

// TestKeyComplete covers the final-element completeness test: a numeric ID or a
// name makes a key complete; neither leaves it incomplete.
func TestKeyComplete(t *testing.T) {
	if !(Key{Kind: "K", ID: 1, HasID: true}).Complete() {
		t.Fatal("a key with an ID must be complete")
	}
	if !(Key{Kind: "K", Name: "n", HasName: true}).Complete() {
		t.Fatal("a key with a name must be complete")
	}
	if (Key{Kind: "K"}).Complete() {
		t.Fatal("a key with neither ID nor name must be incomplete")
	}
}

// TestKeyPathElements verifies PathElements returns ancestors (root first)
// followed by the final element.
func TestKeyPathElements(t *testing.T) {
	root := Key{Kind: "Parent", ID: 1, HasID: true}
	if got := root.PathElements(); len(got) != 1 || got[0].Kind != "Parent" || got[0].ID != 1 || !got[0].HasID {
		t.Fatalf("root PathElements = %+v", got)
	}

	anc := dsstore.PathElement{Kind: "Parent", ID: 2, HasID: true}
	child := Key{Kind: "Child", Name: "c", HasName: true, Ancestors: []dsstore.PathElement{anc}}
	got := child.PathElements()
	if len(got) != 2 {
		t.Fatalf("child PathElements = %+v, want 2 elements", got)
	}
	if got[0].Kind != "Parent" || got[0].ID != 2 || !got[0].HasID {
		t.Fatalf("ancestor element = %+v", got[0])
	}
	if got[1].Kind != "Child" || got[1].Name != "c" || !got[1].HasName {
		t.Fatalf("final element = %+v", got[1])
	}
}

// TestValidateKeyPath covers the structural preconditions a decoded key path
// must satisfy: non-empty kinds, complete ancestors, and an incomplete-but-
// allowed final element.
func TestValidateKeyPath(t *testing.T) {
	ok := []dsstore.PathElement{{Kind: "Parent", ID: 1, HasID: true}, {Kind: "Child", Name: "c", HasName: true}}
	if err := ValidateKeyPath(ok); err != nil {
		t.Fatalf("valid path rejected: %v", err)
	}
	// An empty path means "no key".
	if err := ValidateKeyPath(nil); err != nil {
		t.Fatalf("empty path rejected: %v", err)
	}
	// The final element may be incomplete (awaiting an allocated ID).
	incompleteFinal := []dsstore.PathElement{{Kind: "Parent", ID: 1, HasID: true}, {Kind: "Child"}}
	if err := ValidateKeyPath(incompleteFinal); err != nil {
		t.Fatalf("incomplete final element rejected: %v", err)
	}
	if err := ValidateKeyPath([]dsstore.PathElement{{Name: "x", HasName: true}}); err == nil {
		t.Fatal("an element with an empty kind must be rejected")
	}
	if err := ValidateKeyPath([]dsstore.PathElement{{Kind: "Parent"}, {Kind: "Child", Name: "c", HasName: true}}); err == nil {
		t.Fatal("an incomplete ancestor must be rejected")
	}
}

// TestQueryResultType covers the full/projection/keys-only classification,
// including the "__key__"-only projection the clients use for keys-only.
func TestQueryResultType(t *testing.T) {
	var nilQuery *Query
	if got := nilQuery.ResultType(); got != ResultFull {
		t.Fatalf("nil query ResultType = %v, want ResultFull", got)
	}
	if got := (&Query{}).ResultType(); got != ResultFull {
		t.Fatalf("no projection = %v, want ResultFull", got)
	}
	if got := (&Query{Projection: &Projection{Properties: []string{"a"}}}).ResultType(); got != ResultProjection {
		t.Fatalf("real property projection = %v, want ResultProjection", got)
	}
	if got := (&Query{Projection: &Projection{Properties: []string{"__key__"}}}).ResultType(); got != ResultKeysOnly {
		t.Fatalf("__key__-only projection = %v, want ResultKeysOnly", got)
	}
	// A non-nil projection with no properties is also keys-only.
	if got := (&Query{Projection: &Projection{}}).ResultType(); got != ResultKeysOnly {
		t.Fatalf("empty projection = %v, want ResultKeysOnly", got)
	}
}

// TestProjectEntity covers property projection: a subset is kept, an empty
// named list strips every property (keys-only), and "__key__" is a no-op.
func TestProjectEntity(t *testing.T) {
	e := dsstore.Entity{
		Kind: "K", Key: "k1",
		Properties: map[string]dsstore.Value{"a": valInt(1), "b": valStr("x")},
	}

	sub, err := projectEntity(e, []string{"a"})
	if err != nil {
		t.Fatalf("projectEntity(a): %v", err)
	}
	if len(sub.Properties) != 1 || sub.Properties["a"].IntegerValue == nil {
		t.Fatalf("subset projection = %+v", sub.Properties)
	}
	if sub.Key != "k1" || sub.Kind != "K" {
		t.Fatal("projection must preserve the key and kind")
	}

	keysOnly, err := projectEntity(e, nil)
	if err != nil {
		t.Fatalf("projectEntity(nil): %v", err)
	}
	if len(keysOnly.Properties) != 0 {
		t.Fatalf("keys-only projection kept properties: %+v", keysOnly.Properties)
	}
	if keysOnly.Key != "k1" {
		t.Fatal("keys-only projection must keep the key")
	}

	withKey, err := projectEntity(e, []string{"__key__", "b"})
	if err != nil {
		t.Fatalf("projectEntity(__key__, b): %v", err)
	}
	if len(withKey.Properties) != 1 || withKey.Properties["b"].StringValue == nil {
		t.Fatalf("__key__ must be a no-op: %+v", withKey.Properties)
	}

	// A requested but absent property is simply skipped.
	missing, err := projectEntity(e, []string{"a", "nope"})
	if err != nil {
		t.Fatalf("projectEntity(a, nope): %v", err)
	}
	if len(missing.Properties) != 1 || missing.Properties["a"].IntegerValue == nil {
		t.Fatalf("missing property projection = %+v", missing.Properties)
	}
}

// TestProjectEntityNested pins the dotted-path projection contract: a nested
// leaf is returned inside its enclosing embedded entities, the parents keep
// only the projected structure, multiple leaves under one parent merge, and a
// repeated or scalar parent is rejected as InvalidArgument (DGT6).
func TestProjectEntityNested(t *testing.T) {
	entityVal := func(props map[string]dsstore.Value) dsstore.Value {
		return dsstore.Value{EntityValue: &dsstore.Entity{Properties: props}}
	}
	e := dsstore.Entity{
		Kind: "K", Key: "k1",
		Properties: map[string]dsstore.Value{
			"a": entityVal(map[string]dsstore.Value{
				"b": entityVal(map[string]dsstore.Value{
					"c": valInt(7),
					"d": valStr("drop"),
				}),
				"e": valStr("drop"),
			}),
			"plain": valInt(1),
			"arr":   dsstore.Value{ArrayValue: &dsstore.ArrayValue{Values: []dsstore.Value{entityVal(map[string]dsstore.Value{"b": valInt(2)})}}},
		},
	}

	// a.b keeps the leaf and drops the sibling e.
	got, err := projectEntity(e, []string{"a.b"})
	if err != nil {
		t.Fatalf("projectEntity(a.b): %v", err)
	}
	a, ok := got.Properties["a"]
	if !ok || a.EntityValue == nil {
		t.Fatalf("a.b projection = %+v, want nested entity a", got.Properties)
	}
	if _, ok := a.EntityValue.Properties["e"]; ok {
		t.Fatal("a.b must drop the un-projected sibling a.e")
	}
	b, ok := a.EntityValue.Properties["b"]
	if !ok || b.EntityValue == nil || b.EntityValue.Properties["c"].IntegerValue == nil || *b.EntityValue.Properties["c"].IntegerValue != 7 {
		t.Fatalf("a.b projection = %+v, want a.b.c = 7", a.EntityValue.Properties)
	}
	if d, ok := b.EntityValue.Properties["d"]; !ok || d.StringValue == nil || *d.StringValue != "drop" {
		t.Fatal("a.b must keep the whole embedded entity b (including b.d)")
	}

	// a.b.c resolves the deeper leaf.
	deep, err := projectEntity(e, []string{"a.b.c"})
	if err != nil {
		t.Fatalf("projectEntity(a.b.c): %v", err)
	}
	if c := deep.Properties["a"].EntityValue.Properties["b"].EntityValue.Properties["c"]; c.IntegerValue == nil || *c.IntegerValue != 7 {
		t.Fatalf("a.b.c projection = %+v", deep.Properties)
	}

	// Two leaves under one parent merge into one structural parent.
	merged, err := projectEntity(e, []string{"a.b.c", "a.e"})
	if err != nil {
		t.Fatalf("projectEntity(merged): %v", err)
	}
	ap := merged.Properties["a"].EntityValue.Properties
	if ap["b"].EntityValue == nil || ap["b"].EntityValue.Properties["c"].IntegerValue == nil {
		t.Fatalf("merged projection lost a.b.c: %+v", ap)
	}
	if ap["e"].StringValue == nil || *ap["e"].StringValue != "drop" {
		t.Fatalf("merged projection lost a.e: %+v", ap)
	}

	// A missing leaf keeps the enclosing parent as an empty structure.
	absent, err := projectEntity(e, []string{"a.zzz"})
	if err != nil {
		t.Fatalf("projectEntity(a.zzz): %v", err)
	}
	az, ok := absent.Properties["a"]
	if !ok || az.EntityValue == nil || len(az.EntityValue.Properties) != 0 {
		t.Fatalf("a.zzz projection = %+v, want empty parent a", absent.Properties)
	}

	// Traversing a scalar or a repeated value is an invalid alignment.
	for _, name := range []string{"plain.x", "arr.b"} {
		if _, err := projectEntity(e, []string{name}); err == nil {
			t.Fatalf("projectEntity(%s) succeeded, want InvalidArgument", name)
		} else if pe, ok := err.(*model.ProviderError); !ok || pe.Code != "InvalidArgument" {
			t.Fatalf("projectEntity(%s) error = %v, want InvalidArgument", name, err)
		}
	}

	// A path with an empty segment is rejected up front.
	for _, name := range []string{"", ".a", "a.", "a..b"} {
		if err := validateProjection(&Projection{Properties: []string{name}}); err == nil {
			t.Fatalf("validateProjection(%q) succeeded, want InvalidArgument", name)
		}
	}
}
