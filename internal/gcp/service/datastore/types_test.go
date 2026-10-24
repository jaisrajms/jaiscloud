package datastore

import (
	"testing"

	dsstore "jaiscloud/internal/gcp/store/datastore"
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

	sub := projectEntity(e, []string{"a"})
	if len(sub.Properties) != 1 || sub.Properties["a"].IntegerValue == nil {
		t.Fatalf("subset projection = %+v", sub.Properties)
	}
	if sub.Key != "k1" || sub.Kind != "K" {
		t.Fatal("projection must preserve the key and kind")
	}

	keysOnly := projectEntity(e, nil)
	if len(keysOnly.Properties) != 0 {
		t.Fatalf("keys-only projection kept properties: %+v", keysOnly.Properties)
	}
	if keysOnly.Key != "k1" {
		t.Fatal("keys-only projection must keep the key")
	}

	withKey := projectEntity(e, []string{"__key__", "b"})
	if len(withKey.Properties) != 1 || withKey.Properties["b"].StringValue == nil {
		t.Fatalf("__key__ must be a no-op: %+v", withKey.Properties)
	}

	// A requested but absent property is simply skipped.
	missing := projectEntity(e, []string{"a", "nope"})
	if len(missing.Properties) != 1 || missing.Properties["a"].IntegerValue == nil {
		t.Fatalf("missing property projection = %+v", missing.Properties)
	}
}
