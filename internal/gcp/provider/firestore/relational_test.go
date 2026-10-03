package firestore

import (
	"testing"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

// TestProjectDocumentsDeepCopiesNestedValues asserts a projection does not
// alias the source document's nested maps: projecting both a parent field and
// one of its children used to reuse the source's map/values, so writing the
// projected child mutated the stored document.
func TestProjectDocumentsDeepCopiesNestedValues(t *testing.T) {
	src := firestorestore.Document{Name: "projects/p/databases/(default)/documents/cities/SF",
		Fields: map[string]*firestorestore.Value{
			"meta": firestorestore.MapVal(map[string]*firestorestore.Value{"n": firestorestore.IntVal(5)}),
		},
	}
	proj := &projection{Fields: []fieldReference{
		{FieldPath: "meta"},
		{FieldPath: "meta.n"},
	}}
	out := ProjectDocuments([]firestorestore.Document{src}, proj)
	if len(out) != 1 {
		t.Fatalf("expected 1 projected document, got %d", len(out))
	}
	// Mutate the projection result; the source must be untouched.
	out[0].Fields["meta"].MapValue.Fields["n"] = firestorestore.IntVal(99)
	if v, _ := src.Fields["meta"].MapValue.Fields["n"].AsInt64(); v != 5 {
		t.Fatalf("projection aliased the source document: source meta.n = %d, want 5", v)
	}
}

// TestFilterDocumentsValidatesLimits asserts the shared filter validation
// (operator/array limits) runs for pipeline filters.
func TestFilterDocumentsValidatesLimits(t *testing.T) {
	empty := &filter{FieldFilter: &fieldFilter{
		Field: fieldReference{FieldPath: "a"},
		Op:    "IN",
		Value: firestorestore.ArrayVal(), // IN requires a non-empty array
	}}
	if _, err := FilterDocuments(nil, empty); err == nil {
		t.Fatal("expected IN with an empty array to be rejected")
	}
}

// TestDistinctDocumentsFieldAndWhole locks the two distinctness modes: with
// fields, documents are deduplicated by those values; with no fields, the whole
// document (including its name) determines distinctness.
func TestDistinctDocumentsFieldAndWhole(t *testing.T) {
	docs := []firestorestore.Document{
		{Name: "a", Fields: map[string]*firestorestore.Value{"x": firestorestore.IntVal(1)}},
		{Name: "b", Fields: map[string]*firestorestore.Value{"x": firestorestore.IntVal(1)}},
		{Name: "c", Fields: map[string]*firestorestore.Value{"x": firestorestore.IntVal(2)}},
	}

	byField := DistinctDocuments(docs, &projection{Fields: []fieldReference{{FieldPath: "x"}}})
	if len(byField) != 2 {
		t.Fatalf("distinct by field: got %d results, want 2", len(byField))
	}
	if byField[0].Name != "a" || byField[1].Name != "c" {
		t.Fatalf("distinct by field must preserve first occurrence: got %s, %s", byField[0].Name, byField[1].Name)
	}

	whole := DistinctDocuments(docs, nil)
	if len(whole) != 3 {
		t.Fatalf("whole-document distinct: got %d results, want 3 (names differ)", len(whole))
	}
}
