package firestore

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// newUniqueService returns a service with an Enterprise edition database named
// dbID (unique indexes are an Enterprise index option) and a document store.
func newUniqueService(t *testing.T, dbID string) *Service {
	t.Helper()
	s := newService(firestorestore.NewMemoryStore(), store.NewMemoryResourceStore())
	if _, _, err := s.CreateDatabaseDef(context.Background(), "proj", dbID, databaseDef{
		LocationID:      "us-central1",
		DatabaseEdition: databaseEditionEnterprise,
	}); err != nil {
		t.Fatalf("CreateDatabaseDef(%s): %v", dbID, err)
	}
	return s
}

// createUniqueIndex registers a unique index on cg. A zero density selects the
// Enterprise default (DENSE).
func createUniqueIndex(t *testing.T, s *Service, dbID, cg string, density string, fields ...indexField) indexDef {
	t.Helper()
	idx, _, err := s.CreateIndexDef(context.Background(), "proj", dbID, cg, indexDef{
		QueryScope: "COLLECTION_GROUP",
		Density:    density,
		Unique:     true,
		Fields:     fields,
	})
	if err != nil {
		t.Fatalf("CreateIndexDef(%s/%s): %v", dbID, cg, err)
	}
	return idx
}

func asc(path string) indexField { return indexField{FieldPath: path, Order: "ASCENDING"} }

// createDoc creates proj/<db>/documents/<path> with fields, returning the error.
func createDoc(s *Service, dbID, path string, fields map[string]*firestorestore.Value) error {
	_, err := s.CreateDocument(context.Background(), "proj", dbID, parentOf(path), docIDOf(path), fields)
	return err
}

func parentOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return ""
}

func docIDOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[i+1:]
		}
	}
	return path
}

func strFields(kv ...string) map[string]*firestorestore.Value {
	out := make(map[string]*firestorestore.Value, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i]] = firestorestore.StringVal(kv[i+1])
	}
	return out
}

// wantAlreadyExists asserts the value is the ProviderError real Firestore
// returns for a duplicate unique-index entry (HTTP 409 / ALREADY_EXISTS).
func wantAlreadyExists(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected ALREADY_EXISTS for a duplicate unique index entry, got nil")
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if pe.HTTPStatus != 409 || pe.Status != "ALREADY_EXISTS" {
		t.Fatalf("expected AlreadyExists@409, got HTTP=%d status=%q (%v)", pe.HTTPStatus, pe.Status, err)
	}
	if pe.Message != uniqueViolationMessage {
		t.Fatalf("message = %q, want %q", pe.Message, uniqueViolationMessage)
	}
}

// TestUniqueIndexCreateRejectsDuplicate pins the base contract: a unique index
// on one collection-group field rejects a second document with the same value
// with the ALREADY_EXISTS envelope, and leaves distinct values alone.
func TestUniqueIndexCreateRejectsDuplicate(t *testing.T) {
	s := newUniqueService(t, "uniquedb")
	createUniqueIndex(t, s, "uniquedb", "users", "", asc("email"))

	if err := createDoc(s, "uniquedb", "users/a", strFields("email", "a@example.com")); err != nil {
		t.Fatalf("create users/a: %v", err)
	}
	wantAlreadyExists(t, createDoc(s, "uniquedb", "users/b", strFields("email", "a@example.com")))
	if err := createDoc(s, "uniquedb", "users/c", strFields("email", "c@example.com")); err != nil {
		t.Fatalf("create users/c with a distinct value: %v", err)
	}
	// The rejected document was not stored.
	if _, err := s.GetDocument(context.Background(), docName("proj", "uniquedb", "users/b"), nil, nil); !isNotFound(err) {
		t.Fatalf("users/b should not exist after the rejected create, got %v", err)
	}
}

// TestUniqueIndexAbsentFieldsCollide follows the docs' "Absent fields in a
// unique index": a DENSE index stores a null entry for a missing field, so two
// documents that both omit it collide, and an explicit null collides with a
// missing field.
func TestUniqueIndexAbsentFieldsCollide(t *testing.T) {
	s := newUniqueService(t, "uniquedb")
	createUniqueIndex(t, s, "uniquedb", "cities", "", asc("name"))

	if err := createDoc(s, "uniquedb", "cities/a", strFields("abbreviation", "LA")); err != nil {
		t.Fatalf("create cities/a without the indexed field: %v", err)
	}
	wantAlreadyExists(t, createDoc(s, "uniquedb", "cities/b", strFields("abbreviation", "NYC")))
	wantAlreadyExists(t, createDoc(s, "uniquedb", "cities/c", map[string]*firestorestore.Value{
		"name": firestorestore.NullVal(),
	}))
	if err := createDoc(s, "uniquedb", "cities/d", strFields("name", "Los Angeles")); err != nil {
		t.Fatalf("create cities/d with a real value next to a null entry: %v", err)
	}
}

// TestUniqueIndexMultiFieldCombination pins that a multi-field unique index
// constrains the combination, not each field.
func TestUniqueIndexMultiFieldCombination(t *testing.T) {
	s := newUniqueService(t, "uniquedb")
	createUniqueIndex(t, s, "uniquedb", "pairs", "", asc("a"), asc("b"))

	mk := func(a, b int64) map[string]*firestorestore.Value {
		return map[string]*firestorestore.Value{"a": firestorestore.IntVal(a), "b": firestorestore.IntVal(b)}
	}
	if err := createDoc(s, "uniquedb", "pairs/one", mk(1, 2)); err != nil {
		t.Fatalf("create pairs/one: %v", err)
	}
	if err := createDoc(s, "uniquedb", "pairs/two", mk(1, 3)); err != nil {
		t.Fatalf("create pairs/two (same a, different b): %v", err)
	}
	wantAlreadyExists(t, createDoc(s, "uniquedb", "pairs/three", mk(1, 2)))
}

// TestUniqueIndexUpdateAndVacate pins update semantics: moving onto another
// document's value is rejected, rewriting its own value is not, and vacating the
// value frees it for a later create.
func TestUniqueIndexUpdateAndVacate(t *testing.T) {
	ctx := context.Background()
	s := newUniqueService(t, "uniquedb")
	createUniqueIndex(t, s, "uniquedb", "users", "", asc("email"))

	if err := createDoc(s, "uniquedb", "users/a", strFields("email", "a@example.com")); err != nil {
		t.Fatalf("create users/a: %v", err)
	}
	if err := createDoc(s, "uniquedb", "users/b", strFields("email", "b@example.com")); err != nil {
		t.Fatalf("create users/b: %v", err)
	}
	_, err := s.PatchDocument(ctx, "proj", "uniquedb", "users/b", strFields("email", "a@example.com"), nil, nil)
	wantAlreadyExists(t, err)
	// Re-writing the document's own value is not a conflict.
	if _, err := s.PatchDocument(ctx, "proj", "uniquedb", "users/b", strFields("email", "b@example.com"), nil, nil); err != nil {
		t.Fatalf("rewrite users/b with its own value: %v", err)
	}
	// Vacating the value on users/a frees it.
	if _, err := s.PatchDocument(ctx, "proj", "uniquedb", "users/a", strFields("email", "z@example.com"), nil, nil); err != nil {
		t.Fatalf("move users/a to a fresh value: %v", err)
	}
	if err := createDoc(s, "uniquedb", "users/c", strFields("email", "a@example.com")); err != nil {
		t.Fatalf("create users/c with the vacated value: %v", err)
	}
}

// TestUniqueIndexCommitSwap pins batch atomicity: a commit that deletes the
// current holder and creates a new one with the same value succeeds, a commit
// that creates a duplicate is rejected, and the rejected commit applies nothing.
func TestUniqueIndexCommitSwap(t *testing.T) {
	ctx := context.Background()
	s := newUniqueService(t, "uniquedb")
	createUniqueIndex(t, s, "uniquedb", "users", "", asc("email"))

	if err := createDoc(s, "uniquedb", "users/a", strFields("email", "a@example.com")); err != nil {
		t.Fatalf("create users/a: %v", err)
	}
	// delete a + create b in one commit: legal, the value is free by the end.
	if _, _, err := s.Commit(ctx, nil, []*writeWire{
		{Delete: docName("proj", "uniquedb", "users/a")},
		{Update: &documentWire{Name: docName("proj", "uniquedb", "users/b"), Fields: strFields("email", "a@example.com")}},
	}); err != nil {
		t.Fatalf("commit delete+create swap: %v", err)
	}
	// Two creates with the same value in one commit: rejected as a whole.
	_, _, err := s.Commit(ctx, nil, []*writeWire{
		{Update: &documentWire{Name: docName("proj", "uniquedb", "users/c"), Fields: strFields("email", "dup@example.com")}},
		{Update: &documentWire{Name: docName("proj", "uniquedb", "users/d"), Fields: strFields("email", "dup@example.com")}},
	})
	wantAlreadyExists(t, err)
	for _, path := range []string{"users/c", "users/d"} {
		if _, err := s.GetDocument(ctx, docName("proj", "uniquedb", path), nil, nil); !isNotFound(err) {
			t.Fatalf("%s should not survive a rejected commit, got %v", path, err)
		}
	}
}

// TestUniqueIndexDensity pins the density gate: DENSE always has an entry,
// SPARSE_ANY only when some indexed field is present, so a document with no
// indexed fields is not part of the duplicate set under SPARSE_ANY.
func TestUniqueIndexDensity(t *testing.T) {
	s := newUniqueService(t, "uniquedb")
	createUniqueIndex(t, s, "uniquedb", "sparse", indexDensitySparseAny, asc("email"))

	if err := createDoc(s, "uniquedb", "sparse/a", strFields("other", "one")); err != nil {
		t.Fatalf("create sparse/a without the indexed field: %v", err)
	}
	if err := createDoc(s, "uniquedb", "sparse/b", strFields("other", "two")); err != nil {
		t.Fatalf("SPARSE_ANY must not index document b (no indexed field): %v", err)
	}
	if err := createDoc(s, "uniquedb", "sparse/c", strFields("email", "c@example.com")); err != nil {
		t.Fatalf("create sparse/c: %v", err)
	}
	wantAlreadyExists(t, createDoc(s, "uniquedb", "sparse/d", strFields("email", "c@example.com")))
}

// TestUniqueIndexSparseAll pins SPARSE_ALL: a document is only indexed when
// every indexed field is present, so a partial document never collides.
func TestUniqueIndexSparseAll(t *testing.T) {
	s := newUniqueService(t, "uniquedb")
	createUniqueIndex(t, s, "uniquedb", "allfields", indexDensitySparseAll, asc("a"), asc("b"))

	partial := map[string]*firestorestore.Value{"a": firestorestore.IntVal(1)}
	if err := createDoc(s, "uniquedb", "allfields/one", partial); err != nil {
		t.Fatalf("create allfields/one: %v", err)
	}
	// Both documents miss b, so neither has an entry: no collision.
	if err := createDoc(s, "uniquedb", "allfields/two", partial); err != nil {
		t.Fatalf("SPARSE_ALL must not index a document missing b: %v", err)
	}
	full := map[string]*firestorestore.Value{"a": firestorestore.IntVal(1), "b": firestorestore.IntVal(2)}
	if err := createDoc(s, "uniquedb", "allfields/three", full); err != nil {
		t.Fatalf("create allfields/three: %v", err)
	}
	wantAlreadyExists(t, createDoc(s, "uniquedb", "allfields/four", full))
}

// TestUniqueIndexCommitFreesValueWithUpdate pins the update variant of the
// swap: a commit may move the holder off a value and hand it to a new document
// in the same batch.
func TestUniqueIndexCommitFreesValueWithUpdate(t *testing.T) {
	ctx := context.Background()
	s := newUniqueService(t, "uniquedb")
	createUniqueIndex(t, s, "uniquedb", "users", "", asc("email"))

	if err := createDoc(s, "uniquedb", "users/a", strFields("email", "a@example.com")); err != nil {
		t.Fatalf("create users/a: %v", err)
	}
	if _, _, err := s.Commit(ctx, nil, []*writeWire{
		{Update: &documentWire{Name: docName("proj", "uniquedb", "users/a"), Fields: strFields("email", "moved@example.com")}},
		{Update: &documentWire{Name: docName("proj", "uniquedb", "users/b"), Fields: strFields("email", "a@example.com")}},
	}); err != nil {
		t.Fatalf("commit update+create swap: %v", err)
	}
}

// TestUniqueIndexCollectionGroupScope pins the index's "collection ID": it spans
// every collection with that ID at any depth, and nothing else.
func TestUniqueIndexCollectionGroupScope(t *testing.T) {
	s := newUniqueService(t, "uniquedb")
	createUniqueIndex(t, s, "uniquedb", "orders", "", asc("code"))

	if err := createDoc(s, "uniquedb", "orders/a", strFields("code", "X")); err != nil {
		t.Fatalf("create orders/a: %v", err)
	}
	// A nested collection with the same ID is the same collection group.
	wantAlreadyExists(t, createDoc(s, "uniquedb", "teams/t1/orders/b", strFields("code", "X")))
	// A different collection ID is a different index.
	if err := createDoc(s, "uniquedb", "invoices/c", strFields("code", "X")); err != nil {
		t.Fatalf("a different collection group must not be constrained: %v", err)
	}
}

// TestUniqueIndexArrayContains follows the docs' unique array-contains rules:
// overlapping elements collide across documents, duplicates inside one document
// are fine, empty/missing/standalone-null values generate no keys, and a null
// element inside an array is indexed.
func TestUniqueIndexArrayContains(t *testing.T) {
	s := newUniqueService(t, "uniquedb")
	createUniqueIndex(t, s, "uniquedb", "tags", "", indexField{FieldPath: "tags", ArrayConfig: "CONTAINS"})

	if err := createDoc(s, "uniquedb", "tags/one", map[string]*firestorestore.Value{
		"tags": firestorestore.ArrayVal(firestorestore.StringVal("news"), firestorestore.StringVal("tech"), firestorestore.StringVal("news")),
	}); err != nil {
		t.Fatalf("create tags/one (duplicates inside one array are allowed): %v", err)
	}
	wantAlreadyExists(t, createDoc(s, "uniquedb", "tags/two", map[string]*firestorestore.Value{
		"tags": firestorestore.ArrayVal(firestorestore.StringVal("sports"), firestorestore.StringVal("tech")),
	}))
	if err := createDoc(s, "uniquedb", "tags/three", map[string]*firestorestore.Value{
		"tags": firestorestore.ArrayVal(firestorestore.StringVal("sports")),
	}); err != nil {
		t.Fatalf("create tags/three: %v", err)
	}
	// Empty array, missing field, and a standalone null generate no keys.
	for _, tc := range []struct {
		id     string
		fields map[string]*firestorestore.Value
	}{
		{"empty", map[string]*firestorestore.Value{"tags": firestorestore.ArrayVal()}},
		{"missing", strFields("other", "x")},
		{"standalone-null", map[string]*firestorestore.Value{"tags": firestorestore.NullVal()}},
	} {
		if err := createDoc(s, "uniquedb", "tags/"+tc.id, tc.fields); err != nil {
			t.Fatalf("create tags/%s: %v", tc.id, err)
		}
	}
	// A null element inside an array is indexed and collides.
	if err := createDoc(s, "uniquedb", "tags/null1", map[string]*firestorestore.Value{
		"tags": firestorestore.ArrayVal(firestorestore.StringVal("nullish"), firestorestore.NullVal()),
	}); err != nil {
		t.Fatalf("create tags/null1: %v", err)
	}
	wantAlreadyExists(t, createDoc(s, "uniquedb", "tags/null2", map[string]*firestorestore.Value{
		"tags": firestorestore.ArrayVal(firestorestore.NullVal()),
	}))
}

// TestUniqueIndexBatchWriteStatus pins that a batchWrite reports the duplicate as
// that write's own google.rpc code 6 while the rest of the batch applies.
func TestUniqueIndexBatchWriteStatus(t *testing.T) {
	ctx := context.Background()
	s := newUniqueService(t, "uniquedb")
	createUniqueIndex(t, s, "uniquedb", "users", "", asc("email"))

	statuses, _, err := s.BatchWrite(ctx, []*writeWire{
		{Update: &documentWire{Name: docName("proj", "uniquedb", "users/a"), Fields: strFields("email", "same@example.com")}},
		{Update: &documentWire{Name: docName("proj", "uniquedb", "users/b"), Fields: strFields("email", "same@example.com")}},
		{Update: &documentWire{Name: docName("proj", "uniquedb", "users/c"), Fields: strFields("email", "other@example.com")}},
	})
	if err != nil {
		t.Fatalf("BatchWrite: %v", err)
	}
	if len(statuses) != 3 {
		t.Fatalf("BatchWrite returned %d statuses, want 3", len(statuses))
	}
	wantCodes := []int64{0, 6, 0}
	for i, want := range wantCodes {
		got, _ := statuses[i].(map[string]any)["code"].(int64)
		if got != want {
			t.Fatalf("status[%d].code = %v, want %d (%v)", i, statuses[i], want, statuses[i])
		}
	}
	if _, err := s.GetDocument(ctx, docName("proj", "uniquedb", "users/b"), nil, nil); !isNotFound(err) {
		t.Fatalf("users/b should not exist after its rejected write, got %v", err)
	}
}

// TestUniqueIndexConcurrentCreates pins the serialization: racing creates of the
// same value have exactly one winner and every loser sees ALREADY_EXISTS (run
// under -race).
func TestUniqueIndexConcurrentCreates(t *testing.T) {
	s := newUniqueService(t, "uniquedb")
	createUniqueIndex(t, s, "uniquedb", "users", "", asc("email"))

	const writers = 8
	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = createDoc(s, "uniquedb", "users/doc"+string(rune('a'+i)), strFields("email", "race@example.com"))
		}(i)
	}
	wg.Wait()
	winners, losers := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			winners++
		default:
			var pe *model.ProviderError
			if !errors.As(err, &pe) || pe.Status != "ALREADY_EXISTS" {
				t.Fatalf("loser error = %v, want AlreadyExists", err)
			}
			losers++
		}
	}
	if winners != 1 || losers != writers-1 {
		t.Fatalf("winners=%d losers=%d, want 1/%d", winners, losers, writers-1)
	}
}

// TestUniqueIndexNotEnforcedWithoutUniqueOption pins that storing a non-unique
// index (and having no index at all) changes nothing.
func TestUniqueIndexNotEnforcedWithoutUniqueOption(t *testing.T) {
	s := newUniqueService(t, "uniquedb")
	if _, _, err := s.CreateIndexDef(context.Background(), "proj", "uniquedb", "users", indexDef{
		QueryScope: "COLLECTION",
		Fields:     []indexField{asc("email"), asc("tenant")},
	}); err != nil {
		t.Fatalf("CreateIndexDef(non-unique): %v", err)
	}
	for _, id := range []string{"a", "b"} {
		if err := createDoc(s, "uniquedb", "users/"+id, strFields("email", "same@example.com")); err != nil {
			t.Fatalf("create users/%s with a non-unique index: %v", id, err)
		}
	}
}

// TestUniqueIndexRESTCommitEnvelope pins the REST transport's rendering of a
// duplicate: the Provider (documents:commit handler) surfaces the same
// AlreadyExists@409 ProviderError the gateway turns into google.rpc
// ALREADY_EXISTS, so both transports agree.
func TestUniqueIndexRESTCommitEnvelope(t *testing.T) {
	ctx := context.Background()
	p := New(firestorestore.NewMemoryStore(), store.NewMemoryResourceStore())
	if _, _, err := p.CreateDatabaseDef(ctx, "proj", "uniquedb", databaseDef{
		LocationID:      "us-central1",
		DatabaseEdition: databaseEditionEnterprise,
	}); err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	if _, _, err := p.CreateIndexDef(ctx, "proj", "uniquedb", "users", indexDef{
		QueryScope: "COLLECTION",
		Unique:     true,
		Fields:     []indexField{asc("email")},
	}); err != nil {
		t.Fatalf("CreateIndexDef: %v", err)
	}

	commit := func(id string) error {
		nr := testNR()
		nr.Params["body"] = map[string]any{
			"writes": []any{
				map[string]any{
					"update": map[string]any{
						"name":   "projects/proj/databases/uniquedb/documents/users/" + id,
						"fields": map[string]any{"email": map[string]any{"stringValue": "dup@example.com"}},
					},
				},
			},
		}
		_, err := p.Commit(ctx, nr)
		return err
	}
	if err := commit("a"); err != nil {
		t.Fatalf("commit users/a: %v", err)
	}
	wantAlreadyExists(t, commit("b"))
}

// TestUniqueIndexBuildNeedsRepair pins FE6's build contract: a unique index over
// data that already holds a duplicate entry fails its build. The record is
// stored in NEEDS_REPAIR (readable, but neither served by the planner nor
// enforced on writes), and the documented recovery is delete -> fix the data ->
// re-create.
func TestUniqueIndexBuildNeedsRepair(t *testing.T) {
	ctx := context.Background()
	s := newUniqueService(t, "builddb")
	for _, id := range []string{"a", "b"} {
		if err := createDoc(s, "builddb", "users/"+id, strFields("email", "dup@example.com")); err != nil {
			t.Fatalf("seed users/%s: %v", id, err)
		}
	}

	failed, opName, err := s.CreateIndexDef(ctx, "proj", "builddb", "users", indexDef{
		QueryScope: "COLLECTION",
		Unique:     true,
		Fields:     []indexField{asc("email")},
	})
	var build *IndexBuildError
	if !errors.As(err, &build) {
		t.Fatalf("CreateIndexDef error = %v, want *IndexBuildError", err)
	}
	if build.Code != 9 || opName == "" {
		t.Fatalf("build failure code=%d opName=%q, want FAILED_PRECONDITION and an operation name", build.Code, opName)
	}
	if failed.State != indexStateNeedsRepair {
		t.Fatalf("failed index state = %q, want NEEDS_REPAIR", failed.State)
	}
	got, err := s.GetIndex(ctx, "proj", "builddb", "users", indexID(t, failed.Name))
	if err != nil || got.State != indexStateNeedsRepair {
		t.Fatalf("GetIndex after a failed build = %+v, %v; want NEEDS_REPAIR", got, err)
	}
	// A failed index is not served by the planner and does not enforce.
	if ok, err := s.hasIndex(ctx, "proj", "builddb", []string{"email"}); err != nil || ok {
		t.Fatalf("hasIndex(NEEDS_REPAIR) = %v, %v; want false", ok, err)
	}
	if err := createDoc(s, "builddb", "users/c", strFields("email", "dup@example.com")); err != nil {
		t.Fatalf("a NEEDS_REPAIR index must not enforce uniqueness: %v", err)
	}

	// Recovery: remove the failed index, fix the data, re-create.
	if err := s.DeleteIndex(ctx, "proj", "builddb", "users", indexID(t, failed.Name)); err != nil {
		t.Fatalf("DeleteIndex(failed): %v", err)
	}
	for _, id := range []string{"b", "c"} {
		if err := s.DeleteDocument(ctx, "proj", "builddb", "users/"+id, nil); err != nil {
			t.Fatalf("DeleteDocument(users/%s): %v", id, err)
		}
	}
	created, _, err := s.CreateIndexDef(ctx, "proj", "builddb", "users", indexDef{
		QueryScope: "COLLECTION",
		Unique:     true,
		Fields:     []indexField{asc("email")},
	})
	if err != nil {
		t.Fatalf("re-create after fixing the data: %v", err)
	}
	if created.State != indexStateReady {
		t.Fatalf("re-created index state = %q, want READY", created.State)
	}
	wantAlreadyExists(t, createDoc(s, "builddb", "users/d", strFields("email", "dup@example.com")))
}

// TestUniqueIndexBuildRESTEnvelope pins the REST rendering of a failed build:
// the create answers 200 with a done operation whose error is the google.rpc
// status and no response, and the index is readable in NEEDS_REPAIR.
func TestUniqueIndexBuildRESTEnvelope(t *testing.T) {
	ctx := context.Background()
	docs := firestorestore.NewMemoryStore()
	p := New(docs, store.NewMemoryResourceStore())
	if _, _, err := p.CreateDatabaseDef(ctx, "proj", "builddb", databaseDef{
		LocationID:      "us-central1",
		DatabaseEdition: databaseEditionEnterprise,
	}); err != nil {
		t.Fatalf("CreateDatabaseDef: %v", err)
	}
	for _, id := range []string{"a", "b"} {
		if err := docs.CreateDocument(ctx, firestorestore.Document{
			Name:   docName("proj", "builddb", "users/"+id),
			Fields: strFields("email", "dup@example.com"),
		}); err != nil {
			t.Fatalf("seed users/%s: %v", id, err)
		}
	}

	nr := testNR()
	nr.Params["name"] = "databases/builddb/collectionGroups/users/indexes"
	nr.Params["body"] = map[string]any{
		"queryScope": "COLLECTION",
		"unique":     true,
		"fields":     []any{map[string]any{"fieldPath": "email", "order": "ASCENDING"}},
	}
	resp, err := p.CreateIndex(ctx, nr)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if resp.Data["done"] != true {
		t.Fatalf("operation done = %v, want true", resp.Data["done"])
	}
	if _, ok := resp.Data["response"]; ok {
		t.Fatalf("a failed build must not carry an operation response: %v", resp.Data["response"])
	}
	errObj, ok := resp.Data["error"].(map[string]any)
	if !ok {
		t.Fatalf("operation error = %v, want a google.rpc status object", resp.Data["error"])
	}
	if code, _ := errObj["code"].(int64); code != 9 {
		t.Fatalf("operation error code = %v, want 9 (FAILED_PRECONDITION)", errObj["code"])
	}
	if msg, _ := errObj["message"].(string); !strings.Contains(msg, "unique index") {
		t.Fatalf("operation error message = %q, want the unique-index build failure", msg)
	}

	lresp, lerr := p.ListIndexes(ctx, adminNR("databases/builddb/collectionGroups/users/indexes", nil))
	list := indexData(t, lresp, lerr)
	idxs, _ := list["indexes"].([]any)
	if len(idxs) != 1 {
		t.Fatalf("ListIndexes = %v, want the NEEDS_REPAIR record", list)
	}
	if state := idxs[0].(map[string]any)["state"]; state != "NEEDS_REPAIR" {
		t.Fatalf("listed state = %v, want NEEDS_REPAIR", state)
	}
}
