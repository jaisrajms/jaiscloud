package firestore

import (
	"context"
	"testing"
	"time"

	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

// conflictInjectingStore wraps a real FirestoreStore and, on the Nth call to
// GetDocument for a chosen document name, performs an out-of-band conflicting
// write (via the wrapped store, bypassing the caller entirely) before
// returning the caller its (now-stale) read. This deterministically
// simulates the race window between PatchDocument/buildUpdate's base read and
// its eventual Commit — the same window a real concurrent second writer would
// exploit — without relying on real goroutine timing.
type conflictInjectingStore struct {
	firestorestore.FirestoreStore
	targetName string
	triggerNth int // inject before the Nth GetDocument(targetName) call
	getCalls   int
	inject     func()
	injected   bool
}

func (s *conflictInjectingStore) GetDocument(ctx context.Context, name string) (firestorestore.Document, error) {
	// Capture the result BEFORE injecting the conflicting write, so the
	// caller gets the stale (pre-conflict) snapshot — exactly what a real
	// concurrent reader would have seen — while the store's live state moves
	// on underneath it.
	doc, err := s.FirestoreStore.GetDocument(ctx, name)
	if name == s.targetName {
		s.getCalls++
		if s.getCalls == s.triggerNth && !s.injected {
			s.injected = true
			s.inject()
		}
	}
	return doc, err
}

// TestPatchDocument_ConcurrentWriteDetected verifies the lost-update race fix:
// a write that lands between PatchDocument's base read and its Commit call is
// detected (ABORTED) instead of silently overwritten. Before the fix,
// PatchDocument did GetDocument -> compute merge -> UpdateDocument as two
// separate, unprotected store calls; a conflicting write in between was
// clobbered with no error.
func TestPatchDocument_ConcurrentWriteDetected(t *testing.T) {
	ctx := context.Background()
	name := "projects/proj/databases/(default)/documents/cities/SF"
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Second)
	real := firestorestore.NewMemoryStore()
	if err := real.CreateDocument(ctx, firestorestore.Document{
		Name:       name,
		Fields:     map[string]*firestorestore.Value{"population": intField(1)},
		CreateTime: t0,
		UpdateTime: t0,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	wrapped := &conflictInjectingStore{FirestoreStore: real, targetName: name, triggerNth: 1}
	p := New(wrapped, nil)

	// "Writer B" races in and updates the document AFTER PatchDocument's base
	// read but BEFORE its Commit call. Explicit, distinct UpdateTime values
	// (rather than relying on real-clock timestamping, which two calls this
	// close together could collide on) make the conflict unambiguous.
	wrapped.inject = func() {
		if err := real.UpdateDocument(ctx, firestorestore.Document{
			Name:       name,
			Fields:     map[string]*firestorestore.Value{"population": intField(2)},
			CreateTime: t0,
			UpdateTime: t1,
		}); err != nil {
			t.Fatalf("injected concurrent write: %v", err)
		}
	}

	// "Writer A": patches a DIFFERENT field. Before the fix this would
	// silently overwrite writer B's population=2 with population=1 (the
	// stale base it read), losing the concurrent update with no error.
	_, err := p.PatchDocument(ctx, "proj", "(default)", "cities/SF",
		map[string]*firestorestore.Value{"name": firestorestore.StringVal("San Francisco")},
		[]string{"name"}, nil)
	if err == nil {
		t.Fatal("expected the concurrent write to be detected (ABORTED), got nil error")
	}
	assertProviderError(t, err, 409, "ABORTED")

	// Writer B's update must survive untouched — no lost update.
	got, err := real.GetDocument(ctx, name)
	if err != nil {
		t.Fatalf("get after conflict: %v", err)
	}
	if v, ok := got.Fields["population"].AsInt64(); !ok || v != 2 {
		t.Fatalf("population = %v, want 2 (writer B's update must not be lost)", got.Fields["population"])
	}
	if _, ok := got.Fields["name"]; ok {
		t.Fatalf("writer A's write must not have applied, got fields %+v", got.Fields)
	}
}

// TestCommit_ConcurrentTransformDetected is the same scenario via the
// Commit/BatchWrite path (buildUpdate/buildTransform), which non-transactional
// documents:commit and batchWrite requests use.
func TestCommit_ConcurrentTransformDetected(t *testing.T) {
	ctx := context.Background()
	name := "projects/proj/databases/(default)/documents/cities/SF"
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Second)
	real := firestorestore.NewMemoryStore()
	if err := real.CreateDocument(ctx, firestorestore.Document{
		Name:       name,
		Fields:     map[string]*firestorestore.Value{"n": intField(10)},
		CreateTime: t0,
		UpdateTime: t0,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	wrapped := &conflictInjectingStore{FirestoreStore: real, targetName: name, triggerNth: 1}
	p := New(wrapped, nil)
	wrapped.inject = func() {
		if err := real.UpdateDocument(ctx, firestorestore.Document{
			Name:       name,
			Fields:     map[string]*firestorestore.Value{"n": intField(20)},
			CreateTime: t0,
			UpdateTime: t1,
		}); err != nil {
			t.Fatalf("injected concurrent write: %v", err)
		}
	}

	// A non-transactional Commit with an increment transform: the merge base
	// (n=10) is read, then writer B races in and changes n to 20, then this
	// commit tries to apply increment(5) on top of the STALE base (would
	// silently produce n=15, discarding writer B's n=20 entirely).
	nr := testNR()
	nr.Params["body"] = map[string]any{
		"writes": []any{
			map[string]any{
				"transform": map[string]any{
					"document": name,
					"fieldTransforms": []any{
						map[string]any{"fieldPath": "n", "increment": map[string]any{"integerValue": "5"}},
					},
				},
			},
		},
	}
	_, err := p.Commit(ctx, nr)
	if err == nil {
		t.Fatal("expected the concurrent write to be detected (ABORTED), got nil error")
	}
	assertProviderError(t, err, 409, "ABORTED")

	got, err := real.GetDocument(ctx, name)
	if err != nil {
		t.Fatalf("get after conflict: %v", err)
	}
	if v, ok := got.Fields["n"].AsInt64(); !ok || v != 20 {
		t.Fatalf("n = %v, want 20 (writer B's update must not be lost, and the transform must not have applied to a stale base)", got.Fields["n"])
	}
}
