package firestore

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	firestorestore "jaiscloud/internal/gcp/store/firestore"
)

// TestNextUpdateTime locks the monotonic guard: the stamp applied to a write is
// strictly greater than the document's current update time even when the clock
// is frozen (now == prev) or moves backwards.
func TestNextUpdateTime(t *testing.T) {
	prev := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	later := prev.Add(time.Hour)

	if got := nextUpdateTime(time.Time{}, prev); !got.Equal(prev) {
		t.Errorf("zero prev: got %v, want %v", got, prev)
	}
	if got := nextUpdateTime(prev, later); !got.Equal(later) {
		t.Errorf("advancing clock: got %v, want %v", got, later)
	}
	if got := nextUpdateTime(prev, prev); !got.Equal(prev.Add(time.Microsecond)) {
		t.Errorf("frozen clock: got %v, want %v", got, prev.Add(time.Microsecond))
	}
	if got := nextUpdateTime(prev, prev.Add(-time.Hour)); !got.Equal(prev.Add(time.Microsecond)) {
		t.Errorf("backwards clock: got %v, want %v", got, prev.Add(time.Microsecond))
	}
	// Sub-microsecond components are truncated before comparing: a frozen clock
	// with nanoseconds still yields a strictly greater token after a Postgres
	// TIMESTAMPTZ round-trip, which only keeps microseconds.
	withNanos := prev.Add(123 * time.Nanosecond)
	if got := nextUpdateTime(withNanos, withNanos); !got.Equal(prev.Add(time.Microsecond)) {
		t.Errorf("sub-microsecond frozen clock: got %v, want %v", got, prev.Add(time.Microsecond))
	}
}

// TestFrozenClockConcurrentWritersConflict is the G7 regression: under a frozen
// clock, two writers that both read a document at the same update time must not
// both pass conflict detection. Before the monotonic guard, both writers
// computed the same clock.Now() update time, so the second writer's read-set
// still matched the stored document and its stale merge silently overwrote the
// first writer's change (a lost update).
func TestFrozenClockConcurrentWritersConflict(t *testing.T) {
	ctx := context.Background()
	name := "projects/proj/databases/(default)/documents/cities/SF"
	frozen := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: frozen})
	defer clock.SetGlobalClock(clock.RealClock{})

	real := firestorestore.NewMemoryStore()
	seeder := New(real, nil)
	if _, err := seeder.Service.CreateDocument(ctx, "proj", "(default)", "cities", "SF", map[string]*firestorestore.Value{
		"population": intField(1),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Inject a full second writer (writer B) between writer A's base read and
	// its commit, under the same frozen clock. B goes through the provider, so
	// it receives the monotonic update-time stamp.
	wrapped := &conflictInjectingStore{FirestoreStore: real, targetName: name, triggerNth: 1}
	writerA := New(wrapped, nil)
	writerB := New(real, nil)
	wrapped.inject = func() {
		if _, err := writerB.PatchDocument(ctx, "proj", "(default)", "cities/SF",
			map[string]*firestorestore.Value{"population": intField(2)}, []string{"population"}, nil); err != nil {
			t.Fatalf("writer B: %v", err)
		}
	}

	// Writer A patches a DIFFERENT field from the stale base it read. Without
	// the guard this would clobber writer B's population=2 with population=1.
	if _, err := writerA.PatchDocument(ctx, "proj", "(default)", "cities/SF",
		map[string]*firestorestore.Value{"name": firestorestore.StringVal("San Francisco")}, []string{"name"}, nil); err == nil {
		t.Fatal("expected the frozen-clock concurrent write to be detected (ABORTED), got nil error")
	} else {
		assertProviderError(t, err, 409, "ABORTED")
	}

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
