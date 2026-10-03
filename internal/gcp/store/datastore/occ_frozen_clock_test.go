package datastore

import (
	"context"
	"errors"
	"testing"
	"time"

	"jaiscloud/internal/clock"
)

// TestNextUpdateTime locks the monotonic guard: the stamp applied to a write is
// strictly greater than the entity's current update time even when the clock is
// frozen (now == prev) or moves backwards.
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
	// Sub-microsecond components are truncated: a frozen clock with nanoseconds
	// must still yield a strictly greater token after a Postgres TIMESTAMPTZ
	// round-trip, which only keeps microseconds.
	withNanos := prev.Add(123 * time.Nanosecond)
	if got := nextUpdateTime(withNanos, withNanos); !got.Equal(prev.Add(time.Microsecond)) {
		t.Errorf("sub-microsecond frozen clock: got %v, want %v", got, prev.Add(time.Microsecond))
	}
}

// TestFrozenClockUpdateTimePreconditionConflict is the G7 regression: under a
// frozen clock, two writers that both observe the same entity update_time must
// not both pass conflict detection. update_time is one of Datastore's
// optimistic-concurrency tokens; before the monotonic guard both writers
// computed the same clock.Now() stamp, so the second writer's precondition
// still matched and its stale write silently overwrote the first (a lost
// update).
func TestFrozenClockUpdateTimePreconditionConflict(t *testing.T) {
	ctx := context.Background()
	frozen := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: frozen})
	defer clock.SetGlobalClock(clock.RealClock{})

	s := NewMemoryStore()
	key := KeyOfName("Task", "a")
	intVal := func(n int64) Value { return Value{IntegerValue: &n} }

	inserted, err := s.ApplyMutation(ctx, "proj", MutationInsert, Entity{
		Kind:       "Task",
		Key:        key,
		Properties: map[string]Value{"n": intVal(1)},
	}, nil)
	if err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	if inserted.UpdateTime.IsZero() {
		t.Fatal("insert must stamp a non-zero update time (the OCC token)")
	}
	observed := inserted.UpdateTime

	// Writer B applies an update using the observed token. Its new stamp must
	// be strictly greater so the token can never repeat.
	upd := Entity{Kind: "Task", Key: key, Properties: map[string]Value{"n": intVal(2)}}
	b, err := s.ApplyMutation(ctx, "proj", MutationUpdate, upd, &Precondition{UpdateTime: &observed})
	if err != nil {
		t.Fatalf("writer B update with matching token: %v", err)
	}
	if !b.UpdateTime.Equal(observed.Add(time.Microsecond)) {
		t.Fatalf("writer B update time = %v, want %v (strictly monotonic)", b.UpdateTime, observed.Add(time.Microsecond))
	}

	// Writer A, still holding the stale token, must now conflict instead of
	// overwriting writer B.
	if _, err := s.ApplyMutation(ctx, "proj", MutationUpdate, upd, &Precondition{UpdateTime: &observed}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale update_time precondition = %v, want ErrConflict", err)
	}

	// The same guarantee must hold on the batch Commit path (resolveWrite).
	if _, err := s.Commit(ctx, "proj", nil, []Write{{
		Op:           WriteUpdate,
		Key:          key,
		Entity:       upd,
		Precondition: &Precondition{UpdateTime: &observed},
	}}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale Commit update_time precondition = %v, want ErrConflict", err)
	}

	// Writer B's write must not have been lost.
	got, err := s.Get(ctx, "proj", key)
	if err != nil {
		t.Fatalf("get after conflict: %v", err)
	}
	if got.Properties["n"].IntegerValue == nil || *got.Properties["n"].IntegerValue != 2 {
		t.Fatalf("stored n = %v, want 2 (writer B's update must not be lost)", got.Properties["n"].IntegerValue)
	}
}
