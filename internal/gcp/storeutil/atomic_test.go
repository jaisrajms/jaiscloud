package storeutil

import (
	"errors"
	"sync"
	"testing"
)

func TestAtomicUpdate_CreatesWhenAbsent(t *testing.T) {
	var mu sync.RWMutex
	store := map[string]int{}

	got, err := AtomicUpdate(&mu,
		func() (int, bool) { v, ok := store["k"]; return v, ok },
		func(current int, exists bool) (int, error) {
			if exists {
				t.Fatalf("expected exists=false for a fresh key")
			}
			return 1, nil
		},
		func(v int) { store["k"] = v },
	)
	if err != nil {
		t.Fatalf("AtomicUpdate: %v", err)
	}
	if got != 1 || store["k"] != 1 {
		t.Fatalf("got=%d store=%d, want 1/1", got, store["k"])
	}
}

func TestAtomicUpdate_MergesExisting(t *testing.T) {
	var mu sync.RWMutex
	store := map[string]int{"k": 10}

	got, err := AtomicUpdate(&mu,
		func() (int, bool) { v, ok := store["k"]; return v, ok },
		func(current int, exists bool) (int, error) {
			if !exists || current != 10 {
				t.Fatalf("expected exists=true current=10, got exists=%v current=%d", exists, current)
			}
			return current + 5, nil
		},
		func(v int) { store["k"] = v },
	)
	if err != nil {
		t.Fatalf("AtomicUpdate: %v", err)
	}
	if got != 15 || store["k"] != 15 {
		t.Fatalf("got=%d store=%d, want 15/15", got, store["k"])
	}
}

func TestAtomicUpdate_MutateErrorDoesNotWrite(t *testing.T) {
	var mu sync.RWMutex
	store := map[string]int{"k": 10}
	sentinel := errors.New("nope")
	setCalled := false

	_, err := AtomicUpdate(&mu,
		func() (int, bool) { v, ok := store["k"]; return v, ok },
		func(current int, exists bool) (int, error) { return 0, sentinel },
		func(v int) { setCalled = true; store["k"] = v },
	)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
	if setCalled {
		t.Fatal("set must not be called when mutate returns an error")
	}
	if store["k"] != 10 {
		t.Fatalf("store must be untouched on error, got %d", store["k"])
	}
}

// TestAtomicUpdate_NoLostUpdatesUnderConcurrency is the actual regression
// this primitive exists to fix: many goroutines each doing a
// read-then-increment-then-write on the SAME key must never lose an update.
// Run with -race; a data race here would also indicate the lock isn't doing
// its job.
func TestAtomicUpdate_NoLostUpdatesUnderConcurrency(t *testing.T) {
	var mu sync.RWMutex
	store := map[string]int{"k": 0}

	const goroutines = 50
	const incrementsEach = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsEach; j++ {
				if _, err := AtomicUpdate(&mu,
					func() (int, bool) { v, ok := store["k"]; return v, ok },
					func(current int, exists bool) (int, error) { return current + 1, nil },
					func(v int) { store["k"] = v },
				); err != nil {
					t.Errorf("AtomicUpdate: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	want := goroutines * incrementsEach
	if store["k"] != want {
		t.Fatalf("final count = %d, want %d (a mismatch means a lost update slipped through)", store["k"], want)
	}
}
