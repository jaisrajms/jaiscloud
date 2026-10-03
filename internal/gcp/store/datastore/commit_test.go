package datastore

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// seed inserts an entity via ApplyMutation so it gets a server-stamped version
// (1), the version a transaction read-set records.
func seed(t *testing.T, s *MemoryStore, project string, e Entity) Entity {
	t.Helper()
	applied, err := s.ApplyMutation(context.Background(), project, MutationInsert, e, nil)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	return applied
}

func TestMemoryStoreCommitAppliesAllWritesAtomically(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	a := seed(t, s, "p1", testEntity("Task", "a", map[string]Value{"n": num(1)}))
	b := seed(t, s, "p1", testEntity("Task", "b", map[string]Value{"n": num(1)}))

	writes := []Write{
		{Op: WriteUpdate, Key: a.Key, Entity: testEntity("Task", "a", map[string]Value{"n": num(2)})},
		{Op: WriteInsert, Key: KeyOfName("Task", "c"), Entity: testEntity("Task", "c", map[string]Value{"n": num(3)})},
		{Op: WriteDelete, Key: b.Key},
	}
	applied, err := s.Commit(ctx, "p1", nil, writes)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(applied) != 3 {
		t.Fatalf("applied = %d, want 3", len(applied))
	}
	if applied[0].Version != 2 {
		t.Fatalf("update version = %d, want 2", applied[0].Version)
	}
	if applied[1].Version != 1 {
		t.Fatalf("insert version = %d, want 1", applied[1].Version)
	}

	got, _ := s.Get(ctx, "p1", a.Key)
	if n := got.Properties["n"].IntegerValue; n == nil || *n != 2 {
		t.Fatalf("a.n = %v, want 2", got.Properties["n"])
	}
	if _, err := s.Get(ctx, "p1", KeyOfName("Task", "c")); err != nil {
		t.Fatalf("c should have been inserted: %v", err)
	}
	if _, err := s.Get(ctx, "p1", b.Key); !errors.Is(err, ErrEntityNotFound) {
		t.Fatalf("b should have been deleted, got %v", err)
	}
}

func TestMemoryStoreCommitReadSetConflictAborts(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	a := seed(t, s, "p1", testEntity("Task", "a", map[string]Value{"n": num(1)}))

	reads := []ReadRef{{Key: a.Key, Exists: true, Version: a.Version}}

	// A concurrent writer advances the entity after the read was recorded.
	if _, err := s.ApplyMutation(ctx, "p1", MutationUpdate, testEntity("Task", "a", map[string]Value{"n": num(2)}), nil); err != nil {
		t.Fatalf("concurrent update: %v", err)
	}

	writes := []Write{{Op: WriteUpdate, Key: a.Key, Entity: testEntity("Task", "a", map[string]Value{"n": num(99)})}}
	if _, err := s.Commit(ctx, "p1", reads, writes); !errors.Is(err, ErrAborted) {
		t.Fatalf("commit = %v, want ErrAborted", err)
	}
	got, _ := s.Get(ctx, "p1", a.Key)
	if n := got.Properties["n"].IntegerValue; n == nil || *n != 2 {
		t.Fatalf("entity changed despite abort: n = %v, want 2", got.Properties["n"])
	}
}

func TestMemoryStoreCommitMissingReadDetectsCreate(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	a := testEntity("Task", "a", map[string]Value{"n": num(1)})

	// The key was read as absent.
	reads := []ReadRef{{Key: a.Key, Exists: false, Version: 0}}

	// A concurrent writer creates it.
	if _, err := s.ApplyMutation(ctx, "p1", MutationInsert, a, nil); err != nil {
		t.Fatalf("concurrent insert: %v", err)
	}

	writes := []Write{{Op: WriteUpsert, Key: a.Key, Entity: testEntity("Task", "a", map[string]Value{"n": num(99)})}}
	if _, err := s.Commit(ctx, "p1", reads, writes); !errors.Is(err, ErrAborted) {
		t.Fatalf("commit = %v, want ErrAborted", err)
	}
}

func TestMemoryStoreCommitPreconditionMismatchAborts(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	a := seed(t, s, "p1", testEntity("Task", "a", map[string]Value{"n": num(1)}))

	stale := int64(0)
	writes := []Write{{
		Op:           WriteUpdate,
		Key:          a.Key,
		Entity:       testEntity("Task", "a", map[string]Value{"n": num(99)}),
		Precondition: &Precondition{BaseVersion: &stale},
	}}
	if _, err := s.Commit(ctx, "p1", nil, writes); !errors.Is(err, ErrConflict) {
		t.Fatalf("commit = %v, want ErrConflict", err)
	}
	got, _ := s.Get(ctx, "p1", a.Key)
	if n := got.Properties["n"].IntegerValue; n == nil || *n != 1 {
		t.Fatalf("entity changed despite precondition failure: n = %v, want 1", got.Properties["n"])
	}
}

// TestMemoryStoreCommitConcurrentSameEntity is the no-lost-update guarantee:
// two commits that both observed the same entity version race; exactly one
// wins and the other aborts without applying.
func TestMemoryStoreCommitConcurrentSameEntity(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	a := seed(t, s, "p1", testEntity("Task", "a", map[string]Value{"n": num(1)}))
	reads := []ReadRef{{Key: a.Key, Exists: true, Version: a.Version}}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			writes := []Write{{Op: WriteUpsert, Key: a.Key, Entity: testEntity("Task", "a", map[string]Value{"n": num(int64(100 + i))})}}
			_, errs[i] = s.Commit(ctx, "p1", reads, writes)
		}(i)
	}
	wg.Wait()

	aborted := 0
	succeeded := 0
	for _, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrAborted):
			aborted++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if succeeded != 1 || aborted != 1 {
		t.Fatalf("succeeded=%d aborted=%d, want 1 and 1", succeeded, aborted)
	}
	got, _ := s.Get(ctx, "p1", a.Key)
	if got.Version != 2 {
		t.Fatalf("final version = %d, want 2 (exactly one write applied)", got.Version)
	}
}
