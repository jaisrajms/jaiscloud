//go:build gcp_persistence

package datastore

import (
	"context"
	"errors"
	"os"
	"testing"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// newPostgresStoreForTest connects to JAISCLOUD_DSN, runs migrations, and
// returns a reset PostgresStore. Skips when the DSN is unset.
func newPostgresStoreForTest(t *testing.T) *PostgresStore {
	t.Helper()
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres test")
	}
	ctx := context.Background()
	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pg.Close)
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s := NewPostgresStore(pg.Pool())
	s.Reset(ctx)
	return s
}

// TestPostgresStoreCommit mirrors the memory commit cases against Postgres so
// the transactional Commit path (read-set validation + atomic apply) is
// exercised on the durable backend, not just in memory.
func TestPostgresStoreCommit(t *testing.T) {
	ctx := context.Background()
	s := newPostgresStoreForTest(t)

	a, err := s.ApplyMutation(ctx, "p1", MutationInsert, testEntity("Task", "a", map[string]Value{"n": num(1)}), nil)
	if err != nil {
		t.Fatalf("seed a: %v", err)
	}
	b, err := s.ApplyMutation(ctx, "p1", MutationInsert, testEntity("Task", "b", map[string]Value{"n": num(1)}), nil)
	if err != nil {
		t.Fatalf("seed b: %v", err)
	}

	// All writes applied atomically: update a, insert c, delete b.
	applied, err := s.Commit(ctx, "p1", nil, []Write{
		{Op: WriteUpdate, Key: a.Key, Entity: testEntity("Task", "a", map[string]Value{"n": num(2)})},
		{Op: WriteInsert, Key: KeyOfName("Task", "c"), Entity: testEntity("Task", "c", map[string]Value{"n": num(3)})},
		{Op: WriteDelete, Key: b.Key},
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(applied) != 3 {
		t.Fatalf("applied = %d, want 3", len(applied))
	}
	if applied[0].Version != 2 || applied[1].Version != 1 {
		t.Fatalf("applied versions = %d,%d want 2,1", applied[0].Version, applied[1].Version)
	}
	if got, _ := s.Get(ctx, "p1", a.Key); got.Properties["n"].IntegerValue == nil || *got.Properties["n"].IntegerValue != 2 {
		t.Fatalf("a.n not updated: %+v", got.Properties["n"])
	}
	if _, err := s.Get(ctx, "p1", KeyOfName("Task", "c")); err != nil {
		t.Fatalf("c should exist: %v", err)
	}
	if _, err := s.Get(ctx, "p1", b.Key); !errors.Is(err, ErrEntityNotFound) {
		t.Fatalf("b should be deleted, got %v", err)
	}

	// A stale read-set aborts the whole commit with ErrAborted, applying nothing.
	reads := []ReadRef{{Key: a.Key, Exists: true, Version: 2}}
	if _, err := s.ApplyMutation(ctx, "p1", MutationUpdate, testEntity("Task", "a", map[string]Value{"n": num(9)}), nil); err != nil {
		t.Fatalf("bump a: %v", err)
	}
	if _, err := s.Commit(ctx, "p1", reads, []Write{
		{Op: WriteUpdate, Key: a.Key, Entity: testEntity("Task", "a", map[string]Value{"n": num(10)})},
	}); !errors.Is(err, ErrAborted) {
		t.Fatalf("stale read-set commit = %v, want ErrAborted", err)
	}
	if got, _ := s.Get(ctx, "p1", a.Key); *got.Properties["n"].IntegerValue != 9 {
		t.Fatalf("aborted commit must not apply: a.n = %d, want 9", *got.Properties["n"].IntegerValue)
	}

	// A per-write precondition mismatch aborts with ErrConflict.
	cur, _ := s.Get(ctx, "p1", a.Key)
	stale := int64(999)
	if _, err := s.Commit(ctx, "p1", nil, []Write{
		{Op: WriteUpdate, Key: cur.Key, Entity: cur, Precondition: &Precondition{BaseVersion: &stale}},
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("precondition commit = %v, want ErrConflict", err)
	}
}
