//go:build gcp_persistence

package eventarc

import (
	"bytes"
	"context"
	"os"
	"testing"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresStore runs the shared store matrix against the Postgres backend.
func TestPostgresStore(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres store test")
	}
	ctx := context.Background()

	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresStore(pg.Pool())
	s.Reset(ctx)

	runStoreTests(t, s)

	// Snapshot/restore round-trip, including the persisted operations.
	ctx = context.Background()
	_ = s.CreateTrigger(ctx, "proj", "us-central1", Trigger{Name: "t", UID: "u", Etag: "e", Config: []byte(`{"eventFilters":[]}`)})
	_ = s.CreateOperation(ctx, "proj", "us-central1", Operation{
		ID: "op-1", Verb: "create", Target: "target", Response: []byte(`{"@type":"x"}`),
	})
	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	s.Reset(ctx)
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if _, err := s.GetTrigger(ctx, "proj", "us-central1", "t"); err != nil {
		t.Fatalf("trigger lost after restore: %v", err)
	}
	op, err := s.GetOperation(ctx, "proj", "us-central1", "op-1")
	if err != nil || op.Verb != "create" || !jsonEqual(op.Response, []byte(`{"@type":"x"}`)) {
		t.Fatalf("operation lost after restore: %v %+v", err, op)
	}
	if empty, err := s.IsEmpty(ctx); err != nil || empty {
		t.Fatalf("IsEmpty after restore = %v, %v; want false", empty, err)
	}
}
