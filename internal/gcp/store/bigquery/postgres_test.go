//go:build gcp_persistence

package bigquery

import (
	"context"
	"os"
	"testing"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresStore runs the shared store matrix against the Postgres backend.
// The gcp_persistence gate runs with -p 1 because these tests share one database.
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
}

// TestPostgresSnapshotRoundTrip exercises the snapshot/export/import contract
// against Postgres: the store must round-trip datasets, tables, rows and jobs
// through Snapshot/Restore just like the memory store (BQ4).
func TestPostgresSnapshotRoundTrip(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres snapshot test")
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
	runSnapshotRoundTrip(t, s, func() snapshotStore { return NewPostgresStore(pg.Pool()) })
}

// TestPostgresDeleteDatasetCascades locks in the cascade: deleting a dataset
// removes its tables and rows so no orphaned rows remain in --dsn mode.
func TestPostgresDeleteDatasetCascades(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres cascade test")
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

	if err := s.CreateDataset(ctx, "proj", Dataset{DatasetID: "d"}); err != nil {
		t.Fatalf("create dataset: %v", err)
	}
	if err := s.CreateTable(ctx, "proj", "d", Table{TableID: "t"}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := s.InsertRows(ctx, "proj", "d", "t", []Row{{Data: []byte(`{"a":1}`)}}); err != nil {
		t.Fatalf("insert rows: %v", err)
	}
	if err := s.DeleteDataset(ctx, "proj", "d"); err != nil {
		t.Fatalf("delete dataset: %v", err)
	}

	tables, err := s.ListTables(ctx, "proj", "d")
	if err != nil || len(tables) != 0 {
		t.Fatalf("expected 0 tables after dataset delete, got %v %d", err, len(tables))
	}
	rows, err := s.ListRows(ctx, "proj", "d", "t")
	if err != nil || len(rows) != 0 {
		t.Fatalf("expected 0 rows after dataset delete, got %v %d", err, len(rows))
	}
}
