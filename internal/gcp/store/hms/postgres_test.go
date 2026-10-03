//go:build gcp_persistence

package hms

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresStore runs the shared store matrix against Postgres.
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

// TestPostgresTableJSONRoundTrip verifies the full Table JSON survives a
// Postgres Snapshot/Restore round trip. The column is JSONB, so key order and
// whitespace are normalized — compare semantically, not byte-for-byte.
func TestPostgresTableJSONRoundTrip(t *testing.T) {
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

	const tblJSON = `["o",[[1,["s","t1"]],[2,["s","default"]],[9,["m",11,11,[[["s","metadata_location"],["s","gs://bucket/wh/default/t1/metadata/00001.metadata.json"]],["s","table_type"],["s","ICEBERG"]]]]]]`
	if err := s.CreateDatabase(ctx, Database{Name: "default", LocationURI: "gs://bucket/wh/default"}); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := s.CreateTable(ctx, "default", "t1", Table{DBName: "default", TableName: "t1", TableJSON: json.RawMessage(tblJSON)}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	s.Reset(ctx)
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	got, err := s.GetTable(ctx, "default", "t1")
	if err != nil {
		t.Fatalf("get table after restore: %v", err)
	}
	if !jsonEqual(string(got.TableJSON), tblJSON) {
		t.Fatalf("table JSON not preserved after restore:\n got  %s\n want %s", got.TableJSON, tblJSON)
	}
}

// TestPostgresPartitionJSONRoundTrip verifies the full Partition JSON survives a
// Postgres Snapshot/Restore round trip. The column is JSONB, so key order and
// whitespace are normalized — compare semantically, not byte-for-byte.
func TestPostgresPartitionJSONRoundTrip(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres partition snapshot test")
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

	if err := s.CreateDatabase(ctx, Database{Name: "default"}); err != nil {
		t.Fatalf("create database: %v", err)
	}
	if err := s.CreateTable(ctx, "default", "t1", Table{DBName: "default", TableName: "t1", TableJSON: json.RawMessage(`["o",[[1,["s","t1"]]]]`)}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	const partJSONStr = `["o",[[1,["a",11,[["s","2024"],["s","01"]]]],[2,["s","default"]],[3,["s","t1"]],[4,["i",1700000000]],[7,["m",11,11,[[["s","k"],["s","v"]]]]]]]`
	if err := s.CreatePartition(ctx, "default", "t1", Partition{DBName: "default", TableName: "t1", Values: []string{"2024", "01"}, PartJSON: json.RawMessage(partJSONStr)}); err != nil {
		t.Fatalf("create partition: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	s.Reset(ctx)
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	got, err := s.GetPartition(ctx, "default", "t1", []string{"2024", "01"})
	if err != nil {
		t.Fatalf("get partition after restore: %v", err)
	}
	if !jsonEqual(string(got.PartJSON), partJSONStr) {
		t.Fatalf("partition JSON not preserved after restore:\n got  %s\n want %s", got.PartJSON, partJSONStr)
	}
}
