//go:build gcp_persistence

package logging

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

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

// TestPostgresSinkOptionsRoundTrip proves the LogSink fields AUD2-1 added
// survive the dedicated `jc_log_sinks` column path (they are not in the admin
// JSON blob buckets/views use).
func TestPostgresSinkOptionsRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newPostgresStoreForTest(t)

	now := time.Unix(0, 1).UTC()
	in := LogSink{
		Name:                "s1",
		Destination:         "logging.googleapis.com/projects/p",
		Filter:              "severity>=ERROR",
		IncludeChildren:     true,
		InterceptChildren:   true,
		OutputVersionFormat: "V1",
		BigQueryOptions:     &LogBigQueryOptions{UsePartitionedTables: true},
		CreateTime:          now,
		UpdateTime:          now,
	}
	if err := s.CreateSink(ctx, "projects/p", in); err != nil {
		t.Fatalf("CreateSink: %v", err)
	}
	got, err := s.GetSink(ctx, "projects/p", "s1")
	if err != nil {
		t.Fatalf("GetSink: %v", err)
	}
	if !got.InterceptChildren || got.OutputVersionFormat != "V1" {
		t.Fatalf("scalars not persisted: %+v", got)
	}
	if got.BigQueryOptions == nil || !got.BigQueryOptions.UsePartitionedTables {
		t.Fatalf("bigqueryOptions not persisted: %+v", got.BigQueryOptions)
	}

	// Clearing bigqueryOptions must persist as absent, not a stale value.
	in.BigQueryOptions = nil
	if err := s.UpdateSink(ctx, "projects/p", in); err != nil {
		t.Fatalf("UpdateSink: %v", err)
	}
	got, err = s.GetSink(ctx, "projects/p", "s1")
	if err != nil {
		t.Fatalf("GetSink after update: %v", err)
	}
	if got.BigQueryOptions != nil {
		t.Fatalf("bigqueryOptions not cleared: %+v", got.BigQueryOptions)
	}
}

// TestPostgresSnapshotSinkOptions asserts a sink's AUD2-1 fields survive the
// snapshot → reset → restore round-trip (the --dsn export/import path).
func TestPostgresSnapshotSinkOptions(t *testing.T) {
	ctx := context.Background()
	s := newPostgresStoreForTest(t)

	now := time.Unix(0, 1).UTC()
	in := LogSink{
		Name:                "s1",
		Destination:         "logging.googleapis.com/projects/p",
		IncludeChildren:     true,
		InterceptChildren:   true,
		OutputVersionFormat: "V2",
		BigQueryOptions:     &LogBigQueryOptions{UsePartitionedTables: true, UsesTimestampColumnPartitioning: true},
		CreateTime:          now,
		UpdateTime:          now,
	}
	if err := s.CreateSink(ctx, "projects/p", in); err != nil {
		t.Fatalf("CreateSink: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	s.Reset(ctx)
	if err := s.Restore(ctx, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	got, err := s.GetSink(ctx, "projects/p", "s1")
	if err != nil {
		t.Fatalf("GetSink after restore: %v", err)
	}
	if !got.InterceptChildren || got.OutputVersionFormat != "V2" {
		t.Fatalf("scalars lost: %+v", got)
	}
	if got.BigQueryOptions == nil || !got.BigQueryOptions.UsePartitionedTables || !got.BigQueryOptions.UsesTimestampColumnPartitioning {
		t.Fatalf("bigqueryOptions lost: %+v", got.BigQueryOptions)
	}
}
