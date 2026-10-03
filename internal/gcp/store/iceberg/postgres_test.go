//go:build gcp_persistence

package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresStore runs the shared store matrix against Postgres and verifies
// metadata JSON survives a Snapshot/Restore round trip verbatim.
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

// TestPostgresStoreSnapshotVerbatim verifies the TableMetadata JSON and the
// metadata-location pointer survive a Postgres Snapshot/Restore round trip.
func TestPostgresStoreSnapshotVerbatim(t *testing.T) {
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

	const metadata = `{"format-version":2,"table-uuid":"u-1","location":"s3://w/db/t1","properties":{"a":"b"}}`
	if err := s.CreateNamespace(ctx, "db", map[string]string{"owner": "me"}); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if err := s.CreateTable(ctx, "db", "t1", Table{
		Metadata:         json.RawMessage(metadata),
		MetadataLocation: "s3://w/db/t1/metadata/00000-u-1.metadata.json",
		UUID:             "u-1",
		Version:          0,
	}); err != nil {
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

	props, err := s.GetNamespace(ctx, "db")
	if err != nil || props["owner"] != "me" {
		t.Fatalf("namespace lost after restore: %v %v", err, props)
	}
	got, err := s.GetTable(ctx, "db", "t1")
	if err != nil {
		t.Fatalf("get table after restore: %v", err)
	}
	if !jsonEqual(string(got.Metadata), metadata) {
		t.Fatalf("metadata not preserved verbatim: %s", got.Metadata)
	}
	if got.MetadataLocation != "s3://w/db/t1/metadata/00000-u-1.metadata.json" {
		t.Fatalf("metadata-location lost: %s", got.MetadataLocation)
	}
}

// jsonEqual reports whether two JSON documents are semantically equal, ignoring
// object key order (JSONB normalizes key order on the round trip).
func jsonEqual(a, b string) bool {
	var av, bv any
	if json.Unmarshal([]byte(a), &av) != nil || json.Unmarshal([]byte(b), &bv) != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}
