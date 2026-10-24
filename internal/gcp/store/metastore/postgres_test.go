//go:build gcp_persistence

package metastore

import (
	"bytes"
	"context"
	"os"
	"testing"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresStore runs the shared store matrix against the Postgres backend so
// the two backends are held to the identical contract (the matrix covers the
// CRUD + atomic-update paths; the snapshot round trip has its own test below).
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

// TestPostgresStoreSnapshotVerbatim verifies that service config, backup
// config/description, and import config/description survive a Postgres
// Snapshot/Restore round trip.
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

	const config = `{"network":"default","hiveMetastoreConfig":{"version":"3.1.2","configOverrides":{"a":"b"}}}`
	svc := Service{Name: "s1", Config: []byte(config), Labels: map[string]string{"k": "v"}, State: "ACTIVE"}
	if err := s.CreateService(ctx, "proj", "us-central1", svc); err != nil {
		t.Fatalf("create service: %v", err)
	}
	const bConfig = `{"description":"nightly"}`
	if err := s.CreateBackup(ctx, "proj", "us-central1", "s1", Backup{Name: "b1", Config: []byte(bConfig), Description: "nightly", State: "ACTIVE"}); err != nil {
		t.Fatalf("create backup: %v", err)
	}
	const mConfig = `{"databaseDump":{"gcsUri":"gs://b/dump.sql"}}`
	if err := s.CreateMetadataImport(ctx, "proj", "us-central1", "s1", MetadataImport{Name: "m1", Config: []byte(mConfig), State: "SUCCEEDED"}); err != nil {
		t.Fatalf("create import: %v", err)
	}
	_ = s.CreateOperation(ctx, "proj", "us-central1", Operation{ID: "op1", Done: true, Metadata: `{"@type":"t"}`})
	const fConfig = `{"version":"3.1.2"}`
	if err := s.CreateFederation(ctx, "proj", "us-central1", Federation{Name: "f1", Config: []byte(fConfig), Labels: map[string]string{"k": "v"}, State: "ACTIVE"}); err != nil {
		t.Fatalf("create federation: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	s.Reset(ctx)
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	got, err := s.GetService(ctx, "proj", "us-central1", "s1")
	if err != nil {
		t.Fatalf("get service: %v", err)
	}
	if !jsonEqual(string(got.Config), config) {
		t.Fatalf("config not preserved: %q", got.Config)
	}
	if got.Labels["k"] != "v" || got.State != "ACTIVE" {
		t.Fatalf("service fields lost: %+v", got)
	}

	gotB, err := s.GetBackup(ctx, "proj", "us-central1", "s1", "b1")
	if err != nil || gotB.Description != "nightly" || !jsonEqual(string(gotB.Config), bConfig) {
		t.Fatalf("backup lost after restore: %v %+v", err, gotB)
	}

	gotM, err := s.GetMetadataImport(ctx, "proj", "us-central1", "s1", "m1")
	if err != nil || gotM.State != "SUCCEEDED" || !jsonEqual(string(gotM.Config), mConfig) {
		t.Fatalf("import lost after restore: %v %+v", err, gotM)
	}

	gotF, err := s.GetFederation(ctx, "proj", "us-central1", "f1")
	if err != nil || gotF.State != "ACTIVE" || !jsonEqual(string(gotF.Config), fConfig) {
		t.Fatalf("federation lost after restore: %v %+v", err, gotF)
	}
}
