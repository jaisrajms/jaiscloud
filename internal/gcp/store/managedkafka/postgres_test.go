//go:build gcp_persistence

package managedkafka

import (
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
}

// TestPostgresDeleteClusterCascadesTopics locks in the H3 fix: deleting a
// cluster removes its topics, so no orphaned topic rows remain in --dsn mode.
func TestPostgresDeleteClusterCascadesTopics(t *testing.T) {
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

	if err := s.CreateCluster(ctx, "proj", "us-central1", Cluster{Name: "c1"}); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	if err := s.CreateTopic(ctx, "proj", "us-central1", "c1", Topic{Name: "t1", PartitionCount: 3, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := s.DeleteCluster(ctx, "proj", "us-central1", "c1"); err != nil {
		t.Fatalf("delete cluster: %v", err)
	}

	topics, err := s.ListTopics(ctx, "proj", "us-central1", "c1")
	if err != nil {
		t.Fatalf("list topics after delete: %v", err)
	}
	if len(topics) != 0 {
		t.Fatalf("expected 0 topics after cluster delete, got %d", len(topics))
	}
}
