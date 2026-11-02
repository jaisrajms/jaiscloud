//go:build gcp_persistence

package kms

import (
	"context"
	"os"
	"testing"
	"time"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresListKeyRingsByProject is the Postgres parity case for the
// location-less console aggregate.
func TestPostgresListKeyRingsByProject(t *testing.T) {
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

	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	for _, kr := range []struct{ project, location, id string }{
		{"p1", "europe-west1", "b"},
		{"p1", "global", "z"},
		{"p1", "europe-west1", "a"},
		{"p2", "global", "other"},
	} {
		if err := s.CreateKeyRing(ctx, kr.project, kr.location, kr.id, KeyRing{Location: kr.location, ID: kr.id, CreateTime: now}); err != nil {
			t.Fatalf("CreateKeyRing: %v", err)
		}
	}
	got, err := s.ListKeyRingsByProject(ctx, "p1")
	if err != nil {
		t.Fatalf("ListKeyRingsByProject: %v", err)
	}
	if len(got) != 3 || got[0].ID != "a" || got[2].ID != "z" {
		t.Fatalf("key rings = %+v", got)
	}
}
