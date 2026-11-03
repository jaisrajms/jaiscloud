//go:build gcp_persistence

package hms

import (
	"bytes"
	"context"
	"os"
	"testing"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresCatalogs runs the multi-catalog matrix against Postgres, so the
// memory and --dsn backends are held to the same per-Service isolation,
// per-catalog locks and snapshot behaviour.
func TestPostgresCatalogs(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres catalogs test")
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

	fresh := func() Catalogs {
		c := NewPostgresCatalogs(pg.Pool())
		c.Reset(ctx)
		return c
	}

	t.Run("isolation", func(t *testing.T) { runCatalogIsolation(t, fresh) })
	t.Run("snapshot", func(t *testing.T) { runCatalogSnapshotRoundTrip(t, fresh) })
	t.Run("legacy flat snapshot", func(t *testing.T) {
		// A version 1 document from this backend is the flat single-catalog
		// shape (arrays, not the memory backend's maps).
		legacy := NewPostgresStore(pg.Pool())
		legacy.Reset(ctx)
		if err := legacy.CreateDatabase(ctx, Database{Name: "old"}); err != nil {
			t.Fatalf("create legacy database: %v", err)
		}
		var flat bytes.Buffer
		if err := legacy.Snapshot(ctx, &flat); err != nil {
			t.Fatalf("snapshot legacy catalog: %v", err)
		}
		runLegacyFlatRestore(t, fresh(), flat.Bytes())
	})
}
