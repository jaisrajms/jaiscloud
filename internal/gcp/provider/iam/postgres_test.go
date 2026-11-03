//go:build gcp_persistence

package iam

import (
	"bytes"
	"context"
	"os"
	"testing"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresManagedKeyContract runs the NF15 contract against the PostgreSQL
// backend, so --dsn mode creates the Google-managed key with the account exactly
// as the memory store does and a snapshot/restore (the restart path) keeps it.
func TestPostgresManagedKeyContract(t *testing.T) {
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
	res := store.ResourceStore(pg)
	res.Reset(ctx)
	defer res.Reset(ctx)

	assertManagedKeyContract(t, New(res), "pg-managed")

	// Snapshot/restore round-trip: the managed pair is part of the account's
	// persisted state, so a restart neither loses it nor mints a second one.
	email := createManagedSA(t, New(res), "pg-snap")
	before := listManagedKeys(t, New(res), email)
	if len(before) != 1 {
		t.Fatalf("pre-snapshot keys = %d, want 1", len(before))
	}

	var buf bytes.Buffer
	if err := pg.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	res.Reset(ctx)
	if err := pg.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	p := New(res)
	after := listManagedKeys(t, p, email)
	if len(after) != 1 {
		t.Fatalf("post-restore keys = %d, want 1", len(after))
	}
	requireManagedKey(t, after[0])
	for _, field := range []string{"name", "validAfterTime", "validBeforeTime", "keyType", "keyOrigin"} {
		if before[0][field] != after[0][field] {
			t.Errorf("restored key %s = %v, want %v", field, after[0][field], before[0][field])
		}
	}
	if _, err := p.ServiceAccountSignBlob(ctx, newNR(map[string]any{
		"name": "serviceAccounts/" + email,
		"body": map[string]any{"payload": "aGVsbG8="},
	})); err != nil {
		t.Fatalf("signBlob after restore: %v", err)
	}
	if keys := listManagedKeys(t, p, email); len(keys) != 1 {
		t.Errorf("post-restore signing materialised another key: %d listed", len(keys))
	}
}
