//go:build gcp_persistence

package kms

import (
	"context"
	"os"
	"testing"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresDeleteKeyRing is the Postgres parity case for TF5-4's
// DeleteKeyRing: the ring row is removed (ErrNoSuchKeyRing afterwards), the
// name is reusable, and a re-created ring of the same name starts without the
// previous ring's child rows.
func TestPostgresDeleteKeyRing(t *testing.T) {
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

	if err := s.CreateKeyRing(ctx, "p", "global", "kr", KeyRing{Location: "global", ID: "kr"}); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if err := s.CreateCryptoKey(ctx, "p", "global", "kr", "k", CryptoKey{Location: "global", KeyRingID: "kr", ID: "k"}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}

	if err := s.DeleteKeyRing(ctx, "p", "global", "missing"); err != ErrNoSuchKeyRing {
		t.Fatalf("delete missing key ring = %v, want ErrNoSuchKeyRing", err)
	}
	if err := s.DeleteKeyRing(ctx, "p", "global", "kr"); err != nil {
		t.Fatalf("DeleteKeyRing: %v", err)
	}
	if _, err := s.GetKeyRing(ctx, "p", "global", "kr"); err != ErrNoSuchKeyRing {
		t.Fatalf("get deleted key ring = %v, want ErrNoSuchKeyRing", err)
	}

	if err := s.CreateKeyRing(ctx, "p", "global", "kr", KeyRing{Location: "global", ID: "kr"}); err != nil {
		t.Fatalf("re-create the deleted key ring name: %v", err)
	}
	if keys, err := s.ListCryptoKeys(ctx, "p", "global", "kr"); err != nil || len(keys) != 0 {
		t.Fatalf("re-created key ring children = %v, %v, want none", keys, err)
	}
}
