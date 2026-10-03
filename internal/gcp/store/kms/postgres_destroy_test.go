//go:build gcp_persistence

package kms

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresDestroyLifecycle exercises the destruction timing columns
// (destroy_time / destroy_event_time) against the Postgres backend, including
// idempotent destroy, lazy promotion, restore, and terminal-state rejection.
// It is gated on JAISCLOUD_DSN (skipped otherwise), matching the other
// gcp_persistence store tests.
func TestPostgresDestroyLifecycle(t *testing.T) {
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
	if err := s.CreateKeyRing(ctx, "p", "global", "kr", KeyRing{Location: "global", ID: "kr", CreateTime: now}); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if err := s.CreateCryptoKey(ctx, "p", "global", "kr", "k", CryptoKey{
		Location: "global", KeyRingID: "kr", ID: "k", CreateTime: now, Algorithm: defaultAlgorithm,
	}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	destroyAt := now.Add(DefaultDestroyScheduledDuration)

	v, err := s.DestroyVersion(ctx, "p", "global", "kr", "k", "1", destroyAt)
	if err != nil {
		t.Fatalf("DestroyVersion: %v", err)
	}
	if v.State != "DESTROY_SCHEDULED" || !v.DestroyTime.Equal(destroyAt) {
		t.Fatalf("DestroyVersion = %s @ %v, want DESTROY_SCHEDULED @ %v", v.State, v.DestroyTime, destroyAt)
	}

	// The scheduled state and destroy_time survive a fresh read.
	got, err := s.GetVersion(ctx, "p", "global", "kr", "k", "1")
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if got.State != "DESTROY_SCHEDULED" || !got.DestroyTime.Equal(destroyAt) {
		t.Fatalf("GetVersion = %s @ %v, want DESTROY_SCHEDULED @ %v", got.State, got.DestroyTime, destroyAt)
	}

	// Early promotion is a no-op; due promotion flips to DESTROYED.
	if n, err := s.PromoteDestroyed(ctx, "p", "global", "kr", "k", destroyAt.Add(-time.Second)); err != nil || n != 0 {
		t.Fatalf("PromoteDestroyed (early) = %d, %v; want 0, nil", n, err)
	}
	if n, err := s.PromoteDestroyed(ctx, "p", "global", "kr", "k", destroyAt); err != nil || n != 1 {
		t.Fatalf("PromoteDestroyed (due) = %d, %v; want 1, nil", n, err)
	}
	got, err = s.GetVersion(ctx, "p", "global", "kr", "k", "1")
	if err != nil {
		t.Fatalf("GetVersion (promoted): %v", err)
	}
	if got.State != "DESTROYED" || !got.DestroyTime.IsZero() || !got.DestroyEventTime.Equal(destroyAt) {
		t.Fatalf("promoted = %s destroy_time=%v event_time=%v, want DESTROYED/zero/%v",
			got.State, got.DestroyTime, got.DestroyEventTime, destroyAt)
	}

	// DESTROYED is terminal: restore is rejected.
	if _, err := s.RestoreVersion(ctx, "p", "global", "kr", "k", "1"); !errors.Is(err, ErrNotRestorable) {
		t.Fatalf("RestoreVersion on DESTROYED = %v, want ErrNotRestorable", err)
	}
}
