//go:build gcp_persistence

// Package kms_test verifies the Postgres-backed KMS store against a live
// database, exercising the snapshot/restore and concurrent version-allocation
// paths that the memory-store suite cannot cover.
//
// Required env:
//
//	JAISCLOUD_DSN — PostgreSQL DSN
package kms_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	gcpstore "jaiscloud/internal/gcp/store"
	kms "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/store"
)

// newKMSStore opens a Postgres-backed KMS store against the shared "gcp" schema.
func newKMSStore(t *testing.T, dsn string) *kms.PostgresStore {
	t.Helper()
	pg, err := store.NewPostgresResourceStore(context.Background(), dsn, "gcp")
	if err != nil {
		t.Fatalf("postgres store: %v", err)
	}
	t.Cleanup(pg.Close)
	if err := store.RunMigrations(context.Background(), pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("gcp migrations: %v", err)
	}
	return kms.NewPostgresStore(pg.Pool())
}

func kmsIDs() (project, location, keyring, key string) {
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	return "proj", "global", "kr-" + suffix, "k-" + suffix
}

// TestPostgresDEKSnapshotRoundTrip verifies that a KMS snapshot captures the
// raw server DEK so key material survives export→import (Snapshot → Reset →
// Restore) and remains decryptable.
func TestPostgresDEKSnapshotRoundTrip(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping persistence test")
	}

	ctx := context.Background()
	s := newKMSStore(t, dsn)
	project, location, keyring, key := kmsIDs()

	if err := s.CreateKeyRing(ctx, project, location, keyring, kms.KeyRing{Location: location, ID: keyring}); err != nil {
		t.Fatalf("create keyring: %v", err)
	}
	if err := s.CreateCryptoKey(ctx, project, location, keyring, key, kms.CryptoKey{
		Location: location, KeyRingID: keyring, ID: key, Purpose: "ENCRYPT_DECRYPT", Algorithm: "GOOGLE_SYMMETRIC_ENCRYPTION",
	}); err != nil {
		t.Fatalf("create cryptokey: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Wipe the store (including jc_kms_dek) to simulate a fresh destination.
	s.Reset(ctx)

	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	mat, err := s.KeyMaterial(ctx, project, location, keyring, key, "1")
	if err != nil {
		t.Fatalf("restored key material unreadable (DEK lost?): %v", err)
	}
	if len(mat) != 32 {
		t.Fatalf("expected 32-byte key material, got %d", len(mat))
	}
	ct, err := kms.EncryptData(mat, []byte("hello"), []byte("aad"))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	pt, err := kms.DecryptData(mat, ct, []byte("aad"))
	if err != nil || string(pt) != "hello" {
		t.Fatalf("decrypt round-trip failed: pt=%q err=%v", pt, err)
	}
}

// TestPostgresCryptoKeyLabelsRotation verifies labels/rotation persist through
// create/get/atomic-update and survive snapshot → reset → restore.
func TestPostgresCryptoKeyLabelsRotation(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping persistence test")
	}

	ctx := context.Background()
	s := newKMSStore(t, dsn)
	project, location, keyring, key := kmsIDs()
	create := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	if err := s.CreateKeyRing(ctx, project, location, keyring, kms.KeyRing{Location: location, ID: keyring}); err != nil {
		t.Fatalf("create keyring: %v", err)
	}
	if err := s.CreateCryptoKey(ctx, project, location, keyring, key, kms.CryptoKey{
		Location: location, KeyRingID: keyring, ID: key, Algorithm: "GOOGLE_SYMMETRIC_ENCRYPTION",
		CreateTime: create, Labels: map[string]string{"env": "test"},
		RotationPeriod: 24 * time.Hour, NextRotationTime: create.Add(24 * time.Hour),
	}); err != nil {
		t.Fatalf("create cryptokey: %v", err)
	}

	got, err := s.GetCryptoKey(ctx, project, location, keyring, key)
	if err != nil {
		t.Fatalf("get cryptokey: %v", err)
	}
	if got.Labels["env"] != "test" || got.RotationPeriod != 24*time.Hour || !got.NextRotationTime.Equal(create.Add(24*time.Hour)) {
		t.Fatalf("round-trip mismatch: %+v", got)
	}

	updated, err := s.UpdateCryptoKeyAtomic(ctx, project, location, keyring, key, func(cur kms.CryptoKey) (kms.CryptoKey, error) {
		cur.Labels = map[string]string{"env": "prod"}
		return cur, nil
	})
	if err != nil {
		t.Fatalf("atomic update: %v", err)
	}
	if updated.Labels["env"] != "prod" || updated.RotationPeriod != 24*time.Hour || updated.NextRotationTime.IsZero() {
		t.Fatalf("labels-only update lost rotation: %+v", updated)
	}

	if _, err := s.UpdateCryptoKeyAtomic(ctx, project, location, keyring, "missing", func(c kms.CryptoKey) (kms.CryptoKey, error) {
		return c, nil
	}); err != kms.ErrNoSuchCryptoKey {
		t.Fatalf("missing key err = %v, want ErrNoSuchCryptoKey", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	s.Reset(ctx)
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}

	restored, err := s.GetCryptoKey(ctx, project, location, keyring, key)
	if err != nil {
		t.Fatalf("restored key missing: %v", err)
	}
	if restored.Labels["env"] != "prod" {
		t.Fatalf("restored labels = %v, want env=prod", restored.Labels)
	}
	if restored.RotationPeriod != 24*time.Hour || !restored.NextRotationTime.Equal(create.Add(24*time.Hour)) {
		t.Fatalf("restored rotation = (%v, %v), want (24h, %v)", restored.RotationPeriod, restored.NextRotationTime, create.Add(24*time.Hour))
	}
}

// TestPostgresRotateIfDue verifies lazy rotation persists the new primary and
// advanced schedule through UpdateCryptoKeyAtomic. This specifically covers the
// atomic UPDATE writing primary_version (not just labels/rotation), which the
// memory store already did.
func TestPostgresRotateIfDue(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping persistence test")
	}

	ctx := context.Background()
	s := newKMSStore(t, dsn)
	project, location, keyring, key := kmsIDs()
	t0 := time.Now().UTC().Truncate(time.Second)

	if err := s.CreateKeyRing(ctx, project, location, keyring, kms.KeyRing{Location: location, ID: keyring, CreateTime: t0}); err != nil {
		t.Fatalf("create keyring: %v", err)
	}
	if err := s.CreateCryptoKey(ctx, project, location, keyring, key, kms.CryptoKey{
		Location: location, KeyRingID: keyring, ID: key, Purpose: "ENCRYPT_DECRYPT",
		CreateTime: t0, PrimaryVersion: "1", Algorithm: "GOOGLE_SYMMETRIC_ENCRYPTION",
		RotationPeriod: time.Hour, NextRotationTime: t0.Add(time.Hour),
	}); err != nil {
		t.Fatalf("create cryptokey: %v", err)
	}

	due := t0.Add(2 * time.Hour)
	if v := kms.RotateIfDue(ctx, s, project, location, keyring, key, due); v != "2" {
		t.Fatalf("RotateIfDue = %q, want 2", v)
	}
	ck, err := s.GetCryptoKey(ctx, project, location, keyring, key)
	if err != nil {
		t.Fatalf("get cryptokey: %v", err)
	}
	if ck.PrimaryVersion != "2" {
		t.Fatalf("primary_version = %q, want 2 (atomic update dropped it?)", ck.PrimaryVersion)
	}
	if !ck.NextRotationTime.Equal(t0.Add(3 * time.Hour)) {
		t.Fatalf("nextRotationTime = %v, want %v", ck.NextRotationTime, t0.Add(3*time.Hour))
	}
	vers, err := s.ListVersions(ctx, project, location, keyring, key)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(vers) != 2 {
		t.Fatalf("versions after rotation = %d, want 2", len(vers))
	}

	// Idempotent: the same `now` is no longer due once the schedule advanced.
	if v := kms.RotateIfDue(ctx, s, project, location, keyring, key, due); v != "" {
		t.Fatalf("second RotateIfDue = %q, want empty", v)
	}
	if vers, _ := s.ListVersions(ctx, project, location, keyring, key); len(vers) != 2 {
		t.Fatalf("versions after second call = %d, want 2", len(vers))
	}
}

// TestPostgresConcurrentCreateVersion verifies concurrent CreateVersion calls
// allocate distinct version numbers without a unique-violation.
func TestPostgresConcurrentCreateVersion(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping persistence test")
	}

	ctx := context.Background()
	s := newKMSStore(t, dsn)
	project, location, keyring, key := kmsIDs()

	if err := s.CreateKeyRing(ctx, project, location, keyring, kms.KeyRing{Location: location, ID: keyring}); err != nil {
		t.Fatalf("create keyring: %v", err)
	}
	if err := s.CreateCryptoKey(ctx, project, location, keyring, key, kms.CryptoKey{
		Location: location, KeyRingID: keyring, ID: key, Algorithm: "GOOGLE_SYMMETRIC_ENCRYPTION",
	}); err != nil {
		t.Fatalf("create cryptokey: %v", err)
	}

	const n = 16
	vers := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			vers[i], errs[i] = s.CreateVersion(ctx, project, location, keyring, key, kms.Version{})
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: unexpected error: %v", i, errs[i])
		}
	}
	seen := map[string]bool{}
	for _, v := range vers {
		if seen[v] {
			t.Fatalf("duplicate version %q allocated", v)
		}
		seen[v] = true
	}
	if len(seen) != n {
		t.Fatalf("expected %d distinct versions, got %d", n, len(seen))
	}

	all, err := s.ListVersions(ctx, project, location, keyring, key)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(all) != n+1 {
		t.Fatalf("expected %d total versions (primary + %d), got %d", n+1, n, len(all))
	}
}

// TestPostgresDeleteVersionAndCryptoKey verifies the Postgres delete paths
// (DeleteVersion / DeleteCryptoKey) and that both deletions survive a
// snapshot → reset → restore.
func TestPostgresDeleteVersionAndCryptoKey(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping persistence test")
	}

	ctx := context.Background()
	s := newKMSStore(t, dsn)
	project, location, keyring, key := kmsIDs()

	if err := s.CreateKeyRing(ctx, project, location, keyring, kms.KeyRing{Location: location, ID: keyring}); err != nil {
		t.Fatalf("create keyring: %v", err)
	}
	if err := s.CreateCryptoKey(ctx, project, location, keyring, key, kms.CryptoKey{
		Location: location, KeyRingID: keyring, ID: key, Algorithm: "GOOGLE_SYMMETRIC_ENCRYPTION",
	}); err != nil {
		t.Fatalf("create cryptokey: %v", err)
	}

	// Delete a non-primary version; it must be gone and stay gone across a
	// snapshot/restore.
	extra, err := s.CreateVersion(ctx, project, location, keyring, key, kms.Version{})
	if err != nil {
		t.Fatalf("create version: %v", err)
	}
	if err := s.DeleteVersion(ctx, project, location, keyring, key, extra); err != nil {
		t.Fatalf("delete version: %v", err)
	}
	if _, err := s.GetVersion(ctx, project, location, keyring, key, extra); err != kms.ErrNoSuchVersion {
		t.Fatalf("get deleted version err = %v, want ErrNoSuchVersion", err)
	}
	if err := s.DeleteVersion(ctx, project, location, keyring, key, extra); err != kms.ErrNoSuchVersion {
		t.Fatalf("re-delete version err = %v, want ErrNoSuchVersion", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	s.Reset(ctx)
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if _, err := s.GetVersion(ctx, project, location, keyring, key, extra); err != kms.ErrNoSuchVersion {
		t.Fatalf("restored deleted version err = %v, want ErrNoSuchVersion", err)
	}

	// Delete every remaining version, then the crypto key itself.
	versions, err := s.ListVersions(ctx, project, location, keyring, key)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(versions) == 0 {
		t.Fatal("expected at least the primary version")
	}
	for _, v := range versions {
		if err := s.DeleteVersion(ctx, project, location, keyring, key, v.Version); err != nil {
			t.Fatalf("delete version %s: %v", v.Version, err)
		}
	}
	if err := s.DeleteCryptoKey(ctx, project, location, keyring, key); err != nil {
		t.Fatalf("delete cryptokey: %v", err)
	}
	if _, err := s.GetCryptoKey(ctx, project, location, keyring, key); err != kms.ErrNoSuchCryptoKey {
		t.Fatalf("get deleted cryptokey err = %v, want ErrNoSuchCryptoKey", err)
	}
	if err := s.DeleteCryptoKey(ctx, project, location, keyring, key); err != kms.ErrNoSuchCryptoKey {
		t.Fatalf("re-delete cryptokey err = %v, want ErrNoSuchCryptoKey", err)
	}

	// The key deletion must survive a snapshot/restore too.
	var buf2 bytes.Buffer
	if err := s.Snapshot(ctx, &buf2); err != nil {
		t.Fatalf("snapshot 2: %v", err)
	}
	s.Reset(ctx)
	if err := s.Restore(ctx, &buf2); err != nil {
		t.Fatalf("restore 2: %v", err)
	}
	if _, err := s.GetCryptoKey(ctx, project, location, keyring, key); err != kms.ErrNoSuchCryptoKey {
		t.Fatalf("restored deleted cryptokey err = %v, want ErrNoSuchCryptoKey", err)
	}
}
