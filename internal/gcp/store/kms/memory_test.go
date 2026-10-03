package kms

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestMemoryStoreKeyMaterial(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	if err := s.CreateKeyRing(ctx, "proj", "global", "kr", KeyRing{Location: "global", ID: "kr"}); err != nil {
		t.Fatalf("create keyring: %v", err)
	}
	if err := s.CreateCryptoKey(ctx, "proj", "global", "kr", "k", CryptoKey{Location: "global", KeyRingID: "kr", ID: "k"}); err != nil {
		t.Fatalf("create cryptokey: %v", err)
	}

	mat, err := s.KeyMaterial(ctx, "proj", "global", "kr", "k", "1")
	if err != nil {
		t.Fatalf("key material: %v", err)
	}
	if len(mat) != 32 {
		t.Fatalf("expected 32-byte key material, got %d", len(mat))
	}

	if _, err := s.KeyMaterial(ctx, "proj", "global", "kr", "k", "missing"); err != ErrNoSuchVersion {
		t.Fatalf("expected ErrNoSuchVersion, got %v", err)
	}

	// Reset regenerates the DEK; old material is gone.
	s.Reset(ctx)
	if _, err := s.KeyMaterial(ctx, "proj", "global", "kr", "k", "1"); err != ErrNoSuchVersion {
		t.Fatalf("expected ErrNoSuchVersion after reset, got %v", err)
	}
}

func TestMemoryStoreKeyRingCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	if err := s.CreateKeyRing(ctx, "proj", "global", "kr", KeyRing{Location: "global", ID: "kr"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.CreateKeyRing(ctx, "proj", "global", "kr", KeyRing{ID: "kr"}); err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	kr, err := s.GetKeyRing(ctx, "proj", "global", "kr")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if kr.ID != "kr" || kr.Location != "global" {
		t.Fatalf("unexpected keyring: %+v", kr)
	}
	if _, err := s.GetKeyRing(ctx, "proj", "global", "missing"); err != ErrNoSuchKeyRing {
		t.Fatalf("expected ErrNoSuchKeyRing, got %v", err)
	}

	s.CreateKeyRing(ctx, "proj", "global", "b", KeyRing{ID: "b"})
	s.CreateKeyRing(ctx, "proj", "global", "a", KeyRing{ID: "a"})
	all, err := s.ListKeyRings(ctx, "proj", "global")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 3 || all[0].ID != "a" || all[2].ID != "kr" {
		t.Fatalf("unexpected list: %+v", all)
	}
}

func TestMemoryStoreCryptoKeyCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	s.CreateKeyRing(ctx, "proj", "global", "kr", KeyRing{ID: "kr"})

	if err := s.CreateCryptoKey(ctx, "proj", "global", "kr", "k", CryptoKey{Location: "global", KeyRingID: "kr", ID: "k", Purpose: "ENCRYPT_DECRYPT"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.CreateCryptoKey(ctx, "proj", "global", "kr", "k", CryptoKey{ID: "k"}); err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	ck, err := s.GetCryptoKey(ctx, "proj", "global", "kr", "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ck.Purpose != "ENCRYPT_DECRYPT" || ck.PrimaryVersion != "1" {
		t.Fatalf("unexpected key: %+v", ck)
	}
	if _, err := s.GetCryptoKey(ctx, "proj", "global", "kr", "missing"); err != ErrNoSuchCryptoKey {
		t.Fatalf("expected ErrNoSuchCryptoKey, got %v", err)
	}

	s.CreateCryptoKey(ctx, "proj", "global", "kr", "b", CryptoKey{ID: "b"})
	s.CreateCryptoKey(ctx, "proj", "global", "kr", "a", CryptoKey{ID: "a"})
	all, err := s.ListCryptoKeys(ctx, "proj", "global", "kr")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 3 || all[0].ID != "a" || all[2].ID != "k" {
		t.Fatalf("unexpected list: %+v", all)
	}
}

// TestMemoryStoreCryptoKeyLabelsRotation verifies the labels/rotation fields
// round-trip through create/get and that UpdateCryptoKeyAtomic merges them
// without disturbing unmasked fields.
func TestMemoryStoreCryptoKeyLabelsRotation(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	create := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := s.CreateKeyRing(ctx, "proj", "global", "kr", KeyRing{ID: "kr"}); err != nil {
		t.Fatalf("create keyring: %v", err)
	}
	if err := s.CreateCryptoKey(ctx, "proj", "global", "kr", "k", CryptoKey{
		Location: "global", KeyRingID: "kr", ID: "k", CreateTime: create,
		Labels:           map[string]string{"env": "test"},
		RotationPeriod:   24 * time.Hour,
		NextRotationTime: create.Add(24 * time.Hour),
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	ck, err := s.GetCryptoKey(ctx, "proj", "global", "kr", "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ck.Labels["env"] != "test" {
		t.Fatalf("labels = %v, want env=test", ck.Labels)
	}
	if ck.RotationPeriod != 24*time.Hour {
		t.Fatalf("rotation period = %v, want 24h", ck.RotationPeriod)
	}
	if !ck.NextRotationTime.Equal(create.Add(24 * time.Hour)) {
		t.Fatalf("next rotation = %v, want %v", ck.NextRotationTime, create.Add(24*time.Hour))
	}

	updated, err := s.UpdateCryptoKeyAtomic(ctx, "proj", "global", "kr", "k", func(cur CryptoKey) (CryptoKey, error) {
		cur.Labels = map[string]string{"env": "prod"}
		return cur, nil
	})
	if err != nil {
		t.Fatalf("atomic update: %v", err)
	}
	if updated.Labels["env"] != "prod" {
		t.Fatalf("labels after update = %v, want env=prod", updated.Labels)
	}
	if updated.RotationPeriod != 24*time.Hour || updated.NextRotationTime.IsZero() {
		t.Fatalf("rotation not preserved by labels-only update: %+v", updated)
	}

	if _, err := s.UpdateCryptoKeyAtomic(ctx, "proj", "global", "kr", "missing", func(c CryptoKey) (CryptoKey, error) {
		return c, nil
	}); err != ErrNoSuchCryptoKey {
		t.Fatalf("missing key err = %v, want ErrNoSuchCryptoKey", err)
	}
}

// TestMemoryStoreUpdateCryptoKeyAtomicNoLostUpdates guards the read-modify-write
// discipline: concurrent label patches on the same key must all survive.
func TestMemoryStoreUpdateCryptoKeyAtomicNoLostUpdates(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	s.CreateKeyRing(ctx, "proj", "global", "kr", KeyRing{ID: "kr"})
	s.CreateCryptoKey(ctx, "proj", "global", "kr", "k", CryptoKey{ID: "k"})

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "label-" + strconv.Itoa(i)
			if _, err := s.UpdateCryptoKeyAtomic(ctx, "proj", "global", "kr", "k", func(cur CryptoKey) (CryptoKey, error) {
				if cur.Labels == nil {
					cur.Labels = map[string]string{}
				}
				cur.Labels[key] = "1"
				return cur, nil
			}); err != nil {
				t.Errorf("update %s: %v", key, err)
			}
		}(i)
	}
	wg.Wait()

	ck, err := s.GetCryptoKey(ctx, "proj", "global", "kr", "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(ck.Labels) != n {
		t.Fatalf("labels = %d, want %d (a lost update slipped through)", len(ck.Labels), n)
	}
}

func TestMemoryStoreVersionRotation(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	s.CreateKeyRing(ctx, "proj", "global", "kr", KeyRing{ID: "kr"})
	s.CreateCryptoKey(ctx, "proj", "global", "kr", "k", CryptoKey{ID: "k"})

	// Create two more versions (rotation).
	v2, err := s.CreateVersion(ctx, "proj", "global", "kr", "k", Version{State: "ENABLED"})
	if err != nil {
		t.Fatalf("create version 2: %v", err)
	}
	if v2 != "2" {
		t.Fatalf("expected version 2, got %q", v2)
	}
	if _, err := s.CreateVersion(ctx, "proj", "global", "kr", "k", Version{}); err != nil {
		t.Fatalf("create version 3: %v", err)
	}

	versions, _ := s.ListVersions(ctx, "proj", "global", "kr", "k")
	if len(versions) != 3 || versions[0].Version != "1" || versions[2].Version != "3" {
		t.Fatalf("unexpected versions: %+v", versions)
	}

	// Each version has distinct key material.
	m1, _ := s.KeyMaterial(ctx, "proj", "global", "kr", "k", "1")
	m2, _ := s.KeyMaterial(ctx, "proj", "global", "kr", "k", "2")
	if string(m1) == string(m2) {
		t.Fatal("expected distinct key material per version")
	}

	// Rotate: set version 2 primary.
	if err := s.UpdatePrimaryVersion(ctx, "proj", "global", "kr", "k", "2"); err != nil {
		t.Fatalf("update primary: %v", err)
	}
	ck, _ := s.GetCryptoKey(ctx, "proj", "global", "kr", "k")
	if ck.PrimaryVersion != "2" {
		t.Fatalf("expected primary 2, got %q", ck.PrimaryVersion)
	}
	if err := s.UpdatePrimaryVersion(ctx, "proj", "global", "kr", "k", "99"); err != ErrNoSuchVersion {
		t.Fatalf("expected ErrNoSuchVersion, got %v", err)
	}

	// Disable/destroy a version.
	if err := s.UpdateVersionState(ctx, "proj", "global", "kr", "k", "1", "DISABLED"); err != nil {
		t.Fatalf("disable: %v", err)
	}
	v, _ := s.GetVersion(ctx, "proj", "global", "kr", "k", "1")
	if v.State != "DISABLED" {
		t.Fatalf("expected DISABLED, got %q", v.State)
	}
}

// TestMemoryStoreConcurrentCreateVersion verifies N concurrent CreateVersion
// calls allocate N distinct version numbers with no error (the store's mutex
// already makes this atomic — the regression guard for the Postgres path's
// racy MAX+1 allocation).
func TestMemoryStoreConcurrentCreateVersion(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	s.CreateKeyRing(ctx, "proj", "global", "kr", KeyRing{ID: "kr"})
	s.CreateCryptoKey(ctx, "proj", "global", "kr", "k", CryptoKey{ID: "k"})

	const n = 16
	var wg sync.WaitGroup
	vers := make([]string, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			vers[i], errs[i] = s.CreateVersion(ctx, "proj", "global", "kr", "k", Version{})
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

	all, err := s.ListVersions(ctx, "proj", "global", "kr", "k")
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(all) != n+1 { // primary version 1 + n created
		t.Fatalf("expected %d total versions, got %d", n+1, len(all))
	}
}
