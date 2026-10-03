package kms

import (
	"context"
	"errors"
	"testing"
	"time"
)

// newDestroyTestKey seeds a memory store with keyring "kr" and crypto key "k"
// (primary version 1) and returns the store plus a fixed "now".
func newDestroyTestKey(t *testing.T) (*MemoryStore, time.Time) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	s := NewMemoryStore()
	if err := s.CreateKeyRing(ctx, "p", "global", "kr", KeyRing{Location: "global", ID: "kr", CreateTime: now}); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if err := s.CreateCryptoKey(ctx, "p", "global", "kr", "k", CryptoKey{
		Location: "global", KeyRingID: "kr", ID: "k", CreateTime: now, Algorithm: defaultAlgorithm,
	}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	return s, now
}

// TestMemoryStoreDestroyPromotion covers the schedule → (not due) → due →
// promote → terminal transition and destroy idempotency.
func TestMemoryStoreDestroyPromotion(t *testing.T) {
	ctx := context.Background()
	s, now := newDestroyTestKey(t)
	destroyAt := now.Add(DefaultDestroyScheduledDuration)

	// Scheduling moves version 1 to DESTROY_SCHEDULED with the given instant.
	v, err := s.DestroyVersion(ctx, "p", "global", "kr", "k", "1", destroyAt)
	if err != nil {
		t.Fatalf("DestroyVersion: %v", err)
	}
	if v.State != "DESTROY_SCHEDULED" || !v.DestroyTime.Equal(destroyAt) {
		t.Fatalf("DestroyVersion = %s @ %v, want DESTROY_SCHEDULED @ %v", v.State, v.DestroyTime, destroyAt)
	}
	if !v.DestroyEventTime.IsZero() {
		t.Fatalf("scheduled version DestroyEventTime = %v, want zero", v.DestroyEventTime)
	}

	// Before the window, nothing promotes.
	if n, err := s.PromoteDestroyed(ctx, "p", "global", "kr", "k", destroyAt.Add(-time.Second)); err != nil || n != 0 {
		t.Fatalf("PromoteDestroyed (early) = %d, %v; want 0, nil", n, err)
	}
	if got, _ := s.GetVersion(ctx, "p", "global", "kr", "k", "1"); got.State != "DESTROY_SCHEDULED" {
		t.Fatalf("state before window = %s, want DESTROY_SCHEDULED", got.State)
	}

	// Destroy is idempotent: a later instant does not move the deadline.
	again, err := s.DestroyVersion(ctx, "p", "global", "kr", "k", "1", destroyAt.Add(time.Hour))
	if err != nil {
		t.Fatalf("DestroyVersion (again): %v", err)
	}
	if again.State != "DESTROY_SCHEDULED" || !again.DestroyTime.Equal(destroyAt) {
		t.Fatalf("second destroy = %s @ %v, want unchanged DESTROY_SCHEDULED @ %v", again.State, again.DestroyTime, destroyAt)
	}

	// At the deadline the version is promoted and records the event time.
	n, err := s.PromoteDestroyed(ctx, "p", "global", "kr", "k", destroyAt)
	if err != nil || n != 1 {
		t.Fatalf("PromoteDestroyed (due) = %d, %v; want 1, nil", n, err)
	}
	got, err := s.GetVersion(ctx, "p", "global", "kr", "k", "1")
	if err != nil {
		t.Fatalf("GetVersion: %v", err)
	}
	if got.State != "DESTROYED" {
		t.Fatalf("state after window = %s, want DESTROYED", got.State)
	}
	if !got.DestroyTime.IsZero() {
		t.Fatalf("promoted DestroyTime = %v, want zero", got.DestroyTime)
	}
	if !got.DestroyEventTime.Equal(destroyAt) {
		t.Fatalf("promoted DestroyEventTime = %v, want %v", got.DestroyEventTime, destroyAt)
	}

	// Promoting again is a no-op.
	if n, err := s.PromoteDestroyed(ctx, "p", "global", "kr", "k", destroyAt.Add(time.Hour)); err != nil || n != 0 {
		t.Fatalf("PromoteDestroyed (again) = %d, %v; want 0, nil", n, err)
	}

	// DESTROYED is terminal: destroy stays idempotent, restore is rejected.
	if v, err := s.DestroyVersion(ctx, "p", "global", "kr", "k", "1", destroyAt.Add(2*time.Hour)); err != nil || v.State != "DESTROYED" {
		t.Fatalf("destroy on DESTROYED = %s, %v; want DESTROYED, nil", v.State, err)
	}
	if _, err := s.RestoreVersion(ctx, "p", "global", "kr", "k", "1"); !errors.Is(err, ErrNotRestorable) {
		t.Fatalf("RestoreVersion on DESTROYED = %v, want ErrNotRestorable", err)
	}
}

// TestMemoryStoreRestoreScheduled covers restoring a scheduled version and
// clearing the destruction timestamps.
func TestMemoryStoreRestoreScheduled(t *testing.T) {
	ctx := context.Background()
	s, now := newDestroyTestKey(t)

	if _, err := s.DestroyVersion(ctx, "p", "global", "kr", "k", "1", now.Add(time.Hour)); err != nil {
		t.Fatalf("DestroyVersion: %v", err)
	}
	restored, err := s.RestoreVersion(ctx, "p", "global", "kr", "k", "1")
	if err != nil {
		t.Fatalf("RestoreVersion: %v", err)
	}
	if restored.State != "DISABLED" {
		t.Fatalf("restored state = %s, want DISABLED", restored.State)
	}
	if !restored.DestroyTime.IsZero() || !restored.DestroyEventTime.IsZero() {
		t.Fatalf("restored timestamps = %v/%v, want zero", restored.DestroyTime, restored.DestroyEventTime)
	}

	// A DISABLED version is not restorable (only DESTROY_SCHEDULED is).
	if _, err := s.RestoreVersion(ctx, "p", "global", "kr", "k", "1"); !errors.Is(err, ErrNotRestorable) {
		t.Fatalf("RestoreVersion on DISABLED = %v, want ErrNotRestorable", err)
	}

	// UpdateVersionState clears any stale destruction timestamps too.
	if _, err := s.DestroyVersion(ctx, "p", "global", "kr", "k", "1", now.Add(time.Hour)); err != nil {
		t.Fatalf("DestroyVersion (2): %v", err)
	}
	if err := s.UpdateVersionState(ctx, "p", "global", "kr", "k", "1", "ENABLED"); err != nil {
		t.Fatalf("UpdateVersionState: %v", err)
	}
	got, _ := s.GetVersion(ctx, "p", "global", "kr", "k", "1")
	if got.State != "ENABLED" || !got.DestroyTime.IsZero() || !got.DestroyEventTime.IsZero() {
		t.Fatalf("after UpdateVersionState = %s %v/%v, want ENABLED with zero timestamps", got.State, got.DestroyTime, got.DestroyEventTime)
	}
}

// TestMemoryStorePromoteUnknownVersion verifies promotion of a version that does
// not exist is a harmless no-op (read paths call it best-effort).
func TestMemoryStorePromoteUnknownVersion(t *testing.T) {
	ctx := context.Background()
	s, now := newDestroyTestKey(t)
	if n, err := s.PromoteDestroyed(ctx, "p", "global", "kr", "k", now); err != nil || n != 0 {
		t.Fatalf("PromoteDestroyed(unknown) = %d, %v; want 0, nil", n, err)
	}
}
