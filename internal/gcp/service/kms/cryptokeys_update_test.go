package kms

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	kmsstore "jaiscloud/internal/gcp/store/kms"
)

// TestCoreCryptoKeyRotationValidation covers the rotation-schedule invariants
// the core enforces on create: only ENCRYPT_DECRYPT keys rotate, rotation_period
// is bounded [24h, 876000h] and requires next_rotation_time, and
// next_rotation_time must be in the future.
func TestCoreCryptoKeyRotationValidation(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	ctx := context.Background()
	core, _ := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	future := t0.Add(24 * time.Hour)
	past := t0.Add(-time.Hour)

	negatives := []struct {
		name string
		id   string
		in   CryptoKeyInput
	}{
		{"periodWithoutNext", "p1", CryptoKeyInput{RotationPeriod: 24 * time.Hour}},
		{"periodBelowMin", "p2", CryptoKeyInput{RotationPeriod: 23 * time.Hour, NextRotationTime: &future}},
		{"periodAboveMax", "p3", CryptoKeyInput{RotationPeriod: 876001 * time.Hour, NextRotationTime: &future}},
		{"nextInPast", "p4", CryptoKeyInput{NextRotationTime: &past}},
		{"asymmetricRotation", "p5", CryptoKeyInput{Purpose: "ASYMMETRIC_SIGN", RotationPeriod: 24 * time.Hour, NextRotationTime: &future}},
		{"asymmetricNextOnly", "p6", CryptoKeyInput{Purpose: "ASYMMETRIC_SIGN", NextRotationTime: &future}},
	}
	for _, tc := range negatives {
		if _, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", tc.id, tc.in); err == nil {
			t.Fatalf("%s: CreateCryptoKey err = nil, want INVALID_ARGUMENT", tc.name)
		}
	}

	// A valid schedule round-trips; the core stores the caller's next rotation
	// time rather than deriving one.
	ck, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyInput{RotationPeriod: 24 * time.Hour, NextRotationTime: &future})
	if err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	if ck.RotationPeriod != 24*time.Hour || !ck.NextRotationTime.Equal(future) {
		t.Fatalf("created schedule = %v/%v, want 24h/%v", ck.RotationPeriod, ck.NextRotationTime, future)
	}

	// next_rotation_time alone (no period) is allowed.
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "nrt", CryptoKeyInput{NextRotationTime: &future}); err != nil {
		t.Fatalf("CreateCryptoKey next-only: %v", err)
	}
}

// TestCoreUpdateCryptoKeyMasks covers the mask contract on the shared core: the
// mask is required (an explicitly empty one is a no-op), only writable paths are
// accepted, only masked fields are validated/merged, and the rotation invariants
// hold on the merged result.
func TestCoreUpdateCryptoKeyMasks(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	ctx := context.Background()
	core, _ := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if _, err := core.CreateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyInput{Labels: map[string]string{"env": "dev"}}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	key := func() kmsstore.CryptoKey {
		k, err := core.GetCryptoKey(ctx, "proj", "global", "kr", "sym")
		if err != nil {
			t.Fatalf("GetCryptoKey: %v", err)
		}
		return k
	}

	// A nil mask (field omitted) is required; an explicitly empty mask is a no-op.
	if _, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{Labels: map[string]string{"a": "b"}}); err == nil {
		t.Fatal("nil mask: err = nil, want INVALID_ARGUMENT")
	}
	if _, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{UpdateMask: []string{}, Labels: map[string]string{"a": "b"}}); err != nil {
		t.Fatalf("empty mask: %v", err)
	}
	if got := key().Labels["a"]; got != "" {
		t.Fatalf("empty mask applied labels: %v", key().Labels)
	}

	// A path outside the writable set is rejected, in either spelling.
	for _, path := range []string{"purpose", "rotationPeriodx"} {
		if _, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{UpdateMask: []string{path}}); err == nil {
			t.Fatalf("path %q: err = nil, want INVALID_ARGUMENT", path)
		}
	}

	// A labels mask replaces labels wholesale.
	updated, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{UpdateMask: []string{"labels"}, Labels: map[string]string{"env": "prod"}})
	if err != nil {
		t.Fatalf("labels mask: %v", err)
	}
	if updated.Labels["env"] != "prod" {
		t.Fatalf("labels = %v, want env=prod", updated.Labels)
	}

	// Setting rotation_period on a key with no next rotation time is rejected.
	if _, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{UpdateMask: []string{"rotationPeriod"}, RotationPeriod: durPtr(24 * time.Hour)}); err == nil {
		t.Fatal("period without next rotation time: err = nil, want INVALID_ARGUMENT")
	}

	// A camelCase period + next-rotation-time pair applies.
	later := t0.Add(24 * time.Hour)
	updated, err = core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{
		UpdateMask:       []string{"rotationPeriod", "nextRotationTime"},
		RotationPeriod:   durPtr(24 * time.Hour),
		NextRotationTime: &later,
	})
	if err != nil {
		t.Fatalf("rotation schedule mask: %v", err)
	}
	if updated.RotationPeriod != 24*time.Hour || !updated.NextRotationTime.Equal(later) {
		t.Fatalf("schedule = %v/%v, want 24h/%v", updated.RotationPeriod, updated.NextRotationTime, later)
	}

	// An out-of-bounds period is rejected on update too.
	if _, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{UpdateMask: []string{"rotation_period"}, RotationPeriod: durPtr(time.Hour)}); err == nil {
		t.Fatal("sub-24h update period: err = nil, want INVALID_ARGUMENT")
	}

	// Clearing next_rotation_time while a period is set is rejected; clearing the
	// period keeps next_rotation_time (the fields are independent).
	if _, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{UpdateMask: []string{"next_rotation_time"}}); err == nil {
		t.Fatal("clear next rotation time with a period: err = nil, want INVALID_ARGUMENT")
	}
	cleared, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{UpdateMask: []string{"rotation_period"}})
	if err != nil {
		t.Fatalf("clear period: %v", err)
	}
	if cleared.RotationPeriod != 0 || !cleared.NextRotationTime.Equal(later) {
		t.Fatalf("cleared = %v/%v, want no period and next %v", cleared.RotationPeriod, cleared.NextRotationTime, later)
	}

	// Unmasked body fields are ignored, not validated (AIP-134).
	if _, err := core.UpdateCryptoKey(ctx, "proj", "global", "kr", "sym", CryptoKeyUpdate{
		UpdateMask:       []string{"labels"},
		Labels:           map[string]string{"unmasked": "true"},
		RotationPeriod:   durPtr(time.Second),
		NextRotationTime: &t0,
	}); err != nil {
		t.Fatalf("unmasked invalid rotation fields: %v", err)
	}
}

func durPtr(d time.Duration) *time.Duration { return &d }
