package kms

import (
	"context"
	"testing"
	"time"
)

// seedRotatingKey creates a keyring + crypto key with a rotation schedule and
// returns the store and the key's next-rotation timestamp.
func seedRotatingKey(t *testing.T, s *MemoryStore, id string, period time.Duration, next time.Time) {
	t.Helper()
	ctx := context.Background()
	if err := s.CreateKeyRing(ctx, "proj", "global", "kr", KeyRing{Location: "global", ID: "kr", CreateTime: next.Add(-period)}); err != nil {
		t.Fatalf("create keyring: %v", err)
	}
	if err := s.CreateCryptoKey(ctx, "proj", "global", "kr", id, CryptoKey{
		Location:         "global",
		KeyRingID:        "kr",
		ID:               id,
		Purpose:          "ENCRYPT_DECRYPT",
		CreateTime:       next.Add(-period),
		PrimaryVersion:   "1",
		Algorithm:        "GOOGLE_SYMMETRIC_ENCRYPTION",
		RotationPeriod:   period,
		NextRotationTime: next,
	}); err != nil {
		t.Fatalf("create cryptokey: %v", err)
	}
}

func TestRotateIfDue(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	seedRotatingKey(t, s, "k", time.Hour, t0.Add(time.Hour))

	// Not due yet: no rotation, primary unchanged, still one version.
	if got := RotateIfDue(ctx, s, "proj", "global", "kr", "k", t0); got != "" {
		t.Fatalf("RotateIfDue(not due) = %q, want empty", got)
	}
	ck, err := s.GetCryptoKey(ctx, "proj", "global", "kr", "k")
	if err != nil {
		t.Fatalf("GetCryptoKey: %v", err)
	}
	if ck.PrimaryVersion != "1" {
		t.Fatalf("primary after not-due = %q, want 1", ck.PrimaryVersion)
	}
	if vers, _ := s.ListVersions(ctx, "proj", "global", "kr", "k"); len(vers) != 1 {
		t.Fatalf("versions after not-due = %d, want 1", len(vers))
	}

	// Due at t0+2h: rotate to version 2 and advance the schedule.
	due := t0.Add(2 * time.Hour)
	if got := RotateIfDue(ctx, s, "proj", "global", "kr", "k", due); got != "2" {
		t.Fatalf("RotateIfDue(due) = %q, want 2", got)
	}
	ck, err = s.GetCryptoKey(ctx, "proj", "global", "kr", "k")
	if err != nil {
		t.Fatalf("GetCryptoKey: %v", err)
	}
	if ck.PrimaryVersion != "2" {
		t.Fatalf("primary after rotation = %q, want 2", ck.PrimaryVersion)
	}
	if ck.NextRotationTime != due.Add(time.Hour) {
		t.Fatalf("nextRotationTime = %v, want %v", ck.NextRotationTime, due.Add(time.Hour))
	}
	vers, err := s.ListVersions(ctx, "proj", "global", "kr", "k")
	if err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if len(vers) != 2 {
		t.Fatalf("versions after rotation = %d, want 2", len(vers))
	}

	// A second call at the same (now past) due time is a no-op: the schedule
	// has advanced beyond `due`, so no third version is created.
	if got := RotateIfDue(ctx, s, "proj", "global", "kr", "k", due); got != "" {
		t.Fatalf("RotateIfDue(second call) = %q, want empty", got)
	}
	if vers, _ := s.ListVersions(ctx, "proj", "global", "kr", "k"); len(vers) != 2 {
		t.Fatalf("versions after second call = %d, want 2", len(vers))
	}
}

func TestRotateIfDueDisabledPeriod(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)

	// RotationPeriod == 0 disables rotation even if NextRotationTime is set.
	seedRotatingKey(t, s, "disabled", 0, t0)

	if got := RotateIfDue(ctx, s, "proj", "global", "kr", "disabled", t0.Add(48*time.Hour)); got != "" {
		t.Fatalf("RotateIfDue(disabled) = %q, want empty", got)
	}
	ck, err := s.GetCryptoKey(ctx, "proj", "global", "kr", "disabled")
	if err != nil {
		t.Fatalf("GetCryptoKey: %v", err)
	}
	if ck.PrimaryVersion != "1" {
		t.Fatalf("primary after disabled rotation = %q, want 1", ck.PrimaryVersion)
	}
	if vers, _ := s.ListVersions(ctx, "proj", "global", "kr", "disabled"); len(vers) != 1 {
		t.Fatalf("versions after disabled rotation = %d, want 1", len(vers))
	}
}
