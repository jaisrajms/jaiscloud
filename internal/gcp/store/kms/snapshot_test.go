package kms

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestMemoryStoreSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	s.CreateKeyRing(ctx, "proj", "global", "kr", KeyRing{ID: "kr"})
	create := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.CreateCryptoKey(ctx, "proj", "global", "kr", "k", CryptoKey{
		ID: "k", Algorithm: "GOOGLE_SYMMETRIC_ENCRYPTION", CreateTime: create,
		Labels:           map[string]string{"env": "test"},
		RotationPeriod:   24 * time.Hour,
		NextRotationTime: create.Add(24 * time.Hour),
	})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	dst := NewMemoryStore()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	ck, err := dst.GetCryptoKey(ctx, "proj", "global", "kr", "k")
	if err != nil {
		t.Fatalf("restored key missing: %v", err)
	}
	if ck.Labels["env"] != "test" {
		t.Fatalf("restored labels = %v, want env=test", ck.Labels)
	}
	if ck.RotationPeriod != 24*time.Hour || !ck.NextRotationTime.Equal(create.Add(24*time.Hour)) {
		t.Fatalf("restored rotation = (%v, %v), want (24h, %v)", ck.RotationPeriod, ck.NextRotationTime, create.Add(24*time.Hour))
	}
	// Key material must survive (DEK is snapshotted too).
	if _, err := dst.KeyMaterial(ctx, "proj", "global", "kr", "k", "1"); err != nil {
		t.Fatalf("restored key material unreadable: %v", err)
	}
}
