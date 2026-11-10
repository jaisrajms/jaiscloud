package kms

import (
	"context"
	"testing"
)

func TestMemoryStoreDeleteVersionAndCryptoKey(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	if err := s.CreateKeyRing(ctx, "proj", "global", "kr", KeyRing{Location: "global", ID: "kr"}); err != nil {
		t.Fatalf("create keyring: %v", err)
	}
	if err := s.CreateCryptoKey(ctx, "proj", "global", "kr", "k", CryptoKey{Location: "global", KeyRingID: "kr", ID: "k"}); err != nil {
		t.Fatalf("create cryptokey: %v", err)
	}

	if err := s.DeleteVersion(ctx, "proj", "global", "kr", "k", "missing"); err != ErrNoSuchVersion {
		t.Fatalf("delete missing version = %v, want ErrNoSuchVersion", err)
	}
	if err := s.DeleteVersion(ctx, "proj", "global", "kr", "k", "1"); err != nil {
		t.Fatalf("delete version: %v", err)
	}
	if _, err := s.GetVersion(ctx, "proj", "global", "kr", "k", "1"); err != ErrNoSuchVersion {
		t.Fatalf("get deleted version = %v, want ErrNoSuchVersion", err)
	}

	if err := s.DeleteCryptoKey(ctx, "proj", "global", "kr", "missing"); err != ErrNoSuchCryptoKey {
		t.Fatalf("delete missing key = %v, want ErrNoSuchCryptoKey", err)
	}
	if err := s.DeleteCryptoKey(ctx, "proj", "global", "kr", "k"); err != nil {
		t.Fatalf("delete crypto key: %v", err)
	}
	if _, err := s.GetCryptoKey(ctx, "proj", "global", "kr", "k"); err != ErrNoSuchCryptoKey {
		t.Fatalf("get deleted key = %v, want ErrNoSuchCryptoKey", err)
	}
}

// TestMemoryStoreDeleteKeyRing pins the store half of TF5-4: a key ring is
// removed (a get is ErrNoSuchKeyRing) and its name is reusable — the store keeps
// no key-ring retirement row — while the ring's child records are dropped so a
// re-created ring cannot inherit them.
func TestMemoryStoreDeleteKeyRing(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if err := s.CreateKeyRing(ctx, "proj", "global", "kr", KeyRing{Location: "global", ID: "kr"}); err != nil {
		t.Fatalf("create keyring: %v", err)
	}

	if err := s.DeleteKeyRing(ctx, "proj", "global", "missing"); err != ErrNoSuchKeyRing {
		t.Fatalf("delete missing key ring = %v, want ErrNoSuchKeyRing", err)
	}
	if err := s.DeleteKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatalf("delete key ring: %v", err)
	}
	if _, err := s.GetKeyRing(ctx, "proj", "global", "kr"); err != ErrNoSuchKeyRing {
		t.Fatalf("get deleted key ring = %v, want ErrNoSuchKeyRing", err)
	}
	if err := s.CreateKeyRing(ctx, "proj", "global", "kr", KeyRing{Location: "global", ID: "kr"}); err != nil {
		t.Fatalf("re-create the deleted key ring name: %v, want success", err)
	}
	if keys, err := s.ListCryptoKeys(ctx, "proj", "global", "kr"); err != nil || len(keys) != 0 {
		t.Fatalf("re-created key ring children = %v, %v, want none", keys, err)
	}
}
