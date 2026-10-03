package secretmanager

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryStoreSecretCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	// Create.
	if err := s.CreateSecret(ctx, "proj", "a", Secret{ID: "a", Labels: map[string]string{"k": "v"}, NextVer: 1}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Duplicate → ErrAlreadyExists.
	if err := s.CreateSecret(ctx, "proj", "a", Secret{ID: "a"}); err != ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}

	// Get.
	got, err := s.GetSecret(ctx, "proj", "a")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != "a" || got.Labels["k"] != "v" || got.NextVer != 1 {
		t.Fatalf("unexpected secret: %+v", got)
	}

	// Update.
	if err := s.UpdateSecret(ctx, "proj", "a", Secret{ID: "a", Labels: map[string]string{"k": "v2"}, NextVer: 5}); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, _ = s.GetSecret(ctx, "proj", "a")
	if got.NextVer != 5 {
		t.Fatalf("expected NextVer 5, got %d", got.NextVer)
	}

	// List (sorted).
	s.CreateSecret(ctx, "proj", "c", Secret{ID: "c"})
	s.CreateSecret(ctx, "proj", "b", Secret{ID: "b"})
	all, err := s.ListSecrets(ctx, "proj")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 3 || all[0].ID != "a" || all[1].ID != "b" || all[2].ID != "c" {
		t.Fatalf("unexpected list order: %+v", all)
	}

	// Delete cascades versions.
	s.CreateVersion(ctx, "proj", Version{SecretID: "a", VersionID: "1", State: "ENABLED"})
	if err := s.DeleteSecret(ctx, "proj", "a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.GetSecret(ctx, "proj", "a"); err != ErrNoSuchSecret {
		t.Fatalf("expected ErrNoSuchSecret, got %v", err)
	}
	if _, err := s.GetVersion(ctx, "proj", "a", "1"); err != ErrNoSuchVersion {
		t.Fatalf("expected cascaded version delete, got %v", err)
	}

	// Delete missing.
	if err := s.DeleteSecret(ctx, "proj", "a"); err != ErrNoSuchSecret {
		t.Fatalf("expected ErrNoSuchSecret on delete missing, got %v", err)
	}
}

func TestMemoryStoreVersionLifecycle(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	s.CreateSecret(ctx, "proj", "s", Secret{ID: "s", NextVer: 1})

	// Create versions.
	for _, id := range []string{"1", "2"} {
		if err := s.CreateVersion(ctx, "proj", Version{SecretID: "s", VersionID: id, State: "ENABLED", Data: "aGk=", CreateTime: time.Now()}); err != nil {
			t.Fatalf("create version %s: %v", id, err)
		}
	}
	v, err := s.GetVersion(ctx, "proj", "s", "2")
	if err != nil {
		t.Fatalf("get version: %v", err)
	}
	if v.State != "ENABLED" || v.Data != "aGk=" {
		t.Fatalf("unexpected version: %+v", v)
	}

	// List sorted.
	all, _ := s.ListVersions(ctx, "proj", "s")
	if len(all) != 2 || all[0].VersionID != "1" || all[1].VersionID != "2" {
		t.Fatalf("unexpected version list: %+v", all)
	}

	// Update state.
	if err := s.UpdateVersion(ctx, "proj", Version{SecretID: "s", VersionID: "2", State: "DESTROYED"}); err != nil {
		t.Fatalf("update version: %v", err)
	}
	v, _ = s.GetVersion(ctx, "proj", "s", "2")
	if v.State != "DESTROYED" {
		t.Fatalf("expected DESTROYED, got %s", v.State)
	}

	// Update missing.
	if err := s.UpdateVersion(ctx, "proj", Version{SecretID: "s", VersionID: "9"}); err != ErrNoSuchVersion {
		t.Fatalf("expected ErrNoSuchVersion, got %v", err)
	}
}

func TestMemoryStoreNextVersion(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	s.CreateSecret(ctx, "proj", "s", Secret{ID: "s", NextVer: 1})

	for i := 1; i <= 3; i++ {
		v, err := s.NextVersion(ctx, "proj", "s")
		if err != nil {
			t.Fatalf("next version: %v", err)
		}
		if v != i {
			t.Fatalf("expected version %d, got %d", i, v)
		}
	}
	if _, err := s.NextVersion(ctx, "proj", "missing"); err != ErrNoSuchSecret {
		t.Fatalf("expected ErrNoSuchSecret, got %v", err)
	}
}

func TestMemoryStoreUpdateSecretAtomic(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	s.CreateSecret(ctx, "proj", "a", Secret{ID: "a", Labels: map[string]string{"k": "v"}, NextVer: 1})

	updated, err := s.UpdateSecretAtomic(ctx, "proj", "a", func(cur Secret) (Secret, error) {
		cur.Labels = map[string]string{"k": "v2"}
		return cur, nil
	})
	if err != nil {
		t.Fatalf("UpdateSecretAtomic: %v", err)
	}
	if updated.Labels["k"] != "v2" {
		t.Fatalf("returned value not updated: %+v", updated)
	}
	got, _ := s.GetSecret(ctx, "proj", "a")
	if got.Labels["k"] != "v2" {
		t.Fatalf("stored value not updated: %+v", got)
	}

	// mutate error: no-op, nothing written.
	sentinel := errors.New("nope")
	if _, err := s.UpdateSecretAtomic(ctx, "proj", "a", func(cur Secret) (Secret, error) {
		return Secret{}, sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
	got, _ = s.GetSecret(ctx, "proj", "a")
	if got.Labels["k"] != "v2" {
		t.Fatalf("secret must be untouched after a mutate error, got %+v", got)
	}

	// missing secret → ErrNoSuchSecret, mutate never called.
	mutateCalled := false
	if _, err := s.UpdateSecretAtomic(ctx, "proj", "missing", func(cur Secret) (Secret, error) {
		mutateCalled = true
		return cur, nil
	}); err != ErrNoSuchSecret {
		t.Fatalf("expected ErrNoSuchSecret, got %v", err)
	}
	if mutateCalled {
		t.Fatal("mutate must not be called for a missing secret")
	}
}

func TestMemoryStoreReset(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	s.CreateSecret(ctx, "proj", "a", Secret{ID: "a"})
	s.Reset(ctx)
	if _, err := s.GetSecret(ctx, "proj", "a"); err != ErrNoSuchSecret {
		t.Fatalf("expected ErrNoSuchSecret after reset, got %v", err)
	}
}
