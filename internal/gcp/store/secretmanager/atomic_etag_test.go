package secretmanager

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// errPrecondition stands in for an etag mismatch in the conditional-delete
// tests: the store must return it unchanged and leave everything in place.
var errPrecondition = errors.New("precondition failed")

func TestMemoryDeleteSecretIf(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if err := s.CreateSecret(ctx, "proj", "a", Secret{ID: "a"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.CreateVersion(ctx, "proj", Version{SecretID: "a", VersionID: "1", State: "ENABLED"}); err != nil {
		t.Fatalf("create version: %v", err)
	}

	// A failing condition aborts the delete and leaves the secret + versions.
	if err := s.DeleteSecretIf(ctx, "proj", "a", func(Secret) error { return errPrecondition }); !errors.Is(err, errPrecondition) {
		t.Fatalf("cond error = %v, want errPrecondition", err)
	}
	if _, err := s.GetSecret(ctx, "proj", "a"); err != nil {
		t.Fatalf("aborted delete removed the secret: %v", err)
	}
	if _, err := s.GetVersion(ctx, "proj", "a", "1"); err != nil {
		t.Fatalf("aborted delete removed a version: %v", err)
	}

	// The condition sees the stored value the delete replaces.
	var seen Secret
	if err := s.DeleteSecretIf(ctx, "proj", "a", func(sec Secret) error {
		seen = sec
		return nil
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if seen.ID != "a" {
		t.Fatalf("cond saw %+v, want the stored secret a", seen)
	}
	if _, err := s.GetSecret(ctx, "proj", "a"); !errors.Is(err, ErrNoSuchSecret) {
		t.Fatalf("secret survived: %v", err)
	}
	if _, err := s.GetVersion(ctx, "proj", "a", "1"); !errors.Is(err, ErrNoSuchVersion) {
		t.Fatalf("versions not cascaded: %v", err)
	}

	// Missing secret → ErrNoSuchSecret, cond not called.
	called := false
	if err := s.DeleteSecretIf(ctx, "proj", "missing", func(Secret) error { called = true; return nil }); !errors.Is(err, ErrNoSuchSecret) {
		t.Fatalf("missing delete = %v, want ErrNoSuchSecret", err)
	}
	if called {
		t.Fatal("cond ran for a missing secret")
	}
}

func TestMemoryUpdateVersionIf(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	if err := s.CreateSecret(ctx, "proj", "a", Secret{ID: "a"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	create := time.Date(2026, 10, 17, 1, 2, 3, 0, time.UTC)
	if err := s.CreateVersion(ctx, "proj", Version{SecretID: "a", VersionID: "1", State: "ENABLED", CreateTime: create}); err != nil {
		t.Fatalf("create version: %v", err)
	}

	// A failing mutate aborts the write.
	if _, err := s.UpdateVersionIf(ctx, "proj", "a", "1", func(Version) (Version, error) { return Version{}, errPrecondition }); !errors.Is(err, errPrecondition) {
		t.Fatalf("mutate error = %v, want errPrecondition", err)
	}
	if v, _ := s.GetVersion(ctx, "proj", "a", "1"); v.State != "ENABLED" {
		t.Fatalf("aborted mutate changed state to %q", v.State)
	}

	// The mutate sees the stored value, and the write lands.
	updated, err := s.UpdateVersionIf(ctx, "proj", "a", "1", func(v Version) (Version, error) {
		if v.State != "ENABLED" || !v.CreateTime.Equal(create) {
			t.Fatalf("mutate saw %+v, want the stored version", v)
		}
		v.State = "DISABLED"
		// A mutate must not be able to move the record: identity is the address.
		v.SecretID, v.VersionID = "other", "9"
		return v, nil
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.State != "DISABLED" || updated.SecretID != "a" || updated.VersionID != "1" {
		t.Fatalf("updated = %+v, want DISABLED at a/1", updated)
	}
	if v, err := s.GetVersion(ctx, "proj", "a", "1"); err != nil || v.State != "DISABLED" {
		t.Fatalf("stored version = %+v, %v; want DISABLED", v, err)
	}

	// Missing version → ErrNoSuchVersion, mutate not called.
	called := false
	if _, err := s.UpdateVersionIf(ctx, "proj", "a", "missing", func(v Version) (Version, error) {
		called = true
		return v, nil
	}); !errors.Is(err, ErrNoSuchVersion) {
		t.Fatalf("missing update = %v, want ErrNoSuchVersion", err)
	}
	if called {
		t.Fatal("mutate ran for a missing version")
	}
}

// TestGeneratePassword pins the documented shape of a managed-rotation password:
// a fresh random value on every call, 24 CSPRNG bytes base64-encoded (32
// printable characters).
func TestGeneratePassword(t *testing.T) {
	a, err := GeneratePassword()
	if err != nil {
		t.Fatalf("GeneratePassword: %v", err)
	}
	b, err := GeneratePassword()
	if err != nil {
		t.Fatalf("GeneratePassword: %v", err)
	}
	if len(a) != 32 || strings.ContainsAny(a, " \t\n") {
		t.Fatalf("password %q is not 32 printable base64 characters", a)
	}
	if a == b {
		t.Fatal("consecutive passwords are identical; the value must be random")
	}
}

// TestNullableHelpers covers the SQL-null conversions used by the Postgres
// store: a nil/zero value must round-trip as SQL NULL, a set value as its
// marshalled/typed form.
func TestNullableHelpers(t *testing.T) {
	if v := nullableJSON(nil); v != nil {
		t.Fatalf("nullableJSON(nil) = %v, want nil", v)
	}
	var s Secret
	if v := nullableJSON(&s); v == nil {
		t.Fatal("nullableJSON(&Secret{}) = nil, want JSON")
	}
	raw, ok := nullableJSON(map[string]string{"k": "v"}).(json.RawMessage)
	if !ok || !strings.Contains(string(raw), `"k":"v"`) {
		t.Fatalf("nullableJSON(map) = %#v, want JSON bytes", raw)
	}

	if v := nullableTime(time.Time{}); v != nil {
		t.Fatalf("nullableTime(zero) = %v, want nil", v)
	}
	now := time.Date(2026, 10, 17, 1, 2, 3, 0, time.UTC)
	if v := nullableTime(now); v != now {
		t.Fatalf("nullableTime(now) = %v, want %v", v, now)
	}

	// The constructor is a trivial wrapper, but pin that it is usable.
	if s := NewPostgresStore(nil); s == nil {
		t.Fatal("NewPostgresStore(nil) = nil")
	}
}
