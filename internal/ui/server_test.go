package ui

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOrCreateToken_GeneratesAndPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".session-token")

	token, err := loadOrCreateToken(path, false)
	if err != nil {
		t.Fatalf("loadOrCreateToken: %v", err)
	}
	if !validToken(token) {
		t.Fatalf("generated token = %q, want 64 lowercase hex chars", token)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat token file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Fatalf("token file mode = %o, want 0600", perm)
	}
}

func TestLoadOrCreateToken_ReusesExisting(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".session-token")
	existing := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if err := os.WriteFile(path, []byte(existing), 0600); err != nil {
		t.Fatalf("seed token: %v", err)
	}

	token, err := loadOrCreateToken(path, false)
	if err != nil {
		t.Fatalf("loadOrCreateToken: %v", err)
	}
	if token != existing {
		t.Fatalf("token = %q, want existing %q", token, existing)
	}
}

func TestLoadOrCreateToken_RotatesOnFreshStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".session-token")
	existing := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if err := os.WriteFile(path, []byte(existing), 0600); err != nil {
		t.Fatalf("seed token: %v", err)
	}

	token, err := loadOrCreateToken(path, true)
	if err != nil {
		t.Fatalf("loadOrCreateToken: %v", err)
	}
	if token == existing {
		t.Fatal("token was not rotated on --fresh-start")
	}
	if !validToken(token) {
		t.Fatalf("rotated token = %q, want valid hex", token)
	}
}

func TestLoadOrCreateToken_RotatesInvalid(t *testing.T) {
	for name, content := range map[string]string{
		"empty":    "",
		"garbage":  "not-a-token",
		"tooShort": "abc123",
		"uppercase": "0123456789ABCDEF0123456789ABCDEF" +
			"0123456789ABCDEF0123456789ABCDEF",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), ".session-token")
			if err := os.WriteFile(path, []byte(content), 0600); err != nil {
				t.Fatalf("seed token: %v", err)
			}

			token, err := loadOrCreateToken(path, false)
			if err != nil {
				t.Fatalf("loadOrCreateToken: %v", err)
			}
			if !validToken(token) {
				t.Fatalf("token = %q, want valid hex", token)
			}
			if token == content {
				t.Fatal("invalid token was reused")
			}
		})
	}
}

func TestLoadOrCreateToken_TightensLoosePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".session-token")
	existing := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if err := os.WriteFile(path, []byte(existing), 0644); err != nil {
		t.Fatalf("seed token: %v", err)
	}

	if _, err := loadOrCreateToken(path, false); err != nil {
		t.Fatalf("loadOrCreateToken: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat token file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Fatalf("token file mode = %o, want 0600 after read", perm)
	}
}

func TestNewBootID_DiffersPerCall(t *testing.T) {
	a, err := newBootID()
	if err != nil {
		t.Fatalf("newBootID: %v", err)
	}
	b, err := newBootID()
	if err != nil {
		t.Fatalf("newBootID: %v", err)
	}
	if a == b {
		t.Fatalf("boot id repeated: %q", a)
	}
	if len(a) != 32 || !isHex(a) {
		t.Fatalf("boot id = %q, want 32 hex chars", a)
	}
}
