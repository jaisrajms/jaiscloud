package ui

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"jaiscloud/internal/admin"
	"jaiscloud/internal/config"
	"jaiscloud/internal/events"
	"jaiscloud/internal/ui/sse"
)

// UIServer is the lightweight HTTP server for the UI (port 4567).
// Entirely separate from the emulator gateway (port 4566).
type UIServer struct {
	srv     *http.Server
	broker  *sse.Broker
	token   string
	version string
}

// New creates a UIServer for the supplied registrar. Returns (nil, nil) if
// Assets() == nil (binary built without -tags ui).
// Reuses the persisted 32-byte session token across restarts (rotating it only
// when missing/unreadable or on --fresh-start) and mints a per-process boot ID.
// version is the binary version string (e.g. "dev", "v1.2.3").
func New(providers Registrar, adminHandler *admin.Handler, cfg *config.Config, bus *events.EventBus, version string) (*UIServer, error) {
	assets, err := StaticFS()
	if err != nil {
		return nil, fmt.Errorf("ui: load static assets: %w", err)
	}
	if assets == nil {
		return nil, nil
	}

	tp := tokenPath(cfg)
	if err := os.MkdirAll(filepath.Dir(tp), 0700); err != nil {
		return nil, fmt.Errorf("ui: create token dir: %w", err)
	}
	// Reuse the persisted token across restarts so an already-open browser tab
	// keeps a valid session cookie; rotate it only when it is missing/unreadable
	// or the operator asked for a clean slate (--fresh-start).
	token, err := loadOrCreateToken(tp, cfg.FreshStart)
	if err != nil {
		return nil, fmt.Errorf("ui: load session token: %w", err)
	}

	// bootID changes on every process start. It is exposed via /api/ui/v1/meta so
	// the browser can detect a restart and drop cached data; the persisted
	// instance ID does not change across restarts.
	bootID, err := newBootID()
	if err != nil {
		return nil, fmt.Errorf("ui: generate boot id: %w", err)
	}

	broker := sse.New(bus)

	router := BuildRouter(providers, adminHandler, broker, cfg, token, version, bootID)

	s := &UIServer{
		srv: &http.Server{
			Handler: router,
		},
		broker:  broker,
		token:   token,
		version: version,
	}
	return s, nil
}

// ListenAndServe starts the UI HTTP listener on addr (e.g. ":4567").
func (s *UIServer) ListenAndServe(addr string) error {
	s.srv.Addr = addr
	return s.srv.ListenAndServe()
}

// Shutdown closes the SSE broker then gracefully shuts down the HTTP server.
func (s *UIServer) Shutdown(ctx context.Context) error {
	s.broker.Shutdown()
	return s.srv.Shutdown(ctx)
}

// tokenPath returns the file path for the session token.
func tokenPath(cfg *config.Config) string {
	if cfg.DataDir != "" {
		return filepath.Join(cfg.DataDir, ".session-token")
	}
	base, _ := os.UserConfigDir()
	return filepath.Join(base, "jaiscloud-ui", ".session-token")
}

// loadOrCreateToken returns the session token persisted at path. It reuses an
// existing, valid token unless rotate is true (--fresh-start); otherwise it
// generates and writes a fresh one. Reuse keeps an open browser tab's cookie
// valid across a restart.
func loadOrCreateToken(path string, rotate bool) (string, error) {
	if !rotate {
		if token, ok := readToken(path); ok {
			return token, nil
		}
	}
	return writeToken(path)
}

// readToken reads and validates a persisted token. ok is false when the file is
// absent, unreadable, empty, or not 64 lowercase-hex characters.
func readToken(path string) (token string, ok bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	token = strings.TrimSpace(string(b))
	if !validToken(token) {
		return "", false
	}
	// Defense in depth: a pre-existing token file may have looser permissions.
	_ = os.Chmod(path, 0600)
	return token, true
}

// validToken reports whether s is a 32-byte hex token.
func validToken(s string) bool {
	return len(s) == 64 && isHex(s)
}

// isHex reports whether s consists only of lowercase hex digits.
func isHex(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// newBootID returns a random per-process identifier exposed via
// /api/ui/v1/meta so the browser can detect an emulator restart.
func newBootID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// writeToken generates a 32-byte hex token and writes it atomically (mode 0600).
func writeToken(path string) (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	token := hex.EncodeToString(b)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(token), 0600); err != nil {
		return "", err
	}
	return token, os.Rename(tmp, path)
}
