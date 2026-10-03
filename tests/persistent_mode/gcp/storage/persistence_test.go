//go:build gcp_persistence

// Package storage_test verifies that GCS state persists across a jaiscloud-gcp
// process restart when backed by PostgreSQL (--dsn) and a local blob directory.
//
// Required env:
//
//	JAISCLOUD_DSN — PostgreSQL DSN (e.g. postgres://jaiscloud:jaiscloud@localhost:5432/jaiscloud)
//
// Optional env:
//
//	JAISCLOUD_GCP_BIN        — path to the jaiscloud-gcp binary
//	JAISCLOUD_GCP_PERSIST_PORT — port for the managed server (default 8098)
package storage_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"
)

// TestGCSPersistenceAcrossRestart creates a bucket and object, restarts the
// server against the same Postgres DSN and blob directory, and verifies both
// survive.
func TestGCSPersistenceAcrossRestart(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping persistence test")
	}

	port := persistPort()
	host := fmt.Sprintf("http://localhost:%d", port)
	blobDir := t.TempDir()

	bucket := fmt.Sprintf("persist-bucket-%d", time.Now().UnixNano())
	const object = "data/hello.txt"
	const content = "persisted across restart"

	// ── start, create bucket + object ────────────────────────────────
	proc1 := startGCPProcess(t, port, dsn, blobDir)
	waitForHealth(t, host)

	if code, _ := doRequest(t, host, "POST", "/storage/v1/b?project=proj",
		`{"name":"`+bucket+`"}`, "application/json"); code != http.StatusOK {
		t.Fatalf("create bucket: got HTTP %d", code)
	}
	if code, _ := doRequest(t, host, "POST",
		"/upload/storage/v1/b/"+bucket+"/o?uploadType=media&name="+object,
		content, "text/plain"); code != http.StatusOK {
		t.Fatalf("upload object: got HTTP %d", code)
	}

	// ── Phase 2: restart against the same backend ─────────────────────────────
	stopProcess(t, proc1)

	proc2 := startGCPProcess(t, port, dsn, blobDir)
	defer stopProcess(t, proc2)
	waitForHealth(t, host)

	// Bucket metadata survives.
	if code, _ := doRequest(t, host, "GET", "/storage/v1/b/"+bucket, "", ""); code != http.StatusOK {
		t.Fatalf("get bucket after restart: got HTTP %d", code)
	}

	// Object bytes survive (streamed from the LocalFSBlobStore).
	code, body := doRequest(t, host, "GET", "/storage/v1/b/"+bucket+"/o/"+object+"?alt=media", "", "")
	if code != http.StatusOK {
		t.Fatalf("get object after restart: got HTTP %d", code)
	}
	if body != content {
		t.Fatalf("object content mismatch: got %q, want %q", body, content)
	}
}

// TestGCSVersioningPersistenceAcrossRestart exercises the Postgres
// PutObjectGeneration path (versioning-enabled overwrite) that the memory
// suite covers but the DSN path previously did not. It uploads two generations
// of one object, restarts, and asserts both generations remain readable —
// catching the double-commit bug that made every versioned write fail with
// pgx.ErrTxClosed.
func TestGCSVersioningPersistenceAcrossRestart(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping persistence test")
	}

	port := persistPort()
	host := fmt.Sprintf("http://localhost:%d", port)
	blobDir := t.TempDir()

	bucket := fmt.Sprintf("persist-ver-bucket-%d", time.Now().UnixNano())
	const object = "data/versioned.txt"

	proc1 := startGCPProcess(t, port, dsn, blobDir)
	waitForHealth(t, host)

	// Versioning-enabled bucket.
	if code, _ := doRequest(t, host, "POST", "/storage/v1/b?project=proj",
		`{"name":"`+bucket+`","versioning":{"enabled":true}}`, "application/json"); code != http.StatusOK {
		t.Fatalf("create versioned bucket: got HTTP %d", code)
	}

	upload := func(content string) string {
		code, body := doRequest(t, host, "POST",
			"/upload/storage/v1/b/"+bucket+"/o?uploadType=media&name="+object,
			content, "text/plain")
		if code != http.StatusOK {
			t.Fatalf("upload object: got HTTP %d", code)
		}
		var meta struct {
			Generation string `json:"generation"`
		}
		if err := json.Unmarshal([]byte(body), &meta); err != nil || meta.Generation == "" {
			t.Fatalf("upload response missing generation: %q (%v)", body, err)
		}
		return meta.Generation
	}

	gen1 := upload("one")
	gen2 := upload("two")
	if gen1 == gen2 {
		t.Fatal("expected distinct generations on overwrite")
	}

	// ── Restart against the same backend ───────────────────────────────────────
	stopProcess(t, proc1)

	proc2 := startGCPProcess(t, port, dsn, blobDir)
	defer stopProcess(t, proc2)
	waitForHealth(t, host)

	readGen := func(gen, want string) {
		code, body := doRequest(t, host, "GET",
			"/storage/v1/b/"+bucket+"/o/"+object+"?alt=media&generation="+gen, "", "")
		if code != http.StatusOK {
			t.Fatalf("read generation %s after restart: got HTTP %d", gen, code)
		}
		if body != want {
			t.Fatalf("generation %s content = %q, want %q", gen, body, want)
		}
	}
	readGen(gen1, "one")
	readGen(gen2, "two")
}

func stopProcess(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill jaiscloud-gcp: %v", err)
	}
	// Wait for the port to be released before restarting.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if !portInUse(persistPort()) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("jaiscloud-gcp did not release the port after kill")
}

func portInUse(port int) bool {
	c, err := http.Get(fmt.Sprintf("http://localhost:%d/_jaiscloud/health", port))
	if err != nil {
		return false
	}
	c.Body.Close()
	return true
}
