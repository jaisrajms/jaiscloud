//go:build gcp_persistence

// Package core_test verifies that the GCP core messaging/security services (Pub/Sub, Secret
// Manager, KMS, IAM) survive a jaiscloud-gcp process restart when backed by
// PostgreSQL (--dsn).
//
// Required env:
//
//	JAISCLOUD_DSN — PostgreSQL DSN
//
// Optional env:
//
//	JAISCLOUD_GCP_BIN         — path to the jaiscloud-gcp binary
//	JAISCLOUD_GCP_PERSIST_PORT — port for the managed server (default 8099)
package core_test

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/clock"
)

func gcpBin() string {
	if b := os.Getenv("JAISCLOUD_GCP_BIN"); b != "" {
		return b
	}
	const rel = "../../../../jaiscloud-gcp"
	if _, err := os.Stat(rel); err == nil {
		return rel
	}
	return "jaiscloud-gcp"
}

func persistPort() int {
	if v := os.Getenv("JAISCLOUD_GCP_PERSIST_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			return p
		}
	}
	return 8099
}

type testWriter struct {
	t      *testing.T
	prefix string
	buf    string
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.buf += string(p)
	for {
		idx := strings.IndexByte(w.buf, '\n')
		if idx < 0 {
			break
		}
		w.t.Log(w.prefix + w.buf[:idx])
		w.buf = w.buf[idx+1:]
	}
	return len(p), nil
}

func startGCPProcess(t *testing.T, port int, dsn, blobDir string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(gcpBin(),
		"start",
		"--port", strconv.Itoa(port),
		"--dsn", dsn,
		"--blob-dir", blobDir,
		"--log-level", "warn",
	)
	cmd.Stdout = &testWriter{t: t, prefix: fmt.Sprintf("jaiscloud-gcp[%d]: ", port)}
	cmd.Stderr = &testWriter{t: t, prefix: fmt.Sprintf("jaiscloud-gcp[%d] ERR: ", port)}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start jaiscloud-gcp on port %d: %v", port, err)
	}
	return cmd
}

func waitForHealth(t *testing.T, host string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := clock.RealNow().Add(30 * time.Second)
	for clock.RealNow().Before(deadline) {
		resp, err := client.Get(host + "/_jaiscloud/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("jaiscloud-gcp at %s did not become healthy within 30s", host)
}

func doRequest(t *testing.T, host, method, path, body, contentType string) (int, string) {
	t.Helper()
	var rd *strings.Reader
	if body != "" {
		rd = strings.NewReader(body)
	} else {
		rd = strings.NewReader("")
	}
	req, err := http.NewRequest(method, host+path, rd)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return resp.StatusCode, sb.String()
}

func stopProcess(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill jaiscloud-gcp: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c, err := http.Get(fmt.Sprintf("http://localhost:%d/_jaiscloud/health", persistPort()))
		if err != nil {
			return
		}
		c.Body.Close()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("jaiscloud-gcp did not release the port after kill")
}

// TestPersistenceAcrossRestart creates one resource in each core
// service, restarts against the same DSN, and verifies they survive.
func TestPersistenceAcrossRestart(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping persistence test")
	}

	port := persistPort()
	host := fmt.Sprintf("http://localhost:%d", port)
	blobDir := t.TempDir()

	// ── create resources ──────────────────────────────────────────────
	proc1 := startGCPProcess(t, port, dsn, blobDir)
	waitForHealth(t, host)

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topic := "persist-topic-" + suffix
	secret := "persist-secret-" + suffix
	keyring := "persist-kr-" + suffix
	sa := "persist-sa-" + suffix

	// Pub/Sub topic.
	if code, _ := doRequest(t, host, "PUT", "/v1/projects/proj/topics/"+topic,
		"{}", "application/json"); code != http.StatusOK {
		t.Fatalf("create topic: got HTTP %d", code)
	}
	// Secret Manager secret, with annotations (exercises the annotations column
	// and its migration surviving a restart).
	if code, _ := doRequest(t, host, "POST", "/v1/projects/proj/secrets?secretId="+secret,
		`{"replication":{"automatic":{}},"annotations":{"persist":"yes","suite":"core"}}`, "application/json"); code != http.StatusOK {
		t.Fatalf("create secret: got HTTP %d", code)
	}
	// KMS key ring.
	if code, _ := doRequest(t, host, "POST", "/v1/projects/proj/locations/global/keyRings?keyRingId="+keyring,
		"{}", "application/json"); code != http.StatusOK {
		t.Fatalf("create keyring: got HTTP %d", code)
	}
	// IAM service account.
	if code, _ := doRequest(t, host, "POST", "/v1/projects/proj/serviceAccounts",
		`{"accountId":"`+sa+`"}`, "application/json"); code != http.StatusOK {
		t.Fatalf("create SA: got HTTP %d", code)
	}
	// Cloud Functions function (exercises the source_sha256/source_size/
	// source_blob_key columns added by FD1).
	if code, body := doRequest(t, host, "POST", "/v1/projects/proj/locations/us-central1/functions?functionId=persist-fn",
		`{"runtime":"nodejs20","entryPoint":"h"}`, "application/json"); code != http.StatusOK {
		t.Fatalf("create function: got HTTP %d: %s", code, body)
	}

	// ── Phase 2: restart against the same backend ──────────────────────────────
	stopProcess(t, proc1)

	proc2 := startGCPProcess(t, port, dsn, blobDir)
	defer stopProcess(t, proc2)
	waitForHealth(t, host)

	if code, _ := doRequest(t, host, "GET", "/v1/projects/proj/topics/"+topic, "", ""); code != http.StatusOK {
		t.Fatalf("get topic after restart: got HTTP %d", code)
	}
	if code, body := doRequest(t, host, "GET", "/v1/projects/proj/secrets/"+secret, "", ""); code != http.StatusOK {
		t.Fatalf("get secret after restart: got HTTP %d", code)
	} else if !strings.Contains(body, `"persist":"yes"`) {
		t.Fatalf("secret annotations did not survive restart: %s", body)
	}
	if code, _ := doRequest(t, host, "GET", "/v1/projects/proj/locations/global/keyRings/"+keyring, "", ""); code != http.StatusOK {
		t.Fatalf("get keyring after restart: got HTTP %d", code)
	}
	saEmail := sa + "@proj.iam.gserviceaccount.com"
	if code, _ := doRequest(t, host, "GET", "/v1/projects/proj/serviceAccounts/"+saEmail, "", ""); code != http.StatusOK {
		t.Fatalf("get SA after restart: got HTTP %d", code)
	}
	// The Cloud Functions row (with its source columns) survives the restart.
	if code, body := doRequest(t, host, "GET", "/v1/projects/proj/locations/us-central1/functions/persist-fn", "", ""); code != http.StatusOK {
		t.Fatalf("get function after restart: got HTTP %d: %s", code, body)
	} else if !strings.Contains(body, `"runtime":"nodejs20"`) {
		t.Fatalf("function row did not survive restart: %s", body)
	}
}
