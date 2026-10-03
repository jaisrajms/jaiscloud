//go:build gcp_persistence

package storage_test

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

// gcpBin returns the path to the jaiscloud-gcp binary. Tests run with the
// working directory set to tests/persistent_mode/gcp/storage/, so the project
// root binary is four levels up. JAISCLOUD_GCP_BIN overrides.
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

// persistPort returns the port used by the managed JaisCloud GCP process.
func persistPort() int {
	if v := os.Getenv("JAISCLOUD_GCP_PERSIST_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			return p
		}
	}
	return 8098
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

// startGCPProcess starts a managed jaiscloud-gcp subprocess and returns the
// exec.Cmd. Callers are responsible for killing it when done.
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

// waitForHealth polls /_jaiscloud/health until the server responds 200 or times out.
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

// doRequest performs an HTTP request against the managed server and returns the
// response status and body.
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
