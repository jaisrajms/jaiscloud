//go:build gcp_persistence

// Package paritygrpc_test extends the GCP Postgres persistence-parity coverage
// to the gRPC service surfaces: datastore, logging, monitoring, KMS and
// Pub/Sub.
//
// The REST parity suites (tests/persistent_mode/gcp/{core,parity}) cover the
// REST-exposed providers over HTTP. Datastore, logging and monitoring have no
// usable REST surface (/v1/projects/p:commit, /v2/entries:write and /v3/...
// all 404), while KMS and Pub/Sub have gRPC-only state (RetiredResources /
// ImportJobs, and snapshot/seek ack-state) that the REST probes do not touch.
// All five therefore drive the official gRPC clients here. Those clients live
// in a separate Go module — the same reason as tests/integration/gcp/sdk-* — so
// the heavy cloud.google.com/go dependency graph never enters the main module.
//
// For each service the test:
//
//  1. seeds a resource over gRPC with a name that is unique per run, so re-runs
//     are idempotent and never collide with a previous run's rows,
//  2. restarts jaiscloud-gcp against the SAME --dsn and asserts the resource
//     survived the restart,
//  3. POSTs /_jaiscloud/reset and asserts the resource is gone (resetter parity).
//
// Required env:
//
//	JAISCLOUD_DSN — PostgreSQL DSN
//
// Optional env:
//
//	JAISCLOUD_GCP_BIN          — path to the jaiscloud-gcp binary
//	JAISCLOUD_GCP_PERSIST_PORT — HTTP admin port (default 8099)
//	JAISCLOUD_GCP_GRPC_PORT    — gRPC port (default 8081)
//	GCP_EMULATOR_PROJECT       — project id (default test-project)
//	DATASTORE_EMULATOR_HOST    — datastore client target (default localhost:8081)
//	LOGGING_EMULATOR_HOST      — logging client target (default localhost:8081)
//	MONITORING_EMULATOR_HOST   — monitoring client target (default localhost:8081)
//	KMS_EMULATOR_HOST          — KMS client target (default localhost:8081)
//	PUBSUB_EMULATOR_HOST       — Pub/Sub client target (default localhost:8081)
package paritygrpc_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// probe seeds one representative resource for a service and returns three
// checks: seeded (must find the resource immediately after the write), survived
// (run after a restart with the same DSN; must find it again), and cleared
// (run after POST /_jaiscloud/reset; must succeed, i.e. it asserts absence).
type probe struct {
	service string
	seed    func(d *driver, suffix string) (seeded, survived, cleared func() error, err error)
}

// probes is the table of gRPC-only services under test. Each entry is
// independent and self-describing; failures are reported per service per phase.
var probes = []probe{
	{"datastore", seedDatastore},
	{"logging", seedLogging},
	{"monitoring", seedMonitoring},
	{"kms", seedKMS},
	{"pubsub", seedPubSub},
}

// driver wraps the running emulator's admin URL, its gRPC address, and the HTTP
// client used for health/reset. Closures returned by seed capture the driver, so
// they keep working across the restart (both ports are stable). gRPC clients are
// built per call and closed again, so a restart never leaves a probe holding a
// dead connection.
type driver struct {
	httpBase string
	grpcAddr string
	client   *http.Client
}

// target returns the client-facing address for a service, preferring its
// standard emulator env var (mirroring the CI wiring) and falling back to the
// gRPC listener this harness started.
func (d *driver) target(env string) string {
	if v := os.Getenv(env); v != "" {
		return v
	}
	return d.grpcAddr
}

func (d *driver) do(method, path, body string) (int, string, error) {
	var data []byte
	if body != "" {
		data = []byte(body)
	}
	ct := ""
	if body != "" {
		ct = "application/json"
	}
	return d.doBytes(method, path, data, ct)
}

// doBytes is do for binary/typed payloads (the /_jaiscloud/export tarball and
// its /import round-trip), where the body is raw bytes and the caller chooses
// the Content-Type.
func (d *driver) doBytes(method, path string, data []byte, contentType string) (int, string, error) {
	var rd io.Reader
	if data != nil {
		rd = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, d.httpBase+path, rd)
	if err != nil {
		return 0, "", err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", err
	}
	return resp.StatusCode, string(raw), nil
}

func truncate(s string) string {
	const max = 300
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}

// projectID mirrors tests/integration/gcp/sdk-*: GCP_EMULATOR_PROJECT with a
// test-project fallback.
func projectID() string {
	if p := os.Getenv("GCP_EMULATOR_PROJECT"); p != "" {
		return p
	}
	return "test-project"
}

// dialGRPC opens a plaintext h2c connection to the emulator's gRPC listener.
func dialGRPC(target string) (*grpc.ClientConn, error) {
	return grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// datastoreClient builds an official datastore client. The library's emulator
// path is selected by DATASTORE_EMULATOR_HOST; when the harness is started
// without that override we point it at our own gRPC listener first.
func (d *driver) datastoreClient(ctx context.Context) (*datastore.Client, error) {
	if os.Getenv("DATASTORE_EMULATOR_HOST") == "" {
		os.Setenv("DATASTORE_EMULATOR_HOST", d.grpcAddr)
	}
	return datastore.NewClient(ctx, projectID())
}

// ── process harness (mirrors tests/persistent_mode/gcp/parity, self-contained) ─

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

func grpcPort() int {
	if v := os.Getenv("JAISCLOUD_GCP_GRPC_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			return p
		}
	}
	return 8081
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

func startGCPProcess(t *testing.T, httpPort, grpcPort int, dsn, blobDir string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(gcpBin(),
		"start",
		"--port", strconv.Itoa(httpPort),
		"--grpc-port", strconv.Itoa(grpcPort),
		"--dsn", dsn,
		"--blob-dir", blobDir,
		"--log-level", "warn",
	)
	cmd.Stdout = &testWriter{t: t, prefix: fmt.Sprintf("jaiscloud-gcp[%d]: ", httpPort)}
	cmd.Stderr = &testWriter{t: t, prefix: fmt.Sprintf("jaiscloud-gcp[%d] ERR: ", httpPort)}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start jaiscloud-gcp on port %d: %v", httpPort, err)
	}
	return cmd
}

func waitForHealth(t *testing.T, base string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(base + "/_jaiscloud/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("jaiscloud-gcp at %s did not become healthy within 30s", base)
}

func stopProcess(t *testing.T, cmd *exec.Cmd, port int) {
	t.Helper()
	if cmd == nil || cmd.Process == nil {
		return
	}
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("kill jaiscloud-gcp: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/_jaiscloud/health", port))
		if err != nil {
			return
		}
		resp.Body.Close()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("jaiscloud-gcp did not release port %d after kill", port)
}

// TestPersistenceParity seeds every gRPC-only probe, restarts the emulator
// against the same DSN, asserts survival, resets, and asserts the resources are
// gone.
func TestPersistenceParity(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping gRPC persistence parity test")
	}

	httpPort := persistPort()
	gPort := grpcPort()
	base := fmt.Sprintf("http://localhost:%d", httpPort)
	blobDir := t.TempDir()
	d := &driver{
		httpBase: base,
		grpcAddr: fmt.Sprintf("localhost:%d", gPort),
		client:   &http.Client{Timeout: 30 * time.Second},
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	// ── Phase 1: seed against emulator #1 ────────────────────────────────────
	proc1 := startGCPProcess(t, httpPort, gPort, dsn, blobDir)
	stopped1 := false
	defer func() {
		if !stopped1 {
			stopProcess(t, proc1, httpPort)
		}
	}()
	waitForHealth(t, base)

	type seeded struct {
		service  string
		seeded   func() error
		survived func() error
		cleared  func() error
	}
	var seeds []seeded
	for _, p := range probes {
		seededFn, survived, cleared, err := p.seed(d, suffix)
		if err != nil {
			t.Errorf("seed %s: %v", p.service, err)
			continue
		}
		seeds = append(seeds, seeded{p.service, seededFn, survived, cleared})
	}

	for _, s := range seeds {
		s := s
		t.Run(s.service+"/seeded", func(t *testing.T) {
			if err := s.seeded(); err != nil {
				t.Errorf("resource not visible after seed: %v", err)
			}
		})
	}

	stopProcess(t, proc1, httpPort)
	stopped1 = true

	// ── Phase 2: restart against the same backend ────────────────────────────
	proc2 := startGCPProcess(t, httpPort, gPort, dsn, blobDir)
	defer stopProcess(t, proc2, httpPort)
	waitForHealth(t, base)

	for _, s := range seeds {
		s := s
		t.Run(s.service+"/survived-restart", func(t *testing.T) {
			if err := s.survived(); err != nil {
				t.Errorf("resource did not survive restart: %v", err)
			}
		})
	}

	// ── Phase 3: reset parity ────────────────────────────────────────────────
	if code, body, err := d.do("POST", "/_jaiscloud/reset", ""); err != nil {
		t.Fatalf("POST /_jaiscloud/reset: %v", err)
	} else if code != http.StatusOK {
		t.Fatalf("POST /_jaiscloud/reset: got HTTP %d: %s", code, truncate(body))
	}

	for _, s := range seeds {
		s := s
		t.Run(s.service+"/cleared-by-reset", func(t *testing.T) {
			if err := s.cleared(); err != nil {
				t.Errorf("resource not cleared by reset: %v", err)
			}
		})
	}
}

// TestSnapshotRoundTrip proves the /_jaiscloud/export → reset → import path
// round-trips every gRPC surface's state through the Postgres-backed stores:
// seed, export, reset (asserting the state is gone), re-import the snapshot and
// assert the reads succeed again. A store missing a Resetter fails the reset
// phase; one missing a Snapshotter — or whose Snapshot/Restore drops fields —
// fails the import phase.
func TestSnapshotRoundTrip(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping gRPC snapshot round-trip test")
	}

	httpPort := persistPort()
	gPort := grpcPort()
	base := fmt.Sprintf("http://localhost:%d", httpPort)
	blobDir := t.TempDir()
	d := &driver{
		httpBase: base,
		grpcAddr: fmt.Sprintf("localhost:%d", gPort),
		client:   &http.Client{Timeout: 60 * time.Second},
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	proc := startGCPProcess(t, httpPort, gPort, dsn, blobDir)
	defer stopProcess(t, proc, httpPort)
	waitForHealth(t, base)

	type seeded struct {
		service  string
		survived func() error
		cleared  func() error
	}
	var seeds []seeded
	for _, p := range probes {
		_, survived, cleared, err := p.seed(d, suffix)
		if err != nil {
			t.Errorf("seed %s: %v", p.service, err)
			continue
		}
		seeds = append(seeds, seeded{p.service, survived, cleared})
	}
	if len(seeds) == 0 {
		t.Fatal("no probe seeded — nothing to round-trip")
	}

	// ── Export the full state ────────────────────────────────────────────────
	code, tarball, err := d.doBytes("GET", "/_jaiscloud/export", nil, "")
	if err != nil {
		t.Fatalf("GET /_jaiscloud/export: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("GET /_jaiscloud/export: got HTTP %d: %s", code, truncate(tarball))
	}
	if len(tarball) < 2 || tarball[0] != 0x1f || tarball[1] != 0x8b {
		t.Fatalf("export is not a gzip tarball (first bytes %x)", []byte(tarball[:min(len(tarball), 2)]))
	}

	// ── Reset: every seeded store must clear (Resetter registration) ─────────
	if code, body, err := d.do("POST", "/_jaiscloud/reset", ""); err != nil {
		t.Fatalf("POST /_jaiscloud/reset: %v", err)
	} else if code != http.StatusOK {
		t.Fatalf("POST /_jaiscloud/reset: got HTTP %d: %s", code, truncate(body))
	}
	for _, s := range seeds {
		s := s
		t.Run(s.service+"/cleared-by-reset", func(t *testing.T) {
			if err := s.cleared(); err != nil {
				t.Errorf("resource not cleared by reset: %v", err)
			}
		})
	}

	// ── Import: every seeded store must restore (Snapshotter registration) ──
	code, body, err := d.doBytes("POST", "/_jaiscloud/import?reset_first=true", []byte(tarball), "application/gzip")
	if err != nil {
		t.Fatalf("POST /_jaiscloud/import: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("POST /_jaiscloud/import: got HTTP %d: %s", code, truncate(body))
	}
	for _, s := range seeds {
		s := s
		t.Run(s.service+"/restored-by-import", func(t *testing.T) {
			if err := s.survived(); err != nil {
				t.Errorf("resource not restored by import: %v", err)
			}
		})
	}
}
