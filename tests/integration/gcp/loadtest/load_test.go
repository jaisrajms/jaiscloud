package loadtest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestGCPLoadGate is the TD12 load / concurrency gate. It is NOT part of PR CI:
// it boots an ephemeral emulator (with --metrics) and drives GCS object PUT/GET,
// Pub/Sub publish + fan-out, and Firestore concurrent commits at -c 100 for a
// bounded window, then asserts no 5xx, correct per-request outcomes, and bounded
// goroutine / memory growth in the emulator process.
//
// Run it with `make test-gcp-load` (sets JAISCLOUD_LOAD=1), the nightly
// `.github/workflows/gcp-load.yml`, or point it at a running emulator with
// JAISCLOUD_LOAD_ENDPOINT (that emulator must have been started with --metrics).
func TestGCPLoadGate(t *testing.T) {
	if os.Getenv("JAISCLOUD_LOAD") != "1" {
		t.Skip("load gate is local/nightly only: set JAISCLOUD_LOAD=1 (make test-gcp-load)")
	}

	concurrency := intEnv("JAISCLOUD_LOAD_CONCURRENCY", 100)
	window := durEnv("JAISCLOUD_LOAD_WINDOW", 5*time.Second)
	settle := durEnv("JAISCLOUD_LOAD_SETTLE", 3*time.Second)
	maxPublish := int64(intEnv("JAISCLOUD_LOAD_PUBLISH_MAX", 20000))

	base, stop := startLoadEmulator(t)
	defer stop()
	client := newLoadClient(concurrency)
	a := &api{base: base, client: client}
	ctx := context.Background()

	require.NoError(t, seed(ctx, a), "seed the three hot paths")

	// Bounded, deterministic fan-out contract first (the load window then
	// hammers publish/pull without tracking individual markers).
	require.NoError(t, verifyFanout(ctx, a, 120), "pub/sub fan-out to %d subscriptions", pubSubscriptions)

	// Warm the runtime up before the baseline snapshot: a cold process's heap
	// and connection pools make the growth ratio noisy. The warmup is
	// discarded — only the baseline-to-trailing delta is asserted.
	warmup := time.Second
	if window < 2*warmup {
		warmup = window / 2
	}
	loadGCS(ctx, a, concurrency, warmup)
	loadPubSub(ctx, a, concurrency, warmup, maxPublish/4)
	loadFirestore(ctx, a, concurrency, warmup)
	client.CloseIdleConnections()
	time.Sleep(time.Second)

	baseline, err := FetchMetrics(ctx, client, base)
	require.NoError(t, err, "read baseline /metrics (start the emulator with --metrics)")
	require.Contains(t, baseline, "go_goroutines", "/metrics must expose the Go collector (start with --metrics)")
	t.Logf("baseline: %s", metricLine(baseline))

	t.Run("gcs put/get", func(t *testing.T) {
		r := loadGCS(ctx, a, concurrency, window)
		requireHealthy(t, "gcs put/get", r)
	})
	t.Run("pubsub publish/fan-out", func(t *testing.T) {
		r := loadPubSub(ctx, a, concurrency, window, maxPublish)
		requireHealthy(t, "pubsub publish/fan-out", r)
	})
	t.Run("firestore commit", func(t *testing.T) {
		r := loadFirestore(ctx, a, concurrency, window)
		requireHealthy(t, "firestore commit", r)
	})

	// Let in-flight work drain, then check the process did not leak. Close the
	// client's idle keep-alive connections first: the server holds a goroutine
	// per open connection, and those are not a leak.
	client.CloseIdleConnections()
	time.Sleep(settle)
	after, err := FetchMetrics(ctx, client, base)
	require.NoError(t, err, "read trailing /metrics")
	t.Logf("after:    %s", metricLine(after))
	require.NoError(t, CheckGrowth(baseline, after, growthThresholds()),
		"emulator goroutine/memory growth after the load window")
}

// requireHealthy fails the test (with sampled failures) when a load window saw
// any 5xx, transport error, or unexpected status.
func requireHealthy(t *testing.T, name string, r *LoadResult) {
	t.Helper()
	t.Logf("%s: %s", name, r.Summary())
	if r.Failed() {
		var b strings.Builder
		for _, f := range r.Failures {
			b.WriteString("\n  ")
			b.WriteString(f)
		}
		t.Fatalf("%s saw failures: %s%s", name, r.Summary(), b.String())
	}
}

func metricLine(m Metrics) string {
	return fmt.Sprintf("goroutines=%.0f heap=%s rss=%s",
		m["go_goroutines"], humanBytes(m["go_memstats_alloc_bytes"]), humanBytes(m["process_resident_memory_bytes"]))
}

func humanBytes(v float64) string {
	switch {
	case v <= 0:
		return "n/a"
	case v < 1<<20:
		return fmt.Sprintf("%.0fKiB", v/1024)
	case v < 1<<30:
		return fmt.Sprintf("%.0fMiB", v/(1<<20))
	default:
		return fmt.Sprintf("%.1fGiB", v/(1<<30))
	}
}

func growthThresholds() GrowthThresholds {
	return GrowthThresholds{
		MaxGoroutines:  floatEnv("JAISCLOUD_LOAD_MAX_GOROUTINES", 64),
		MaxHeapPercent: floatEnv("JAISCLOUD_LOAD_MAX_HEAP_PCT", 250),
		MaxRSSPercent:  floatEnv("JAISCLOUD_LOAD_MAX_RSS_PCT", 150),
	}
}

func newLoadClient(concurrency int) *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        concurrency * 4,
			MaxIdleConnsPerHost: concurrency * 4,
		},
	}
}

// startLoadEmulator boots a private ephemeral jaiscloud-gcp with --metrics on
// free ports, or reuses JAISCLOUD_LOAD_ENDPOINT. It returns the base URL and a
// stop function.
func startLoadEmulator(t *testing.T) (string, func()) {
	t.Helper()
	if ep := os.Getenv("JAISCLOUD_LOAD_ENDPOINT"); ep != "" {
		base := strings.TrimRight(ep, "/")
		waitHealthy(t, base)
		return base, func() {}
	}

	bin := resolveGCPBinary()
	if bin == "" {
		t.Skip("jaiscloud-gcp binary not found: run `make test-gcp-load` (builds it) or `go build -o jaiscloud-gcp ./cmd/jaiscloud-gcp/`")
	}
	port := freePort(t)
	grpcPort := freePort(t)
	logPath := os.Getenv("JAISCLOUD_LOAD_LOG")
	if logPath == "" {
		logPath = "/tmp/jaiscloud-gcp-load.log"
	}
	logf, err := os.Create(logPath)
	require.NoError(t, err)
	t.Logf("starting %s (REST :%d, gRPC :%d, log %s)", bin, port, grpcPort, logPath)

	cmd := exec.Command(bin, "start",
		"--port", strconv.Itoa(port),
		"--grpc-port", strconv.Itoa(grpcPort),
		"--ephemeral",
		"--metrics",
	)
	cmd.Stdout = logf
	cmd.Stderr = logf
	require.NoError(t, cmd.Start(), "start emulator")

	stop := func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
		_ = logf.Close()
	}

	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitHealthy(t, base)
	return base, stop
}

func resolveGCPBinary() string {
	if b := os.Getenv("JAISCLOUD_GCP_BINARY"); b != "" {
		return b
	}
	candidates := []string{
		"../../../../jaiscloud-gcp", // CI / `make test-gcp-load` build at the repo root
	}
	if gp := os.Getenv("GOPATH"); gp != "" {
		candidates = append(candidates, filepath.Join(gp, "bin", "jaiscloud-gcp"))
	}
	for _, c := range candidates {
		if st, err := os.Stat(c); err == nil && !st.IsDir() {
			abs, err := filepath.Abs(c)
			if err == nil {
				return abs
			}
			return c
		}
	}
	return ""
}

func waitHealthy(t *testing.T, base string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := http.Get(base + "/_jaiscloud/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
			lastErr = fmt.Errorf("health status %d", resp.StatusCode)
		} else {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("emulator at %s did not become healthy: %v", base, lastErr)
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func intEnv(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func floatEnv(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func durEnv(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
