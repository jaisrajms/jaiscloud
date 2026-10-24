// Package loadtest is the GCP load / concurrency gate (TD12).
//
// It drives three hot paths at -c 100 for a bounded window — GCS object
// PUT/GET, Pub/Sub publish + fan-out to N subscriptions, and Firestore
// concurrent commits — and asserts (a) no 5xx, (b) correct per-request
// outcomes, and (c) bounded goroutine / memory growth in the emulator process.
//
// The live gate (TestGCPLoadGate in load_test.go) is deliberately NOT part of
// PR CI: it boots an ephemeral emulator with --metrics and runs only under
// `make test-gcp-load` (JAISCLOUD_LOAD=1) or the nightly
// `.github/workflows/gcp-load.yml`. The harness mechanics — metric parsing, the
// worker pool, and the health/growth assertions — are unit-tested in
// harness_test.go without a server, so the gate's own assertions are proven to
// bite (a seeded 5xx or a corrupted round-trip fails).
package loadtest

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Metrics is a parsed /metrics snapshot (unlabelled sample name -> value).
type Metrics map[string]float64

// ParseMetrics parses the Prometheus text exposition format. Only unlabelled
// samples are kept: a data line is `name value` (whitespace-separated).
// Comment (`#`) lines and labelled samples (`name{...} value`) are ignored, as
// are lines that do not parse as a float — so a collector gaining a label never
// silently shifts a metric this gate reads.
func ParseMetrics(text string) Metrics {
	m := Metrics{}
	for _, raw := range strings.Split(text, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		name := fields[0]
		if strings.ContainsAny(name, "{}") {
			continue
		}
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		m[name] = v
	}
	return m
}

// FetchMetrics GETs base+/metrics and parses the exposition text.
func FetchMetrics(ctx context.Context, client *http.Client, base string) (Metrics, error) {
	url := strings.TrimRight(base, "/") + "/metrics"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	return ParseMetrics(string(body)), nil
}

// GrowthThresholds bound the growth a load window may leave behind in the
// emulator process. They are leak detectors, not budgets: an emulator is
// allowed to keep caches, warm buffers and pooled connections, so the defaults
// are deliberately generous. A zero threshold disables that check.
type GrowthThresholds struct {
	MaxGoroutines  float64 // absolute increase over baseline
	MaxHeapPercent float64 // go_memstats_alloc_bytes increase, percent
	MaxRSSPercent  float64 // process_resident_memory_bytes increase, percent
}

// CheckGrowth returns a non-nil error on the first threshold breach between two
// metric snapshots. A metric absent from either snapshot is skipped, so a
// platform without the process collector still checks goroutines and heap.
func CheckGrowth(baseline, after Metrics, th GrowthThresholds) error {
	if th.MaxGoroutines > 0 {
		if before, ok := baseline["go_goroutines"]; ok {
			if now, ok := after["go_goroutines"]; ok {
				if d := now - before; d > th.MaxGoroutines {
					return fmt.Errorf("goroutine growth %.0f over baseline %.0f (limit +%.0f): possible leak",
						d, before, th.MaxGoroutines)
				}
			}
		}
	}
	checks := []struct {
		name string
		max  float64
	}{
		{"go_memstats_alloc_bytes", th.MaxHeapPercent},
		{"process_resident_memory_bytes", th.MaxRSSPercent},
	}
	for _, c := range checks {
		if c.max <= 0 {
			continue
		}
		before, ok := baseline[c.name]
		if !ok || before <= 0 {
			continue
		}
		now, ok := after[c.name]
		if !ok {
			continue
		}
		if pct := (now - before) / before * 100; pct > c.max {
			return fmt.Errorf("%s grew %.0f%% (%.0f -> %.0f, limit +%.0f%%): possible unbounded growth",
				c.name, pct, before, now, c.max)
		}
	}
	return nil
}

// LoadResult aggregates the outcomes of one bounded load window.
type LoadResult struct {
	Requests    int64
	ClientError int64 // transport error (no HTTP status)
	ServerError int64 // HTTP 5xx
	Unexpected  int64 // a status the scenario does not accept
	// Failures samples the first few failure descriptions (bounded).
	Failures []string
}

// Failed reports whether any request in the window failed.
func (r *LoadResult) Failed() bool {
	return r.ClientError > 0 || r.ServerError > 0 || r.Unexpected > 0
}

// Summary renders a one-line outcome for logs and error messages.
func (r *LoadResult) Summary() string {
	return fmt.Sprintf("requests=%d clientErr=%d 5xx=%d unexpected=%d",
		r.Requests, r.ClientError, r.ServerError, r.Unexpected)
}

const maxFailureSamples = 20

// RunLoad drives fn from `concurrency` goroutines until ctx is cancelled. Each
// call returns the HTTP status it observed plus an optional error (a non-2xx
// status, or a correctness mismatch after a 2xx). A status outside accept, a
// 5xx, or a transport error is counted as a failure. fn must be
// goroutine-safe; its own request context is expected to outlive ctx cancel so
// a window that ends mid-request does not manufacture transport errors.
func RunLoad(ctx context.Context, concurrency int, accept func(status int) bool, fn func(worker int) (int, error)) *LoadResult {
	if concurrency < 1 {
		concurrency = 1
	}
	var (
		requests, clientErr, serverErr, unexpected int64
		mu                                         sync.Mutex
		failures                                   []string
	)
	sample := func(msg string) {
		mu.Lock()
		if len(failures) < maxFailureSamples {
			failures = append(failures, msg)
		}
		mu.Unlock()
	}
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for ctx.Err() == nil {
				status, err := fn(worker)
				atomic.AddInt64(&requests, 1)
				switch {
				case err != nil:
					atomic.AddInt64(&clientErr, 1)
					sample(fmt.Sprintf("worker %d: err=%v", worker, err))
				case status >= 500:
					atomic.AddInt64(&serverErr, 1)
					sample(fmt.Sprintf("worker %d: status %d", worker, status))
				case status < 0 || !accept(status):
					atomic.AddInt64(&unexpected, 1)
					sample(fmt.Sprintf("worker %d: unexpected status %d", worker, status))
				}
			}
		}(w)
	}
	wg.Wait()
	mu.Lock()
	sampled := append([]string(nil), failures...)
	mu.Unlock()
	return &LoadResult{
		Requests:    atomic.LoadInt64(&requests),
		ClientError: atomic.LoadInt64(&clientErr),
		ServerError: atomic.LoadInt64(&serverErr),
		Unexpected:  atomic.LoadInt64(&unexpected),
		Failures:    sampled,
	}
}
