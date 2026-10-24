package loadtest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseMetrics(t *testing.T) {
	text := `# HELP go_goroutines Number of goroutines.
# TYPE go_goroutines gauge
go_goroutines 19
go_memstats_alloc_bytes 1.234e+07
http_requests_total{code="200"} 5
process_resident_memory_bytes 6.5e+07
go_info{version="go1.26"} 1
not_a_number borked
`
	m := ParseMetrics(text)
	require.EqualValues(t, 19, m["go_goroutines"])
	require.EqualValues(t, 1.234e7, m["go_memstats_alloc_bytes"])
	require.EqualValues(t, 6.5e7, m["process_resident_memory_bytes"])
	require.NotContains(t, m, "http_requests_total", "labelled samples are ignored")
	require.NotContains(t, m, "go_info")
	require.Len(t, m, 3)
}

func TestRunLoadHealthy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	r := RunLoad(ctx, 4, func(s int) bool { return s == http.StatusOK },
		func(int) (int, error) { return http.StatusOK, nil })
	require.Greater(t, r.Requests, int64(0))
	require.False(t, r.Failed(), r.Summary())
}

func TestRunLoadCountsFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	var seq int64
	r := RunLoad(ctx, 8, func(s int) bool { return s == http.StatusOK },
		func(int) (int, error) {
			switch atomic.AddInt64(&seq, 1) % 4 {
			case 0:
				return http.StatusOK, nil
			case 1:
				return http.StatusInternalServerError, nil
			case 2:
				return http.StatusNotFound, nil
			default:
				return 0, errors.New("boom")
			}
		})
	require.Greater(t, r.Requests, int64(0))
	require.Greater(t, r.ServerError, int64(0), "seeded 5xx must count as a server error")
	require.Greater(t, r.Unexpected, int64(0), "a 404 outside accept must count as unexpected")
	require.Greater(t, r.ClientError, int64(0), "a transport error must count as a client error")
	require.True(t, r.Failed())
	require.NotEmpty(t, r.Failures)
	require.LessOrEqual(t, len(r.Failures), maxFailureSamples)
}

func TestCheckGrowth(t *testing.T) {
	base := Metrics{
		"go_goroutines":                 20,
		"go_memstats_alloc_bytes":       1 << 20,
		"process_resident_memory_bytes": 10 << 20,
	}
	th := GrowthThresholds{MaxGoroutines: 10, MaxHeapPercent: 50, MaxRSSPercent: 50}

	require.NoError(t, CheckGrowth(base, Metrics{
		"go_goroutines":                 25,
		"go_memstats_alloc_bytes":       1<<20 + (100 << 10), // +9.7%
		"process_resident_memory_bytes": 11 << 20,            // +10%
	}, th))

	require.Error(t, CheckGrowth(base, Metrics{
		"go_goroutines": 100, "go_memstats_alloc_bytes": 1 << 20, "process_resident_memory_bytes": 10 << 20,
	}, th), "a goroutine spike must fail")

	require.Error(t, CheckGrowth(base, Metrics{
		"go_goroutines": 20, "go_memstats_alloc_bytes": 4 << 20, "process_resident_memory_bytes": 10 << 20,
	}, th), "heap growth past the ceiling must fail")

	// Metrics absent from either snapshot are skipped, not treated as zero.
	require.NoError(t, CheckGrowth(
		Metrics{"go_goroutines": 20},
		Metrics{"go_goroutines": 20},
		th))
}

// ─── scenario assertions, proven against a fake emulator ─────────────────────

func TestLoadGCSDetectsCorruption(t *testing.T) {
	fake := newFakeEmulator()
	defer fake.Close()
	a := &api{base: fake.URL(), client: http.DefaultClient}
	ctx := context.Background()

	clean := loadGCS(ctx, a, 4, 200*time.Millisecond)
	require.False(t, clean.Failed(), clean.Summary())

	fake.setCorruptGCS(true)
	dirty := loadGCS(ctx, a, 4, 200*time.Millisecond)
	require.True(t, dirty.Failed(), "a corrupted download must fail the gate: %s", dirty.Summary())
	require.Greater(t, dirty.ClientError, int64(0))
}

func TestLoadFirestoreDetectsBadCommit(t *testing.T) {
	fake := newFakeEmulator()
	defer fake.Close()
	a := &api{base: fake.URL(), client: http.DefaultClient}
	ctx := context.Background()

	fake.setCommitStatus(http.StatusInternalServerError)
	down := loadFirestore(ctx, a, 4, 200*time.Millisecond)
	require.Greater(t, down.ServerError, int64(0), "a 5xx commit must count as a server error")

	fake.setCommitStatus(http.StatusOK)
	fake.setCommitBody([]byte(`{"writeResults":[]}`)) // no commitTime
	bad := loadFirestore(ctx, a, 4, 200*time.Millisecond)
	require.Greater(t, bad.ClientError, int64(0), "a commit missing commitTime/writeResults must fail")
}

func TestVerifyFanoutDetectsDroppedCopy(t *testing.T) {
	fake := newFakeEmulator()
	defer fake.Close()
	a := &api{base: fake.URL(), client: http.DefaultClient}
	ctx := context.Background()

	require.NoError(t, verifyFanout(ctx, a, 20), "a complete fan-out must pass")

	fake.setDropSub(pubSubName(pubSubscriptions - 1))
	err := verifyFanout(ctx, a, 20)
	require.Error(t, err, "a subscription missing a fan-out copy must fail")
	require.Contains(t, err.Error(), pubSubName(pubSubscriptions-1))
}

// ─── fake emulator ───────────────────────────────────────────────────────────

type fakeMsg struct {
	ackID string
	data  string
}

// fakeEmulator implements just enough of the GCS / Pub/Sub / Firestore REST
// surface for the scenario assertions to run without a real emulator. The knobs
// inject the faults the tests expect the harness to catch.
type fakeEmulator struct {
	srv *httptest.Server

	mu         sync.Mutex
	objects    map[string][]byte
	queues     map[string][]fakeMsg
	corruptGCS bool
	commitBody []byte
	commitCode int
	dropSub    string
	seq        int64
}

func newFakeEmulator() *fakeEmulator {
	f := &fakeEmulator{
		objects:    map[string][]byte{},
		queues:     map[string][]fakeMsg{},
		commitCode: http.StatusOK,
		commitBody: []byte(`{"commitTime":"2026-10-14T00:00:00Z","writeResults":[{}]}`),
	}
	for i := 0; i < pubSubscriptions; i++ {
		f.queues[pubSubName(i)] = nil
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	return f
}

func (f *fakeEmulator) URL() string { return f.srv.URL }
func (f *fakeEmulator) Close()      { f.srv.Close() }

func (f *fakeEmulator) setCorruptGCS(v bool)   { f.mu.Lock(); f.corruptGCS = v; f.mu.Unlock() }
func (f *fakeEmulator) setDropSub(s string)    { f.mu.Lock(); f.dropSub = s; f.mu.Unlock() }
func (f *fakeEmulator) setCommitStatus(c int)  { f.mu.Lock(); f.commitCode = c; f.mu.Unlock() }
func (f *fakeEmulator) setCommitBody(b []byte) { f.mu.Lock(); f.commitBody = b; f.mu.Unlock() }

func (f *fakeEmulator) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/upload/storage/v1/b/") && r.Method == http.MethodPost:
		f.handleUpload(w, r)
	case strings.HasPrefix(r.URL.Path, "/storage/v1/b/") && r.Method == http.MethodGet:
		f.handleMedia(w, r)
	case strings.HasSuffix(r.URL.Path, ":publish"):
		f.handlePublish(w, r)
	case strings.HasSuffix(r.URL.Path, ":pull"):
		f.handlePull(w, r)
	case strings.HasSuffix(r.URL.Path, ":acknowledge"):
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	case strings.HasSuffix(r.URL.Path, "documents:commit"):
		f.handleCommit(w)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeEmulator) handleUpload(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.objects[name] = body
	f.mu.Unlock()
	_, _ = w.Write([]byte(`{}`))
}

func (f *fakeEmulator) handleMedia(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/storage/v1/b/")
	i := strings.Index(rest, "/o/")
	obj := rest[i+3:]
	f.mu.Lock()
	data := append([]byte(nil), f.objects[obj]...)
	corrupt := f.corruptGCS
	f.mu.Unlock()
	if corrupt && len(data) > 0 {
		data[0] ^= 0xff
	}
	_, _ = w.Write(data)
}

func (f *fakeEmulator) handlePublish(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Messages []struct {
			Data string `json:"data"`
		} `json:"messages"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	id := atomic.AddInt64(&f.seq, 1)
	f.mu.Lock()
	for i := 0; i < pubSubscriptions; i++ {
		sub := pubSubName(i)
		if sub == f.dropSub {
			continue
		}
		for _, m := range req.Messages {
			f.queues[sub] = append(f.queues[sub], fakeMsg{ackID: strconv.FormatInt(id, 10), data: m.Data})
		}
	}
	f.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{"messageIds": []string{strconv.FormatInt(id, 10)}})
}

func (f *fakeEmulator) handlePull(w http.ResponseWriter, r *http.Request) {
	sub := subFromPath(r.URL.Path)
	var req struct {
		MaxMessages int `json:"maxMessages"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.MaxMessages <= 0 {
		req.MaxMessages = 100
	}
	f.mu.Lock()
	q := f.queues[sub]
	n := req.MaxMessages
	if n > len(q) {
		n = len(q)
	}
	claimed := append([]fakeMsg(nil), q[:n]...)
	f.queues[sub] = append([]fakeMsg(nil), q[n:]...)
	f.mu.Unlock()
	received := make([]map[string]any, 0, len(claimed))
	for _, m := range claimed {
		received = append(received, map[string]any{
			"ackId":   m.ackID,
			"message": map[string]any{"data": m.data},
		})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"receivedMessages": received})
}

func (f *fakeEmulator) handleCommit(w http.ResponseWriter) {
	f.mu.Lock()
	code, body := f.commitCode, f.commitBody
	f.mu.Unlock()
	if code != http.StatusOK {
		w.WriteHeader(code)
		_, _ = w.Write([]byte(`{"error":{"code":500,"message":"boom","status":"INTERNAL"}}`))
		return
	}
	_, _ = w.Write(body)
}

func subFromPath(path string) string {
	i := strings.Index(path, "/subscriptions/")
	if i < 0 {
		return ""
	}
	rest := path[i+len("/subscriptions/"):]
	if j := strings.Index(rest, ":"); j >= 0 {
		return rest[:j]
	}
	return rest
}
