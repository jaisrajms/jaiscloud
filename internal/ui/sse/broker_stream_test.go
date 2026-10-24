package sse

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"jaiscloud/internal/events"
)

// flushRecorder is a race-safe ResponseWriter that records whether Flush was
// called, so a streaming test can synchronize on the handler's first write.
type flushRecorder struct {
	mu      sync.Mutex
	body    strings.Builder
	flushed chan struct{}
}

func newFlushRecorder() *flushRecorder {
	return &flushRecorder{flushed: make(chan struct{}, 1)}
}

func (r *flushRecorder) Header() http.Header { return http.Header{} }
func (r *flushRecorder) WriteHeader(int)     {}
func (r *flushRecorder) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.body.Write(p)
}

func (r *flushRecorder) Flush() {
	select {
	case r.flushed <- struct{}{}:
	default:
	}
}

func (r *flushRecorder) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.body.String()
}

// The stream must flush a comment immediately so EventSource fires onopen
// without waiting for the first event; otherwise the console sits in its
// disconnected/polling state against a perfectly healthy stream.
func TestServeHTTP_FlushesOnConnect(t *testing.T) {
	b := New(events.NewEventBus())
	t.Cleanup(b.Shutdown)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/ui/v1/events/stream", nil).WithContext(ctx)
	rec := newFlushRecorder()

	done := make(chan struct{})
	go func() {
		b.ServeHTTP(rec, req)
		close(done)
	}()

	<-rec.flushed
	if got := rec.String(); !strings.Contains(got, ": connected") {
		t.Fatalf("first flush = %q, want an initial : connected comment", got)
	}

	cancel()
	<-done
}
