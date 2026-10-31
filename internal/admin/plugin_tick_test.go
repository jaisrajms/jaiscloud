package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type recordingTicker struct {
	mu       sync.Mutex
	services []string
}

func (r *recordingTicker) Tick(_ context.Context, service string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.services = append(r.services, service)
}

// Each AWS test hook tells plugins that own the service, even when the built-in
// has nothing registered (a plugin replaced the whole service).
func TestTestHooksTickPluginsThatOwnTheService(t *testing.T) {
	for _, tc := range []struct {
		path    string
		handler func(*Handler) http.HandlerFunc
		service string
	}{
		{"/_jaiscloud/eb-tick", func(h *Handler) http.HandlerFunc { return h.EBTickHandler }, "events"},
		{"/_jaiscloud/cw-evaluate", func(h *Handler) http.HandlerFunc { return h.CWEvaluateHandler }, "monitoring"},
		{"/_jaiscloud/ttl-sweep", func(h *Handler) http.HandlerFunc { return h.TTLSweepHandler }, "dynamodb"},
		{"/_jaiscloud/firehose/flush", func(h *Handler) http.HandlerFunc { return h.FirehoseFlushHandler }, "firehose"},
	} {
		t.Run(tc.service, func(t *testing.T) {
			h := NewHandler()
			rec := &recordingTicker{}
			h.SetPluginTicker(rec)
			w := httptest.NewRecorder()
			tc.handler(h)(w, httptest.NewRequest(http.MethodPost, tc.path, nil))
			require.Less(t, w.Code, 300)
			require.Equal(t, []string{tc.service}, rec.services)
		})
	}
}

func TestTestHooksWorkWithoutPlugins(t *testing.T) {
	h := NewHandler()
	w := httptest.NewRecorder()
	h.EBTickHandler(w, httptest.NewRequest(http.MethodPost, "/_jaiscloud/eb-tick", nil))
	require.Equal(t, http.StatusNoContent, w.Code)
}
