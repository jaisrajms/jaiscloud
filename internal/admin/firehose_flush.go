package admin

import (
	"context"
	"encoding/json"
	"net/http"
)

// FirehoseFlusher can immediately flush all buffered Firehose records to S3.
type FirehoseFlusher interface {
	FlushAll(ctx context.Context)
}

// SetFirehoseFlusher wires the Firehose flusher into the admin handler.
func (h *Handler) SetFirehoseFlusher(f FirehoseFlusher) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.firehoseFlusher = f
}

// FirehoseFlushHandler handles POST /_jaiscloud/firehose/flush
func (h *Handler) FirehoseFlushHandler(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	f := h.firehoseFlusher
	h.mu.Unlock()
	if f != nil {
		f.FlushAll(r.Context())
	}
	h.tickPlugins(r.Context(), "firehose")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "flushed"}) //nolint:errcheck
}
