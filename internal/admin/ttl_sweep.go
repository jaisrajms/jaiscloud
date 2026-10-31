package admin

import (
	"context"
	"net/http"
)

// TTLSweeper can immediately trigger a synchronous DynamoDB TTL sweep.
// Used by integration tests to expire items without waiting for the ticker.
type TTLSweeper interface {
	SweepNow(ctx context.Context)
}

// RegisterTTLSweeper wires the TTL worker into the admin handler.
func (h *Handler) RegisterTTLSweeper(s TTLSweeper) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ttlSweeper = s
}

// TTLSweepHandler handles POST /_jaiscloud/ttl-sweep.
// Runs one synchronous TTL sweep using the current clock.Now() value.
func (h *Handler) TTLSweepHandler(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	sw := h.ttlSweeper
	h.mu.Unlock()
	if sw != nil {
		sw.SweepNow(r.Context())
	}
	h.tickPlugins(r.Context(), "dynamodb")
	w.WriteHeader(http.StatusNoContent)
}
