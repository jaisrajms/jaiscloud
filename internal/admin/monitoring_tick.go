package admin

import (
	"context"
	"net/http"
)

// MonitoringTicker can synchronously evaluate Cloud Monitoring alert policies
// once. Used by integration tests and the demo: the evaluator uses a wall-time
// 30s ticker internally — advancing the frozen clock does not wake it, so a
// deterministic trigger is needed to see an incident fire without waiting.
// The Monitoring alert-policy evaluator implements it.
type MonitoringTicker interface {
	TickNow(ctx context.Context)
}

// RegisterMonitoringTicker wires a Monitoring evaluator into the admin handler.
func (h *Handler) RegisterMonitoringTicker(m MonitoringTicker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.monitoringTicker = m
}

// MonitoringTickHandler handles POST /_jaiscloud/monitoring-tick. Synchronously
// evaluates every enabled alert policy in every known project once, opening or
// closing incidents (and delivering notifications) as of clock.Now().
func (h *Handler) MonitoringTickHandler(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	tick := h.monitoringTicker
	h.mu.Unlock()
	if tick != nil {
		tick.TickNow(r.Context())
	}
	w.WriteHeader(http.StatusNoContent)
}
