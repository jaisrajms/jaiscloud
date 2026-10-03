package admin

import (
	"context"
	"net/http"
)

// SchedulerTicker can synchronously fire a scheduler loop once. Used by
// integration tests: the scheduler uses a wall-time ticker internally —
// advancing the frozen clock does not wake that ticker, so tests need this
// trigger. Cloud Scheduler's cron engine implements it.
type SchedulerTicker interface {
	TickNow(ctx context.Context)
}

// RegisterSchedulerTicker wires a scheduler engine into the admin handler.
func (h *Handler) RegisterSchedulerTicker(s SchedulerTicker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.schedulerTicker = s
}

// SchedulerTickHandler handles POST /_jaiscloud/scheduler-tick.
// Synchronously evaluates all pending scheduled jobs against clock.Now().
func (h *Handler) SchedulerTickHandler(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	sched := h.schedulerTicker
	h.mu.Unlock()
	if sched != nil {
		sched.TickNow(r.Context())
	}
	w.WriteHeader(http.StatusNoContent)
}
