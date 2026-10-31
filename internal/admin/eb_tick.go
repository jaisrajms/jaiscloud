package admin

import (
	"context"
	"net/http"
)

// EBSchedulerTicker can synchronously fire the EventBridge scheduler loop once.
// Used by integration tests: the scheduler uses a wall-time timer internally —
// advancing the frozen clock does not wake that timer, so tests need this trigger.
type EBSchedulerTicker interface {
	TickNow(ctx context.Context)
}

// RegisterEBScheduler wires the EventBridge scheduler into the admin handler.
func (h *Handler) RegisterEBScheduler(s EBSchedulerTicker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ebScheduler = s
}

// EBTickHandler handles POST /_jaiscloud/eb-tick.
// Synchronously evaluates all pending EventBridge rules against clock.Now().
func (h *Handler) EBTickHandler(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	sched := h.ebScheduler
	h.mu.Unlock()
	if sched != nil {
		// Target deliveries run in goroutines that outlive this request.
		sched.TickNow(context.WithoutCancel(r.Context()))
	}
	h.tickPlugins(r.Context(), "events")
	w.WriteHeader(http.StatusNoContent)
}
