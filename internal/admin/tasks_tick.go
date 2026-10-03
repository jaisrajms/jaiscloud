package admin

import (
	"context"
	"net/http"
)

// TasksTicker can synchronously run one Cloud Tasks dispatch pass. Used by
// integration tests: the engine uses a wall-time ticker internally — advancing
// the frozen clock does not wake that ticker, so tests need this trigger. The
// Cloud Tasks dispatch engine implements it.
type TasksTicker interface {
	TickNow(ctx context.Context)
}

// RegisterTasksTicker wires a Cloud Tasks engine into the admin handler.
func (h *Handler) RegisterTasksTicker(t TasksTicker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tasksTicker = t
}

// TasksTickHandler handles POST /_jaiscloud/tasks-tick. Synchronously dispatches
// every due task against clock.Now().
func (h *Handler) TasksTickHandler(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	tick := h.tasksTicker
	h.mu.Unlock()
	if tick != nil {
		tick.TickNow(r.Context())
	}
	w.WriteHeader(http.StatusNoContent)
}
