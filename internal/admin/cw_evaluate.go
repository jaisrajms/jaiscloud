package admin

import (
	"context"
	"encoding/json"
	"net/http"
)

// CWAlarmEvaluator can immediately trigger a single alarm-evaluation pass.
// Used by integration tests to verify state transitions without waiting 30 s.
type CWAlarmEvaluator interface {
	EvaluateAll(ctx context.Context)
}

// SetCWAlarmEvaluator wires the CloudWatch alarm evaluator into the admin handler.
func (h *Handler) SetCWAlarmEvaluator(e CWAlarmEvaluator) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cwAlarmEvaluator = e
}

// CWEvaluateHandler handles POST /_jaiscloud/cw-evaluate.
// It triggers one synchronous alarm-evaluation pass and returns immediately.
func (h *Handler) CWEvaluateHandler(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	ev := h.cwAlarmEvaluator
	h.mu.Unlock()
	if ev != nil {
		// Alarm actions fan out in goroutines that outlive this request.
		ev.EvaluateAll(context.WithoutCancel(r.Context()))
	}
	h.tickPlugins(r.Context(), "monitoring")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "evaluated"}) //nolint:errcheck
}
