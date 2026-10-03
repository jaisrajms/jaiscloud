package admin

import (
	"io"
	"net/http"
)

// ThrottleController is an optional capability a cloud may register to expose
// runtime throttle arm/disarm over the admin plane. It mirrors the clock
// endpoint: a test can change the failure-injection policy mid-run rather than
// restarting the emulator with new environment variables. The payloads are
// cloud-owned JSON documents, so the shared admin plane stays cloud-neutral.
type ThrottleController interface {
	// SetThrottleConfig applies a control document and returns the resulting
	// status document. A non-nil error rejects the request (HTTP 400).
	SetThrottleConfig(body []byte) ([]byte, error)
	// ThrottleStatus returns the current status document.
	ThrottleStatus() []byte
}

// maxThrottleBody bounds the control document read from the request body.
const maxThrottleBody = 64 << 10

// RegisterThrottle wires a cloud's throttle injector into the admin handler.
func (h *Handler) RegisterThrottle(c ThrottleController) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.throttle = c
}

// SetThrottle handles POST /_jaiscloud/throttle — arms, disarms, or retunes the
// throttle injector at runtime. The JSON body fully replaces the running
// configuration and clears accumulated counters (see the cloud controller for
// the schema). Responds with the resulting status document.
func (h *Handler) SetThrottle(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	c := h.throttle
	h.mu.Unlock()
	if c == nil {
		http.Error(w, "throttle control not configured", http.StatusNotImplemented)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxThrottleBody))
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}
	status, err := c.SetThrottleConfig(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeThrottleStatus(w, status)
}

// GetThrottle handles GET /_jaiscloud/throttle — returns the current throttle
// status document.
func (h *Handler) GetThrottle(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	c := h.throttle
	h.mu.Unlock()
	if c == nil {
		http.Error(w, "throttle control not configured", http.StatusNotImplemented)
		return
	}
	writeThrottleStatus(w, c.ThrottleStatus())
}

func writeThrottleStatus(w http.ResponseWriter, status []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(status)
}
