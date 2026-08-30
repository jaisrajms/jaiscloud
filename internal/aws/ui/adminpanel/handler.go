// Package adminpanel provides the UI API backend for the Admin Panel.
// It delegates to the existing admin.Handler rather than re-implementing logic.
package adminpanel

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/admin"
	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler wraps the existing admin.Handler for UI-friendly responses.
type Handler struct {
	admin *admin.Handler
	cfg   *config.Config
}

// NewHandler creates a Handler.
func NewHandler(a *admin.Handler, cfg *config.Config) *Handler {
	return &Handler{admin: a, cfg: cfg}
}

// GET /admin/status  — returns doctor info + snapshotter names
func (h *Handler) Status(w http.ResponseWriter, r *http.Request) {
	// Proxy GET /_jaiscloud/doctor?verbose=true and enrich with snapshotter list.
	rec := &responseRecorder{}
	req, _ := http.NewRequestWithContext(r.Context(), "GET", "/_jaiscloud/doctor?verbose=true", nil)
	h.admin.Doctor(rec, req)

	var doctorResp map[string]any
	json.Unmarshal(rec.body.Bytes(), &doctorResp) //nolint:errcheck

	snapshotters := make([]string, 0)
	for name := range h.admin.Snapshotters() {
		snapshotters = append(snapshotters, name)
	}
	if doctorResp == nil {
		doctorResp = map[string]any{}
	}
	doctorResp["snapshotters"] = snapshotters

	uihelper.WriteJSON(w, doctorResp)
}

// POST /admin/reset
func (h *Handler) Reset(w http.ResponseWriter, r *http.Request) {
	req, _ := http.NewRequestWithContext(r.Context(), "POST", "/_jaiscloud/reset", nil)
	h.admin.Reset(w, req)
}

// GET /admin/export  — triggers export and returns tarball or JSON URL for download
func (h *Handler) ExportInfo(w http.ResponseWriter, r *http.Request) {
	uihelper.WriteJSON(w, map[string]string{
		"downloadUrl": fmt.Sprintf("/_jaiscloud/export"),
		"info":        "Use the download URL to export state as a gzip tarball",
	})
}

// GET /admin/clock
func (h *Handler) GetClock(w http.ResponseWriter, r *http.Request) {
	req, _ := http.NewRequestWithContext(r.Context(), "GET", "/_jaiscloud/clock", nil)
	h.admin.GetClock(w, req)
}

// POST /admin/clock  body: { "mode": "real" | "fixed" | "offset", "time": "..." }
func (h *Handler) SetClock(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		uihelper.UIError(w, "BadRequest", "failed to read body", http.StatusBadRequest)
		return
	}
	req, _ := http.NewRequestWithContext(r.Context(), "POST", "/_jaiscloud/clock", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	h.admin.SetClock(w, req)
}

// GET /admin/snapshots
func (h *Handler) ListSnapshots(w http.ResponseWriter, r *http.Request) {
	req, _ := http.NewRequestWithContext(r.Context(), "GET", "/_jaiscloud/snapshots", nil)
	h.admin.SnapshotList(w, req)
}

// POST /admin/snapshots  body: { "name": "...", "description": "..." }
func (h *Handler) CreateSnapshot(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		uihelper.UIError(w, "BadRequest", "failed to read body", http.StatusBadRequest)
		return
	}
	req, _ := http.NewRequestWithContext(r.Context(), "POST", "/_jaiscloud/snapshot", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	h.admin.SnapshotCreate(w, req)
}

// POST /admin/snapshots/{name}/revert
func (h *Handler) RevertSnapshot(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}
	target := fmt.Sprintf("/_jaiscloud/snapshot/%s/revert?reset_first=true", url.PathEscape(name))
	req, _ := http.NewRequestWithContext(r.Context(), "POST", target, nil)
	h.admin.SnapshotRevert(w, req)
}

// DELETE /admin/snapshots/{name}
func (h *Handler) DeleteSnapshot(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}
	target := fmt.Sprintf("/_jaiscloud/snapshot/%s?yes=true", url.PathEscape(name))
	req, _ := http.NewRequestWithContext(r.Context(), "DELETE", target, nil)
	h.admin.SnapshotDelete(w, req)
}

// responseRecorder captures response body without writing to the wire.
type responseRecorder struct {
	body       bytes.Buffer
	statusCode int
	header     http.Header
}

func (rec *responseRecorder) Header() http.Header {
	if rec.header == nil {
		rec.header = make(http.Header)
	}
	return rec.header
}

func (rec *responseRecorder) Write(b []byte) (int, error) {
	return rec.body.Write(b)
}

func (rec *responseRecorder) WriteHeader(code int) {
	rec.statusCode = code
}
