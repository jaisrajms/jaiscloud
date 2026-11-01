package admin

import (
	"context"
	"encoding/json"
	"net/http"
)

// ProjectRow is one project the emulator knows about, as served by
// GET /_jaiscloud/projects. The kinds mirror the console's own
// project-discovery API (configured at startup, registered through Resource
// Manager, or merely observed on the wire) so a shell and the UI agree.
type ProjectRow struct {
	ID            string   `json:"id"`
	Kind          string   `json:"kind"`
	State         string   `json:"state,omitempty"`
	ProjectNumber string   `json:"projectNumber,omitempty"`
	DisplayName   string   `json:"displayName,omitempty"`
	CreateTime    string   `json:"createTime,omitempty"`
	FirstSeen     string   `json:"firstSeen,omitempty"`
	LastSeen      string   `json:"lastSeen,omitempty"`
	Requests      int64    `json:"requests,omitempty"`
	Services      []string `json:"services,omitempty"`
}

// ProjectLister enumerates every project the running binary can see. The GCP
// binary registers the Resource Manager-backed implementation; other binaries
// leave it unset, so the route answers 501 rather than pretending there are no
// projects.
type ProjectLister interface {
	ListProjectRows(ctx context.Context) ([]ProjectRow, error)
}

// RegisterProjectLister wires project discovery into the admin plane. Call it
// after constructing the handler and before starting the server.
func (h *Handler) RegisterProjectLister(l ProjectLister) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.projectLister = l
}

// Projects handles GET /_jaiscloud/projects — the same configured ∪ registered
// ∪ observed view the console's session-gated read API serves, on the
// unauthenticated admin plane so a shell or CI job can ask "which project did
// the workload use?".
func (h *Handler) Projects(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	lister := h.projectLister
	h.mu.Unlock()

	if lister == nil {
		http.Error(w, "project discovery not configured", http.StatusNotImplemented)
		return
	}

	rows, err := lister.ListProjectRows(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if rows == nil {
		rows = []ProjectRow{}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Projects []ProjectRow `json:"projects"`
	}{Projects: rows})
}
