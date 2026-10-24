package resourcemanagerui

import (
	"encoding/json"
	"net/http"
	"time"

	resourcemanagercore "jaiscloud/internal/gcp/service/resourcemanager"
	"jaiscloud/internal/gcp/ui/uihelper"
)

// Handler serves Resource Manager UI API requests by calling the Cloud
// Resource Manager core.
type Handler struct {
	provider ProviderInterface
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface) *Handler {
	return &Handler{provider: p}
}

// ─── mapping helpers ─────────────────────────────────────────────────────────

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// render flattens a core project into the console row.
func render(p resourcemanagercore.Project) Project {
	return Project{
		ProjectID:     p.ProjectID,
		ProjectNumber: p.ProjectNumber,
		DisplayName:   p.DisplayName,
		State:         p.State,
		Etag:          p.Etag,
		Parent:        p.Parent,
		CreateTime:    formatTime(p.CreateTime),
		UpdateTime:    formatTime(p.UpdateTime),
		DeleteTime:    formatTime(p.DeleteTime),
		Labels:        p.Labels,
	}
}

// ─── Projects ────────────────────────────────────────────────────────────────

// GET /projects
//
// The manager shows every project, including DELETE_REQUESTED ones (so they can
// be undeleted), and aggregates across cursor pages so the console never has to
// stitch pagination together.
func (h *Handler) ListProjects(w http.ResponseWriter, r *http.Request) {
	out := []Project{}
	token := ""
	for {
		page, next, err := h.provider.ListProjects(r.Context(), 0, token, true, "")
		if err != nil {
			uihelper.WriteError(w, err)
			return
		}
		for _, p := range page {
			out = append(out, render(p))
		}
		if next == "" {
			break
		}
		token = next
	}
	uihelper.WriteJSON(w, ListProjectsResponse{Projects: out, Total: len(out)})
}

// POST /projects
func (h *Handler) CreateProject(w http.ResponseWriter, r *http.Request) {
	var in CreateProjectInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		uihelper.UIError(w, "InvalidArgument", "invalid JSON body", http.StatusBadRequest)
		return
	}
	proj, _, err := h.provider.CreateProject(r.Context(), resourcemanagercore.CreateProjectInput{
		ProjectID:   in.ProjectID,
		DisplayName: in.DisplayName,
		Labels:      in.Labels,
	})
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, render(proj))
}

// GET /projects/{project}
func (h *Handler) GetProject(w http.ResponseWriter, r *http.Request) {
	id, ok := uihelper.Segment(r, "project")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid project", http.StatusBadRequest)
		return
	}
	proj, err := h.provider.GetProject(r.Context(), id)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, render(proj))
}

// DELETE /projects/{project} — marks the project DELETE_REQUESTED and returns
// the updated project (real GCP keeps it restorable for a 30-day window).
func (h *Handler) DeleteProject(w http.ResponseWriter, r *http.Request) {
	id, ok := uihelper.Segment(r, "project")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid project", http.StatusBadRequest)
		return
	}
	proj, _, err := h.provider.DeleteProject(r.Context(), id)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, render(proj))
}

// POST /projects/{project}/undelete — restores a DELETE_REQUESTED project.
func (h *Handler) UndeleteProject(w http.ResponseWriter, r *http.Request) {
	id, ok := uihelper.Segment(r, "project")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid project", http.StatusBadRequest)
		return
	}
	proj, _, err := h.provider.UndeleteProject(r.Context(), id)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, render(proj))
}
