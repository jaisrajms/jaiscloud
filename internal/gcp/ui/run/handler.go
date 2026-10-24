package runui

import (
	"net/http"
	"strings"

	"jaiscloud/internal/config"
	runcore "jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/gcp/ui/uihelper"
)

// Handler serves Cloud Run UI API requests by calling the Cloud Run core.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// account resolves the project for a request, falling back to the configured
// project when the inject-config middleware has not populated the context.
func (h *Handler) account(r *http.Request) string {
	if a := uihelper.AccountFrom(r); a != "" {
		return a
	}
	return h.cfg.AccountID
}

// ─── mapping helpers ─────────────────────────────────────────────────────────

func str(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

// segment returns the decoded, single-segment value of a URL path parameter.
// A decoded '/' is rejected: regions and service ids are single path segments.
func segment(r *http.Request, key string) (string, bool) {
	v := uihelper.PathParam(r, key)
	if v == "" || strings.Contains(v, "/") {
		return "", false
	}
	return v, true
}

// ready reports whether the rendered service's terminal condition succeeded.
func ready(js map[string]any) bool {
	cond, _ := js["terminalCondition"].(map[string]any)
	return str(cond, "state") == "CONDITION_SUCCEEDED"
}

// summarise flattens a stored service into the list row, reusing the core's
// wire rendering so the console shows exactly what the API returns.
func summarise(s runstore.Service) Service {
	js := runcore.ServiceJSON(s)
	return Service{
		ID:                    s.ID,
		Region:                s.Location,
		Name:                  str(js, "name"),
		URI:                   str(js, "uri"),
		LatestReadyRevision:   str(js, "latestReadyRevision"),
		LatestCreatedRevision: str(js, "latestCreatedRevision"),
		Generation:            str(js, "generation"),
		CreateTime:            str(js, "createTime"),
		UpdateTime:            str(js, "updateTime"),
		Ready:                 ready(js),
	}
}

// ─── Services ────────────────────────────────────────────────────────────────

// GET /services
func (h *Handler) ListServices(w http.ResponseWriter, r *http.Request) {
	svcs, err := h.provider.ListAllServices(r.Context(), h.account(r))
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	services := make([]Service, 0, len(svcs))
	for _, s := range svcs {
		services = append(services, summarise(s))
	}
	uihelper.WriteJSON(w, ListServicesResponse{Services: services, Total: len(services)})
}

// GET /services/{region}/{service}
func (h *Handler) GetService(w http.ResponseWriter, r *http.Request) {
	region, service, ok := h.target(w, r)
	if !ok {
		return
	}
	svc, err := h.provider.GetService(r.Context(), h.account(r), region, service)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, runcore.ServiceJSON(svc))
}

// DELETE /services/{region}/{service}
func (h *Handler) DeleteService(w http.ResponseWriter, r *http.Request) {
	region, service, ok := h.target(w, r)
	if !ok {
		return
	}
	if _, err := h.provider.DeleteService(r.Context(), h.account(r), region, service, false, ""); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ─── Revisions ───────────────────────────────────────────────────────────────

// GET /services/{region}/{service}/revisions
func (h *Handler) ListRevisions(w http.ResponseWriter, r *http.Request) {
	region, service, ok := h.target(w, r)
	if !ok {
		return
	}
	revs, err := h.provider.ListRevisions(r.Context(), h.account(r), region, service)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	revisions := make([]map[string]any, 0, len(revs))
	for _, rev := range revs {
		revisions = append(revisions, runcore.RevisionJSON(rev))
	}
	uihelper.WriteJSON(w, ListRevisionsResponse{Revisions: revisions, Total: len(revisions)})
}

// GET /services/{region}/{service}/revisions/{revision}
func (h *Handler) GetRevision(w http.ResponseWriter, r *http.Request) {
	region, service, ok := h.target(w, r)
	if !ok {
		return
	}
	revision, ok := segment(r, "revision")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid revision", http.StatusBadRequest)
		return
	}
	rev, err := h.provider.GetRevision(r.Context(), h.account(r), region, service, revision)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, runcore.RevisionJSON(rev))
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// target reads and validates the {region}/{service} path parameters, writing a
// 400 and returning false when either is missing or malformed.
func (h *Handler) target(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	region, ok := segment(r, "region")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid region", http.StatusBadRequest)
		return "", "", false
	}
	service, ok := segment(r, "service")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid service", http.StatusBadRequest)
		return "", "", false
	}
	return region, service, true
}
