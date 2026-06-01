package eksui

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves EKS UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /clusters
func (h *Handler) ListClusters(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "eks", "ListClusters", region, account)

	resp, err := h.provider.ListClusters(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawNames, _ := resp.Data["clusters"].([]any)
	items := make([]Cluster, 0, len(rawNames))
	for _, raw := range rawNames {
		if name, ok := raw.(string); ok {
			items = append(items, Cluster{Name: name})
		}
	}
	nextToken, _ := resp.Data["nextToken"].(string)
	uihelper.WriteJSON(w, ListClustersResponse{Items: items, Total: len(items), NextToken: nextToken})
}

// POST /clusters  body: { "name": "...", "version": "..." }
func (h *Handler) CreateCluster(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "eks", "CreateCluster", region, account)
	nr.Params["name"] = req.Name
	if req.Version != "" {
		nr.Params["version"] = req.Version
	}

	resp, err := h.provider.CreateCluster(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /clusters/{name}
func (h *Handler) DeleteCluster(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "eks", "DeleteCluster", region, account)
	nr.Params["name"] = name

	if _, err := h.provider.DeleteCluster(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
