package elasticacheui

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves ElastiCache UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /clusters
func (h *Handler) ListCacheClusters(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "elasticache", "DescribeCacheClusters", region, account)

	resp, err := h.provider.DescribeCacheClusters(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawClusters, _ := resp.Data["CacheClusters"].([]any)
	items := make([]CacheCluster, 0, len(rawClusters))
	for _, raw := range rawClusters {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapCacheCluster(m))
		}
	}
	uihelper.WriteJSON(w, ListCacheClustersResponse{Items: items, Total: len(items)})
}

// POST /clusters  body: { "id": "...", "engine": "redis", "nodeType": "...", "numNodes": 1 }
func (h *Handler) CreateCacheCluster(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID       string `json:"id"`
		Engine   string `json:"engine"`
		NodeType string `json:"nodeType"`
		NumNodes int    `json:"numNodes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.ID == "" {
		uihelper.UIError(w, "BadRequest", "id is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "elasticache", "CreateCacheCluster", region, account)
	nr.Params["CacheClusterId"] = req.ID
	if req.Engine != "" {
		nr.Params["Engine"] = req.Engine
	}
	if req.NodeType != "" {
		nr.Params["CacheNodeType"] = req.NodeType
	}
	if req.NumNodes > 0 {
		nr.Params["NumCacheNodes"] = fmt.Sprintf("%d", req.NumNodes)
	}

	resp, err := h.provider.CreateCacheCluster(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /clusters/{id}
func (h *Handler) DeleteCacheCluster(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "elasticache", "DeleteCacheCluster", region, account)
	nr.Params["CacheClusterId"] = id

	if _, err := h.provider.DeleteCacheCluster(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// helpers

func mapCacheCluster(m map[string]any) CacheCluster {
	c := CacheCluster{
		ID:       strAny(m, "CacheClusterId"),
		Status:   strAny(m, "CacheClusterStatus"),
		Engine:   strAny(m, "Engine"),
		NodeType: strAny(m, "CacheNodeType"),
	}
	if n, ok := m["NumCacheNodes"].(float64); ok {
		c.NumNodes = int(n)
	}
	if ep, ok := m["ConfigurationEndpoint"].(map[string]any); ok {
		addr := strAny(ep, "Address")
		port := ""
		if p, ok := ep["Port"].(float64); ok {
			port = fmt.Sprintf(":%d", int(p))
		}
		c.Endpoint = addr + port
	}
	return c
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
