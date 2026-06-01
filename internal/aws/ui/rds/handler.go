package rdsui

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves RDS UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /instances
func (h *Handler) ListDBInstances(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "rds", "DescribeDBInstances", region, account)

	resp, err := h.provider.DescribeDBInstances(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawInstances, _ := resp.Data["DBInstances"].([]any)
	items := make([]DBInstance, 0, len(rawInstances))
	for _, raw := range rawInstances {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapDBInstance(m))
		}
	}
	uihelper.WriteJSON(w, ListDBInstancesResponse{Items: items, Total: len(items)})
}

// POST /instances  body: { "id": "...", "engine": "mysql", "class": "db.t3.micro", "username": "admin", "password": "..." }
func (h *Handler) CreateDBInstance(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID       string `json:"id"`
		Engine   string `json:"engine"`
		Class    string `json:"class"`
		Username string `json:"username"`
		Password string `json:"password"`
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
	nr := uihelper.NR(r.Context(), h.cfg, "rds", "CreateDBInstance", region, account)
	nr.Params["DBInstanceIdentifier"] = req.ID
	if req.Engine != "" {
		nr.Params["Engine"] = req.Engine
	}
	if req.Class != "" {
		nr.Params["DBInstanceClass"] = req.Class
	}
	if req.Username != "" {
		nr.Params["MasterUsername"] = req.Username
	}
	if req.Password != "" {
		nr.Params["MasterUserPassword"] = req.Password
	}

	resp, err := h.provider.CreateDBInstance(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /instances/{id}
func (h *Handler) DeleteDBInstance(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "rds", "DeleteDBInstance", region, account)
	nr.Params["DBInstanceIdentifier"] = id

	if _, err := h.provider.DeleteDBInstance(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /instances/{id}/start
func (h *Handler) StartDBInstance(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "rds", "StartDBInstance", region, account)
	nr.Params["DBInstanceIdentifier"] = id

	if _, err := h.provider.StartDBInstance(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /instances/{id}/stop
func (h *Handler) StopDBInstance(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "rds", "StopDBInstance", region, account)
	nr.Params["DBInstanceIdentifier"] = id

	if _, err := h.provider.StopDBInstance(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// helpers

func mapDBInstance(m map[string]any) DBInstance {
	d := DBInstance{
		ID:     strAny(m, "DBInstanceIdentifier"),
		Status: strAny(m, "DBInstanceStatus"),
		Engine: strAny(m, "Engine"),
		Class:  strAny(m, "DBInstanceClass"),
	}
	if ep, ok := m["Endpoint"].(map[string]any); ok {
		addr := strAny(ep, "Address")
		if p, ok := ep["Port"].(float64); ok {
			d.Endpoint = addr
			d.Port = int(p)
		} else {
			d.Endpoint = addr
		}
	}
	return d
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
