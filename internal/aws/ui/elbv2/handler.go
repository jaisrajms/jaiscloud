package elbv2ui

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves ELBv2 UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /load-balancers
func (h *Handler) ListLoadBalancers(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "elasticloadbalancing", "DescribeLoadBalancers", region, account)

	resp, err := h.provider.DescribeLoadBalancers(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawLBs, _ := resp.Data["LoadBalancers"].([]any)
	items := make([]LoadBalancer, 0, len(rawLBs))
	for _, raw := range rawLBs {
		if m, ok := raw.(map[string]any); ok {
			lb := LoadBalancer{
				ARN:     strAny(m, "LoadBalancerArn"),
				Name:    strAny(m, "LoadBalancerName"),
				DNSName: strAny(m, "DNSName"),
				Scheme:  strAny(m, "Scheme"),
				Type:    strAny(m, "Type"),
			}
			if state, ok := m["State"].(map[string]any); ok {
				lb.State = strAny(state, "Code")
			}
			items = append(items, lb)
		}
	}
	uihelper.WriteJSON(w, ListLoadBalancersResponse{Items: items, Total: len(items)})
}

// POST /load-balancers  body: { "name": "...", "type": "application", "scheme": "internet-facing" }
func (h *Handler) CreateLoadBalancer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name   string `json:"name"`
		Type   string `json:"type"`
		Scheme string `json:"scheme"`
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
	nr := uihelper.NR(r.Context(), h.cfg, "elasticloadbalancing", "CreateLoadBalancer", region, account)
	nr.Params["Name"] = req.Name
	if req.Type != "" {
		nr.Params["Type"] = req.Type
	}
	if req.Scheme != "" {
		nr.Params["Scheme"] = req.Scheme
	}

	resp, err := h.provider.CreateLoadBalancer(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /load-balancers/{arn}
func (h *Handler) DeleteLoadBalancer(w http.ResponseWriter, r *http.Request) {
	arnParam := chi.URLParam(r, "arn")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "elasticloadbalancing", "DeleteLoadBalancer", region, account)
	nr.Params["LoadBalancerArn"] = arnParam

	if _, err := h.provider.DeleteLoadBalancer(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// helpers

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
