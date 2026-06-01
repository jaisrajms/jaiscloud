package route53ui

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves Route53 UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /zones
func (h *Handler) ListHostedZones(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "route53", "ListHostedZones", region, account)

	resp, err := h.provider.ListHostedZones(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawZones, _ := resp.Data["HostedZones"].([]any)
	items := make([]HostedZone, 0, len(rawZones))
	for _, raw := range rawZones {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapHostedZone(m))
		}
	}
	nextToken, _ := resp.Data["NextMarker"].(string)
	uihelper.WriteJSON(w, ListHostedZonesResponse{Items: items, Total: len(items), NextToken: nextToken})
}

// POST /zones  body: { "name": "example.com" }
func (h *Handler) CreateHostedZone(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
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
	nr := uihelper.NR(r.Context(), h.cfg, "route53", "CreateHostedZone", region, account)
	nr.Params["Name"] = req.Name

	resp, err := h.provider.CreateHostedZone(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /zones/{id}
func (h *Handler) DeleteHostedZone(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "route53", "DeleteHostedZone", region, account)
	nr.Params["Id"] = id

	if _, err := h.provider.DeleteHostedZone(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /zones/{id}/records
func (h *Handler) ListResourceRecordSets(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "route53", "ListResourceRecordSets", region, account)
	nr.Params["HostedZoneId"] = id

	resp, err := h.provider.ListResourceRecordSets(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawSets, _ := resp.Data["ResourceRecordSets"].([]any)
	items := make([]RecordSet, 0, len(rawSets))
	for _, raw := range rawSets {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapRecordSet(m))
		}
	}
	nextToken, _ := resp.Data["NextRecordName"].(string)
	uihelper.WriteJSON(w, ListRecordSetsResponse{Items: items, Total: len(items), NextToken: nextToken})
}

// helpers

func mapHostedZone(m map[string]any) HostedZone {
	z := HostedZone{
		ID:   strings.TrimPrefix(strAny(m, "Id"), "/hostedzone/"),
		Name: strAny(m, "Name"),
	}
	if cfg, ok := m["Config"].(map[string]any); ok {
		z.Private, _ = cfg["PrivateZone"].(bool)
	}
	if cnt, ok := m["ResourceRecordSetCount"].(float64); ok {
		z.RecordCount = int(cnt)
	}
	return z
}

func mapRecordSet(m map[string]any) RecordSet {
	rs := RecordSet{
		Name: strAny(m, "Name"),
		Type: strAny(m, "Type"),
	}
	if ttl, ok := m["TTL"].(float64); ok {
		rs.TTL = int(ttl)
	}
	if recs, ok := m["ResourceRecords"].([]any); ok {
		for _, rec := range recs {
			if rm, ok := rec.(map[string]any); ok {
				rs.Records = append(rs.Records, strAny(rm, "Value"))
			}
		}
	}
	return rs
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
