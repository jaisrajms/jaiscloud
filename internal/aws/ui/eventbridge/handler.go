package eventbridgeui

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves EventBridge UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /rules?bus=...&nextToken=...
func (h *Handler) ListRules(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "events", "ListRules", region, account)
	if bus := r.URL.Query().Get("bus"); bus != "" {
		nr.Params["EventBusName"] = bus
	}
	if tok := r.URL.Query().Get("nextToken"); tok != "" {
		nr.Params["NextToken"] = tok
	}

	resp, err := h.provider.ListRules(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawRules, _ := resp.Data["Rules"].([]any)
	items := make([]Rule, 0, len(rawRules))
	for _, raw := range rawRules {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapRule(m))
		}
	}
	nextToken, _ := resp.Data["NextToken"].(string)

	uihelper.WriteJSON(w, ListRulesResponse{Items: items, Total: len(items), NextToken: nextToken})
}

// POST /rules  body: { "name": "...", "eventPattern": "...", "scheduleExpression": "...", "state": "...", "description": "...", "bus": "..." }
func (h *Handler) PutRule(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name               string `json:"name"`
		EventPattern       string `json:"eventPattern"`
		ScheduleExpression string `json:"scheduleExpression"`
		State              string `json:"state"`
		Description        string `json:"description"`
		Bus                string `json:"bus"`
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

	nr := uihelper.NR(r.Context(), h.cfg, "events", "PutRule", region, account)
	nr.Params["Name"] = req.Name
	if req.EventPattern != "" {
		nr.Params["EventPattern"] = req.EventPattern
	}
	if req.ScheduleExpression != "" {
		nr.Params["ScheduleExpression"] = req.ScheduleExpression
	}
	if req.State != "" {
		nr.Params["State"] = req.State
	}
	if req.Description != "" {
		nr.Params["Description"] = req.Description
	}
	if req.Bus != "" {
		nr.Params["EventBusName"] = req.Bus
	}

	resp, err := h.provider.PutRule(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /rules/{name}?bus=...
func (h *Handler) DeleteRule(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "events", "DeleteRule", region, account)
	nr.Params["Name"] = name
	if bus := r.URL.Query().Get("bus"); bus != "" {
		nr.Params["EventBusName"] = bus
	}

	if _, err := h.provider.DeleteRule(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /rules/{name}/enable?bus=...
func (h *Handler) EnableRule(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "events", "EnableRule", region, account)
	nr.Params["Name"] = name
	if bus := r.URL.Query().Get("bus"); bus != "" {
		nr.Params["EventBusName"] = bus
	}

	if _, err := h.provider.EnableRule(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /rules/{name}/disable?bus=...
func (h *Handler) DisableRule(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "events", "DisableRule", region, account)
	nr.Params["Name"] = name
	if bus := r.URL.Query().Get("bus"); bus != "" {
		nr.Params["EventBusName"] = bus
	}

	if _, err := h.provider.DisableRule(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /rules/{name}/targets?bus=...
func (h *Handler) ListTargets(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "events", "ListTargetsByRule", region, account)
	nr.Params["Rule"] = name
	if bus := r.URL.Query().Get("bus"); bus != "" {
		nr.Params["EventBusName"] = bus
	}

	resp, err := h.provider.ListTargetsByRule(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawTargets, _ := resp.Data["Targets"].([]any)
	items := make([]Target, 0, len(rawTargets))
	for _, raw := range rawTargets {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, Target{
				ID:  strAny(m, "Id"),
				ARN: strAny(m, "Arn"),
			})
		}
	}

	uihelper.WriteJSON(w, ListTargetsResponse{Items: items, Total: len(items)})
}

// POST /rules/{name}/targets  body: { "targets": [{ "id": "...", "arn": "..." }], "bus": "..." }
func (h *Handler) PutTargets(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	var req struct {
		Targets []struct {
			ID  string `json:"id"`
			ARN string `json:"arn"`
		} `json:"targets"`
		Bus string `json:"bus"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "events", "PutTargets", region, account)
	nr.Params["Rule"] = name
	if req.Bus != "" {
		nr.Params["EventBusName"] = req.Bus
	}
	targets := make([]any, len(req.Targets))
	for i, t := range req.Targets {
		targets[i] = map[string]any{"Id": t.ID, "Arn": t.ARN}
	}
	nr.Params["Targets"] = targets

	resp, err := h.provider.PutTargets(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /rules/{name}/targets/{targetId}?bus=...
func (h *Handler) RemoveTarget(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	targetID := chi.URLParam(r, "targetId")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "events", "RemoveTargets", region, account)
	nr.Params["Rule"] = name
	nr.Params["Ids"] = []any{targetID}
	if bus := r.URL.Query().Get("bus"); bus != "" {
		nr.Params["EventBusName"] = bus
	}

	if _, err := h.provider.RemoveTargets(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /events  body: { "entries": [{ "source": "...", "detailType": "...", "detail": "{}", "bus": "..." }] }
func (h *Handler) PutEvents(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Entries []struct {
			Source     string `json:"source"`
			DetailType string `json:"detailType"`
			Detail     string `json:"detail"`
			Bus        string `json:"bus"`
		} `json:"entries"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Entries) == 0 {
		uihelper.UIError(w, "BadRequest", "entries array is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "events", "PutEvents", region, account)
	entries := make([]any, len(req.Entries))
	for i, e := range req.Entries {
		entry := map[string]any{
			"Source":     e.Source,
			"DetailType": e.DetailType,
			"Detail":     e.Detail,
		}
		if e.Bus != "" {
			entry["EventBusName"] = e.Bus
		}
		entries[i] = entry
	}
	nr.Params["Entries"] = entries

	resp, err := h.provider.PutEvents(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /buses
func (h *Handler) ListEventBuses(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "events", "ListEventBuses", region, account)

	resp, err := h.provider.ListEventBuses(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawBuses, _ := resp.Data["EventBuses"].([]any)
	items := make([]EventBus, 0, len(rawBuses))
	for _, raw := range rawBuses {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, EventBus{
				Name: strAny(m, "Name"),
				ARN:  strAny(m, "Arn"),
			})
		}
	}

	uihelper.WriteJSON(w, ListEventBusesResponse{Items: items, Total: len(items)})
}

// POST /buses  body: { "name": "..." }
func (h *Handler) CreateEventBus(w http.ResponseWriter, r *http.Request) {
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

	nr := uihelper.NR(r.Context(), h.cfg, "events", "CreateEventBus", region, account)
	nr.Params["Name"] = req.Name

	resp, err := h.provider.CreateEventBus(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /buses/{name}
func (h *Handler) DeleteEventBus(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "events", "DeleteEventBus", region, account)
	nr.Params["Name"] = name

	if _, err := h.provider.DeleteEventBus(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// helpers

func mapRule(m map[string]any) Rule {
	return Rule{
		Name:               strAny(m, "Name"),
		ARN:                strAny(m, "Arn"),
		EventBusName:       strAny(m, "EventBusName"),
		State:              strAny(m, "State"),
		EventPattern:       strAny(m, "EventPattern"),
		ScheduleExpression: strAny(m, "ScheduleExpression"),
	}
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
