package sfnui

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves Step Functions UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /state-machines?nextToken=...
func (h *Handler) ListStateMachines(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "states", "ListStateMachines", region, account)
	if tok := r.URL.Query().Get("nextToken"); tok != "" {
		nr.Params["nextToken"] = tok
	}

	resp, err := h.provider.ListStateMachines(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawSMs, _ := resp.Data["stateMachines"].([]any)
	items := make([]StateMachine, 0, len(rawSMs))
	for _, raw := range rawSMs {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapStateMachine(m))
		}
	}
	nextToken, _ := resp.Data["nextToken"].(string)

	uihelper.WriteJSON(w, ListStateMachinesResponse{Items: items, Total: len(items), NextToken: nextToken})
}

// POST /state-machines  body: { "name": "...", "definition": "{}", "roleArn": "...", "type": "STANDARD" }
func (h *Handler) CreateStateMachine(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name       string `json:"name"`
		Definition string `json:"definition"`
		RoleARN    string `json:"roleArn"`
		Type       string `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}
	if req.Definition == "" {
		req.Definition = `{"Comment":"Created via JaisCloud UI","StartAt":"Pass","States":{"Pass":{"Type":"Pass","End":true}}}`
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "states", "CreateStateMachine", region, account)
	nr.Params["name"] = req.Name
	nr.Params["definition"] = req.Definition
	if req.RoleARN != "" {
		nr.Params["roleArn"] = req.RoleARN
	}
	if req.Type != "" {
		nr.Params["type"] = req.Type
	}

	resp, err := h.provider.CreateStateMachine(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /state-machines/{arn}
func (h *Handler) DeleteStateMachine(w http.ResponseWriter, r *http.Request) {
	arn := chi.URLParam(r, "arn")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "states", "DeleteStateMachine", region, account)
	nr.Params["stateMachineArn"] = arn

	if _, err := h.provider.DeleteStateMachine(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /state-machines/{arn}/executions  body: { "name": "...", "input": "{}" }
func (h *Handler) StartExecution(w http.ResponseWriter, r *http.Request) {
	arn := chi.URLParam(r, "arn")

	var req struct {
		Name  string `json:"name"`
		Input string `json:"input"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Input == "" {
		req.Input = "{}"
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "states", "StartExecution", region, account)
	nr.Params["stateMachineArn"] = arn
	nr.Params["input"] = req.Input
	if req.Name != "" {
		nr.Params["name"] = req.Name
	}

	resp, err := h.provider.StartExecution(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// GET /state-machines/{arn}/executions?statusFilter=...
func (h *Handler) ListExecutions(w http.ResponseWriter, r *http.Request) {
	arn := chi.URLParam(r, "arn")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "states", "ListExecutions", region, account)
	nr.Params["stateMachineArn"] = arn
	if sf := r.URL.Query().Get("statusFilter"); sf != "" {
		nr.Params["statusFilter"] = sf
	}

	resp, err := h.provider.ListExecutions(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawExecs, _ := resp.Data["executions"].([]any)
	items := make([]Execution, 0, len(rawExecs))
	for _, raw := range rawExecs {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapExecution(m))
		}
	}
	nextToken, _ := resp.Data["nextToken"].(string)

	uihelper.WriteJSON(w, ListExecutionsResponse{Items: items, Total: len(items), NextToken: nextToken})
}

// POST /executions/{arn}/stop
func (h *Handler) StopExecution(w http.ResponseWriter, r *http.Request) {
	arn := chi.URLParam(r, "arn")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "states", "StopExecution", region, account)
	nr.Params["executionArn"] = arn

	if _, err := h.provider.StopExecution(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /executions/{arn}/history
func (h *Handler) GetExecutionHistory(w http.ResponseWriter, r *http.Request) {
	arn := chi.URLParam(r, "arn")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "states", "GetExecutionHistory", region, account)
	nr.Params["executionArn"] = arn

	resp, err := h.provider.GetExecutionHistory(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawEvents, _ := resp.Data["events"].([]any)
	events := make([]HistoryEvent, 0, len(rawEvents))
	for _, raw := range rawEvents {
		if m, ok := raw.(map[string]any); ok {
			ev := HistoryEvent{
				Type: strAny(m, "type"),
			}
			if id, ok := m["id"].(float64); ok {
				ev.ID = int64(id)
			}
			if ts, ok := m["timestamp"].(float64); ok {
				ev.Timestamp = int64(ts)
			}
			events = append(events, ev)
		}
	}

	uihelper.WriteJSON(w, ExecutionHistoryResponse{Events: events})
}

// helpers

func mapStateMachine(m map[string]any) StateMachine {
	sm := StateMachine{
		ARN:     strAny(m, "stateMachineArn"),
		Name:    strAny(m, "name"),
		Type:    strAny(m, "type"),
		Status:  strAny(m, "status"),
		RoleARN: strAny(m, "roleArn"),
	}
	if v, ok := m["creationDate"].(float64); ok {
		sm.CreatedAt = int64(v)
	}
	return sm
}

func mapExecution(m map[string]any) Execution {
	e := Execution{
		ARN:             strAny(m, "executionArn"),
		Name:            strAny(m, "name"),
		StateMachineARN: strAny(m, "stateMachineArn"),
		Status:          strAny(m, "status"),
		Input:           strAny(m, "input"),
		Output:          strAny(m, "output"),
	}
	if v, ok := m["startDate"].(float64); ok {
		e.StartDate = int64(v)
	}
	if v, ok := m["stopDate"].(float64); ok {
		e.StopDate = int64(v)
	}
	return e
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
