package emrui

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves EMR UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /clusters?state=...&nextToken=...
func (h *Handler) ListClusters(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "emr", "ListClusters", region, account)
	if state := r.URL.Query().Get("state"); state != "" {
		nr.Params["ClusterStates"] = []any{state}
	}
	if tok := r.URL.Query().Get("nextToken"); tok != "" {
		nr.Params["Marker"] = tok
	}

	resp, err := h.provider.ListClusters(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawClusters, _ := resp.Data["Clusters"].([]any)
	nextToken, _ := resp.Data["Marker"].(string)

	items := make([]ClusterSummary, 0, len(rawClusters))
	for _, raw := range rawClusters {
		if m, ok := raw.(map[string]any); ok {
			status, _ := m["Status"].(map[string]any)
			state := ""
			if status != nil {
				state, _ = status["State"].(string)
			}
			items = append(items, ClusterSummary{
				ID:    strAny(m, "Id"),
				Name:  strAny(m, "Name"),
				State: state,
				ARN:   strAny(m, "ClusterArn"),
			})
		}
	}

	uihelper.WriteJSON(w, ListClustersResponse{Items: items, NextToken: nextToken, Total: len(items)})
}

// POST /clusters  body: { name, releaseLabel, logUri, keepAlive, ... }
func (h *Handler) RunJobFlow(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name         string `json:"name"`
		ReleaseLabel string `json:"releaseLabel"`
		LogURI       string `json:"logUri"`
		KeepAlive    bool   `json:"keepAlive"`
		ServiceRole  string `json:"serviceRole"`
		JobFlowRole  string `json:"jobFlowRole"`
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

	nr := uihelper.NR(r.Context(), h.cfg, "emr", "RunJobFlow", region, account)
	nr.Params["Name"] = req.Name
	if req.ReleaseLabel != "" {
		nr.Params["ReleaseLabel"] = req.ReleaseLabel
	}
	if req.LogURI != "" {
		nr.Params["LogUri"] = req.LogURI
	}
	if req.ServiceRole != "" {
		nr.Params["ServiceRole"] = req.ServiceRole
	}
	if req.JobFlowRole != "" {
		nr.Params["JobFlowRole"] = req.JobFlowRole
	}
	nr.Params["Instances"] = map[string]any{"KeepJobFlowAliveWhenNoSteps": req.KeepAlive}

	resp, err := h.provider.RunJobFlow(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// GET /clusters/{id}
func (h *Handler) DescribeCluster(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "emr", "DescribeCluster", region, account)
	nr.Params["ClusterId"] = id

	resp, err := h.provider.DescribeCluster(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	cluster, _ := resp.Data["Cluster"].(map[string]any)
	if cluster == nil {
		cluster = map[string]any{}
	}
	uihelper.WriteJSON(w, mapClusterDetail(cluster))
}

// DELETE /clusters/{id}
func (h *Handler) TerminateCluster(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "emr", "TerminateJobFlows", region, account)
	nr.Params["JobFlowIds"] = []any{id}

	if _, err := h.provider.TerminateJobFlows(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /clusters/{id}/status
func (h *Handler) GetClusterStatus(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "emr", "DescribeCluster", region, account)
	nr.Params["ClusterId"] = id

	resp, err := h.provider.DescribeCluster(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	cluster, _ := resp.Data["Cluster"].(map[string]any)
	if cluster == nil {
		cluster = map[string]any{}
	}

	status, _ := cluster["Status"].(map[string]any)
	state := ""
	stateChangeReason := ""
	if status != nil {
		state, _ = status["State"].(string)
		if scr, ok := status["StateChangeReason"].(map[string]any); ok {
			stateChangeReason, _ = scr["Message"].(string)
		}
	}

	// Fetch steps separately
	nrSteps := uihelper.NR(r.Context(), h.cfg, "emr", "ListSteps", region, account)
	nrSteps.Params["ClusterId"] = id

	var stepSummaries []StepSummary
	if stepsResp, stepsErr := h.provider.ListSteps(r.Context(), nrSteps); stepsErr == nil {
		if rawSteps, ok := stepsResp.Data["Steps"].([]any); ok {
			for _, raw := range rawSteps {
				if m, ok := raw.(map[string]any); ok {
					cfg, _ := m["Config"].(map[string]any)
					name := ""
					if cfg != nil {
						name, _ = cfg["Name"].(string)
					}
					if name == "" {
						name = strAny(m, "Name")
					}
					stepStatus, _ := m["Status"].(map[string]any)
					stepState := ""
					if stepStatus != nil {
						stepState, _ = stepStatus["State"].(string)
					}
					stepSummaries = append(stepSummaries, StepSummary{
						ID:    strAny(m, "Id"),
						Name:  name,
						State: stepState,
					})
				}
			}
		}
	}

	if stepSummaries == nil {
		stepSummaries = []StepSummary{}
	}

	uihelper.WriteJSON(w, ClusterStatus{
		State:             state,
		StateChangeReason: stateChangeReason,
		Steps:             stepSummaries,
	})
}

// GET /clusters/{id}/steps?nextToken=...
func (h *Handler) ListSteps(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "emr", "ListSteps", region, account)
	nr.Params["ClusterId"] = id
	if tok := r.URL.Query().Get("nextToken"); tok != "" {
		nr.Params["Marker"] = tok
	}

	resp, err := h.provider.ListSteps(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawSteps, _ := resp.Data["Steps"].([]any)
	nextToken, _ := resp.Data["Marker"].(string)

	items := make([]Step, 0, len(rawSteps))
	for _, raw := range rawSteps {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapStep(m))
		}
	}

	uihelper.WriteJSON(w, ListStepsResponse{Items: items, NextToken: nextToken, Total: len(items)})
}

// POST /clusters/{id}/steps  body: { "steps": [...] }
func (h *Handler) AddSteps(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	var req struct {
		Steps []any `json:"steps"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Steps) == 0 {
		uihelper.UIError(w, "BadRequest", "steps array is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "emr", "AddJobFlowSteps", region, account)
	nr.Params["JobFlowId"] = id
	nr.Params["Steps"] = req.Steps

	resp, err := h.provider.AddJobFlowSteps(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// GET /clusters/{id}/steps/{stepId}
func (h *Handler) DescribeStep(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	stepID := chi.URLParam(r, "stepId")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "emr", "DescribeStep", region, account)
	nr.Params["ClusterId"] = id
	nr.Params["StepId"] = stepID

	resp, err := h.provider.DescribeStep(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	step, _ := resp.Data["Step"].(map[string]any)
	if step == nil {
		step = map[string]any{}
	}
	uihelper.WriteJSON(w, mapStep(step))
}

// POST /clusters/{id}/steps/{stepId}/cancel
func (h *Handler) CancelStep(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	stepID := chi.URLParam(r, "stepId")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "emr", "CancelSteps", region, account)
	nr.Params["ClusterId"] = id
	nr.Params["StepIds"] = []any{stepID}

	if _, err := h.provider.CancelSteps(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// helpers

func mapClusterDetail(m map[string]any) ClusterDetail {
	status, _ := m["Status"].(map[string]any)
	state := ""
	stateChangeReason := ""
	if status != nil {
		state, _ = status["State"].(string)
		if scr, ok := status["StateChangeReason"].(map[string]any); ok {
			stateChangeReason, _ = scr["Message"].(string)
		}
	}

	apps := []string{}
	if rawApps, ok := m["Applications"].([]any); ok {
		for _, a := range rawApps {
			if am, ok := a.(map[string]any); ok {
				if n, ok := am["Name"].(string); ok && n != "" {
					apps = append(apps, n)
				}
			}
		}
	}

	tags := []TagEntry{}
	if rawTags, ok := m["Tags"].([]any); ok {
		for _, t := range rawTags {
			if tm, ok := t.(map[string]any); ok {
				k, _ := tm["Key"].(string)
				v, _ := tm["Value"].(string)
				if k != "" {
					tags = append(tags, TagEntry{Key: k, Value: v})
				}
			}
		}
	}

	autoTerm := false
	if v, ok := m["AutoTerminate"].(bool); ok {
		autoTerm = v
	}
	termProt := false
	if v, ok := m["TerminationProtected"].(bool); ok {
		termProt = v
	}

	return ClusterDetail{
		ID:                   strAny(m, "Id"),
		Name:                 strAny(m, "Name"),
		State:                state,
		StateChangeReason:    stateChangeReason,
		ARN:                  strAny(m, "ClusterArn"),
		ReleaseLabel:         strAny(m, "ReleaseLabel"),
		LogURI:               strAny(m, "LogUri"),
		AutoTerminate:        autoTerm,
		TerminationProtected: termProt,
		Applications:         apps,
		Tags:                 tags,
	}
}

func mapStep(m map[string]any) Step {
	status, _ := m["Status"].(map[string]any)
	state := ""
	if status != nil {
		state, _ = status["State"].(string)
	}
	cfg, _ := m["Config"].(map[string]any)
	name := strAny(m, "Name")
	if name == "" && cfg != nil {
		name, _ = cfg["Name"].(string)
	}
	return Step{
		ID:     strAny(m, "Id"),
		Name:   fmt.Sprintf("%s", name),
		State:  state,
		Config: cfg,
	}
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
