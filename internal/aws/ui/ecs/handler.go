package ecsui

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves ECS UI API requests.
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
	nr := uihelper.NR(r.Context(), h.cfg, "ecs", "ListClusters", region, account)

	resp, err := h.provider.ListClusters(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	// ListClusters returns ARNs; describe them to get full info.
	rawARNs, _ := resp.Data["clusterArns"].([]any)
	arns := make([]any, 0, len(rawARNs))
	for _, arn := range rawARNs {
		arns = append(arns, arn)
	}

	items := make([]ECSCluster, 0, len(arns))
	if len(arns) > 0 {
		descNR := uihelper.NR(r.Context(), h.cfg, "ecs", "DescribeClusters", region, account)
		descNR.Params["clusters"] = arns
		descResp, err := h.provider.DescribeClusters(r.Context(), descNR)
		if err == nil {
			rawClusters, _ := descResp.Data["clusters"].([]any)
			for _, raw := range rawClusters {
				if m, ok := raw.(map[string]any); ok {
					items = append(items, mapCluster(m))
				}
			}
		}
	}
	nextToken, _ := resp.Data["nextToken"].(string)
	uihelper.WriteJSON(w, ListECSClustersResponse{Items: items, Total: len(items), NextToken: nextToken})
}

// POST /clusters  body: { "name": "..." }
func (h *Handler) CreateCluster(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "ecs", "CreateCluster", region, account)
	if req.Name != "" {
		nr.Params["clusterName"] = req.Name
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
	nr := uihelper.NR(r.Context(), h.cfg, "ecs", "DeleteCluster", region, account)
	nr.Params["cluster"] = name

	if _, err := h.provider.DeleteCluster(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /clusters/{name}/tasks
func (h *Handler) ListTasks(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "ecs", "ListTasks", region, account)
	nr.Params["cluster"] = name

	resp, err := h.provider.ListTasks(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawARNs, _ := resp.Data["taskArns"].([]any)
	items := make([]ECSTask, 0, len(rawARNs))
	for _, arn := range rawARNs {
		if s, ok := arn.(string); ok {
			items = append(items, ECSTask{ARN: s, ClusterARN: name})
		}
	}
	uihelper.WriteJSON(w, ListECSTasksResponse{Items: items, Total: len(items)})
}

// POST /clusters/{name}/tasks  body: { "taskDefinition": "..." }
func (h *Handler) RunTask(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	var req struct {
		TaskDefinition string `json:"taskDefinition"`
		Count          int    `json:"count"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.TaskDefinition == "" {
		uihelper.UIError(w, "BadRequest", "taskDefinition is required", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "ecs", "RunTask", region, account)
	nr.Params["cluster"] = name
	nr.Params["taskDefinition"] = req.TaskDefinition
	if req.Count > 0 {
		nr.Params["count"] = req.Count
	}

	resp, err := h.provider.RunTask(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// POST /tasks/{arn}/stop
func (h *Handler) StopTask(w http.ResponseWriter, r *http.Request) {
	arn := chi.URLParam(r, "arn")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "ecs", "StopTask", region, account)
	nr.Params["task"] = arn

	if _, err := h.provider.StopTask(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /clusters/{name}/services
func (h *Handler) ListServices(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "ecs", "ListServices", region, account)
	nr.Params["cluster"] = name

	resp, err := h.provider.ListServices(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawARNs, _ := resp.Data["serviceArns"].([]any)
	items := make([]ECSService, 0, len(rawARNs))
	for _, arn := range rawARNs {
		if s, ok := arn.(string); ok {
			items = append(items, ECSService{ARN: s})
		}
	}
	uihelper.WriteJSON(w, ListECSServicesResponse{Items: items, Total: len(items)})
}

// helpers

func mapCluster(m map[string]any) ECSCluster {
	return ECSCluster{
		Name:   strAny(m, "clusterName"),
		ARN:    strAny(m, "clusterArn"),
		Status: strAny(m, "status"),
	}
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
