package apigwui

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves API Gateway UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /apis
func (h *Handler) ListRestAPIs(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "apigateway", "GetRestApis", region, account)

	resp, err := h.provider.GetRestApis(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawAPIs, _ := resp.Data["item"].([]any)
	items := make([]RestAPI, 0, len(rawAPIs))
	for _, raw := range rawAPIs {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapRestAPI(m))
		}
	}

	uihelper.WriteJSON(w, ListRestAPIsResponse{Items: items, Total: len(items)})
}

// POST /apis  body: { "name": "...", "description": "..." }
func (h *Handler) CreateRestAPI(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
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

	nr := uihelper.NR(r.Context(), h.cfg, "apigateway", "CreateRestApi", region, account)
	nr.Params["name"] = req.Name
	if req.Description != "" {
		nr.Params["description"] = req.Description
	}

	resp, err := h.provider.CreateRestApi(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /apis/{id}
func (h *Handler) DeleteRestAPI(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "apigateway", "DeleteRestApi", region, account)
	nr.Params["restApiId"] = id

	if _, err := h.provider.DeleteRestApi(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /apis/{id}/resources
func (h *Handler) ListResources(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "apigateway", "GetResources", region, account)
	nr.Params["restApiId"] = id

	resp, err := h.provider.GetResources(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawResources, _ := resp.Data["item"].([]any)
	items := make([]Resource, 0, len(rawResources))
	for _, raw := range rawResources {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapResource(m))
		}
	}

	uihelper.WriteJSON(w, ListResourcesResponse{Items: items, Total: len(items)})
}

// GET /apis/{id}/stages
func (h *Handler) ListStages(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "apigateway", "GetStages", region, account)
	nr.Params["restApiId"] = id

	resp, err := h.provider.GetStages(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawStages, _ := resp.Data["item"].([]any)
	items := make([]Stage, 0, len(rawStages))
	for _, raw := range rawStages {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapStage(m))
		}
	}

	uihelper.WriteJSON(w, ListStagesResponse{Items: items, Total: len(items)})
}

// POST /apis/{id}/deployments  body: { "stageName": "...", "description": "..." }
func (h *Handler) CreateDeployment(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	var req struct {
		StageName   string `json:"stageName"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "apigateway", "CreateDeployment", region, account)
	nr.Params["restApiId"] = id
	if req.StageName != "" {
		nr.Params["stageName"] = req.StageName
	}
	if req.Description != "" {
		nr.Params["description"] = req.Description
	}

	resp, err := h.provider.CreateDeployment(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// GET /apis/{id}/deployments
func (h *Handler) ListDeployments(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "apigateway", "GetDeployments", region, account)
	nr.Params["restApiId"] = id

	resp, err := h.provider.GetDeployments(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawDeps, _ := resp.Data["item"].([]any)
	items := make([]Deployment, 0, len(rawDeps))
	for _, raw := range rawDeps {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapDeployment(m))
		}
	}

	uihelper.WriteJSON(w, ListDeploymentsResponse{Items: items, Total: len(items)})
}

// helpers

func mapRestAPI(m map[string]any) RestAPI {
	a := RestAPI{
		ID:          strAny(m, "id"),
		Name:        strAny(m, "name"),
		Description: strAny(m, "description"),
	}
	if v, ok := m["createdDate"]; ok {
		a.CreatedDate = fmt.Sprintf("%v", v)
	}
	return a
}

func mapResource(m map[string]any) Resource {
	return Resource{
		ID:       strAny(m, "id"),
		Path:     strAny(m, "path"),
		PathPart: strAny(m, "pathPart"),
		ParentID: strAny(m, "parentId"),
	}
}

func mapStage(m map[string]any) Stage {
	return Stage{
		Name:         strAny(m, "stageName"),
		DeploymentID: strAny(m, "deploymentId"),
		Description:  strAny(m, "description"),
	}
}

func mapDeployment(m map[string]any) Deployment {
	d := Deployment{
		ID:          strAny(m, "id"),
		Description: strAny(m, "description"),
	}
	if v, ok := m["createdDate"]; ok {
		d.CreatedDate = fmt.Sprintf("%v", v)
	}
	return d
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
