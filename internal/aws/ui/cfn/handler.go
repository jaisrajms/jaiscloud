package cfnui

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves CloudFormation UI API requests.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /stacks
func (h *Handler) ListStacks(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "cloudformation", "ListStacks", region, account)

	resp, err := h.provider.ListStacks(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawStacks, _ := resp.Data["StackSummaries"].([]any)
	items := make([]Stack, 0, len(rawStacks))
	for _, raw := range rawStacks {
		if m, ok := raw.(map[string]any); ok {
			items = append(items, mapStack(m))
		}
	}
	nextToken, _ := resp.Data["NextToken"].(string)
	uihelper.WriteJSON(w, ListStacksResponse{Items: items, Total: len(items), NextToken: nextToken})
}

// POST /stacks  body: { "name": "...", "templateBody": "...", "templateUrl": "..." }
func (h *Handler) CreateStack(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name         string `json:"name"`
		TemplateBody string `json:"templateBody"`
		TemplateURL  string `json:"templateUrl"`
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
	nr := uihelper.NR(r.Context(), h.cfg, "cloudformation", "CreateStack", region, account)
	nr.Params["StackName"] = req.Name
	if req.TemplateBody != "" {
		nr.Params["TemplateBody"] = req.TemplateBody
	}
	if req.TemplateURL != "" {
		nr.Params["TemplateURL"] = req.TemplateURL
	}

	resp, err := h.provider.CreateStack(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /stacks/{name}
func (h *Handler) DeleteStack(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	nr := uihelper.NR(r.Context(), h.cfg, "cloudformation", "DeleteStack", region, account)
	nr.Params["StackName"] = name

	if _, err := h.provider.DeleteStack(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// helpers

func mapStack(m map[string]any) Stack {
	return Stack{
		Name:        strAny(m, "StackName"),
		Status:      strAny(m, "StackStatus"),
		StackID:     strAny(m, "StackId"),
		Description: strAny(m, "TemplateDescription"),
		CreatedAt:   strAny(m, "CreationTime"),
	}
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
