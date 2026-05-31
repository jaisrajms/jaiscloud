package lambdaui

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
	"jaiscloud/internal/reqctx"
)

// Handler serves Lambda UI API requests by calling the Lambda provider directly.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /functions?pageSize=50&nextToken=...
func (h *Handler) ListFunctions(w http.ResponseWriter, r *http.Request) {
	pageSize := uihelper.PageSizeFrom(r, 50, 1000)
	pageToken := uihelper.PageTokenFrom(r)
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "lambda", "ListFunctions", region, account)
	nr.Params["MaxItems"] = float64(pageSize) // provider expects float64
	if pageToken != "" {
		nr.Params["Marker"] = pageToken
	}

	resp, err := h.provider.ListFunctions(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawFuncs, _ := resp.Data["Functions"].([]map[string]any)
	nextMarker, _ := resp.Data["NextMarker"].(string)

	functions := make([]Function, 0, len(rawFuncs))
	for _, f := range rawFuncs {
		functions = append(functions, mapFunction(f))
	}

	uihelper.WriteJSON(w, ListFunctionsResponse{
		Items:     functions,
		NextToken: nextMarker,
	})
}

// GET /functions/{name}
func (h *Handler) GetFunction(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "lambda", "GetFunction", region, account)
	nr.Params["_function_name"] = name

	resp, err := h.provider.GetFunction(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	cfg, _ := resp.Data["Configuration"].(map[string]any)
	uihelper.WriteJSON(w, mapFunction(cfg))
}

// DELETE /functions/{name}
func (h *Handler) DeleteFunction(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "lambda", "DeleteFunction", region, account)
	nr.Params["_function_name"] = name

	if _, err := h.provider.DeleteFunction(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /functions/{name}/invoke  body: InvokeRequest
func (h *Handler) InvokeFunction(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	var req InvokeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.InvocationType == "" {
		req.InvocationType = "RequestResponse"
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	// Inject a fresh request ID so buildPlatformLogs and the response share the same UUID.
	invocationID := uuid.New().String()
	ctx := reqctx.WithRequestID(r.Context(), invocationID)

	nr := uihelper.NR(ctx, h.cfg, "lambda", "InvokeFunction", region, account)
	nr.Params["_function_name"] = name
	nr.Params["_payload"] = []byte(req.Payload)
	nr.Params["_invocation_type"] = req.InvocationType
	nr.Params["_log_type"] = "Tail"

	resp, err := h.provider.InvokeFunction(ctx, nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	var payload string
	if raw, ok := resp.Data["_payload"].([]byte); ok {
		payload = string(raw)
	}

	funcErr, _ := resp.Data["_function_error"].(string)
	logResult, _ := resp.Data["LogResult"].(string)

	uihelper.WriteJSON(w, InvokeResponse{
		StatusCode:      resp.HTTPStatus,
		FunctionError:   funcErr,
		ExecutedVersion: "$LATEST",
		Payload:         payload,
		LogResult:       logResult,
		RequestId:       invocationID,
	})
}

// PATCH /functions/{name}/config  body: UpdateConfigRequest
func (h *Handler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")

	var req UpdateConfigRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "lambda", "UpdateFunctionConfiguration", region, account)
	nr.Params["_function_name"] = name
	if req.Timeout != nil {
		nr.Params["Timeout"] = *req.Timeout
	}
	if req.MemorySize != nil {
		nr.Params["MemorySize"] = *req.MemorySize
	}
	if req.Handler != nil {
		nr.Params["Handler"] = *req.Handler
	}
	if req.Description != nil {
		nr.Params["Description"] = *req.Description
	}
	if req.RoleARN != nil {
		nr.Params["Role"] = *req.RoleARN
	}
	if req.EnvVars != nil {
		envVars := make(map[string]any, len(req.EnvVars))
		for k, v := range req.EnvVars {
			envVars[k] = v
		}
		nr.Params["Environment"] = map[string]any{"Variables": envVars}
	}

	resp, err := h.provider.UpdateFunctionConfiguration(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, mapFunction(resp.Data))
}

// GET /functions/{name}/status
func (h *Handler) GetFunctionStatus(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "lambda", "GetFunction", region, account)
	nr.Params["_function_name"] = name

	resp, err := h.provider.GetFunction(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	cfg, _ := resp.Data["Configuration"].(map[string]any)
	state, _ := cfg["State"].(string)
	uihelper.WriteJSON(w, map[string]string{"state": state})
}

// mapFunction converts a cfgToWire map to a typed Function struct.
func mapFunction(m map[string]any) Function {
	f := Function{
		Name:         strVal(m, "FunctionName"),
		ARN:          strVal(m, "FunctionArn"),
		Runtime:      strVal(m, "Runtime"),
		Handler:      strVal(m, "Handler"),
		RoleARN:      strVal(m, "Role"),
		LastModified: strVal(m, "LastModified"),
		Description:  strVal(m, "Description"),
		State:        strVal(m, "State"),
	}
	if v, ok := m["Timeout"].(float64); ok {
		f.Timeout = int(v)
	}
	if v, ok := m["MemorySize"].(float64); ok {
		f.MemorySize = int(v)
	}
	if env, ok := m["Environment"].(map[string]any); ok {
		if vars, ok := env["Variables"].(map[string]any); ok {
			f.EnvVars = make(map[string]string, len(vars))
			for k, v := range vars {
				if s, ok := v.(string); ok {
					f.EnvVars[k] = s
				}
			}
		}
	}
	return f
}

func strVal(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, _ := m[key].(string)
	return v
}

