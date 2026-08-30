package ssmui

import (
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves SSM UI API requests by calling the SSM provider directly.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /parameters?path=<prefix>&nextToken=...
func (h *Handler) ListParameters(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	pageToken := r.URL.Query().Get("nextToken")
	path := r.URL.Query().Get("path")

	var resp any
	var err error

	if path != "" {
		// Use GetParametersByPath for prefix search.
		nr := uihelper.NR(r.Context(), h.cfg, "ssm", "GetParametersByPath", region, account)
		nr.Params["Path"] = path
		nr.Params["Recursive"] = true
		if pageToken != "" {
			nr.Params["NextToken"] = pageToken
		}
		var provResp interface{ GetData() map[string]any }
		_ = provResp
		pResp, pErr := h.provider.GetParametersByPath(r.Context(), nr)
		if pErr != nil {
			uihelper.WriteError(w, pErr)
			return
		}
		rawParams, _ := pResp.Data["Parameters"].([]map[string]any)
		nextToken, _ := pResp.Data["NextToken"].(string)
		params := make([]Parameter, 0, len(rawParams))
		for _, p := range rawParams {
			params = append(params, parseParameter(p))
		}
		uihelper.WriteJSON(w, ListParametersResponse{Items: params, NextToken: nextToken, Total: len(params)})
		return
	}

	// DescribeParameters for listing all.
	nr := uihelper.NR(r.Context(), h.cfg, "ssm", "DescribeParameters", region, account)
	if pageToken != "" {
		nr.Params["NextToken"] = pageToken
	}
	pResp, pErr := h.provider.DescribeParameters(r.Context(), nr)
	if pErr != nil {
		uihelper.WriteError(w, pErr)
		return
	}
	_ = resp
	_ = err

	rawParams, _ := pResp.Data["Parameters"].([]map[string]any)
	nextToken, _ := pResp.Data["NextToken"].(string)
	params := make([]Parameter, 0, len(rawParams))
	for _, p := range rawParams {
		params = append(params, parseParameterMeta(p))
	}
	uihelper.WriteJSON(w, ListParametersResponse{Items: params, NextToken: nextToken, Total: len(params)})
}

// POST /parameters
func (h *Handler) PutParameter(w http.ResponseWriter, r *http.Request) {
	var req PutParameterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}
	if req.Value == "" {
		uihelper.UIError(w, "BadRequest", "value is required", http.StatusBadRequest)
		return
	}
	if req.Type == "" {
		req.Type = "String"
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "ssm", "PutParameter", region, account)
	nr.Params["Name"] = req.Name
	nr.Params["Value"] = req.Value
	nr.Params["Type"] = req.Type
	nr.Params["Overwrite"] = req.Overwrite
	if req.Description != "" {
		nr.Params["Description"] = req.Description
	}
	if req.KeyID != "" {
		nr.Params["KeyId"] = req.KeyID
	}
	if req.Tags != nil && !req.Overwrite {
		tagList := make([]any, 0, len(req.Tags))
		for k, v := range req.Tags {
			tagList = append(tagList, map[string]any{"Key": k, "Value": v})
		}
		nr.Params["Tags"] = tagList
	}

	resp, err := h.provider.PutParameter(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	uihelper.WriteJSON(w, resp.Data)
}

// GET /parameters/value?name=<name>
func (h *Handler) GetParameter(w http.ResponseWriter, r *http.Request) {
	name, _ := url.QueryUnescape(r.URL.Query().Get("name"))
	if name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "ssm", "GetParameter", region, account)
	nr.Params["Name"] = name
	nr.Params["WithDecryption"] = true

	resp, err := h.provider.GetParameter(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /parameters/{name}  (name may contain slashes — pass as encoded path param)
func (h *Handler) DeleteParameter(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "*")
	if name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}
	decoded, _ := url.PathUnescape(name)
	if decoded != "" {
		name = decoded
	}
	// Ensure leading slash.
	if name[0] != '/' {
		name = "/" + name
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "ssm", "DeleteParameter", region, account)
	nr.Params["Name"] = name

	if _, err := h.provider.DeleteParameter(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /parameters/history?name=<name>
func (h *Handler) GetParameterHistory(w http.ResponseWriter, r *http.Request) {
	name, _ := url.QueryUnescape(r.URL.Query().Get("name"))
	if name == "" {
		uihelper.UIError(w, "BadRequest", "name is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	pageToken := r.URL.Query().Get("nextToken")

	nr := uihelper.NR(r.Context(), h.cfg, "ssm", "GetParameterHistory", region, account)
	nr.Params["Name"] = name
	nr.Params["WithDecryption"] = true
	if pageToken != "" {
		nr.Params["NextToken"] = pageToken
	}

	resp, err := h.provider.GetParameterHistory(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawHistory, _ := resp.Data["Parameters"].([]map[string]any)
	nextToken, _ := resp.Data["NextToken"].(string)

	items := make([]ParameterHistoryEntry, 0, len(rawHistory))
	for _, h := range rawHistory {
		items = append(items, ParameterHistoryEntry{
			Name:             strAny(h, "Name"),
			Type:             strAny(h, "Type"),
			Value:            strAny(h, "Value"),
			LastModifiedDate: strAny(h, "LastModifiedDate"),
			LastModifiedUser: strAny(h, "LastModifiedUser"),
			Description:      strAny(h, "Description"),
		})
	}
	uihelper.WriteJSON(w, ListParameterHistoryResponse{Items: items, NextToken: nextToken})
}

// parseParameter converts a full parameter object (from GetParametersByPath) to Parameter.
func parseParameter(p map[string]any) Parameter {
	param := Parameter{
		Name:             strAny(p, "Name"),
		Type:             strAny(p, "Type"),
		Value:            strAny(p, "Value"),
		LastModifiedDate: strAny(p, "LastModifiedDate"),
		ARN:              strAny(p, "ARN"),
		DataType:         strAny(p, "DataType"),
	}
	if v, ok := p["Version"]; ok {
		switch n := v.(type) {
		case float64:
			param.Version = int64(n)
		case int64:
			param.Version = n
		case int:
			param.Version = int64(n)
		}
	}
	return param
}

// parseParameterMeta converts DescribeParameters metadata to Parameter (no value).
func parseParameterMeta(p map[string]any) Parameter {
	param := Parameter{
		Name:             strAny(p, "Name"),
		Type:             strAny(p, "Type"),
		LastModifiedDate: strAny(p, "LastModifiedDate"),
		LastModifiedUser: strAny(p, "LastModifiedUser"),
		Description:      strAny(p, "Description"),
		KeyID:            strAny(p, "KeyId"),
		Tier:             strAny(p, "Tier"),
		DataType:         strAny(p, "DataType"),
	}
	if v, ok := p["Version"]; ok {
		switch n := v.(type) {
		case float64:
			param.Version = int64(n)
		case int64:
			param.Version = n
		}
	}
	return param
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
