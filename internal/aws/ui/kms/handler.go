package kmsui

import (
	"encoding/json"
	"net/http"
	"net/url"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves KMS UI API requests by calling the KMS provider directly.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// GET /keys
func (h *Handler) ListKeys(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	pageToken := r.URL.Query().Get("nextToken")

	nr := uihelper.NR(r.Context(), h.cfg, "kms", "ListKeys", region, account)
	if pageToken != "" {
		nr.Params["Marker"] = pageToken
	}

	resp, err := h.provider.ListKeys(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawKeys, _ := resp.Data["Keys"].([]map[string]any)
	nextToken, _ := resp.Data["NextMarker"].(string)

	keys := make([]KMSKey, 0, len(rawKeys))
	for _, k := range rawKeys {
		keyID, _ := k["KeyId"].(string)
		keyARN, _ := k["KeyArn"].(string)
		key := h.describeKey(r, keyID, keyARN, region, account)
		keys = append(keys, key)
	}

	uihelper.WriteJSON(w, ListKeysResponse{Items: keys, NextToken: nextToken, Total: len(keys)})
}

// POST /keys
func (h *Handler) CreateKey(w http.ResponseWriter, r *http.Request) {
	var req CreateKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "kms", "CreateKey", region, account)
	if req.Description != "" {
		nr.Params["Description"] = req.Description
	}
	if req.KeyUsage != "" {
		nr.Params["KeyUsage"] = req.KeyUsage
	}
	if req.KeySpec != "" {
		nr.Params["KeySpec"] = req.KeySpec
	}
	if req.Tags != nil {
		tagList := make([]any, 0, len(req.Tags))
		for k, v := range req.Tags {
			tagList = append(tagList, map[string]any{"TagKey": k, "TagValue": v})
		}
		nr.Params["Tags"] = tagList
	}

	resp, err := h.provider.CreateKey(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// GET /keys/detail?keyId=<id>
func (h *Handler) GetKey(w http.ResponseWriter, r *http.Request) {
	keyID, _ := url.QueryUnescape(r.URL.Query().Get("keyId"))
	if keyID == "" {
		uihelper.UIError(w, "BadRequest", "keyId is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "kms", "DescribeKey", region, account)
	nr.Params["KeyId"] = keyID

	resp, err := h.provider.DescribeKey(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// POST /keys/enable?keyId=<id>
func (h *Handler) EnableKey(w http.ResponseWriter, r *http.Request) {
	keyID, _ := url.QueryUnescape(r.URL.Query().Get("keyId"))
	if keyID == "" {
		uihelper.UIError(w, "BadRequest", "keyId is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "kms", "EnableKey", region, account)
	nr.Params["KeyId"] = keyID

	if _, err := h.provider.EnableKey(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /keys/disable?keyId=<id>
func (h *Handler) DisableKey(w http.ResponseWriter, r *http.Request) {
	keyID, _ := url.QueryUnescape(r.URL.Query().Get("keyId"))
	if keyID == "" {
		uihelper.UIError(w, "BadRequest", "keyId is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "kms", "DisableKey", region, account)
	nr.Params["KeyId"] = keyID

	if _, err := h.provider.DisableKey(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /keys/schedule-deletion?keyId=<id>  body: { "pendingWindowInDays": 7 }
func (h *Handler) ScheduleKeyDeletion(w http.ResponseWriter, r *http.Request) {
	keyID, _ := url.QueryUnescape(r.URL.Query().Get("keyId"))
	if keyID == "" {
		uihelper.UIError(w, "BadRequest", "keyId is required", http.StatusBadRequest)
		return
	}
	var req struct {
		PendingWindowInDays int `json:"pendingWindowInDays"`
	}
	json.NewDecoder(r.Body).Decode(&req) //nolint:errcheck
	if req.PendingWindowInDays == 0 {
		req.PendingWindowInDays = 30
	}

	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "kms", "ScheduleKeyDeletion", region, account)
	nr.Params["KeyId"] = keyID
	nr.Params["PendingWindowInDays"] = req.PendingWindowInDays

	resp, err := h.provider.ScheduleKeyDeletion(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// POST /keys/cancel-deletion?keyId=<id>
func (h *Handler) CancelKeyDeletion(w http.ResponseWriter, r *http.Request) {
	keyID, _ := url.QueryUnescape(r.URL.Query().Get("keyId"))
	if keyID == "" {
		uihelper.UIError(w, "BadRequest", "keyId is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "kms", "CancelKeyDeletion", region, account)
	nr.Params["KeyId"] = keyID

	resp, err := h.provider.CancelKeyDeletion(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// GET /aliases?keyId=<optional>
func (h *Handler) ListAliases(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	keyID := r.URL.Query().Get("keyId")
	pageToken := r.URL.Query().Get("nextToken")

	nr := uihelper.NR(r.Context(), h.cfg, "kms", "ListAliases", region, account)
	if keyID != "" {
		nr.Params["KeyId"] = keyID
	}
	if pageToken != "" {
		nr.Params["Marker"] = pageToken
	}

	resp, err := h.provider.ListAliases(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawAliases, _ := resp.Data["Aliases"].([]map[string]any)
	nextToken, _ := resp.Data["NextMarker"].(string)

	aliases := make([]KMSAlias, 0, len(rawAliases))
	for _, a := range rawAliases {
		aliases = append(aliases, KMSAlias{
			AliasName:   strAny(a, "AliasName"),
			AliasARN:    strAny(a, "AliasArn"),
			TargetKeyID: strAny(a, "TargetKeyId"),
		})
	}
	uihelper.WriteJSON(w, ListAliasesResponse{Items: aliases, NextToken: nextToken})
}

// POST /aliases  body: { "aliasName": "...", "targetKeyId": "..." }
func (h *Handler) CreateAlias(w http.ResponseWriter, r *http.Request) {
	var req CreateAliasRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.AliasName == "" || req.TargetKeyID == "" {
		uihelper.UIError(w, "BadRequest", "aliasName and targetKeyId are required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "kms", "CreateAlias", region, account)
	nr.Params["AliasName"] = req.AliasName
	nr.Params["TargetKeyId"] = req.TargetKeyID

	if _, err := h.provider.CreateAlias(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, KMSAlias{AliasName: req.AliasName, TargetKeyID: req.TargetKeyID})
}

// DELETE /aliases?aliasName=<name>
func (h *Handler) DeleteAlias(w http.ResponseWriter, r *http.Request) {
	aliasName := r.URL.Query().Get("aliasName")
	if aliasName == "" {
		uihelper.UIError(w, "BadRequest", "aliasName is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "kms", "DeleteAlias", region, account)
	nr.Params["AliasName"] = aliasName

	if _, err := h.provider.DeleteAlias(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// describeKey calls DescribeKey and assembles a KMSKey.
func (h *Handler) describeKey(r *http.Request, keyID, keyARN, region, account string) KMSKey {
	nr := uihelper.NR(r.Context(), h.cfg, "kms", "DescribeKey", region, account)
	nr.Params["KeyId"] = keyID

	resp, err := h.provider.DescribeKey(r.Context(), nr)
	if err != nil {
		return KMSKey{KeyID: keyID, ARN: keyARN}
	}

	md, _ := resp.Data["KeyMetadata"].(map[string]any)
	if md == nil {
		return KMSKey{KeyID: keyID, ARN: keyARN}
	}

	key := KMSKey{
		KeyID:    strAny(md, "KeyId"),
		ARN:      strAny(md, "Arn"),
		KeyUsage: strAny(md, "KeyUsage"),
		KeySpec:  strAny(md, "KeySpec"),
		KeyState: strAny(md, "KeyState"),
		Origin:   strAny(md, "Origin"),
		CreatedAt: strAny(md, "CreationDate"),
	}
	if md["Description"] != nil {
		key.Description = strAny(md, "Description")
	}
	if s, ok := md["Enabled"].(bool); ok {
		key.Enabled = s
	}
	if key.ARN == "" {
		key.ARN = keyARN
	}
	if key.KeyID == "" {
		key.KeyID = keyID
	}
	return key
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
