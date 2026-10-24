package iamui

import (
	"encoding/json"
	"net/http"
	"strings"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/ui/uihelper"
)

// Handler serves IAM UI API requests by calling the IAM provider.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// account resolves the project for a request, falling back to the configured
// project when the inject-config middleware has not populated the context.
func (h *Handler) account(r *http.Request) string {
	if a := uihelper.AccountFrom(r); a != "" {
		return a
	}
	return h.cfg.AccountID
}

// ─── mapping helpers ─────────────────────────────────────────────────────────

func saFromMap(m map[string]any) ServiceAccount {
	return ServiceAccount{
		Name:        uihelper.Str(m, "name"),
		Email:       uihelper.Str(m, "email"),
		DisplayName: uihelper.Str(m, "displayName"),
		ProjectID:   uihelper.Str(m, "projectId"),
		UniqueID:    uihelper.Str(m, "uniqueId"),
		Description: uihelper.Str(m, "description"),
		Disabled:    boolAt(m, "disabled"),
		Etag:        uihelper.Str(m, "etag"),
	}
}

func keyFromMap(m map[string]any) ServiceAccountKey {
	name := uihelper.Str(m, "name")
	return ServiceAccountKey{
		Name:           name,
		KeyID:          keyIDFromName(name),
		KeyAlgorithm:   uihelper.Str(m, "keyAlgorithm"),
		KeyOrigin:      uihelper.Str(m, "keyOrigin"),
		KeyType:        uihelper.Str(m, "keyType"),
		ValidAfterTime: uihelper.Str(m, "validAfterTime"),
		Disabled:       boolAt(m, "disabled"),
		DisableTime:    uihelper.Str(m, "disableTime"),
		PublicKeyData:  uihelper.Str(m, "publicKeyData"),
		PrivateKeyData: uihelper.Str(m, "privateKeyData"),
	}
}

// keyIDFromName returns the key id, which real GCP exposes only as the trailing
// segment of the key's resource name (there is no keyId field).
func keyIDFromName(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
}

func boolAt(m map[string]any, key string) bool {
	b, _ := m[key].(bool)
	return b
}

func policyToMap(p IamPolicy) map[string]any {
	return uihelper.IamPolicyBody(p)
}

// ─── target helpers ──────────────────────────────────────────────────────────

// serviceAccountName builds "serviceAccounts/{email}" from the {email} path
// param, or writes a 400.
func (h *Handler) serviceAccountName(w http.ResponseWriter, r *http.Request) (string, bool) {
	email, ok := uihelper.Segment(r, "email")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid service account", http.StatusBadRequest)
		return "", false
	}
	return "serviceAccounts/" + email, true
}

// keyName builds "serviceAccounts/{email}/keys/{key}" or writes a 400.
func (h *Handler) keyName(w http.ResponseWriter, r *http.Request) (string, bool) {
	base, ok := h.serviceAccountName(w, r)
	if !ok {
		return "", false
	}
	key, ok := uihelper.Segment(r, "key")
	if !ok {
		uihelper.UIError(w, "BadRequest", "invalid key", http.StatusBadRequest)
		return "", false
	}
	return base + "/keys/" + key, true
}

// ─── Service accounts ────────────────────────────────────────────────────────

// GET /serviceAccounts
func (h *Handler) ListServiceAccounts(w http.ResponseWriter, r *http.Request) {
	nr := uihelper.NR(r.Context(), h.cfg, "iam", "IAM.ServiceAccountList", "global", h.account(r))
	uihelper.PageParams(r, nr.Params)

	resp, err := h.provider.List(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	raw := uihelper.AsSlice(resp.Data["accounts"])
	accounts := make([]ServiceAccount, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			accounts = append(accounts, saFromMap(m))
		}
	}
	uihelper.WriteJSON(w, ListServiceAccountsResponse{
		Accounts:      accounts,
		Total:         len(accounts),
		NextPageToken: uihelper.Str(resp.Data, "nextPageToken"),
	})
}

// POST /serviceAccounts
func (h *Handler) CreateServiceAccount(w http.ResponseWriter, r *http.Request) {
	var req CreateServiceAccountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.AccountID == "" {
		uihelper.UIError(w, "BadRequest", "accountId is required", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "iam", "IAM.ServiceAccountCreate", "global", h.account(r))
	nr.Params["accountId"] = req.AccountID
	sa := map[string]any{}
	if req.DisplayName != "" {
		sa["displayName"] = req.DisplayName
	}
	if req.Description != "" {
		sa["description"] = req.Description
	}
	nr.Params["body"] = map[string]any{"serviceAccount": sa}

	resp, err := h.provider.Create(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, resp.Data)
}

// GET /serviceAccounts/{email}
func (h *Handler) GetServiceAccount(w http.ResponseWriter, r *http.Request) {
	name, ok := h.serviceAccountName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "iam", "IAM.ServiceAccountGet", "global", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.Get(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// PATCH /serviceAccounts/{email}
func (h *Handler) UpdateServiceAccount(w http.ResponseWriter, r *http.Request) {
	name, ok := h.serviceAccountName(w, r)
	if !ok {
		return
	}
	var req UpdateServiceAccountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "iam", "IAM.ServiceAccountPatch", "global", h.account(r))
	nr.Params["name"] = name
	nr.Params["updateMask"] = "displayName,description"
	body := map[string]any{"displayName": req.DisplayName, "description": req.Description}
	if req.Etag != "" {
		body["etag"] = req.Etag
	}
	nr.Params["body"] = body

	resp, err := h.provider.Update(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /serviceAccounts/{email}
func (h *Handler) DeleteServiceAccount(w http.ResponseWriter, r *http.Request) {
	name, ok := h.serviceAccountName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "iam", "IAM.ServiceAccountDelete", "global", h.account(r))
	nr.Params["name"] = name

	if _, err := h.provider.Delete(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /serviceAccounts/{email}/disable
func (h *Handler) DisableServiceAccount(w http.ResponseWriter, r *http.Request) {
	h.setServiceAccountDisabled(w, r, true)
}

// POST /serviceAccounts/{email}/enable
func (h *Handler) EnableServiceAccount(w http.ResponseWriter, r *http.Request) {
	h.setServiceAccountDisabled(w, r, false)
}

func (h *Handler) setServiceAccountDisabled(w http.ResponseWriter, r *http.Request, disabled bool) {
	name, ok := h.serviceAccountName(w, r)
	if !ok {
		return
	}
	action := "IAM.ServiceAccountEnable"
	if disabled {
		action = "IAM.ServiceAccountDisable"
	}
	nr := uihelper.NR(r.Context(), h.cfg, "iam", action, "global", h.account(r))
	nr.Params["name"] = name

	call := h.provider.Enable
	if disabled {
		call = h.provider.Disable
	}
	resp, err := call(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// ─── IAM policy ──────────────────────────────────────────────────────────────

// GET /serviceAccounts/{email}/iam
func (h *Handler) GetIamPolicy(w http.ResponseWriter, r *http.Request) {
	name, ok := h.serviceAccountName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "iam", "IAM.ServiceAccountGetIamPolicy", "global", h.account(r))
	nr.Params["name"] = name
	if v := r.URL.Query().Get("requestedPolicyVersion"); v != "" {
		nr.Params["options.requestedPolicyVersion"] = v
	}

	resp, err := h.provider.GetIamPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// PUT /serviceAccounts/{email}/iam
func (h *Handler) SetIamPolicy(w http.ResponseWriter, r *http.Request) {
	name, ok := h.serviceAccountName(w, r)
	if !ok {
		return
	}
	var req SetIamPolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "iam", "IAM.ServiceAccountSetIamPolicy", "global", h.account(r))
	nr.Params["name"] = name
	nr.Params["body"] = map[string]any{"policy": policyToMap(req.Policy)}

	resp, err := h.provider.SetIamPolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// POST /serviceAccounts/{email}/iam/testIamPermissions
func (h *Handler) TestIamPermissions(w http.ResponseWriter, r *http.Request) {
	name, ok := h.serviceAccountName(w, r)
	if !ok {
		return
	}
	var req TestIamPermissionsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "iam", "IAM.ServiceAccountTestIamPermissions", "global", h.account(r))
	nr.Params["name"] = name
	nr.Params["body"] = map[string]any{"permissions": uihelper.StringsToAny(req.Permissions)}

	resp, err := h.provider.TestIamPermissions(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// ─── Keys ────────────────────────────────────────────────────────────────────

// GET /serviceAccounts/{email}/keys
func (h *Handler) ListKeys(w http.ResponseWriter, r *http.Request) {
	base, ok := h.serviceAccountName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "iam", "IAM.ServiceAccountKeyList", "global", h.account(r))
	nr.Params["name"] = base + "/keys"
	uihelper.PageParams(r, nr.Params)

	resp, err := h.provider.ServiceAccountKeyList(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	raw := uihelper.AsSlice(resp.Data["keys"])
	keys := make([]ServiceAccountKey, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			keys = append(keys, keyFromMap(m))
		}
	}
	uihelper.WriteJSON(w, ListServiceAccountKeysResponse{
		Keys:          keys,
		Total:         len(keys),
		NextPageToken: uihelper.Str(resp.Data, "nextPageToken"),
	})
}

// POST /serviceAccounts/{email}/keys
func (h *Handler) CreateKey(w http.ResponseWriter, r *http.Request) {
	base, ok := h.serviceAccountName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "iam", "IAM.ServiceAccountKeyCreate", "global", h.account(r))
	nr.Params["name"] = base + "/keys"

	resp, err := h.provider.ServiceAccountKeyCreate(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSONStatus(w, http.StatusCreated, keyFromMap(resp.Data))
}

// GET /serviceAccounts/{email}/keys/{key}
func (h *Handler) GetKey(w http.ResponseWriter, r *http.Request) {
	name, ok := h.keyName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "iam", "IAM.ServiceAccountKeyGet", "global", h.account(r))
	nr.Params["name"] = name

	resp, err := h.provider.ServiceAccountKeyGet(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, keyFromMap(resp.Data))
}

// DELETE /serviceAccounts/{email}/keys/{key}
func (h *Handler) DeleteKey(w http.ResponseWriter, r *http.Request) {
	name, ok := h.keyName(w, r)
	if !ok {
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "iam", "IAM.ServiceAccountKeyDelete", "global", h.account(r))
	nr.Params["name"] = name

	if _, err := h.provider.ServiceAccountKeyDelete(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// POST /serviceAccounts/{email}/keys/{key}/disable
func (h *Handler) DisableKey(w http.ResponseWriter, r *http.Request) {
	h.setKeyDisabled(w, r, true)
}

// POST /serviceAccounts/{email}/keys/{key}/enable
func (h *Handler) EnableKey(w http.ResponseWriter, r *http.Request) {
	h.setKeyDisabled(w, r, false)
}

func (h *Handler) setKeyDisabled(w http.ResponseWriter, r *http.Request, disabled bool) {
	name, ok := h.keyName(w, r)
	if !ok {
		return
	}
	action := "IAM.ServiceAccountKeyEnable"
	if disabled {
		action = "IAM.ServiceAccountKeyDisable"
	}
	nr := uihelper.NR(r.Context(), h.cfg, "iam", action, "global", h.account(r))
	nr.Params["name"] = name

	call := h.provider.ServiceAccountKeyEnable
	if disabled {
		call = h.provider.ServiceAccountKeyDisable
	}
	resp, err := call(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, keyFromMap(resp.Data))
}

// ─── Signing ─────────────────────────────────────────────────────────────────

// POST /serviceAccounts/{email}/signBlob
func (h *Handler) SignBlob(w http.ResponseWriter, r *http.Request) {
	name, ok := h.serviceAccountName(w, r)
	if !ok {
		return
	}
	var req SignBlobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.BytesToSign == "" {
		uihelper.UIError(w, "BadRequest", "bytesToSign is required", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "iam", "IAM.ServiceAccountSignBlob", "global", h.account(r))
	nr.Params["name"] = name
	nr.Params["body"] = map[string]any{"bytesToSign": req.BytesToSign}

	resp, err := h.provider.ServiceAccountSignBlob(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, SignBlobResponse{
		KeyID:     uihelper.Str(resp.Data, "keyId"),
		Signature: uihelper.Str(resp.Data, "signature"),
	})
}

// POST /serviceAccounts/{email}/signJwt
func (h *Handler) SignJwt(w http.ResponseWriter, r *http.Request) {
	name, ok := h.serviceAccountName(w, r)
	if !ok {
		return
	}
	var req SignJwtRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Payload == "" {
		uihelper.UIError(w, "BadRequest", "payload is required", http.StatusBadRequest)
		return
	}
	nr := uihelper.NR(r.Context(), h.cfg, "iam", "IAM.ServiceAccountSignJwt", "global", h.account(r))
	nr.Params["name"] = name
	nr.Params["body"] = map[string]any{"payload": req.Payload}

	resp, err := h.provider.ServiceAccountSignJwt(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, SignJwtResponse{
		KeyID:     uihelper.Str(resp.Data, "keyId"),
		SignedJwt: uihelper.Str(resp.Data, "signedJwt"),
	})
}
