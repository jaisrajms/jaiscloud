package iamui

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/aws/ui/uihelper"
	"jaiscloud/internal/config"
)

// Handler serves IAM UI API requests by calling the IAM provider directly.
type Handler struct {
	provider ProviderInterface
	cfg      *config.Config
}

// NewHandler creates a Handler.
func NewHandler(p ProviderInterface, cfg *config.Config) *Handler {
	return &Handler{provider: p, cfg: cfg}
}

// ── Roles ─────────────────────────────────────────────────────────────────────

// GET /roles
func (h *Handler) ListRoles(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	marker := r.URL.Query().Get("marker")

	nr := uihelper.NR(r.Context(), h.cfg, "iam", "ListRoles", region, account)
	if marker != "" {
		nr.Params["Marker"] = marker
	}

	resp, err := h.provider.ListRoles(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawRoles, _ := resp.Data["Roles"].([]map[string]any)
	nextMarker, _ := resp.Data["Marker"].(string)
	roles := make([]IAMRole, 0, len(rawRoles))
	for _, r := range rawRoles {
		roles = append(roles, parseRole(r))
	}
	uihelper.WriteJSON(w, ListRolesResponse{Items: roles, Marker: nextMarker, Total: len(roles)})
}

// POST /roles
func (h *Handler) CreateRole(w http.ResponseWriter, r *http.Request) {
	var req CreateRoleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.RoleName == "" {
		uihelper.UIError(w, "BadRequest", "roleName is required", http.StatusBadRequest)
		return
	}
	if req.AssumeRolePolicyDocument == "" {
		req.AssumeRolePolicyDocument = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "iam", "CreateRole", region, account)
	nr.Params["RoleName"] = req.RoleName
	nr.Params["AssumeRolePolicyDocument"] = req.AssumeRolePolicyDocument
	if req.Description != "" {
		nr.Params["Description"] = req.Description
	}
	if req.MaxSessionDuration > 0 {
		nr.Params["MaxSessionDuration"] = req.MaxSessionDuration
	}

	resp, err := h.provider.CreateRole(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// GET /roles/{roleName}
func (h *Handler) GetRole(w http.ResponseWriter, r *http.Request) {
	roleName := chi.URLParam(r, "roleName")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "iam", "GetRole", region, account)
	nr.Params["RoleName"] = roleName

	resp, err := h.provider.GetRole(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /roles/{roleName}
func (h *Handler) DeleteRole(w http.ResponseWriter, r *http.Request) {
	roleName := chi.URLParam(r, "roleName")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "iam", "DeleteRole", region, account)
	nr.Params["RoleName"] = roleName

	if _, err := h.provider.DeleteRole(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /roles/{roleName}/policies
func (h *Handler) ListAttachedRolePolicies(w http.ResponseWriter, r *http.Request) {
	roleName := chi.URLParam(r, "roleName")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "iam", "ListAttachedRolePolicies", region, account)
	nr.Params["RoleName"] = roleName

	resp, err := h.provider.ListAttachedRolePolicies(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawPolicies, _ := resp.Data["AttachedPolicies"].([]map[string]any)
	policies := make([]AttachedPolicy, 0, len(rawPolicies))
	for _, p := range rawPolicies {
		policies = append(policies, AttachedPolicy{
			PolicyName: strAny(p, "PolicyName"),
			PolicyARN:  strAny(p, "PolicyArn"),
		})
	}
	uihelper.WriteJSON(w, map[string]any{"items": policies})
}

// POST /roles/{roleName}/policies  body: { "policyArn": "..." }
func (h *Handler) AttachRolePolicy(w http.ResponseWriter, r *http.Request) {
	roleName := chi.URLParam(r, "roleName")
	var req struct {
		PolicyARN string `json:"policyArn"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.PolicyARN == "" {
		uihelper.UIError(w, "BadRequest", "policyArn is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "iam", "AttachRolePolicy", region, account)
	nr.Params["RoleName"] = roleName
	nr.Params["PolicyArn"] = req.PolicyARN

	if _, err := h.provider.AttachRolePolicy(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// DELETE /roles/{roleName}/policies?policyArn=...
func (h *Handler) DetachRolePolicy(w http.ResponseWriter, r *http.Request) {
	roleName := chi.URLParam(r, "roleName")
	policyARN := r.URL.Query().Get("policyArn")
	if policyARN == "" {
		uihelper.UIError(w, "BadRequest", "policyArn is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "iam", "DetachRolePolicy", region, account)
	nr.Params["RoleName"] = roleName
	nr.Params["PolicyArn"] = policyARN

	if _, err := h.provider.DetachRolePolicy(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ── Users ─────────────────────────────────────────────────────────────────────

// GET /users
func (h *Handler) ListUsers(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	marker := r.URL.Query().Get("marker")

	nr := uihelper.NR(r.Context(), h.cfg, "iam", "ListUsers", region, account)
	if marker != "" {
		nr.Params["Marker"] = marker
	}

	resp, err := h.provider.ListUsers(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawUsers, _ := resp.Data["Users"].([]map[string]any)
	nextMarker, _ := resp.Data["Marker"].(string)
	users := make([]IAMUser, 0, len(rawUsers))
	for _, u := range rawUsers {
		users = append(users, parseUser(u))
	}
	uihelper.WriteJSON(w, ListUsersResponse{Items: users, Marker: nextMarker, Total: len(users)})
}

// POST /users
func (h *Handler) CreateUser(w http.ResponseWriter, r *http.Request) {
	var req CreateUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.UserName == "" {
		uihelper.UIError(w, "BadRequest", "userName is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "iam", "CreateUser", region, account)
	nr.Params["UserName"] = req.UserName
	if req.Path != "" {
		nr.Params["Path"] = req.Path
	}

	resp, err := h.provider.CreateUser(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /users/{userName}
func (h *Handler) DeleteUser(w http.ResponseWriter, r *http.Request) {
	userName := chi.URLParam(r, "userName")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "iam", "DeleteUser", region, account)
	nr.Params["UserName"] = userName

	if _, err := h.provider.DeleteUser(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /users/{userName}/access-keys
func (h *Handler) ListAccessKeys(w http.ResponseWriter, r *http.Request) {
	userName := chi.URLParam(r, "userName")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "iam", "ListAccessKeys", region, account)
	nr.Params["UserName"] = userName

	resp, err := h.provider.ListAccessKeys(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	rawKeys, _ := resp.Data["AccessKeyMetadata"].([]map[string]any)
	keys := make([]AccessKey, 0, len(rawKeys))
	for _, k := range rawKeys {
		keys = append(keys, AccessKey{
			AccessKeyID: strAny(k, "AccessKeyId"),
			Status:      strAny(k, "Status"),
			CreateDate:  strAny(k, "CreateDate"),
		})
	}
	uihelper.WriteJSON(w, map[string]any{"items": keys})
}

// POST /users/{userName}/access-keys
func (h *Handler) CreateAccessKey(w http.ResponseWriter, r *http.Request) {
	userName := chi.URLParam(r, "userName")
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "iam", "CreateAccessKey", region, account)
	nr.Params["UserName"] = userName

	resp, err := h.provider.CreateAccessKey(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /users/{userName}/access-keys?accessKeyId=...
func (h *Handler) DeleteAccessKey(w http.ResponseWriter, r *http.Request) {
	userName := chi.URLParam(r, "userName")
	accessKeyID := r.URL.Query().Get("accessKeyId")
	if accessKeyID == "" {
		uihelper.UIError(w, "BadRequest", "accessKeyId is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "iam", "DeleteAccessKey", region, account)
	nr.Params["UserName"] = userName
	nr.Params["AccessKeyId"] = accessKeyID

	if _, err := h.provider.DeleteAccessKey(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ── Policies ──────────────────────────────────────────────────────────────────

// GET /policies
func (h *Handler) ListPolicies(w http.ResponseWriter, r *http.Request) {
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)
	marker := r.URL.Query().Get("marker")
	scope := r.URL.Query().Get("scope") // "Local" | "AWS" | "All"
	if scope == "" {
		scope = "Local"
	}

	nr := uihelper.NR(r.Context(), h.cfg, "iam", "ListPolicies", region, account)
	nr.Params["Scope"] = scope
	if marker != "" {
		nr.Params["Marker"] = marker
	}

	resp, err := h.provider.ListPolicies(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}

	rawPolicies, _ := resp.Data["Policies"].([]map[string]any)
	nextMarker, _ := resp.Data["Marker"].(string)
	policies := make([]IAMPolicy, 0, len(rawPolicies))
	for _, p := range rawPolicies {
		policies = append(policies, parsePolicy(p))
	}
	uihelper.WriteJSON(w, ListPoliciesResponse{Items: policies, Marker: nextMarker, Total: len(policies)})
}

// POST /policies
func (h *Handler) CreatePolicy(w http.ResponseWriter, r *http.Request) {
	var req CreatePolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		uihelper.UIError(w, "BadRequest", "invalid request body", http.StatusBadRequest)
		return
	}
	if req.PolicyName == "" {
		uihelper.UIError(w, "BadRequest", "policyName is required", http.StatusBadRequest)
		return
	}
	if req.PolicyDocument == "" {
		uihelper.UIError(w, "BadRequest", "policyDocument is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "iam", "CreatePolicy", region, account)
	nr.Params["PolicyName"] = req.PolicyName
	nr.Params["PolicyDocument"] = req.PolicyDocument
	if req.Description != "" {
		nr.Params["Description"] = req.Description
	}

	resp, err := h.provider.CreatePolicy(r.Context(), nr)
	if err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	uihelper.WriteJSON(w, resp.Data)
}

// DELETE /policies?policyArn=...
func (h *Handler) DeletePolicy(w http.ResponseWriter, r *http.Request) {
	policyARN := r.URL.Query().Get("policyArn")
	if policyARN == "" {
		uihelper.UIError(w, "BadRequest", "policyArn is required", http.StatusBadRequest)
		return
	}
	region := uihelper.RegionFrom(r)
	account := uihelper.AccountFrom(r)

	nr := uihelper.NR(r.Context(), h.cfg, "iam", "DeletePolicy", region, account)
	nr.Params["PolicyArn"] = policyARN

	if _, err := h.provider.DeletePolicy(r.Context(), nr); err != nil {
		uihelper.WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func parseRole(m map[string]any) IAMRole {
	role := IAMRole{
		RoleName:   strAny(m, "RoleName"),
		RoleID:     strAny(m, "RoleId"),
		ARN:        strAny(m, "Arn"),
		Description: strAny(m, "Description"),
		CreateDate: strAny(m, "CreateDate"),
	}
	if doc, ok := m["AssumeRolePolicyDocument"].(string); ok {
		role.AssumeRolePolicyDocument = doc
	}
	if v, ok := m["MaxSessionDuration"]; ok {
		switch n := v.(type) {
		case float64:
			role.MaxSessionDuration = int(n)
		case int:
			role.MaxSessionDuration = n
		}
	}
	return role
}

func parseUser(m map[string]any) IAMUser {
	return IAMUser{
		UserName:   strAny(m, "UserName"),
		UserID:     strAny(m, "UserId"),
		ARN:        strAny(m, "Arn"),
		CreateDate: strAny(m, "CreateDate"),
	}
}

func parsePolicy(m map[string]any) IAMPolicy {
	pol := IAMPolicy{
		PolicyName:  strAny(m, "PolicyName"),
		PolicyID:    strAny(m, "PolicyId"),
		ARN:         strAny(m, "Arn"),
		Description: strAny(m, "Description"),
		CreateDate:  strAny(m, "CreateDate"),
		UpdateDate:  strAny(m, "UpdateDate"),
	}
	if v, ok := m["AttachmentCount"]; ok {
		switch n := v.(type) {
		case float64:
			pol.AttachmentCount = int(n)
		case int:
			pol.AttachmentCount = n
		}
	}
	return pol
}

func strAny(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}
