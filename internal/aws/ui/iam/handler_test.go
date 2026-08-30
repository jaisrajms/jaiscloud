package iamui

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
)

type mockIAMProvider struct {
	listRolesResp               *model.ProviderResponse
	listRolesErr                error
	createRoleResp              *model.ProviderResponse
	createRoleErr               error
	getRoleResp                 *model.ProviderResponse
	getRoleErr                  error
	deleteRoleErr               error
	attachRolePolicyErr         error
	detachRolePolicyErr         error
	listAttachedPoliciesResp    *model.ProviderResponse
	listAttachedPoliciesErr     error

	listUsersResp    *model.ProviderResponse
	listUsersErr     error
	createUserResp   *model.ProviderResponse
	createUserErr    error
	getUserResp      *model.ProviderResponse
	getUserErr       error
	deleteUserErr    error
	listKeysResp     *model.ProviderResponse
	listKeysErr      error
	createKeyResp    *model.ProviderResponse
	createKeyErr     error
	deleteKeyErr     error

	listPoliciesResp  *model.ProviderResponse
	listPoliciesErr   error
	createPolicyResp  *model.ProviderResponse
	createPolicyErr   error
	getPolicyResp     *model.ProviderResponse
	getPolicyErr      error
	deletePolicyErr   error

	lastNR         *model.NormalizedRequest
	createRoleNR   *model.NormalizedRequest
	createUserNR   *model.NormalizedRequest
	createPolicyNR *model.NormalizedRequest
}

func (m *mockIAMProvider) ListRoles(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.listRolesResp != nil {
		return m.listRolesResp, m.listRolesErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Roles": []map[string]any{}}}, m.listRolesErr
}
func (m *mockIAMProvider) CreateRole(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.createRoleNR = nr
	m.lastNR = nr
	if m.createRoleResp != nil {
		return m.createRoleResp, m.createRoleErr
	}
	return &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{}}, m.createRoleErr
}
func (m *mockIAMProvider) GetRole(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.getRoleResp != nil {
		return m.getRoleResp, m.getRoleErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.getRoleErr
}
func (m *mockIAMProvider) DeleteRole(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.deleteRoleErr
}
func (m *mockIAMProvider) AttachRolePolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.attachRolePolicyErr
}
func (m *mockIAMProvider) DetachRolePolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.detachRolePolicyErr
}
func (m *mockIAMProvider) ListAttachedRolePolicies(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.listAttachedPoliciesResp != nil {
		return m.listAttachedPoliciesResp, m.listAttachedPoliciesErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"AttachedPolicies": []map[string]any{}}}, m.listAttachedPoliciesErr
}
func (m *mockIAMProvider) ListUsers(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.listUsersResp != nil {
		return m.listUsersResp, m.listUsersErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Users": []map[string]any{}}}, m.listUsersErr
}
func (m *mockIAMProvider) CreateUser(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.createUserNR = nr
	m.lastNR = nr
	if m.createUserResp != nil {
		return m.createUserResp, m.createUserErr
	}
	return &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{}}, m.createUserErr
}
func (m *mockIAMProvider) GetUser(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return m.getUserResp, m.getUserErr
}
func (m *mockIAMProvider) DeleteUser(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.deleteUserErr
}
func (m *mockIAMProvider) ListAccessKeys(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.listKeysResp != nil {
		return m.listKeysResp, m.listKeysErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"AccessKeyMetadata": []map[string]any{}}}, m.listKeysErr
}
func (m *mockIAMProvider) CreateAccessKey(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.createKeyResp != nil {
		return m.createKeyResp, m.createKeyErr
	}
	return &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{
		"AccessKey": map[string]any{"AccessKeyId": "AKIAIOSFODNN7EXAMPLE", "SecretAccessKey": "secret"},
	}}, m.createKeyErr
}
func (m *mockIAMProvider) DeleteAccessKey(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.deleteKeyErr
}
func (m *mockIAMProvider) ListPolicies(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.listPoliciesResp != nil {
		return m.listPoliciesResp, m.listPoliciesErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Policies": []map[string]any{}}}, m.listPoliciesErr
}
func (m *mockIAMProvider) CreatePolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.createPolicyNR = nr
	m.lastNR = nr
	if m.createPolicyResp != nil {
		return m.createPolicyResp, m.createPolicyErr
	}
	return &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{}}, m.createPolicyErr
}
func (m *mockIAMProvider) GetPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return m.getPolicyResp, m.getPolicyErr
}
func (m *mockIAMProvider) DeletePolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.deletePolicyErr
}

func testIAMCfg() *config.Config {
	return &config.Config{Port: 4566, UIPort: 4567, Region: "us-east-1", AccountID: "000000000000", Clock: clock.RealClock{}}
}

func testIAMHandler(p *mockIAMProvider) *Handler {
	return NewHandler(p, testIAMCfg())
}

func withChiParamIAM(r *http.Request, key, val string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add(key, val)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

// ── Roles ──────────────────────────────────────────────────────────────────────

func TestIAMListRoles_Returns200(t *testing.T) {
	p := &mockIAMProvider{
		listRolesResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Roles": []map[string]any{
				{"RoleName": "my-role", "Arn": "arn:aws:iam::000000000000:role/my-role"},
			},
		}},
	}
	h := testIAMHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/roles", nil)
	w := httptest.NewRecorder()
	h.ListRoles(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListRolesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].RoleName != "my-role" {
		t.Fatalf("unexpected items: %+v", resp.Items)
	}
}

func TestIAMCreateRole_Returns201(t *testing.T) {
	p := &mockIAMProvider{}
	h := testIAMHandler(p)
	body, _ := json.Marshal(map[string]string{"roleName": "my-lambda-role"})
	req := httptest.NewRequest(http.MethodPost, "/roles", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.CreateRole(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if p.createRoleNR.Params["RoleName"] != "my-lambda-role" {
		t.Fatalf("expected RoleName=my-lambda-role, got %v", p.createRoleNR.Params["RoleName"])
	}
}

func TestIAMCreateRole_DefaultsLambdaTrustPolicy(t *testing.T) {
	p := &mockIAMProvider{}
	h := testIAMHandler(p)
	body, _ := json.Marshal(map[string]string{"roleName": "r"})
	req := httptest.NewRequest(http.MethodPost, "/roles", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.CreateRole(w, req)

	doc, _ := p.createRoleNR.Params["AssumeRolePolicyDocument"].(string)
	if doc == "" {
		t.Fatal("expected AssumeRolePolicyDocument to be set to Lambda trust policy default")
	}
}

func TestIAMCreateRole_MissingRoleName_Returns400(t *testing.T) {
	p := &mockIAMProvider{}
	h := testIAMHandler(p)
	body, _ := json.Marshal(map[string]string{"description": "no name"})
	req := httptest.NewRequest(http.MethodPost, "/roles", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.CreateRole(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestIAMDeleteRole_Returns204(t *testing.T) {
	p := &mockIAMProvider{}
	h := testIAMHandler(p)
	req := httptest.NewRequest(http.MethodDelete, "/roles/my-role", nil)
	req = withChiParamIAM(req, "roleName", "my-role")
	w := httptest.NewRecorder()
	h.DeleteRole(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if p.lastNR.Params["RoleName"] != "my-role" {
		t.Fatalf("expected RoleName=my-role, got %v", p.lastNR.Params["RoleName"])
	}
}

// ── Users ──────────────────────────────────────────────────────────────────────

func TestIAMListUsers_Returns200(t *testing.T) {
	p := &mockIAMProvider{
		listUsersResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Users": []map[string]any{
				{"UserName": "alice", "Arn": "arn:aws:iam::000000000000:user/alice"},
			},
		}},
	}
	h := testIAMHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/users", nil)
	w := httptest.NewRecorder()
	h.ListUsers(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListUsersResponse
	json.Unmarshal(w.Body.Bytes(), &resp) //nolint:errcheck
	if len(resp.Items) != 1 || resp.Items[0].UserName != "alice" {
		t.Fatalf("unexpected items: %+v", resp.Items)
	}
}

func TestIAMCreateUser_SetsUserName(t *testing.T) {
	p := &mockIAMProvider{}
	h := testIAMHandler(p)
	body, _ := json.Marshal(map[string]string{"userName": "bob"})
	req := httptest.NewRequest(http.MethodPost, "/users", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.CreateUser(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if p.createUserNR.Params["UserName"] != "bob" {
		t.Fatalf("expected UserName=bob, got %v", p.createUserNR.Params["UserName"])
	}
}

func TestIAMDeleteUser_Returns204(t *testing.T) {
	p := &mockIAMProvider{}
	h := testIAMHandler(p)
	req := httptest.NewRequest(http.MethodDelete, "/users/alice", nil)
	req = withChiParamIAM(req, "userName", "alice")
	w := httptest.NewRecorder()
	h.DeleteUser(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
}

func TestIAMDeleteAccessKey_MissingKeyId_Returns400(t *testing.T) {
	p := &mockIAMProvider{}
	h := testIAMHandler(p)
	req := httptest.NewRequest(http.MethodDelete, "/users/alice/access-keys", nil)
	req = withChiParamIAM(req, "userName", "alice")
	w := httptest.NewRecorder()
	h.DeleteAccessKey(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// ── Policies ──────────────────────────────────────────────────────────────────

func TestIAMListPolicies_DefaultsScopeToLocal(t *testing.T) {
	p := &mockIAMProvider{}
	h := testIAMHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/policies", nil)
	w := httptest.NewRecorder()
	h.ListPolicies(w, req)

	if p.lastNR.Params["Scope"] != "Local" {
		t.Fatalf("expected Scope=Local, got %v", p.lastNR.Params["Scope"])
	}
}

func TestIAMCreatePolicy_SetsParams(t *testing.T) {
	p := &mockIAMProvider{}
	h := testIAMHandler(p)
	body, _ := json.Marshal(map[string]string{
		"policyName":     "my-policy",
		"policyDocument": `{"Version":"2012-10-17","Statement":[]}`,
	})
	req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.CreatePolicy(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if p.createPolicyNR.Params["PolicyName"] != "my-policy" {
		t.Fatalf("expected PolicyName=my-policy, got %v", p.createPolicyNR.Params["PolicyName"])
	}
	if p.createPolicyNR.Params["PolicyDocument"] == "" {
		t.Fatal("expected PolicyDocument to be non-empty")
	}
}

func TestIAMCreatePolicy_MissingDocument_Returns400(t *testing.T) {
	p := &mockIAMProvider{}
	h := testIAMHandler(p)
	body, _ := json.Marshal(map[string]string{"policyName": "p"})
	req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.CreatePolicy(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestIAMDeletePolicy_Returns204(t *testing.T) {
	p := &mockIAMProvider{}
	h := testIAMHandler(p)
	req := httptest.NewRequest(http.MethodDelete, "/policies?policyArn=arn:aws:iam::000000000000:policy/p", nil)
	w := httptest.NewRecorder()
	h.DeletePolicy(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if p.lastNR.Params["PolicyArn"] != "arn:aws:iam::000000000000:policy/p" {
		t.Fatalf("expected PolicyArn set, got %v", p.lastNR.Params["PolicyArn"])
	}
}

func TestIAMDeletePolicy_MissingArn_Returns400(t *testing.T) {
	p := &mockIAMProvider{}
	h := testIAMHandler(p)
	req := httptest.NewRequest(http.MethodDelete, "/policies", nil)
	w := httptest.NewRecorder()
	h.DeletePolicy(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestIAMNR_PortIsWirePort(t *testing.T) {
	p := &mockIAMProvider{}
	h := testIAMHandler(p)
	body, _ := json.Marshal(map[string]string{"roleName": "r"})
	req := httptest.NewRequest(http.MethodPost, "/roles", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.CreateRole(w, req)

	if p.createRoleNR.Port != testIAMCfg().Port {
		t.Fatalf("expected Port=%d, got %d", testIAMCfg().Port, p.createRoleNR.Port)
	}
}

func TestIAMNR_ClockIsRealClock(t *testing.T) {
	p := &mockIAMProvider{}
	h := testIAMHandler(p)
	body, _ := json.Marshal(map[string]string{"roleName": "r"})
	req := httptest.NewRequest(http.MethodPost, "/roles", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.CreateRole(w, req)

	if p.createRoleNR.Clock == nil {
		t.Fatal("expected nr.Clock to be non-nil")
	}
}
