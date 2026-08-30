package secretsmanagerui

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

type mockSMProvider struct {
	listSecretsResp          *model.ProviderResponse
	listSecretsErr           error
	createSecretResp         *model.ProviderResponse
	createSecretErr          error
	describeSecretResp       *model.ProviderResponse
	describeSecretErr        error
	getSecretValueResp       *model.ProviderResponse
	getSecretValueErr        error
	putSecretValueResp       *model.ProviderResponse
	putSecretValueErr        error
	updateSecretResp         *model.ProviderResponse
	updateSecretErr          error
	deleteSecretErr          error
	restoreSecretResp        *model.ProviderResponse
	restoreSecretErr         error
	listVersionsResp         *model.ProviderResponse
	listVersionsErr          error
	tagResourceErr           error

	lastNR           *model.NormalizedRequest
	createSecretNR   *model.NormalizedRequest
	putSecretValueNR *model.NormalizedRequest
}

func (m *mockSMProvider) ListSecrets(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return m.listSecretsResp, m.listSecretsErr
}
func (m *mockSMProvider) CreateSecret(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.createSecretNR = nr
	m.lastNR = nr
	return m.createSecretResp, m.createSecretErr
}
func (m *mockSMProvider) DescribeSecret(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.describeSecretResp != nil {
		return m.describeSecretResp, m.describeSecretErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.describeSecretErr
}
func (m *mockSMProvider) GetSecretValue(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.getSecretValueResp != nil {
		return m.getSecretValueResp, m.getSecretValueErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.getSecretValueErr
}
func (m *mockSMProvider) PutSecretValue(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.putSecretValueNR = nr
	m.lastNR = nr
	if m.putSecretValueResp != nil {
		return m.putSecretValueResp, m.putSecretValueErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.putSecretValueErr
}
func (m *mockSMProvider) UpdateSecret(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return m.updateSecretResp, m.updateSecretErr
}
func (m *mockSMProvider) DeleteSecret(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.deleteSecretErr
}
func (m *mockSMProvider) RestoreSecret(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return m.restoreSecretResp, m.restoreSecretErr
}
func (m *mockSMProvider) ListSecretVersionIds(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.listVersionsResp != nil {
		return m.listVersionsResp, m.listVersionsErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Versions": []map[string]any{}}}, m.listVersionsErr
}
func (m *mockSMProvider) TagResource(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.tagResourceErr
}

func testSMCfg() *config.Config {
	return &config.Config{Port: 4566, UIPort: 4567, Region: "us-east-1", AccountID: "000000000000", Clock: clock.RealClock{}}
}

func testSMHandler(p *mockSMProvider) *Handler {
	return NewHandler(p, testSMCfg())
}

func withChiParamSM(r *http.Request, key, val string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add(key, val)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

func TestSMListSecrets_Returns200(t *testing.T) {
	p := &mockSMProvider{
		listSecretsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"SecretList": []map[string]any{
				{"Name": "my-secret", "ARN": "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret"},
			},
		}},
	}
	h := testSMHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/secrets", nil)
	w := httptest.NewRecorder()
	h.ListSecrets(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListSecretsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].Name != "my-secret" {
		t.Fatalf("unexpected items: %+v", resp.Items)
	}
}

func TestSMListSecrets_Total(t *testing.T) {
	p := &mockSMProvider{
		listSecretsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"SecretList": []map[string]any{
				{"Name": "s1", "ARN": "arn:aws:secretsmanager:us-east-1:000000000000:secret:s1"},
				{"Name": "s2", "ARN": "arn:aws:secretsmanager:us-east-1:000000000000:secret:s2"},
			},
		}},
	}
	h := testSMHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/secrets", nil)
	w := httptest.NewRecorder()
	h.ListSecrets(w, req)

	var resp ListSecretsResponse
	json.Unmarshal(w.Body.Bytes(), &resp) //nolint:errcheck
	if resp.Total != 2 {
		t.Fatalf("expected Total=2, got %d", resp.Total)
	}
}

func TestSMCreateSecret_Returns201(t *testing.T) {
	p := &mockSMProvider{
		createSecretResp: &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{
			"Name": "new-secret", "ARN": "arn:aws:secretsmanager:us-east-1:000000000000:secret:new-secret",
		}},
	}
	h := testSMHandler(p)
	body, _ := json.Marshal(map[string]string{"name": "new-secret", "secretString": "my-value"})
	req := httptest.NewRequest(http.MethodPost, "/secrets", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.CreateSecret(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if p.createSecretNR.Params["Name"] != "new-secret" {
		t.Fatalf("expected Name=new-secret, got %v", p.createSecretNR.Params["Name"])
	}
	if p.createSecretNR.Params["SecretString"] != "my-value" {
		t.Fatalf("expected SecretString=my-value, got %v", p.createSecretNR.Params["SecretString"])
	}
}

func TestSMCreateSecret_MissingName_Returns400(t *testing.T) {
	p := &mockSMProvider{}
	h := testSMHandler(p)
	body, _ := json.Marshal(map[string]string{"secretString": "val"})
	req := httptest.NewRequest(http.MethodPost, "/secrets", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.CreateSecret(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestSMCreateSecret_NrPortIsWirePort(t *testing.T) {
	p := &mockSMProvider{
		createSecretResp: &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{}},
	}
	h := testSMHandler(p)
	body, _ := json.Marshal(map[string]string{"name": "s"})
	req := httptest.NewRequest(http.MethodPost, "/secrets", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.CreateSecret(w, req)

	if p.createSecretNR.Port != testSMCfg().Port {
		t.Fatalf("expected Port=%d, got %d", testSMCfg().Port, p.createSecretNR.Port)
	}
}

func TestSMGetSecretValue_SetsSecretId(t *testing.T) {
	p := &mockSMProvider{}
	h := testSMHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/secrets/my-secret/value", nil)
	req = withChiParamSM(req, "name", "my-secret")
	w := httptest.NewRecorder()
	h.GetSecretValue(w, req)

	if p.lastNR.Params["SecretId"] != "my-secret" {
		t.Fatalf("expected SecretId=my-secret, got %v", p.lastNR.Params["SecretId"])
	}
}

func TestSMPutSecretValue_SetsParams(t *testing.T) {
	p := &mockSMProvider{}
	h := testSMHandler(p)
	body, _ := json.Marshal(map[string]string{"secretString": "new-value"})
	req := httptest.NewRequest(http.MethodPost, "/secrets/my-secret/value", bytes.NewReader(body))
	req = withChiParamSM(req, "name", "my-secret")
	w := httptest.NewRecorder()
	h.PutSecretValue(w, req)

	if p.putSecretValueNR.Params["SecretId"] != "my-secret" {
		t.Fatalf("expected SecretId=my-secret, got %v", p.putSecretValueNR.Params["SecretId"])
	}
	if p.putSecretValueNR.Params["SecretString"] != "new-value" {
		t.Fatalf("expected SecretString=new-value, got %v", p.putSecretValueNR.Params["SecretString"])
	}
}

func TestSMPutSecretValue_EmptyBody_Returns400(t *testing.T) {
	p := &mockSMProvider{}
	h := testSMHandler(p)
	body, _ := json.Marshal(map[string]string{"secretString": ""})
	req := httptest.NewRequest(http.MethodPost, "/secrets/my-secret/value", bytes.NewReader(body))
	req = withChiParamSM(req, "name", "my-secret")
	w := httptest.NewRecorder()
	h.PutSecretValue(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestSMDeleteSecret_Returns204(t *testing.T) {
	p := &mockSMProvider{}
	h := testSMHandler(p)
	req := httptest.NewRequest(http.MethodDelete, "/secrets/my-secret", nil)
	req = withChiParamSM(req, "name", "my-secret")
	w := httptest.NewRecorder()
	h.DeleteSecret(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if p.lastNR.Params["SecretId"] != "my-secret" {
		t.Fatalf("expected SecretId=my-secret, got %v", p.lastNR.Params["SecretId"])
	}
	if p.lastNR.Params["ForceDeleteWithoutRecovery"] != true {
		t.Fatalf("expected ForceDeleteWithoutRecovery=true, got %v", p.lastNR.Params["ForceDeleteWithoutRecovery"])
	}
}

func TestSMListSecretVersions_Returns200(t *testing.T) {
	p := &mockSMProvider{
		listVersionsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Versions": []map[string]any{
				{"VersionId": "v1", "VersionStages": []any{"AWSCURRENT"}, "CreatedDate": "2024-01-01"},
			},
		}},
	}
	h := testSMHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/secrets/my-secret/versions", nil)
	req = withChiParamSM(req, "name", "my-secret")
	w := httptest.NewRecorder()
	h.ListSecretVersions(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListSecretVersionsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].VersionID != "v1" {
		t.Fatalf("unexpected items: %+v", resp.Items)
	}
}

func TestSMNR_ClockIsRealClock(t *testing.T) {
	p := &mockSMProvider{
		listSecretsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"SecretList": []map[string]any{},
		}},
	}
	h := testSMHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/secrets", nil)
	w := httptest.NewRecorder()
	h.ListSecrets(w, req)

	if p.lastNR.Clock == nil {
		t.Fatal("expected nr.Clock to be non-nil")
	}
}
