package ssmui

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

type mockSSMProvider struct {
	describeParamsResp     *model.ProviderResponse
	describeParamsErr      error
	getParamResp           *model.ProviderResponse
	getParamErr            error
	getParamsByPathResp    *model.ProviderResponse
	getParamsByPathErr     error
	putParamResp           *model.ProviderResponse
	putParamErr            error
	deleteParamErr         error
	getHistoryResp         *model.ProviderResponse
	getHistoryErr          error
	listTagsResp           *model.ProviderResponse
	listTagsErr            error

	lastNR           *model.NormalizedRequest
	putParamNR       *model.NormalizedRequest
	deleteParamNR    *model.NormalizedRequest
	getParamsByPathNR *model.NormalizedRequest
}

func (m *mockSSMProvider) DescribeParameters(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.describeParamsResp != nil {
		return m.describeParamsResp, m.describeParamsErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Parameters": []map[string]any{}}}, m.describeParamsErr
}
func (m *mockSSMProvider) GetParameter(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.getParamResp != nil {
		return m.getParamResp, m.getParamErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Parameter": map[string]any{}}}, m.getParamErr
}
func (m *mockSSMProvider) GetParametersByPath(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.getParamsByPathNR = nr
	m.lastNR = nr
	if m.getParamsByPathResp != nil {
		return m.getParamsByPathResp, m.getParamsByPathErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Parameters": []map[string]any{}}}, m.getParamsByPathErr
}
func (m *mockSSMProvider) PutParameter(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.putParamNR = nr
	m.lastNR = nr
	if m.putParamResp != nil {
		return m.putParamResp, m.putParamErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.putParamErr
}
func (m *mockSSMProvider) DeleteParameter(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.deleteParamNR = nr
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.deleteParamErr
}
func (m *mockSSMProvider) GetParameterHistory(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.getHistoryResp != nil {
		return m.getHistoryResp, m.getHistoryErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Parameters": []map[string]any{}}}, m.getHistoryErr
}
func (m *mockSSMProvider) ListTagsForResource(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return m.listTagsResp, m.listTagsErr
}

func testSSMCfg() *config.Config {
	return &config.Config{Port: 4566, UIPort: 4567, Region: "us-east-1", AccountID: "000000000000", Clock: clock.RealClock{}}
}

func testSSMHandler(p *mockSSMProvider) *Handler {
	return NewHandler(p, testSSMCfg())
}

func withChiParamSSM(r *http.Request, key, val string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add(key, val)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

func TestSSMListParameters_Returns200(t *testing.T) {
	p := &mockSSMProvider{
		describeParamsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Parameters": []map[string]any{
				{"Name": "/myapp/db", "Type": "String"},
			},
		}},
	}
	h := testSSMHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/parameters", nil)
	w := httptest.NewRecorder()
	h.ListParameters(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListParametersResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].Name != "/myapp/db" {
		t.Fatalf("unexpected items: %+v", resp.Items)
	}
}

func TestSSMListParameters_WithPath_UsesGetParametersByPath(t *testing.T) {
	p := &mockSSMProvider{
		getParamsByPathResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Parameters": []map[string]any{
				{"Name": "/myapp/key", "Type": "SecureString"},
			},
		}},
	}
	h := testSSMHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/parameters?path=/myapp/", nil)
	w := httptest.NewRecorder()
	h.ListParameters(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if p.getParamsByPathNR == nil {
		t.Fatal("expected GetParametersByPath to be called")
	}
	if p.getParamsByPathNR.Params["Path"] != "/myapp/" {
		t.Fatalf("expected Path=/myapp/, got %v", p.getParamsByPathNR.Params["Path"])
	}
	if p.getParamsByPathNR.Params["Recursive"] != true {
		t.Fatalf("expected Recursive=true, got %v", p.getParamsByPathNR.Params["Recursive"])
	}
}

func TestSSMPutParameter_SetsParams(t *testing.T) {
	p := &mockSSMProvider{}
	h := testSSMHandler(p)
	body, _ := json.Marshal(map[string]string{"name": "/myapp/secret", "value": "hunter2", "type": "SecureString"})
	req := httptest.NewRequest(http.MethodPost, "/parameters", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.PutParameter(w, req)

	if p.putParamNR.Params["Name"] != "/myapp/secret" {
		t.Fatalf("expected Name=/myapp/secret, got %v", p.putParamNR.Params["Name"])
	}
	if p.putParamNR.Params["Value"] != "hunter2" {
		t.Fatalf("expected Value=hunter2, got %v", p.putParamNR.Params["Value"])
	}
	if p.putParamNR.Params["Type"] != "SecureString" {
		t.Fatalf("expected Type=SecureString, got %v", p.putParamNR.Params["Type"])
	}
}

func TestSSMPutParameter_DefaultsTypeToString(t *testing.T) {
	p := &mockSSMProvider{}
	h := testSSMHandler(p)
	body, _ := json.Marshal(map[string]string{"name": "/x", "value": "y"})
	req := httptest.NewRequest(http.MethodPost, "/parameters", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.PutParameter(w, req)

	if p.putParamNR.Params["Type"] != "String" {
		t.Fatalf("expected Type=String (default), got %v", p.putParamNR.Params["Type"])
	}
}

func TestSSMPutParameter_MissingName_Returns400(t *testing.T) {
	p := &mockSSMProvider{}
	h := testSSMHandler(p)
	body, _ := json.Marshal(map[string]string{"value": "val"})
	req := httptest.NewRequest(http.MethodPost, "/parameters", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.PutParameter(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestSSMPutParameter_MissingValue_Returns400(t *testing.T) {
	p := &mockSSMProvider{}
	h := testSSMHandler(p)
	body, _ := json.Marshal(map[string]string{"name": "/x"})
	req := httptest.NewRequest(http.MethodPost, "/parameters", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.PutParameter(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestSSMGetParameter_SetsNameAndDecryption(t *testing.T) {
	p := &mockSSMProvider{}
	h := testSSMHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/parameters/value?name=/myapp/secret", nil)
	w := httptest.NewRecorder()
	h.GetParameter(w, req)

	if p.lastNR.Params["Name"] != "/myapp/secret" {
		t.Fatalf("expected Name=/myapp/secret, got %v", p.lastNR.Params["Name"])
	}
	if p.lastNR.Params["WithDecryption"] != true {
		t.Fatalf("expected WithDecryption=true, got %v", p.lastNR.Params["WithDecryption"])
	}
}

func TestSSMDeleteParameter_AddsLeadingSlash(t *testing.T) {
	p := &mockSSMProvider{}
	h := testSSMHandler(p)
	req := httptest.NewRequest(http.MethodDelete, "/parameters/myapp/db", nil)
	req = withChiParamSSM(req, "*", "myapp/db")
	w := httptest.NewRecorder()
	h.DeleteParameter(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if p.deleteParamNR.Params["Name"] != "/myapp/db" {
		t.Fatalf("expected Name=/myapp/db, got %v", p.deleteParamNR.Params["Name"])
	}
}

func TestSSMDeleteParameter_PreservesLeadingSlash(t *testing.T) {
	p := &mockSSMProvider{}
	h := testSSMHandler(p)
	req := httptest.NewRequest(http.MethodDelete, "/parameters/%2Fmyapp%2Fkey", nil)
	req = withChiParamSSM(req, "*", "%2Fmyapp%2Fkey")
	w := httptest.NewRecorder()
	h.DeleteParameter(w, req)

	if p.deleteParamNR.Params["Name"] != "/myapp/key" {
		t.Fatalf("expected Name=/myapp/key, got %v", p.deleteParamNR.Params["Name"])
	}
}

func TestSSMNR_PortIsWirePort(t *testing.T) {
	p := &mockSSMProvider{}
	h := testSSMHandler(p)
	body, _ := json.Marshal(map[string]string{"name": "/n", "value": "v"})
	req := httptest.NewRequest(http.MethodPost, "/parameters", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.PutParameter(w, req)

	if p.putParamNR.Port != testSSMCfg().Port {
		t.Fatalf("expected Port=%d, got %d", testSSMCfg().Port, p.putParamNR.Port)
	}
}

func TestSSMNR_ClockIsRealClock(t *testing.T) {
	p := &mockSSMProvider{}
	h := testSSMHandler(p)
	body, _ := json.Marshal(map[string]string{"name": "/n", "value": "v"})
	req := httptest.NewRequest(http.MethodPost, "/parameters", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.PutParameter(w, req)

	if p.putParamNR.Clock == nil {
		t.Fatal("expected nr.Clock to be non-nil")
	}
}
