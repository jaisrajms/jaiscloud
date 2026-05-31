package lambdaui

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

type mockFunctionProvider struct {
	listFuncsResp   *model.ProviderResponse
	listFuncsErr    error
	getFuncResp     *model.ProviderResponse
	getFuncErr      error
	deleteFuncErr   error
	invokeResp      *model.ProviderResponse
	invokeErr       error
	updateCfgResp   *model.ProviderResponse
	updateCfgErr    error

	lastNR *model.NormalizedRequest
}

func (m *mockFunctionProvider) capture(nr *model.NormalizedRequest) { m.lastNR = nr }

func (m *mockFunctionProvider) ListFunctions(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return m.listFuncsResp, m.listFuncsErr
}
func (m *mockFunctionProvider) GetFunction(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return m.getFuncResp, m.getFuncErr
}
func (m *mockFunctionProvider) DeleteFunction(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 204, Data: map[string]any{}}, m.deleteFuncErr
}
func (m *mockFunctionProvider) InvokeFunction(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return m.invokeResp, m.invokeErr
}
func (m *mockFunctionProvider) UpdateFunctionConfiguration(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return m.updateCfgResp, m.updateCfgErr
}
func (m *mockFunctionProvider) GetFunctionConfiguration(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.capture(nr)
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

func testCfg() *config.Config {
	return &config.Config{
		Port:      4566,
		UIPort:    4567,
		Region:    "us-east-1",
		AccountID: "000000000000",
		Clock:     clock.RealClock{},
	}
}

// withChiParam adds a Chi URL param to the request context.
func withChiParam(r *http.Request, key, value string) *http.Request {
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add(key, value)
	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
}

func TestListFunctions_Returns200(t *testing.T) {
	mock := &mockFunctionProvider{
		listFuncsResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Functions": []map[string]any{
				{"FunctionName": "my-fn", "Runtime": "python3.11"},
			},
		}},
	}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/functions", nil)
	rr := httptest.NewRecorder()
	h.ListFunctions(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	var resp ListFunctionsResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if len(resp.Items) != 1 {
		t.Errorf("want 1 function, got %d", len(resp.Items))
	}
}

func TestDeleteFunction_Returns204(t *testing.T) {
	mock := &mockFunctionProvider{}
	h := NewHandler(mock, testCfg())
	req := withChiParam(httptest.NewRequest(http.MethodDelete, "/functions/my-fn", nil), "name", "my-fn")
	rr := httptest.NewRecorder()
	h.DeleteFunction(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("want 204, got %d", rr.Code)
	}
}

// ── Params key invariant tests ─────────────────────────────────────────────

func TestInvokeFunction_NrParamsFunctionNameKey(t *testing.T) {
	mock := &mockFunctionProvider{
		invokeResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"_payload": []byte(`{}`),
		}},
	}
	h := NewHandler(mock, testCfg())
	body := `{"payload":"{}","invocationType":"RequestResponse"}`
	req := withChiParam(
		httptest.NewRequest(http.MethodPost, "/functions/my-fn/invoke", bytes.NewBufferString(body)),
		"name", "my-fn",
	)
	req.Header.Set("Content-Type", "application/json")
	h.InvokeFunction(httptest.NewRecorder(), req)

	if v, ok := mock.lastNR.Params["_function_name"]; !ok || v == "" {
		t.Error("nr.Params must contain key _function_name (not FunctionName or functionName)")
	}
	if _, ok := mock.lastNR.Params["FunctionName"]; ok {
		t.Error("nr.Params must NOT contain FunctionName (AWS wire name); use _function_name")
	}
}

func TestInvokeFunction_NrParamsPayloadIsBytes(t *testing.T) {
	mock := &mockFunctionProvider{
		invokeResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"_payload": []byte(`{}`),
		}},
	}
	h := NewHandler(mock, testCfg())
	body := `{"payload":"{\"key\":\"val\"}"}`
	req := withChiParam(
		httptest.NewRequest(http.MethodPost, "/functions/my-fn/invoke", bytes.NewBufferString(body)),
		"name", "my-fn",
	)
	h.InvokeFunction(httptest.NewRecorder(), req)

	rawPayload, ok := mock.lastNR.Params["_payload"]
	if !ok {
		t.Fatal("nr.Params must contain key _payload")
	}
	if _, isBytesSlice := rawPayload.([]byte); !isBytesSlice {
		t.Errorf("nr.Params[\"_payload\"] must be []byte, got %T", rawPayload)
	}
}

func TestInvokeFunction_NrParamsLogTypeKey(t *testing.T) {
	mock := &mockFunctionProvider{
		invokeResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"_payload": []byte(`{}`),
		}},
	}
	h := NewHandler(mock, testCfg())
	body := `{"payload":"{}"}`
	req := withChiParam(
		httptest.NewRequest(http.MethodPost, "/functions/my-fn/invoke", bytes.NewBufferString(body)),
		"name", "my-fn",
	)
	h.InvokeFunction(httptest.NewRecorder(), req)

	if v, _ := mock.lastNR.Params["_log_type"].(string); v != "Tail" {
		t.Errorf("nr.Params[\"_log_type\"] = %q, want \"Tail\"", v)
	}
}

func TestInvokeFunction_RespDataFunctionError(t *testing.T) {
	mock := &mockFunctionProvider{
		invokeResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"_function_error": "Unhandled",
			"_payload":        []byte(`{"errorMessage":"oops"}`),
		}},
	}
	h := NewHandler(mock, testCfg())
	body := `{"payload":"{}"}`
	req := withChiParam(
		httptest.NewRequest(http.MethodPost, "/functions/my-fn/invoke", bytes.NewBufferString(body)),
		"name", "my-fn",
	)
	rr := httptest.NewRecorder()
	h.InvokeFunction(rr, req)

	var resp InvokeResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if resp.FunctionError != "Unhandled" {
		t.Errorf("FunctionError = %q, want Unhandled (reads _function_error, not FunctionError)", resp.FunctionError)
	}
}

func TestInvokeFunction_RespDataPayloadIsBytes(t *testing.T) {
	expectedPayload := `{"result":"ok"}`
	mock := &mockFunctionProvider{
		invokeResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"_payload": []byte(expectedPayload),
		}},
	}
	h := NewHandler(mock, testCfg())
	body := `{"payload":"{}"}`
	req := withChiParam(
		httptest.NewRequest(http.MethodPost, "/functions/my-fn/invoke", bytes.NewBufferString(body)),
		"name", "my-fn",
	)
	rr := httptest.NewRecorder()
	h.InvokeFunction(rr, req)

	var resp InvokeResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if resp.Payload != expectedPayload {
		t.Errorf("Payload = %q, want %q (reads resp.Data[\"_payload\"].([]byte))", resp.Payload, expectedPayload)
	}
}
