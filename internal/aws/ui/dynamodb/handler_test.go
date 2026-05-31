package dynamodbui

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

type mockDynamoDBProvider struct {
	listTablesResp   *model.ProviderResponse
	listTablesErr    error
	createTableResp  *model.ProviderResponse
	createTableErr   error
	describeTableResp *model.ProviderResponse
	describeTableErr  error
	deleteTableErr   error
	scanResp         *model.ProviderResponse
	scanErr          error
	queryResp        *model.ProviderResponse
	queryErr         error
	putItemErr       error
	getItemResp      *model.ProviderResponse
	getItemErr       error
	deleteItemErr    error

	lastNR          *model.NormalizedRequest
	listTablesNR    *model.NormalizedRequest
	createTableNR   *model.NormalizedRequest
}

func (m *mockDynamoDBProvider) ListTables(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.listTablesNR = nr
	m.lastNR = nr
	if m.listTablesResp != nil {
		return m.listTablesResp, m.listTablesErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"TableNames": []string{}}}, m.listTablesErr
}
func (m *mockDynamoDBProvider) CreateTable(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.createTableNR = nr
	m.lastNR = nr
	if m.createTableResp != nil {
		return m.createTableResp, m.createTableErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.createTableErr
}
func (m *mockDynamoDBProvider) DescribeTable(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.describeTableResp != nil {
		return m.describeTableResp, m.describeTableErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"Table": map[string]any{"TableStatus": "ACTIVE", "TableName": nr.Params["TableName"]},
	}}, m.describeTableErr
}
func (m *mockDynamoDBProvider) DeleteTable(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.deleteTableErr
}
func (m *mockDynamoDBProvider) Scan(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.scanResp != nil {
		return m.scanResp, m.scanErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"Items": []map[string]any{}, "Count": 0, "ScannedCount": 0,
	}}, m.scanErr
}
func (m *mockDynamoDBProvider) Query(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.queryResp != nil {
		return m.queryResp, m.queryErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Items": []map[string]any{}}}, m.queryErr
}
func (m *mockDynamoDBProvider) PutItem(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.putItemErr
}
func (m *mockDynamoDBProvider) GetItem(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.getItemResp != nil {
		return m.getItemResp, m.getItemErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Item": map[string]any{}}}, m.getItemErr
}
func (m *mockDynamoDBProvider) DeleteItem(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.deleteItemErr
}
func (m *mockDynamoDBProvider) DescribeTimeToLive(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}
func (m *mockDynamoDBProvider) ListTagsOfResource(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"Tags": []map[string]any{}}}, nil
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

// withChiParam sets a chi URL param on the request context.
func withChiParam(r *http.Request, key, value string) *http.Request {
	rctx := chi.RouteContext(r.Context())
	if rctx == nil {
		rctx = chi.NewRouteContext()
		r = r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx))
	}
	rctx.URLParams.Add(key, value)
	return r
}

// ── ListTables ────────────────────────────────────────────────────────────────

func TestDynamoListTables_Returns200WithItems(t *testing.T) {
	mock := &mockDynamoDBProvider{
		listTablesResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"TableNames": []string{"users", "orders"},
		}},
		describeTableResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Table": map[string]any{"TableStatus": "ACTIVE"},
		}},
	}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/tables", nil)
	rr := httptest.NewRecorder()
	h.ListTables(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	var resp ListTablesResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("want 2 items, got %d", len(resp.Items))
	}
}

func TestDynamoListTables_EmptyReturns200(t *testing.T) {
	mock := &mockDynamoDBProvider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/tables", nil)
	rr := httptest.NewRecorder()
	h.ListTables(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	var resp ListTablesResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if resp.Items == nil {
		t.Error("items should be non-nil empty slice")
	}
}

func TestDynamoListTables_ProviderErrorReturnsErrorShape(t *testing.T) {
	mock := &mockDynamoDBProvider{
		listTablesErr: model.NewProviderError("InternalError", "fail", 500),
	}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/tables", nil)
	rr := httptest.NewRecorder()
	h.ListTables(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("want 500, got %d", rr.Code)
	}
}

// ── CreateTable ───────────────────────────────────────────────────────────────

func TestDynamoCreateTable_ValidBodyReturns201(t *testing.T) {
	mock := &mockDynamoDBProvider{
		createTableResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}},
	}
	h := NewHandler(mock, testCfg())
	body := `{
		"tableName": "test-table",
		"keySchema": [{"attributeName": "pk", "keyType": "HASH"}],
		"attributeDefinitions": [{"attributeName": "pk", "attributeType": "S"}],
		"billingMode": "PAY_PER_REQUEST"
	}`
	req := httptest.NewRequest(http.MethodPost, "/tables", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CreateTable(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("want 201, got %d; body: %s", rr.Code, rr.Body.String())
	}
}

func TestDynamoCreateTable_MissingTableNameReturns400(t *testing.T) {
	mock := &mockDynamoDBProvider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodPost, "/tables", bytes.NewBufferString(`{"tableName":""}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CreateTable(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

func TestDynamoCreateTable_InvalidBodyReturns400(t *testing.T) {
	mock := &mockDynamoDBProvider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodPost, "/tables", bytes.NewBufferString(`not-json`))
	rr := httptest.NewRecorder()
	h.CreateTable(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

// ── DeleteTable ───────────────────────────────────────────────────────────────

func TestDynamoDeleteTable_Returns204(t *testing.T) {
	mock := &mockDynamoDBProvider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodDelete, "/tables/users", nil)
	req = withChiParam(req, "table", "users")
	rr := httptest.NewRecorder()
	h.DeleteTable(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("want 204, got %d", rr.Code)
	}
}

// ── ScanTable ─────────────────────────────────────────────────────────────────

func TestDynamoScanTable_Returns200WithItems(t *testing.T) {
	mock := &mockDynamoDBProvider{
		scanResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Items": []map[string]any{
				{"pk": map[string]any{"S": "row1"}},
			},
			"Count":        1,
			"ScannedCount": 1,
		}},
	}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/tables/users/scan", nil)
	req = withChiParam(req, "table", "users")
	rr := httptest.NewRecorder()
	h.ScanTable(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	var resp ScanResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if len(resp.Items) != 1 {
		t.Errorf("want 1 item, got %d", len(resp.Items))
	}
}

// ── PutItem ───────────────────────────────────────────────────────────────────

func TestDynamoPutItem_Returns200(t *testing.T) {
	mock := &mockDynamoDBProvider{}
	h := NewHandler(mock, testCfg())
	body := `{"item":{"pk":{"S":"row1"},"val":{"N":"42"}}}`
	req := httptest.NewRequest(http.MethodPost, "/tables/users/items", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req = withChiParam(req, "table", "users")
	rr := httptest.NewRecorder()
	h.PutItem(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
}

func TestDynamoPutItem_InvalidBodyReturns400(t *testing.T) {
	mock := &mockDynamoDBProvider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodPost, "/tables/users/items", bytes.NewBufferString(`not-json`))
	req = withChiParam(req, "table", "users")
	rr := httptest.NewRecorder()
	h.PutItem(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

// ── NR invariant tests ────────────────────────────────────────────────────────

func TestDynamoNR_PortIsWirePortNotUIPort(t *testing.T) {
	cfg := testCfg()
	mock := &mockDynamoDBProvider{}
	h := NewHandler(mock, cfg)
	req := httptest.NewRequest(http.MethodGet, "/tables", nil)
	h.ListTables(httptest.NewRecorder(), req)

	if mock.listTablesNR == nil {
		t.Fatal("NR was not captured")
	}
	if mock.listTablesNR.Port != cfg.Port {
		t.Errorf("nr.Port = %d, want wire port %d (not UI port %d)", mock.listTablesNR.Port, cfg.Port, cfg.UIPort)
	}
}

func TestDynamoNR_ClockIsNotNil(t *testing.T) {
	mock := &mockDynamoDBProvider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/tables", nil)
	h.ListTables(httptest.NewRecorder(), req)

	if mock.listTablesNR == nil {
		t.Fatal("NR was not captured")
	}
	if mock.listTablesNR.Clock == nil {
		t.Error("nr.Clock must not be nil")
	}
}

func TestDynamoCreateTable_NrParamsTableNameKey(t *testing.T) {
	mock := &mockDynamoDBProvider{
		createTableResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}},
	}
	h := NewHandler(mock, testCfg())
	body := `{"tableName":"my-table","keySchema":[{"attributeName":"pk","keyType":"HASH"}],"attributeDefinitions":[{"attributeName":"pk","attributeType":"S"}]}`
	req := httptest.NewRequest(http.MethodPost, "/tables", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	h.CreateTable(httptest.NewRecorder(), req)

	if mock.createTableNR == nil {
		t.Fatal("createTableNR was not captured")
	}
	if v, ok := mock.createTableNR.Params["TableName"]; !ok || v == "" {
		t.Error("nr.Params must contain key TableName (capital T)")
	}
	if _, ok := mock.createTableNR.Params["tableName"]; ok {
		t.Error("nr.Params must NOT contain lowercase tableName key")
	}
}

func TestDynamoListTables_NrParamsLimitKey(t *testing.T) {
	mock := &mockDynamoDBProvider{}
	h := NewHandler(mock, testCfg())
	req := httptest.NewRequest(http.MethodGet, "/tables?limit=25", nil)
	h.ListTables(httptest.NewRecorder(), req)

	if mock.listTablesNR == nil {
		t.Fatal("listTablesNR was not captured")
	}
	if _, ok := mock.listTablesNR.Params["Limit"]; !ok {
		t.Error("nr.Params must contain key Limit (capital L)")
	}
}
