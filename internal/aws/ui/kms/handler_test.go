package kmsui

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
)



type mockKMSProvider struct {
	listKeysResp           *model.ProviderResponse
	listKeysErr            error
	createKeyResp          *model.ProviderResponse
	createKeyErr           error
	describeKeyResp        *model.ProviderResponse
	describeKeyErr         error
	enableKeyErr           error
	disableKeyErr          error
	scheduleKeyResp        *model.ProviderResponse
	scheduleKeyErr         error
	cancelKeyResp          *model.ProviderResponse
	cancelKeyErr           error
	listAliasesResp        *model.ProviderResponse
	listAliasesErr         error
	createAliasErr         error
	deleteAliasErr         error
	listResourceTagsResp   *model.ProviderResponse
	listResourceTagsErr    error

	lastNR       *model.NormalizedRequest
	createKeyNR  *model.NormalizedRequest
	enableKeyNR  *model.NormalizedRequest
}

func (m *mockKMSProvider) ListKeys(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return m.listKeysResp, m.listKeysErr
}
func (m *mockKMSProvider) CreateKey(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.createKeyNR = nr
	m.lastNR = nr
	return m.createKeyResp, m.createKeyErr
}
func (m *mockKMSProvider) DescribeKey(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return m.describeKeyResp, m.describeKeyErr
}
func (m *mockKMSProvider) EnableKey(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.enableKeyNR = nr
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.enableKeyErr
}
func (m *mockKMSProvider) DisableKey(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.disableKeyErr
}
func (m *mockKMSProvider) ScheduleKeyDeletion(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.scheduleKeyResp != nil {
		return m.scheduleKeyResp, m.scheduleKeyErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.scheduleKeyErr
}
func (m *mockKMSProvider) CancelKeyDeletion(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.cancelKeyResp != nil {
		return m.cancelKeyResp, m.cancelKeyErr
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.cancelKeyErr
}
func (m *mockKMSProvider) ListAliases(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return m.listAliasesResp, m.listAliasesErr
}
func (m *mockKMSProvider) CreateAlias(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.createAliasErr
}
func (m *mockKMSProvider) DeleteAlias(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.deleteAliasErr
}
func (m *mockKMSProvider) ListResourceTags(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	return m.listResourceTagsResp, m.listResourceTagsErr
}

func testKMSCfg() *config.Config {
	return &config.Config{Port: 4566, UIPort: 4567, Region: "us-east-1", AccountID: "000000000000", Clock: clock.RealClock{}}
}

func testKMSHandler(p *mockKMSProvider) *Handler {
	return NewHandler(p, testKMSCfg())
}

func TestKMSListKeys_Returns200(t *testing.T) {
	p := &mockKMSProvider{
		listKeysResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Keys": []map[string]any{},
		}},
		describeKeyResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"KeyMetadata": map[string]any{},
		}},
	}
	h := testKMSHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/keys", nil)
	w := httptest.NewRecorder()
	h.ListKeys(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListKeysResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
}

func TestKMSListKeys_PaginationToken(t *testing.T) {
	p := &mockKMSProvider{
		listKeysResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Keys": []map[string]any{},
		}},
		describeKeyResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"KeyMetadata": map[string]any{},
		}},
	}
	h := testKMSHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/keys?nextToken=tok1", nil)
	w := httptest.NewRecorder()
	h.ListKeys(w, req)

	if p.lastNR.Params["Marker"] != "tok1" {
		t.Fatalf("expected Marker=tok1, got %v", p.lastNR.Params["Marker"])
	}
}

func TestKMSCreateKey_Returns201(t *testing.T) {
	p := &mockKMSProvider{
		createKeyResp: &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{
			"KeyMetadata": map[string]any{"KeyId": "key-abc"},
		}},
	}
	h := testKMSHandler(p)
	body, _ := json.Marshal(map[string]any{"description": "test key", "keyUsage": "ENCRYPT_DECRYPT"})
	req := httptest.NewRequest(http.MethodPost, "/keys", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.CreateKey(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if p.createKeyNR.Params["Description"] != "test key" {
		t.Fatalf("expected Description=test key, got %v", p.createKeyNR.Params["Description"])
	}
	if p.createKeyNR.Params["KeyUsage"] != "ENCRYPT_DECRYPT" {
		t.Fatalf("expected KeyUsage=ENCRYPT_DECRYPT, got %v", p.createKeyNR.Params["KeyUsage"])
	}
}

func TestKMSCreateKey_NrPortIsWirePort(t *testing.T) {
	p := &mockKMSProvider{
		createKeyResp: &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{}},
	}
	h := testKMSHandler(p)
	body, _ := json.Marshal(map[string]string{"description": "d"})
	req := httptest.NewRequest(http.MethodPost, "/keys", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.CreateKey(w, req)

	if p.createKeyNR.Port != testKMSCfg().Port {
		t.Fatalf("expected Port=%d, got %d", testKMSCfg().Port, p.createKeyNR.Port)
	}
}

func TestKMSCreateKey_ClockNotNil(t *testing.T) {
	p := &mockKMSProvider{
		createKeyResp: &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{}},
	}
	h := testKMSHandler(p)
	body, _ := json.Marshal(map[string]string{})
	req := httptest.NewRequest(http.MethodPost, "/keys", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.CreateKey(w, req)

	if p.createKeyNR.Clock == nil {
		t.Fatal("expected nr.Clock to be non-nil")
	}
	now := p.createKeyNR.Clock.Now()
	if now.IsZero() {
		t.Fatal("expected Clock.Now() to be non-zero")
	}
}

func TestKMSEnableKey_Returns204(t *testing.T) {
	p := &mockKMSProvider{}
	h := testKMSHandler(p)
	req := httptest.NewRequest(http.MethodPost, "/keys/enable?keyId=key-123", nil)
	w := httptest.NewRecorder()
	h.EnableKey(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if p.enableKeyNR.Params["KeyId"] != "key-123" {
		t.Fatalf("expected KeyId=key-123, got %v", p.enableKeyNR.Params["KeyId"])
	}
}

func TestKMSEnableKey_MissingKeyId_Returns400(t *testing.T) {
	p := &mockKMSProvider{}
	h := testKMSHandler(p)
	req := httptest.NewRequest(http.MethodPost, "/keys/enable", nil)
	w := httptest.NewRecorder()
	h.EnableKey(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestKMSDisableKey_Returns204(t *testing.T) {
	p := &mockKMSProvider{}
	h := testKMSHandler(p)
	req := httptest.NewRequest(http.MethodPost, "/keys/disable?keyId=key-456", nil)
	w := httptest.NewRecorder()
	h.DisableKey(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
}

func TestKMSScheduleKeyDeletion_DefaultsPendingWindow(t *testing.T) {
	p := &mockKMSProvider{}
	h := testKMSHandler(p)
	req := httptest.NewRequest(http.MethodPost, "/keys/schedule-deletion?keyId=key-789", bytes.NewReader([]byte(`{}`)))
	w := httptest.NewRecorder()
	h.ScheduleKeyDeletion(w, req)

	if p.lastNR.Params["PendingWindowInDays"] != 30 {
		t.Fatalf("expected PendingWindowInDays=30, got %v", p.lastNR.Params["PendingWindowInDays"])
	}
}

func TestKMSScheduleKeyDeletion_CustomWindow(t *testing.T) {
	p := &mockKMSProvider{}
	h := testKMSHandler(p)
	body, _ := json.Marshal(map[string]int{"pendingWindowInDays": 14})
	req := httptest.NewRequest(http.MethodPost, "/keys/schedule-deletion?keyId=key-789", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ScheduleKeyDeletion(w, req)

	if p.lastNR.Params["PendingWindowInDays"] != 14 {
		t.Fatalf("expected PendingWindowInDays=14, got %v", p.lastNR.Params["PendingWindowInDays"])
	}
}

func TestKMSListAliases_Returns200WithItems(t *testing.T) {
	p := &mockKMSProvider{
		listAliasesResp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
			"Aliases": []map[string]any{
				{"AliasName": "alias/test", "AliasArn": "arn:aws:kms:us-east-1:000000000000:alias/test", "TargetKeyId": "key-abc"},
			},
		}},
	}
	h := testKMSHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/aliases", nil)
	w := httptest.NewRecorder()
	h.ListAliases(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp ListAliasesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].AliasName != "alias/test" {
		t.Fatalf("unexpected aliases: %+v", resp.Items)
	}
}

func TestKMSCreateAlias_Returns201(t *testing.T) {
	p := &mockKMSProvider{}
	h := testKMSHandler(p)
	body, _ := json.Marshal(map[string]string{"aliasName": "alias/mykey", "targetKeyId": "key-abc"})
	req := httptest.NewRequest(http.MethodPost, "/aliases", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.CreateAlias(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	if p.lastNR.Params["AliasName"] != "alias/mykey" {
		t.Fatalf("expected AliasName=alias/mykey, got %v", p.lastNR.Params["AliasName"])
	}
	if p.lastNR.Params["TargetKeyId"] != "key-abc" {
		t.Fatalf("expected TargetKeyId=key-abc, got %v", p.lastNR.Params["TargetKeyId"])
	}
}

func TestKMSCreateAlias_MissingFields_Returns400(t *testing.T) {
	p := &mockKMSProvider{}
	h := testKMSHandler(p)
	body, _ := json.Marshal(map[string]string{"aliasName": "alias/mykey"})
	req := httptest.NewRequest(http.MethodPost, "/aliases", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.CreateAlias(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestKMSDeleteAlias_Returns204(t *testing.T) {
	p := &mockKMSProvider{}
	h := testKMSHandler(p)
	req := httptest.NewRequest(http.MethodDelete, "/aliases?aliasName=alias/mykey", nil)
	w := httptest.NewRecorder()
	h.DeleteAlias(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
	if p.lastNR.Params["AliasName"] != "alias/mykey" {
		t.Fatalf("expected AliasName=alias/mykey, got %v", p.lastNR.Params["AliasName"])
	}
}

func TestKMSDeleteAlias_MissingAliasName_Returns400(t *testing.T) {
	p := &mockKMSProvider{}
	h := testKMSHandler(p)
	req := httptest.NewRequest(http.MethodDelete, "/aliases", nil)
	w := httptest.NewRecorder()
	h.DeleteAlias(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestKMSNR_ServiceIsKMS(t *testing.T) {
	p := &mockKMSProvider{
		createKeyResp: &model.ProviderResponse{HTTPStatus: 201, Data: map[string]any{}},
	}
	h := testKMSHandler(p)
	body, _ := json.Marshal(map[string]string{})
	req := httptest.NewRequest(http.MethodPost, "/keys", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.CreateKey(w, req)

	if p.createKeyNR.Service != "kms" {
		t.Fatalf("expected service=kms, got %s", p.createKeyNR.Service)
	}
}

func TestKMSNR_ClockIsRealClock(t *testing.T) {
	p := &mockKMSProvider{}
	h := testKMSHandler(p)
	req := httptest.NewRequest(http.MethodGet, "/keys/enable?keyId=k", nil)
	w := httptest.NewRecorder()
	h.EnableKey(w, req)

	if p.enableKeyNR.Clock == nil {
		t.Fatal("expected nr.Clock to be non-nil")
	}
}
