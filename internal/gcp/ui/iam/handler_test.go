package iamui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
)

// mockProvider records the last NormalizedRequest and returns a canned response.
type mockProvider struct {
	resp   *model.ProviderResponse
	err    error
	lastNR *model.NormalizedRequest
}

func (m *mockProvider) reply(nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m.lastNR = nr
	if m.resp != nil {
		return m.resp, m.err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, m.err
}

func (m *mockProvider) Create(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) List(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) Get(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) Update(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) Delete(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) Disable(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) Enable(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) GetIamPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) SetIamPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) TestIamPermissions(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ServiceAccountKeyCreate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ServiceAccountKeyList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ServiceAccountKeyGet(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ServiceAccountKeyDelete(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ServiceAccountKeyDisable(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ServiceAccountKeyEnable(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ServiceAccountSignBlob(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ServiceAccountSignJwt(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}

func testCfg() *config.Config {
	return &config.Config{
		Port:      8080,
		UIPort:    4567,
		Region:    "global",
		AccountID: "test-project",
		ProjectID: "test-project",
		Clock:     clock.RealClock{},
	}
}

func do(t *testing.T, mock *mockProvider, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	router := BuildRouter(mock, testCfg())
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func TestListServiceAccounts_Flattens(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"accounts": []any{
				map[string]any{"name": "projects/p/serviceAccounts/a@p.iam.gserviceaccount.com", "email": "a@p.iam.gserviceaccount.com", "displayName": "A", "disabled": true, "etag": "x"},
				map[string]any{"name": "projects/p/serviceAccounts/b@p.iam.gserviceaccount.com", "email": "b@p.iam.gserviceaccount.com"},
			},
			"nextPageToken": "tok",
		},
	}}

	w := do(t, mock, http.MethodGet, "/serviceAccounts", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListServiceAccountsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 2 || len(resp.Accounts) != 2 || resp.NextPageToken != "tok" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if !resp.Accounts[0].Disabled {
		t.Fatalf("expected first account disabled: %+v", resp.Accounts[0])
	}
}

func TestCreateServiceAccount_RequiresAccountID(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodPost, "/serviceAccounts", `{}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestCreateServiceAccount_SetsAccountIDAndBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"email": "svc@p.iam.gserviceaccount.com"}}}
	w := do(t, mock, http.MethodPost, "/serviceAccounts", `{"accountId":"svc","displayName":"Svc","description":"d"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["accountId"] != "svc" {
		t.Fatalf("accountId = %v", mock.lastNR.Params["accountId"])
	}
	sa := mock.lastNR.Params["body"].(map[string]any)["serviceAccount"].(map[string]any)
	if sa["displayName"] != "Svc" || sa["description"] != "d" {
		t.Fatalf("serviceAccount body = %+v", sa)
	}
}

func TestUpdateServiceAccount_SetsNameAndMask(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"email": "a@p.iam.gserviceaccount.com"}}}
	w := do(t, mock, http.MethodPatch, "/serviceAccounts/a@p.iam.gserviceaccount.com", `{"displayName":"New","etag":"e"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["name"]; got != "serviceAccounts/a@p.iam.gserviceaccount.com" {
		t.Fatalf("name = %v", got)
	}
	if got := mock.lastNR.Params["updateMask"]; got != "displayName,description" {
		t.Fatalf("updateMask = %v", got)
	}
	body := mock.lastNR.Params["body"].(map[string]any)
	if body["etag"] != "e" {
		t.Fatalf("etag = %v", body["etag"])
	}
}

func TestDeleteServiceAccount_204(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/serviceAccounts/a@p.iam.gserviceaccount.com", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if got := mock.lastNR.Params["name"]; got != "serviceAccounts/a@p.iam.gserviceaccount.com" {
		t.Fatalf("name = %v", got)
	}
}

func TestSetIamPolicy_WrapsPolicy(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"etag": "e"}}}
	w := do(t, mock, http.MethodPut, "/serviceAccounts/a@p.iam.gserviceaccount.com/iam",
		`{"policy":{"etag":"old","version":1,"bindings":[{"role":"roles/iam.serviceAccountUser","members":["user:x@y.z"]}]}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body := mock.lastNR.Params["body"].(map[string]any)
	policy := body["policy"].(map[string]any)
	if policy["etag"] != "old" {
		t.Fatalf("policy etag = %v", policy["etag"])
	}
	bindings := policy["bindings"].([]any)
	if len(bindings) != 1 {
		t.Fatalf("bindings = %+v", bindings)
	}
}

func TestCreateKey_ReturnsPrivateKeyData(t *testing.T) {
	// The provider returns no top-level keyId (real GCP exposes the id only in
	// name); the UI derives it from the name's trailing segment.
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"name": "projects/p/serviceAccounts/a@p.iam.gserviceaccount.com/keys/k1", "privateKeyData": "cHJpdg==",
	}}}
	w := do(t, mock, http.MethodPost, "/serviceAccounts/a@p.iam.gserviceaccount.com/keys", "")
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	var key ServiceAccountKey
	if err := json.Unmarshal(w.Body.Bytes(), &key); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if key.KeyID != "k1" || key.PrivateKeyData != "cHJpdg==" {
		t.Fatalf("key = %+v", key)
	}
	if got := mock.lastNR.Params["name"]; got != "serviceAccounts/a@p.iam.gserviceaccount.com/keys" {
		t.Fatalf("name = %v", got)
	}
}

func TestListKeys_UsesCollectionName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"keys": []any{map[string]any{"name": "projects/p/serviceAccounts/a@p.iam.gserviceaccount.com/keys/k1"}},
	}}}
	w := do(t, mock, http.MethodGet, "/serviceAccounts/a@p.iam.gserviceaccount.com/keys", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["name"]; got != "serviceAccounts/a@p.iam.gserviceaccount.com/keys" {
		t.Fatalf("name = %v", got)
	}
}

func TestDisableKey_UsesKeyName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"name": "projects/p/serviceAccounts/a@p.iam.gserviceaccount.com/keys/k1",
	}}}
	w := do(t, mock, http.MethodPost, "/serviceAccounts/a@p.iam.gserviceaccount.com/keys/k1/disable", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if got := mock.lastNR.Params["name"]; got != "serviceAccounts/a@p.iam.gserviceaccount.com/keys/k1" {
		t.Fatalf("name = %v", got)
	}
}

func TestSignBlob_SetsNameAndBytesToSign(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"keyId": "k1", "signature": "c2ln",
	}}}
	w := do(t, mock, http.MethodPost, "/serviceAccounts/a@p.iam.gserviceaccount.com/signBlob",
		`{"bytesToSign":"aGVsbG8="}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Action != "IAM.ServiceAccountSignBlob" {
		t.Fatalf("action = %v", mock.lastNR.Action)
	}
	if got := mock.lastNR.Params["name"]; got != "serviceAccounts/a@p.iam.gserviceaccount.com" {
		t.Fatalf("name = %v", got)
	}
	body := mock.lastNR.Params["body"].(map[string]any)
	if body["bytesToSign"] != "aGVsbG8=" {
		t.Fatalf("bytesToSign = %v", body["bytesToSign"])
	}
	var resp SignBlobResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.KeyID != "k1" || resp.Signature != "c2ln" {
		t.Fatalf("response = %+v", resp)
	}
}

func TestSignBlob_RequiresBytesToSign(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodPost, "/serviceAccounts/a@p.iam.gserviceaccount.com/signBlob", `{}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestSignJwt_SetsNameAndPayload(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"keyId": "k1", "signedJwt": "a.b.c",
	}}}
	w := do(t, mock, http.MethodPost, "/serviceAccounts/a@p.iam.gserviceaccount.com/signJwt",
		`{"payload":"{\"iss\":\"a@p.iam.gserviceaccount.com\"}"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Action != "IAM.ServiceAccountSignJwt" {
		t.Fatalf("action = %v", mock.lastNR.Action)
	}
	if got := mock.lastNR.Params["name"]; got != "serviceAccounts/a@p.iam.gserviceaccount.com" {
		t.Fatalf("name = %v", got)
	}
	body := mock.lastNR.Params["body"].(map[string]any)
	if body["payload"] != `{"iss":"a@p.iam.gserviceaccount.com"}` {
		t.Fatalf("payload = %v", body["payload"])
	}
	var resp SignJwtResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.KeyID != "k1" || resp.SignedJwt != "a.b.c" {
		t.Fatalf("response = %+v", resp)
	}
}

func TestSignJwt_RequiresPayload(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodPost, "/serviceAccounts/a@p.iam.gserviceaccount.com/signJwt", `{}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestSignBlob_MalformedBody(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodPost, "/serviceAccounts/a@p.iam.gserviceaccount.com/signBlob", `{`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestSignBlob_ProviderErrorPassthrough(t *testing.T) {
	mock := &mockProvider{err: model.NewProviderError("InvalidRequest", "payload must be base64", 400)}
	w := do(t, mock, http.MethodPost, "/serviceAccounts/a@p.iam.gserviceaccount.com/signBlob",
		`{"bytesToSign":"!!!"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["code"] != "InvalidRequest" {
		t.Fatalf("code = %v", body["code"])
	}
}
