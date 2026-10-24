package kmsui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
)

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

func (m *mockProvider) KeyRingCreate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) KeyRingList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) KeyRingGet(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyCreate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyGet(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyVersionCreate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyVersionList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyVersionGet(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyVersionDestroy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyVersionUpdate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyUpdatePrimaryVersion(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyEncrypt(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyDecrypt(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyVersionAsymmetricSign(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyVersionAsymmetricDecrypt(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyVersionMacSign(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyVersionMacVerify(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) CryptoKeyVersionGetPublicKey(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
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

func TestListKeyRings_Flattens(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"keyRings": []any{map[string]any{
				"name":       "projects/p/locations/global/keyRings/kr1",
				"createTime": "2024-01-01T00:00:00Z",
			}},
			"nextPageToken": "tok",
		},
	}}
	w := do(t, mock, http.MethodGet, "/locations/global/keyRings", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListKeyRingsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 || resp.KeyRings[0].KeyRingID != "kr1" || resp.KeyRings[0].Location != "global" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	if mock.lastNR.Params["location"] != "global" {
		t.Fatalf("location = %v", mock.lastNR.Params["location"])
	}
}

func TestCreateKeyRing_SetsLocationAndID(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"name": "projects/p/locations/global/keyRings/kr1",
	}}}
	w := do(t, mock, http.MethodPost, "/locations/global/keyRings", `{"keyRingId":"kr1"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["keyRingId"] != "kr1" || mock.lastNR.Params["location"] != "global" {
		t.Fatalf("params = %+v", mock.lastNR.Params)
	}
}

func TestCreateCryptoKey_BuildsBody(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"name":    "projects/p/locations/global/keyRings/kr1/cryptoKeys/ck1",
		"purpose": "ENCRYPT_DECRYPT",
	}}}
	w := do(t, mock, http.MethodPost, "/locations/global/keyRings/kr1/cryptoKeys",
		`{"cryptoKeyId":"ck1","purpose":"ENCRYPT_DECRYPT","algorithm":"GOOGLE_SYMMETRIC_ENCRYPTION","rotationPeriod":"86400s"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["name"]; got != "locations/global/keyRings/kr1" {
		t.Fatalf("name = %v", got)
	}
	body := mock.lastNR.Params["body"].(map[string]any)
	if body["rotationPeriod"] != "86400s" {
		t.Fatalf("body = %+v", body)
	}
	// A rotation_schedule requires next_rotation_time; the console derives it one
	// period out when the caller did not supply one.
	if got, _ := body["nextRotationTime"].(string); got != t0.Add(24*time.Hour).Format(time.RFC3339Nano) {
		t.Fatalf("nextRotationTime = %q, want %q", got, t0.Add(24*time.Hour).Format(time.RFC3339Nano))
	}
	if vt, ok := body["versionTemplate"].(map[string]any); !ok || vt["algorithm"] != "GOOGLE_SYMMETRIC_ENCRYPTION" {
		t.Fatalf("versionTemplate = %+v", body["versionTemplate"])
	}

	// An explicit nextRotationTime is passed through untouched.
	w = do(t, mock, http.MethodPost, "/locations/global/keyRings/kr1/cryptoKeys",
		`{"cryptoKeyId":"ck2","rotationPeriod":"86400s","nextRotationTime":"2026-08-01T00:00:00Z"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	body = mock.lastNR.Params["body"].(map[string]any)
	if got, _ := body["nextRotationTime"].(string); got != "2026-08-01T00:00:00Z" {
		t.Fatalf("explicit nextRotationTime = %q, want 2026-08-01T00:00:00Z", got)
	}
}

func TestSetVersionState_Disable(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"name": "projects/p/locations/global/keyRings/kr1/cryptoKeys/ck1/cryptoKeyVersions/2", "state": "DISABLED",
	}}}
	w := do(t, mock, http.MethodPost, "/locations/global/keyRings/kr1/cryptoKeys/ck1/versions/2/disable", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["name"]; got != "locations/global/keyRings/kr1/cryptoKeys/ck1/cryptoKeyVersions/2" {
		t.Fatalf("name = %v", got)
	}
	if got := mock.lastNR.Params["updateMask"]; got != "state" {
		t.Fatalf("updateMask = %v", got)
	}
	if got := mock.lastNR.Params["body"].(map[string]any)["state"]; got != "DISABLED" {
		t.Fatalf("state = %v", got)
	}
}

func TestSetPrimaryVersion(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"name": "projects/p/locations/global/keyRings/kr1/cryptoKeys/ck1",
	}}}
	w := do(t, mock, http.MethodPost, "/locations/global/keyRings/kr1/cryptoKeys/ck1/setPrimary", `{"cryptoKeyVersionId":"3"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["body"].(map[string]any)["cryptoKeyVersionId"]; got != "3" {
		t.Fatalf("cryptoKeyVersionId = %v", got)
	}
}

func TestGetKeyRingIam_UsesName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"etag": "e"}}}
	w := do(t, mock, http.MethodGet, "/locations/global/keyRings/kr1/iam", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if got := mock.lastNR.Params["name"]; got != "locations/global/keyRings/kr1" {
		t.Fatalf("name = %v", got)
	}
}

func TestGetCryptoKeyIam_UsesName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"etag": "e"}}}
	w := do(t, mock, http.MethodGet, "/locations/global/keyRings/kr1/cryptoKeys/ck1/iam", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if got := mock.lastNR.Params["name"]; got != "locations/global/keyRings/kr1/cryptoKeys/ck1" {
		t.Fatalf("name = %v", got)
	}
}

func TestEncrypt_BuildsNameAndBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{
		"name":       "projects/p/locations/global/keyRings/kr1/cryptoKeys/ck1/cryptoKeyVersions/1",
		"ciphertext": "AAAA",
	}}}
	w := do(t, mock, http.MethodPost, "/locations/global/keyRings/kr1/cryptoKeys/ck1/encrypt",
		`{"plaintext":"aGVsbG8=","additionalAuthenticatedData":"YWFk"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["name"]; got != "locations/global/keyRings/kr1/cryptoKeys/ck1" {
		t.Fatalf("name = %v", got)
	}
	body := mock.lastNR.Params["body"].(map[string]any)
	if body["plaintext"] != "aGVsbG8=" || body["additionalAuthenticatedData"] != "YWFk" {
		t.Fatalf("body = %+v", body)
	}
}

func TestEncrypt_MissingPlaintext(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodPost, "/locations/global/keyRings/kr1/cryptoKeys/ck1/encrypt", `{}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
}

func TestDecrypt_BuildsNameAndBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"plaintext": "aGVsbG8="}}}
	w := do(t, mock, http.MethodPost, "/locations/global/keyRings/kr1/cryptoKeys/ck1/decrypt",
		`{"ciphertext":"AAAA"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["name"]; got != "locations/global/keyRings/kr1/cryptoKeys/ck1" {
		t.Fatalf("name = %v", got)
	}
	if got := mock.lastNR.Params["body"].(map[string]any)["ciphertext"]; got != "AAAA" {
		t.Fatalf("ciphertext = %v", got)
	}
}

func TestAsymmetricSign_PassesDigest(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"signature": "c2ln"}}}
	w := do(t, mock, http.MethodPost,
		"/locations/global/keyRings/kr1/cryptoKeys/ck1/versions/1/asymmetricSign",
		`{"digest":{"sha256":"aGFzaA=="}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["name"]; got != "locations/global/keyRings/kr1/cryptoKeys/ck1/cryptoKeyVersions/1" {
		t.Fatalf("name = %v", got)
	}
	digest := mock.lastNR.Params["body"].(map[string]any)["digest"].(map[string]any)
	if digest["sha256"] != "aGFzaA==" {
		t.Fatalf("digest = %+v", digest)
	}
}

func TestAsymmetricSign_MissingDigest(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodPost,
		"/locations/global/keyRings/kr1/cryptoKeys/ck1/versions/1/asymmetricSign", `{"digest":{}}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}
}

func TestMacVerify_RequiresDataAndMac(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodPost,
		"/locations/global/keyRings/kr1/cryptoKeys/ck1/versions/1/macVerify", `{"data":"ZGF0YQ=="}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", w.Code, w.Body.String())
	}

	mock = &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"success": true}}}
	w = do(t, mock, http.MethodPost,
		"/locations/global/keyRings/kr1/cryptoKeys/ck1/versions/1/macVerify",
		`{"data":"ZGF0YQ==","mac":"bWFj"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body := mock.lastNR.Params["body"].(map[string]any)
	if body["data"] != "ZGF0YQ==" || body["mac"] != "bWFj" {
		t.Fatalf("body = %+v", body)
	}
}

func TestGetPublicKey_UsesVersionName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"pem": "-----BEGIN PUBLIC KEY-----"}}}
	w := do(t, mock, http.MethodGet, "/locations/global/keyRings/kr1/cryptoKeys/ck1/versions/1/publicKey", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["name"]; got != "locations/global/keyRings/kr1/cryptoKeys/ck1/cryptoKeyVersions/1" {
		t.Fatalf("name = %v", got)
	}
}

func TestAsymmetricDecrypt_BuildsNameAndBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"plaintext": "aGVsbG8="}}}
	w := do(t, mock, http.MethodPost,
		"/locations/global/keyRings/kr1/cryptoKeys/ck1/versions/1/asymmetricDecrypt", `{"ciphertext":"AAAA"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["name"]; got != "locations/global/keyRings/kr1/cryptoKeys/ck1/cryptoKeyVersions/1" {
		t.Fatalf("name = %v", got)
	}
	if got := mock.lastNR.Params["body"].(map[string]any)["ciphertext"]; got != "AAAA" {
		t.Fatalf("ciphertext = %v", got)
	}
}

func TestMacSign_BuildsNameAndBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"mac": "dGFn"}}}
	w := do(t, mock, http.MethodPost,
		"/locations/global/keyRings/kr1/cryptoKeys/ck1/versions/1/macSign", `{"data":"ZGF0YQ=="}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if got := mock.lastNR.Params["body"].(map[string]any)["data"]; got != "ZGF0YQ==" {
		t.Fatalf("data = %v", got)
	}
}

func TestCryptoOps_MissingRequiredFields(t *testing.T) {
	cases := []struct {
		name, method, path string
	}{
		{"decrypt missing ciphertext", http.MethodPost, "/locations/global/keyRings/kr1/cryptoKeys/ck1/decrypt"},
		{"asymmetricDecrypt missing ciphertext", http.MethodPost, "/locations/global/keyRings/kr1/cryptoKeys/ck1/versions/1/asymmetricDecrypt"},
		{"macSign missing data", http.MethodPost, "/locations/global/keyRings/kr1/cryptoKeys/ck1/versions/1/macSign"},
		{"macVerify missing mac", http.MethodPost, "/locations/global/keyRings/kr1/cryptoKeys/ck1/versions/1/macVerify"},
		{"asymmetricSign multiple digests", http.MethodPost, "/locations/global/keyRings/kr1/cryptoKeys/ck1/versions/1/asymmetricSign"},
	}
	bodies := []string{`{}`, `{}`, `{}`, `{"data":"ZGF0YQ=="}`, `{"digest":{"sha256":"YQ==","sha384":"YQ=="}}`}
	for i, tc := range cases {
		mock := &mockProvider{}
		w := do(t, mock, tc.method, tc.path, bodies[i])
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400: %s", tc.name, w.Code, w.Body.String())
		}
	}
}
