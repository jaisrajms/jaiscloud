package storageui

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/wire"
	"jaiscloud/internal/model"
)

// mockProvider records the last NormalizedRequest and returns a canned
// response. Every ProviderInterface method funnels through reply so tests can
// set the response once per case.
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

func (m *mockProvider) BucketsList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) BucketsInsert(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) BucketsGet(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) BucketsUpdate(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) BucketsLockRetentionPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) BucketsDelete(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ObjectsList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ObjectsInsert(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ObjectsGet(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ObjectsGetMedia(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ObjectsPatch(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ObjectsDelete(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ObjectsRestore(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) BucketsGetIamPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) BucketsSetIamPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) BucketACLList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) BucketACLInsert(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ObjectsGetIamPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ObjectsSetIamPolicy(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ObjectACLList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) ObjectACLInsert(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
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
	return doCT(t, mock, method, path, body, "")
}

func doCT(t *testing.T, mock *mockProvider, method, path, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	router := BuildRouter(mock, testCfg())
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func TestListBuckets_MapsItems(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{"items": []any{
			map[string]any{
				"name":         "media",
				"location":     "US",
				"storageClass": "STANDARD",
				"timeCreated":  "2026-01-01T00:00:00Z",
				"versioning":   map[string]any{"enabled": true},
			},
		}},
	}}

	w := do(t, mock, http.MethodGet, "/buckets", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListBucketsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 || len(resp.Items) != 1 {
		t.Fatalf("got %+v, want 1 item", resp)
	}
	if resp.Items[0].Name != "media" || !resp.Items[0].Versioning {
		t.Fatalf("unexpected bucket: %+v", resp.Items[0])
	}
	if mock.lastNR.Cloud != model.CloudGCP || mock.lastNR.AccountID != "test-project" {
		t.Fatalf("NR not GCP-scoped: %+v", mock.lastNR)
	}
}

func TestCreateBucket_RequiresName(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/buckets", `{}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestCreateBucket_PassesBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"name": "logs", "location": "EU"},
	}}
	w := do(t, mock, http.MethodPost, "/buckets", `{"name":"logs","location":"EU"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	if body["name"] != "logs" || body["location"] != "EU" {
		t.Fatalf("body = %+v", body)
	}
}

func TestDeleteBucket_PassesName(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/buckets/media", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if mock.lastNR.Params["bucket"] != "media" {
		t.Fatalf("bucket param = %v", mock.lastNR.Params["bucket"])
	}
}

// TestPathParams_DecodePercentEncoding covers UI17: the console client
// percent-encodes the bucket path segment (encodeURIComponent), and chi leaves
// characters Go treats as path-safe ('+', '&', ':') escaped unless the handler
// decodes via uihelper.Segment. Bucket routes must see the decoded name, not
// the raw escape.
func TestPathParams_DecodePercentEncoding(t *testing.T) {
	cases := []struct {
		name string
		verb string
		path string
		body string
		want string
	}{
		{"get +", http.MethodGet, "/buckets/a%2Bb", "", "a+b"},
		{"delete &", http.MethodDelete, "/buckets/a%26b", "", "a&b"},
		{"iam :", http.MethodGet, "/buckets/a%3Ab/iam", "", "a:b"},
		{"objects +", http.MethodGet, "/buckets/a%2Bb/objects", "", "a+b"},
		{"versioning :", http.MethodPut, "/buckets/a%3Ab/versioning", `{"enabled":true}`, "a:b"},
		{"acl &", http.MethodGet, "/buckets/a%26b/acl", "", "a&b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockProvider{}
			w := do(t, mock, tc.verb, tc.path, tc.body)
			if w.Code != http.StatusOK && w.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want 200/204: %s", w.Code, w.Body.String())
			}
			if got := mock.lastNR.Params["bucket"]; got != tc.want {
				t.Fatalf("bucket param = %v, want %q", got, tc.want)
			}
		})
	}
}

// TestPathParams_RejectsDecodedSlash: a decoded '/' would smuggle a second path
// segment into the bucket name, so the handler rejects it with 400 before
// calling the provider.
func TestPathParams_RejectsDecodedSlash(t *testing.T) {
	for _, path := range []string{"/buckets/a%2Fb", "/buckets/a%2Fb/objects", "/buckets/a%2Fb/iam", "/buckets/a%2Fb/objects/iam"} {
		mock := &mockProvider{}
		w := do(t, mock, http.MethodGet, path, "")
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400", path, w.Code)
		}
		if mock.lastNR != nil {
			t.Fatalf("%s: provider called with %+v", path, mock.lastNR)
		}
	}
}

func TestListObjects_ReturnsItemsAndPrefixes(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"items":    []any{map[string]any{"name": "a.txt", "size": "3", "contentType": "text/plain"}},
			"prefixes": []string{"dir/"},
		},
	}}
	w := do(t, mock, http.MethodGet, "/buckets/media/objects?prefix=&delimiter=/", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListObjectsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].Name != "a.txt" {
		t.Fatalf("items = %+v", resp.Items)
	}
	if len(resp.Prefixes) != 1 || resp.Prefixes[0] != "dir/" {
		t.Fatalf("prefixes = %+v", resp.Prefixes)
	}
}

func TestListObjects_VersionsSuppressesDelimiter(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodGet, "/buckets/media/objects?versions=true", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if mock.lastNR.Params["versions"] != "true" {
		t.Fatalf("versions param = %v", mock.lastNR.Params["versions"])
	}
	if _, ok := mock.lastNR.Params["delimiter"]; ok {
		t.Fatalf("delimiter should not be set for versions listing: %v", mock.lastNR.Params)
	}
}

func TestUploadObject_PassesMediaAndName(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       map[string]any{"name": "a.txt", "size": "5"},
	}}
	w := doCT(t, mock, http.MethodPut, "/buckets/media/objects?name=a.txt", "hello", "text/plain")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.lastNR.Params["object"] != "a.txt" {
		t.Fatalf("object param = %v", mock.lastNR.Params["object"])
	}
	media, _ := mock.lastNR.Params[wire.MediaKey].([]byte)
	if string(media) != "hello" {
		t.Fatalf("media = %q", media)
	}
	if mock.lastNR.Params[wire.ContentTypeKey] != "text/plain" {
		t.Fatalf("contentType = %v", mock.lastNR.Params[wire.ContentTypeKey])
	}
}

func TestUploadObject_RequiresName(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPut, "/buckets/media/objects", "x")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestDownloadObject_StreamsBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"_stream":           io.NopCloser(strings.NewReader("hello")),
			wire.ContentTypeKey: "text/plain",
		},
	}}
	w := do(t, mock, http.MethodGet, "/buckets/media/objects/download?name=a.txt", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if w.Body.String() != "hello" {
		t.Fatalf("body = %q", w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/plain" {
		t.Fatalf("content-type = %q", ct)
	}
}

func TestGetObject_PassesGeneration(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"name": "a.txt", "generation": "7"}}}
	w := do(t, mock, http.MethodGet, "/buckets/media/objects/metadata?name=a.txt&generation=7", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if mock.lastNR.Params["generation"] != "7" {
		t.Fatalf("generation = %v", mock.lastNR.Params["generation"])
	}
}

func TestPatchObject_PreservesFalseHold(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"name": "a.txt"}}}
	w := do(t, mock, http.MethodPatch, "/buckets/media/objects?name=a.txt", `{"temporaryHold":false,"metadata":{"k":"v"}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	if hold, ok := body["temporaryHold"].(bool); !ok || hold {
		t.Fatalf("temporaryHold = %#v", body["temporaryHold"])
	}
	md, _ := body["metadata"].(map[string]any)
	if md["k"] != "v" {
		t.Fatalf("metadata = %#v", body["metadata"])
	}
}

func TestDeleteObject_PassesGeneration(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/buckets/media/objects?name=a.txt&generation=3", "")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if mock.lastNR.Params["generation"] != "3" {
		t.Fatalf("generation = %v", mock.lastNR.Params["generation"])
	}
}

func TestRestoreObject_RequiresGeneration(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/buckets/media/objects/restore?name=a.txt", "")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestPutBucketVersioning_PassesBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"name": "media"}}}
	w := do(t, mock, http.MethodPut, "/buckets/media/versioning", `{"enabled":true}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	v, _ := body["versioning"].(map[string]any)
	if v["enabled"] != true {
		t.Fatalf("versioning = %#v", body["versioning"])
	}
}

func TestGetBucketLifecycle_ReturnsField(t *testing.T) {
	lifecycle := map[string]any{"rule": []any{map[string]any{"action": map[string]any{"type": "Delete"}}}}
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"lifecycle": lifecycle}}}
	w := do(t, mock, http.MethodGet, "/buckets/media/lifecycle", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := resp["lifecycle"]; !ok {
		t.Fatalf("lifecycle missing: %s", w.Body.String())
	}
}

func TestPutBucketRetention_PassesBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"name": "media"}}}
	w := do(t, mock, http.MethodPut, "/buckets/media/retention", `{"retentionPolicy":{"retentionPeriod":"600"}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	rp, _ := body["retentionPolicy"].(map[string]any)
	if rp["retentionPeriod"] != "600" {
		t.Fatalf("retentionPolicy = %#v", body["retentionPolicy"])
	}
}

func TestLockBucketRetention(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"name": "media"}}}
	w := do(t, mock, http.MethodPost, "/buckets/media/retention/lock", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if mock.lastNR.Params["bucket"] != "media" {
		t.Fatalf("bucket = %v", mock.lastNR.Params["bucket"])
	}
}

func TestSetBucketIam_PassesBody(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"etag": "x"}}}
	w := do(t, mock, http.MethodPut, "/buckets/media/iam", `{"bindings":[{"role":"roles/storage.objectViewer","members":["allUsers"]}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	if _, ok := body["bindings"].([]any); !ok {
		t.Fatalf("bindings not passed through: %#v", body["bindings"])
	}
}

func TestInsertBucketACL_PassesEntityRole(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{"entity": "allUsers"}}}
	w := do(t, mock, http.MethodPost, "/buckets/media/acl", `{"entity":"allUsers","role":"READER"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	body, _ := mock.lastNR.Params["body"].(map[string]any)
	if body["entity"] != "allUsers" || body["role"] != "READER" {
		t.Fatalf("body = %#v", body)
	}
}

func TestInsertObjectACL_RequiresEntityRole(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodPost, "/buckets/media/objects/acl?name=a.txt", `{}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestObjectIam_PassesObject(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}}
	w := do(t, mock, http.MethodGet, "/buckets/media/objects/iam?name=a.txt", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if mock.lastNR.Params["object"] != "a.txt" || mock.lastNR.Params["bucket"] != "media" {
		t.Fatalf("params = %#v", mock.lastNR.Params)
	}
}
