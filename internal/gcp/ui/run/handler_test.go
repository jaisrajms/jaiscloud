package runui

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
	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/model"
)

// mockProvider implements ProviderInterface with canned records, recording the
// arguments the handler resolves.
type mockProvider struct {
	services  []runstore.Service
	service   runstore.Service
	revisions []runstore.Revision
	revision  runstore.Revision
	err       error

	gotRegion   string
	gotService  string
	gotRevision string
	gotBody     map[string]any
	gotMask     string
	gotIamBody  map[string]any
	gotPerms    []string
	iam         map[string]any
	deleted     bool
	created     bool
	updated     bool
}

func (m *mockProvider) ListAllServices(_ context.Context, _ string) ([]runstore.Service, error) {
	return m.services, m.err
}

func (m *mockProvider) GetService(_ context.Context, _, region, service string) (runstore.Service, error) {
	m.gotRegion, m.gotService = region, service
	return m.service, m.err
}

func (m *mockProvider) ListRevisions(_ context.Context, _, region, service string) ([]runstore.Revision, error) {
	m.gotRegion, m.gotService = region, service
	return m.revisions, m.err
}

func (m *mockProvider) GetRevision(_ context.Context, _, region, service, revision string) (runstore.Revision, error) {
	m.gotRegion, m.gotService, m.gotRevision = region, service, revision
	return m.revision, m.err
}

func (m *mockProvider) DeleteService(_ context.Context, _, region, service string, _ bool, _ string) (runstore.Operation, error) {
	m.gotRegion, m.gotService = region, service
	m.deleted = true
	return runstore.Operation{}, m.err
}

func (m *mockProvider) CreateService(_ context.Context, _, region, id string, body map[string]any, _ bool) (runstore.Service, error) {
	m.gotRegion, m.gotService, m.gotBody = region, id, body
	m.created = true
	return m.service, m.err
}

func (m *mockProvider) UpdateService(_ context.Context, _, region, id string, body map[string]any, mask string, _, _ bool) (runstore.Service, error) {
	m.gotRegion, m.gotService, m.gotBody, m.gotMask = region, id, body, mask
	m.updated = true
	return m.service, m.err
}

func (m *mockProvider) ServiceGetIamPolicy(_ context.Context, _, region, service string) (map[string]any, error) {
	m.gotRegion, m.gotService = region, service
	if m.err != nil {
		return nil, m.err
	}
	return m.iam, nil
}

func (m *mockProvider) ServiceSetIamPolicy(_ context.Context, _, region, service string, body map[string]any) (map[string]any, error) {
	m.gotRegion, m.gotService, m.gotIamBody = region, service, body
	if m.err != nil {
		return nil, m.err
	}
	return m.iam, nil
}

func (m *mockProvider) ServiceTestIamPermissions(_ context.Context, _, region, service string, perms []string) ([]string, error) {
	m.gotRegion, m.gotService, m.gotPerms = region, service, perms
	return perms, m.err
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

func do(t *testing.T, mock *mockProvider, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	router := BuildRouter(mock, testCfg())
	r := httptest.NewRequest(method, path, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func doBody(t *testing.T, mock *mockProvider, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	router := BuildRouter(mock, testCfg())
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func svc(id, region string) runstore.Service {
	return runstore.Service{
		ProjectID:  "test-project",
		Location:   region,
		ID:         id,
		UID:        "uid-" + id,
		Uri:        "https://" + id + ".example.run.app",
		CreateTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		UpdateTime: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		Data:       map[string]any{"template": map[string]any{"containers": []any{}}},
	}
}

func TestListServices_FlattensAcrossRegions(t *testing.T) {
	mock := &mockProvider{services: []runstore.Service{
		svc("alpha", "europe-west1"),
		svc("beta", "us-central1"),
	}}

	w := do(t, mock, http.MethodGet, "/services")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListServicesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 2 || len(resp.Services) != 2 {
		t.Fatalf("got %+v, want 2 services", resp)
	}
	if resp.Services[0].ID != "alpha" || resp.Services[0].Region != "europe-west1" {
		t.Fatalf("unexpected first row: %+v", resp.Services[0])
	}
	if !resp.Services[0].Ready {
		t.Fatalf("service should render ready: %+v", resp.Services[0])
	}
	if want := "projects/test-project/locations/europe-west1/services/alpha"; resp.Services[0].Name != want {
		t.Fatalf("name = %q, want %q", resp.Services[0].Name, want)
	}
}

func TestGetService_PassesRegionAndReturnsWireShape(t *testing.T) {
	mock := &mockProvider{service: svc("alpha", "us-central1")}
	w := do(t, mock, http.MethodGet, "/services/us-central1/alpha")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if mock.gotRegion != "us-central1" || mock.gotService != "alpha" {
		t.Fatalf("resolved region/service = %q/%q", mock.gotRegion, mock.gotService)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if want := "projects/test-project/locations/us-central1/services/alpha"; body["name"] != want {
		t.Fatalf("name = %v, want %q", body["name"], want)
	}
	if _, ok := body["trafficStatuses"]; !ok {
		t.Fatalf("response missing derived trafficStatuses: %v", body)
	}
}

func TestDeleteService_NoContent(t *testing.T) {
	mock := &mockProvider{}
	w := do(t, mock, http.MethodDelete, "/services/us-central1/alpha")
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if !mock.deleted || mock.gotRegion != "us-central1" || mock.gotService != "alpha" {
		t.Fatalf("delete not dispatched: %+v", mock)
	}
}

func TestListRevisions_PassesTarget(t *testing.T) {
	mock := &mockProvider{revisions: []runstore.Revision{{
		ProjectID: "test-project",
		Location:  "us-central1",
		Service:   "alpha",
		ID:        "alpha-00001",
		Data:      map[string]any{"containers": []any{map[string]any{"image": "gcr.io/p/img"}}},
	}}}
	w := do(t, mock, http.MethodGet, "/services/us-central1/alpha/revisions")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListRevisionsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 1 {
		t.Fatalf("total = %d, want 1", resp.Total)
	}
	if want := "projects/test-project/locations/us-central1/services/alpha/revisions/alpha-00001"; resp.Revisions[0]["name"] != want {
		t.Fatalf("revision name = %v, want %q", resp.Revisions[0]["name"], want)
	}
}

func TestGetRevision_RejectsEncodedSlash(t *testing.T) {
	w := do(t, &mockProvider{}, http.MethodGet, "/services/us-central1/alpha/revisions/a%2Fb")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestListServices_ProviderErrorMapped(t *testing.T) {
	mock := &mockProvider{err: model.NewProviderError("NotFound", "nope", http.StatusNotFound)}
	w := do(t, mock, http.MethodGet, "/services")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if !strings.Contains(w.Body.String(), "nope") {
		t.Fatalf("body = %s", w.Body.String())
	}
}

func TestCreateService_PassesDeployBody(t *testing.T) {
	mock := &mockProvider{service: svc("web", "us-central1")}
	body := `{"region":"us-central1","id":"web","body":{"template":{"containers":[{"image":"nginx:latest"}]}}}`
	w := doBody(t, mock, http.MethodPost, "/services", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	if !mock.created || mock.gotRegion != "us-central1" || mock.gotService != "web" {
		t.Fatalf("create not dispatched: %+v", mock)
	}
	containers, _ := mock.gotBody["template"].(map[string]any)["containers"].([]any)
	if len(containers) != 1 {
		t.Fatalf("body = %+v", mock.gotBody)
	}
}

func TestCreateService_RequiresBody(t *testing.T) {
	for _, body := range []string{`{}`, `{"region":"us-central1","id":"web"}`} {
		w := doBody(t, &mockProvider{}, http.MethodPost, "/services", body)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("body %s status = %d, want 400", body, w.Code)
		}
	}
}

func TestUpdateService_JoinsMask(t *testing.T) {
	mock := &mockProvider{service: svc("web", "us-central1")}
	body := `{"body":{"template":{"containers":[{"image":"nginx:2"}]}},` +
		`"updateMask":["template.containers","traffic"]}`
	w := doBody(t, mock, http.MethodPatch, "/services/us-central1/web", body)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if !mock.updated || mock.gotMask != "template.containers,traffic" {
		t.Fatalf("update not dispatched with mask: %+v", mock)
	}
}

func TestServiceIam_Triple(t *testing.T) {
	mock := &mockProvider{iam: map[string]any{"etag": "ACAB", "bindings": []any{}}}
	w := do(t, mock, http.MethodGet, "/services/us-central1/web/iam")
	if w.Code != http.StatusOK {
		t.Fatalf("get status = %d, want 200: %s", w.Code, w.Body.String())
	}

	body := `{"policy":{"bindings":[{"role":"roles/run.viewer","members":["user:a@example.com"]}]}}`
	w = doBody(t, mock, http.MethodPut, "/services/us-central1/web/iam", body)
	if w.Code != http.StatusOK {
		t.Fatalf("put status = %d, want 200: %s", w.Code, w.Body.String())
	}
	pol, _ := mock.gotIamBody["policy"].(map[string]any)
	if len(pol["bindings"].([]any)) != 1 {
		t.Fatalf("policy body = %+v", mock.gotIamBody)
	}

	w = doBody(t, mock, http.MethodPost, "/services/us-central1/web/iam/testIamPermissions",
		`{"permissions":["run.services.get","run.services.update"]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("test status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp TestIamPermissionsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Permissions) != 2 || resp.Permissions[0] != "run.services.get" {
		t.Fatalf("permissions = %+v", resp.Permissions)
	}
}

func TestDeployAndIam_ErrorPaths(t *testing.T) {
	boom := model.NewProviderError("InvalidArgument", "bad body", http.StatusBadRequest)
	cases := []struct {
		name, method, path, body string
	}{
		{"create invalid json", http.MethodPost, "/services", `{`},
		{"create provider error", http.MethodPost, "/services", `{"region":"us-central1","id":"web","body":{}}`},
		{"update invalid json", http.MethodPatch, "/services/us-central1/web", `{`},
		{"update missing body", http.MethodPatch, "/services/us-central1/web", `{}`},
		{"update provider error", http.MethodPatch, "/services/us-central1/web", `{"body":{}}`},
		{"get iam provider error", http.MethodGet, "/services/us-central1/web/iam", ""},
		{"set iam invalid json", http.MethodPut, "/services/us-central1/web/iam", `{`},
		{"set iam provider error", http.MethodPut, "/services/us-central1/web/iam", `{"policy":{}}`},
		{"test iam invalid json", http.MethodPost, "/services/us-central1/web/iam/testIamPermissions", `{`},
		{"test iam provider error", http.MethodPost, "/services/us-central1/web/iam/testIamPermissions", `{"permissions":["a"]}`},
	}
	for _, tc := range cases {
		mock := &mockProvider{err: boom}
		w := doBody(t, mock, tc.method, tc.path, tc.body)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400: %s", tc.name, w.Code, w.Body.String())
		}
	}
}
