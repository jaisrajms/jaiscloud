package computeui

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

func (m *mockProvider) InstancesAggregatedList(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) InstancesGet(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) InstancesStart(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) InstancesStop(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return m.reply(nr)
}
func (m *mockProvider) InstancesDelete(_ context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
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

func instanceBody(name, zone string) map[string]any {
	return map[string]any{
		"name":              name,
		"zone":              "https://www.googleapis.com/compute/v1/projects/test-project/zones/" + zone,
		"status":            "RUNNING",
		"machineType":       "https://www.googleapis.com/compute/v1/projects/test-project/zones/" + zone + "/machineTypes/e2-micro",
		"cpuPlatform":       "Intel Broadwell",
		"creationTimestamp": "2026-01-01T00:00:00Z",
		"networkInterfaces": []any{
			map[string]any{
				"networkIP": "10.0.0.2",
				"accessConfigs": []any{
					map[string]any{"natIP": "34.1.2.3"},
				},
			},
		},
	}
}

func TestListInstances_FlattensAggregated(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"items": map[string]any{
				"zones/us-central1-a": map[string]any{"instances": []any{
					instanceBody("vm-b", "us-central1-a"),
					instanceBody("vm-a", "us-central1-a"),
				}},
				"zones/europe-west1-b": map[string]any{"instances": []any{
					instanceBody("vm-c", "europe-west1-b"),
				}},
			},
		},
	}}

	w := do(t, mock, http.MethodGet, "/instances", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp ListInstancesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 3 || len(resp.Instances) != 3 {
		t.Fatalf("got %+v, want 3 instances", resp)
	}
	// Sorted by zone, then name.
	wantOrder := []string{"vm-c", "vm-a", "vm-b"}
	for i, name := range wantOrder {
		if resp.Instances[i].Name != name {
			t.Fatalf("instance[%d] = %q, want %q (%+v)", i, resp.Instances[i].Name, name, resp.Instances)
		}
	}
	got := resp.Instances[1]
	if got.Zone != "us-central1-a" || got.MachineType != "e2-micro" || got.Status != "RUNNING" {
		t.Fatalf("short fields not flattened: %+v", got)
	}
	if got.InternalIP != "10.0.0.2" || got.ExternalIP != "34.1.2.3" {
		t.Fatalf("IPs not flattened: %+v", got)
	}
	if mock.lastNR.Cloud != model.CloudGCP || mock.lastNR.AccountID != "test-project" {
		t.Fatalf("NR not GCP-scoped: %+v", mock.lastNR)
	}
	if mock.lastNR.Action != "Compute.InstancesAggregatedList" {
		t.Fatalf("action = %q", mock.lastNR.Action)
	}
}

func TestListInstances_ZoneFromGroupKeyWhenBodyMissing(t *testing.T) {
	inst := instanceBody("vm-1", "us-east1-b")
	delete(inst, "zone")
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data: map[string]any{
			"items": map[string]any{
				"zones/us-east1-b": map[string]any{"instances": []any{inst}},
			},
		},
	}}

	var resp ListInstancesResponse
	if err := json.Unmarshal(do(t, mock, http.MethodGet, "/instances", "").Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Instances) != 1 || resp.Instances[0].Zone != "us-east1-b" {
		t.Fatalf("zone fallback failed: %+v", resp.Instances)
	}
}

func TestGetInstance_SetsScopeAndInstance(t *testing.T) {
	mock := &mockProvider{resp: &model.ProviderResponse{
		HTTPStatus: 200,
		Data:       instanceBody("vm-1", "us-central1-a"),
	}}

	w := do(t, mock, http.MethodGet, "/instances/us-central1-a/vm-1", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var detail InstanceDetail
	if err := json.Unmarshal(w.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if detail.Name != "vm-1" || detail.Zone != "us-central1-a" || detail.MachineType != "e2-micro" {
		t.Fatalf("unexpected detail: %+v", detail)
	}
	if len(detail.NetworkInterfaces) != 1 {
		t.Fatalf("networkInterfaces not passed through: %+v", detail.NetworkInterfaces)
	}
	if mock.lastNR.Action != "Compute.InstancesGet" {
		t.Fatalf("action = %q", mock.lastNR.Action)
	}
	if mock.lastNR.Params["scope"] != "us-central1-a" || mock.lastNR.Params["instance"] != "vm-1" {
		t.Fatalf("scope/instance = %v/%v", mock.lastNR.Params["scope"], mock.lastNR.Params["instance"])
	}
}

func TestInstanceStateActions(t *testing.T) {
	cases := []struct {
		name       string
		method     string
		path       string
		wantAction string
		wantStatus int
	}{
		{"start", http.MethodPost, "/instances/us-central1-a/vm-1/start", "Compute.InstancesStart", http.StatusOK},
		{"stop", http.MethodPost, "/instances/us-central1-a/vm-1/stop", "Compute.InstancesStop", http.StatusOK},
		{"delete", http.MethodDelete, "/instances/us-central1-a/vm-1", "Compute.InstancesDelete", http.StatusNoContent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockProvider{resp: &model.ProviderResponse{
				HTTPStatus: 200,
				Data:       map[string]any{"kind": "compute#operation", "status": "DONE"},
			}}
			w := do(t, mock, tc.method, tc.path, "")
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", w.Code, tc.wantStatus, w.Body.String())
			}
			if mock.lastNR.Action != tc.wantAction {
				t.Fatalf("action = %q, want %q", mock.lastNR.Action, tc.wantAction)
			}
			if mock.lastNR.Params["scope"] != "us-central1-a" || mock.lastNR.Params["instance"] != "vm-1" {
				t.Fatalf("scope/instance = %v/%v", mock.lastNR.Params["scope"], mock.lastNR.Params["instance"])
			}
		})
	}
}

func TestGetInstance_PropagatesProviderError(t *testing.T) {
	mock := &mockProvider{err: model.NewProviderError("NotFound", "instance not found", http.StatusNotFound)}
	w := do(t, mock, http.MethodGet, "/instances/us-central1-a/missing", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: %s", w.Code, w.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["code"] != "NotFound" {
		t.Fatalf("code = %q, want NotFound", body["code"])
	}
}
