package gcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	computeprovider "jaiscloud/internal/gcp/provider/compute"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// TestProjectsSetCommonInstanceMetadataRouteDispatch drives a raw Compute Engine
// REST request through the real adapter (service detection + codec) into the
// operation registry, and then through compute.projects.get. It fails if the
// method only decodes: an unregistered action would fall through to the
// fail-loud Compute.Unimplemented handler, and the project would keep reporting
// no metadata (TF1-5).
func TestProjectsSetCommonInstanceMetadataRouteDispatch(t *testing.T) {
	body := []byte(`{"items":[{"key":"foo","value":"bar"}],"fingerprint":""}`)
	r := httptest.NewRequest(http.MethodPost,
		"/compute/v1/projects/proj/setCommonInstanceMetadata", bytes.NewReader(body))

	a := New()
	nr, _, err := a.DetectAndDecode(r, body)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Service != "compute" {
		t.Fatalf("service = %q, want compute", nr.Service)
	}
	if nr.Action != "ProjectsSetCommonInstanceMetadata" {
		t.Fatalf("action = %q, want ProjectsSetCommonInstanceMetadata", nr.Action)
	}
	if nr.Params["project"] != "proj" {
		t.Fatalf("project = %v, want proj", nr.Params["project"])
	}

	// The dispatch key is exactly what the registry registers (the prefix comes
	// from the adapter's service descriptor, not a test literal).
	registry := provider.NewRegistry().Register(computeprovider.New(store.NewMemoryResourceStore()))
	status, data := dispatchRaw(t, registry, a.ServiceToProvider(nr.Service)+"."+nr.Action, nr)
	if status != http.StatusOK {
		t.Fatalf("setCommonInstanceMetadata status = %d (%v), want 200", status, data)
	}
	op, _ := data.(map[string]any)
	if op["kind"] != "compute#operation" || op["status"] != "DONE" {
		t.Fatalf("response = %v, want a DONE compute#operation", data)
	}
	if op["operationType"] != "setCommonInstanceMetadata" {
		t.Errorf("operationType = %v, want setCommonInstanceMetadata", op["operationType"])
	}

	// A follow-up projects.get reports the items the write stored.
	get := httptest.NewRequest(http.MethodGet, "/compute/v1/projects/proj", nil)
	gnr, _, err := a.DetectAndDecode(get, nil)
	if err != nil {
		t.Fatalf("decode get: %v", err)
	}
	if gnr.Action != "ProjectsGet" {
		t.Fatalf("get action = %q, want ProjectsGet", gnr.Action)
	}
	status, data = dispatchRaw(t, registry, a.ServiceToProvider(gnr.Service)+"."+gnr.Action, gnr)
	if status != http.StatusOK {
		t.Fatalf("projects.get status = %d (%v), want 200", status, data)
	}
	project, _ := data.(map[string]any)
	md, _ := project["commonInstanceMetadata"].(map[string]any)
	items, _ := md["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("commonInstanceMetadata.items = %v, want the written entry", md["items"])
	}
	item, _ := items[0].(map[string]any)
	if item["key"] != "foo" || item["value"] != "bar" {
		t.Errorf("stored item = %v, want foo=bar", item)
	}
	if md["fingerprint"] == "" {
		t.Error("commonInstanceMetadata.fingerprint is empty")
	}
}

// dispatchRaw runs an action through the registry and renders the response the
// way the compute codec would: the wire status plus the JSON-decoded data. A
// provider error is returned as its status and message.
func dispatchRaw(t *testing.T, registry *provider.Registry, key string, nr *model.NormalizedRequest) (int, any) {
	t.Helper()
	resp, err := registry.Dispatch(context.Background(), key, nr)
	if err != nil {
		var perr *model.ProviderError
		if errors.As(err, &perr) {
			return perr.HTTPStatus, perr.Message
		}
		t.Fatalf("dispatch %s: %v", key, err)
	}
	raw, err := json.Marshal(resp.Data)
	if err != nil {
		t.Fatalf("marshal %s: %v", key, err)
	}
	var data any
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatalf("unmarshal %s: %v", key, err)
	}
	status := resp.HTTPStatus
	if status == 0 {
		status = http.StatusOK
	}
	return status, data
}
