package container

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCodecRoutesClustersAndOperations(t *testing.T) {
	cases := []struct {
		method string
		path   string
		action string
		item   string
	}{
		{http.MethodGet, "/v1/projects/p/locations/us-central1/clusters", "ListClusters", ""},
		{http.MethodPost, "/v1/projects/p/locations/us-central1/clusters", "CreateCluster", ""},
		{http.MethodGet, "/v1/projects/p/locations/us-central1/clusters/c1", "GetCluster", "c1"},
		{http.MethodDelete, "/v1/projects/p/locations/us-central1/clusters/c1", "DeleteCluster", "c1"},
		{http.MethodGet, "/v1/projects/p/locations/us-central1/operations", "ListOperations", ""},
		{http.MethodGet, "/v1/projects/p/locations/us-central1/operations/operation-1", "GetOperation", "operation-1"},
	}
	codec := NewCodec()
	for _, tc := range cases {
		t.Run(tc.action, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			nr, err := codec.Decode(r, nil)
			if err != nil {
				t.Fatalf("Decode(%s %s): %v", tc.method, tc.path, err)
			}
			if nr.Service != ServiceName || nr.Action != tc.action {
				t.Fatalf("service/action = %q/%q, want %q/%q", nr.Service, nr.Action, ServiceName, tc.action)
			}
			if nr.Params["project"] != "p" || nr.Params["location"] != "us-central1" {
				t.Fatalf("params = %v", nr.Params)
			}
			if tc.item != "" {
				key := "cluster"
				if strings.HasPrefix(tc.action, "GetOperation") {
					key = "operation"
				}
				if nr.Params[key] != tc.item {
					t.Fatalf("%s param = %v, want %q", key, nr.Params[key], tc.item)
				}
			}
		})
	}
}

// TestCodecAcceptsContainerPrefix locks the Terraform/gcloud path-mode form and
// confirms the host-mode canonical form both decode identically.
func TestCodecAcceptsContainerPrefix(t *testing.T) {
	codec := NewCodec()
	canonical := httptest.NewRequest(http.MethodGet, "/v1/projects/p/locations/l/clusters/c1", nil)
	prefixed := httptest.NewRequest(http.MethodGet, "/container/v1/projects/p/locations/l/clusters/c1", nil)
	a, err := codec.Decode(canonical, nil)
	if err != nil {
		t.Fatalf("canonical Decode: %v", err)
	}
	b, err := codec.Decode(prefixed, nil)
	if err != nil {
		t.Fatalf("prefixed Decode: %v", err)
	}
	if a.Action != b.Action || a.Params["project"] != b.Params["project"] ||
		a.Params["location"] != b.Params["location"] || a.Params["cluster"] != b.Params["cluster"] {
		t.Fatalf("canonical %v/%v != prefixed %v/%v", a.Action, a.Params, b.Action, b.Params)
	}
}

func TestCodecParsesCreateBody(t *testing.T) {
	codec := NewCodec()
	body := []byte(`{"cluster":{"name":"c1","initialNodeCount":2}}`)
	r := httptest.NewRequest(http.MethodPost, "/v1/projects/p/locations/l/clusters", strings.NewReader(string(body)))
	nr, err := codec.Decode(r, body)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if nr.Action != "CreateCluster" {
		t.Fatalf("action = %q", nr.Action)
	}
	m, ok := nr.Params["body"].(map[string]any)
	if !ok {
		t.Fatalf("body = %T", nr.Params["body"])
	}
	cluster, ok := m["cluster"].(map[string]any)
	if !ok || cluster["name"] != "c1" {
		t.Fatalf("body cluster = %v", m["cluster"])
	}
}

func TestCodecRoutesNodePoolsAndSetters(t *testing.T) {
	cases := []struct {
		method  string
		path    string
		action  string
		cluster string
		pool    string
	}{
		{http.MethodPut, "/v1/projects/p/locations/l/clusters/c1", "UpdateCluster", "c1", ""},
		{http.MethodPost, "/v1/projects/p/locations/l/clusters/c1:setAddons", "SetAddonsConfig", "c1", ""},
		{http.MethodPost, "/v1/projects/p/locations/l/clusters/c1:setResourceLabels", "SetLabels", "c1", ""},
		{http.MethodPost, "/v1/projects/p/locations/l/clusters/c1:setMasterAuth", "SetMasterAuth", "c1", ""},
		{http.MethodPost, "/v1/projects/p/locations/l/clusters/c1:updateMaster", "UpdateMaster", "c1", ""},
		{http.MethodPost, "/v1/projects/p/locations/l/clusters/c1:startIpRotation", "StartIPRotation", "c1", ""},
		{http.MethodGet, "/v1/projects/p/locations/l/clusters/c1/nodePools", "ListNodePools", "c1", ""},
		{http.MethodPost, "/v1/projects/p/locations/l/clusters/c1/nodePools", "CreateNodePool", "c1", ""},
		{http.MethodGet, "/v1/projects/p/locations/l/clusters/c1/nodePools/np1", "GetNodePool", "c1", "np1"},
		{http.MethodDelete, "/v1/projects/p/locations/l/clusters/c1/nodePools/np1", "DeleteNodePool", "c1", "np1"},
		{http.MethodPut, "/v1/projects/p/locations/l/clusters/c1/nodePools/np1", "UpdateNodePool", "c1", "np1"},
		{http.MethodPost, "/v1/projects/p/locations/l/clusters/c1/nodePools/np1:setSize", "SetNodePoolSize", "c1", "np1"},
		{http.MethodPost, "/v1/projects/p/locations/l/clusters/c1/nodePools/np1:setAutoscaling", "SetNodePoolAutoscaling", "c1", "np1"},
		{http.MethodPost, "/v1/projects/p/locations/l/clusters/c1/nodePools/np1:rollback", "RollbackNodePoolUpgrade", "c1", "np1"},
		{http.MethodPost, "/v1/projects/p/locations/l/operations/operation-1:cancel", "CancelOperation", "", ""},
	}
	codec := NewCodec()
	for _, tc := range cases {
		t.Run(tc.action, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			nr, err := codec.Decode(r, nil)
			if err != nil {
				t.Fatalf("Decode(%s %s): %v", tc.method, tc.path, err)
			}
			if nr.Action != tc.action {
				t.Fatalf("action = %q, want %q", nr.Action, tc.action)
			}
			if tc.cluster != "" && nr.Params["cluster"] != tc.cluster {
				t.Fatalf("cluster = %v, want %q", nr.Params["cluster"], tc.cluster)
			}
			if tc.pool != "" && nr.Params["nodepool"] != tc.pool {
				t.Fatalf("nodepool = %v, want %q", nr.Params["nodepool"], tc.pool)
			}
		})
	}
}

func TestCodecRejectsUnsupported(t *testing.T) {
	codec := NewCodec()
	for _, tc := range []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/projects/p/locations/l/clusters/c1"},
		{http.MethodGet, "/v1/projects/p/locations/l/nodePools"},
		{http.MethodPost, "/v1/projects/p/locations/l/clusters/c1:bogus"},
		{http.MethodGet, "/v1/projects/p/locations/l/clusters/c1/nodePools/np1:bogus"},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		if _, err := codec.Decode(r, nil); err == nil {
			t.Errorf("Decode(%s %s): expected error", tc.method, tc.path)
		}
	}
}
