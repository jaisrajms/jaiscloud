package run

import (
	"net/http"
	"net/url"
	"testing"
)

func TestCodecDecodeActions(t *testing.T) {
	cases := []struct {
		method string
		path   string
		want   string
		params map[string]string
	}{
		{http.MethodGet, "/v2/projects/p/locations/l/services", "ListServices", nil},
		{http.MethodPost, "/v2/projects/p/locations/l/services?serviceId=svc", "CreateService", map[string]string{"serviceId": "svc"}},
		{http.MethodGet, "/v2/projects/p/locations/l/services/svc", "GetService", map[string]string{"service": "svc"}},
		{http.MethodPatch, "/v2/projects/p/locations/l/services/svc?updateMask=template", "UpdateService", map[string]string{"service": "svc"}},
		{http.MethodDelete, "/v2/projects/p/locations/l/services/svc", "DeleteService", map[string]string{"service": "svc"}},
		{http.MethodGet, "/v2/projects/p/locations/l/services/svc:getIamPolicy", "GetIamPolicy", map[string]string{"service": "svc"}},
		{http.MethodPost, "/v2/projects/p/locations/l/services/svc:setIamPolicy", "SetIamPolicy", map[string]string{"service": "svc"}},
		{http.MethodPost, "/v2/projects/p/locations/l/services/svc:testIamPermissions", "TestIamPermissions", map[string]string{"service": "svc"}},
		{http.MethodGet, "/v2/projects/p/locations/l/services/svc/revisions", "ListRevisions", map[string]string{"service": "svc"}},
		{http.MethodGet, "/v2/projects/p/locations/l/services/svc/revisions/svc-00001", "GetRevision", map[string]string{"service": "svc", "revision": "svc-00001"}},
		{http.MethodGet, "/v2/projects/p/locations/l/operations/operation-run-1", "GetOperation", map[string]string{"operation": "operation-run-1"}},
		{http.MethodPost, "/v2/projects/p/locations/l/operations/operation-run-1:wait", "WaitOperation", map[string]string{"operation": "operation-run-1"}},
		{http.MethodPost, "/v2/projects/p/locations/l/operations/operation-run-1:cancel", "CancelOperation", map[string]string{"operation": "operation-run-1"}},
		{http.MethodDelete, "/v2/projects/p/locations/l/operations/operation-run-1", "DeleteOperation", map[string]string{"operation": "operation-run-1"}},
		{http.MethodGet, "/v2/projects/p/locations/l/operations", "ListOperations", nil},
		// Prefixed form (Terraform/gcloud).
		{http.MethodGet, "/run/v2/projects/p/locations/l/services/svc", "GetService", map[string]string{"service": "svc"}},
	}
	for _, tc := range cases {
		r := &http.Request{Method: tc.method, URL: mustURL(t, tc.path)}
		nr, err := NewCodec().Decode(r, nil)
		if err != nil {
			t.Errorf("%s %s: Decode error %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.want {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.want)
		}
		if nr.Params["project"] != "p" || nr.Params["location"] != "l" {
			t.Errorf("%s %s: project/location = %v/%v", tc.method, tc.path, nr.Params["project"], nr.Params["location"])
		}
		for k, v := range tc.params {
			if nr.Params[k] != v {
				t.Errorf("%s %s: param %s = %v, want %q", tc.method, tc.path, k, nr.Params[k], v)
			}
		}
	}
}

func TestCodecDecodeUnsupported(t *testing.T) {
	r := &http.Request{Method: http.MethodGet, URL: mustURL(t, "/v2/projects/p/locations/l/unknown")}
	if _, err := NewCodec().Decode(r, nil); err == nil {
		t.Fatal("expected error for unsupported path")
	}
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return u
}
