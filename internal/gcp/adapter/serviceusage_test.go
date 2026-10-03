package gcp

import (
	"net/http/httptest"
	"testing"
)

// TestDetectServiceServiceUsageNotStorage pins the routing fix: a bare
// /v1/projects/{p}/services GET must be claimed as serviceusage, not fall
// through to the GCS raw-media fallback (which logged it as
// service=storage action=ObjectsGetMedia).
func TestDetectServiceServiceUsageNotStorage(t *testing.T) {
	for _, path := range []string{
		"/v1/projects/p/services",
		"/v1/projects/p/services?filter=state:ENABLED",
	} {
		if svc, _ := DetectService(httptest.NewRequest("GET", path, nil)); svc != "serviceusage" {
			t.Errorf("DetectService(%q) = %q, want serviceusage", path, svc)
		}
	}
	if svc, _ := DetectService(httptest.NewRequest("GET", "/v1/projects/p:getIamPolicy", nil)); svc != "resourcemanager" {
		t.Errorf("DetectService(project IAM) = %q, want resourcemanager", svc)
	}
	if svc, _ := DetectService(httptest.NewRequest("GET", "/v1/projects/p", nil)); svc != "resourcemanager" {
		t.Errorf("DetectService(bare project) = %q, want resourcemanager", svc)
	}
}
