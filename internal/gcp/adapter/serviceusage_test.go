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

// TestDetectServiceServiceUsageV1Beta1 pins the v1beta1 routing for
// services.generateServiceIdentity (the google-beta provider's
// google_project_service_identity) and its top-level operations poll path. The
// rest of the v1beta1 Service Usage surface is deliberately not claimed: the
// emulator serves it on v1 and v1beta1's response shapes differ.
func TestDetectServiceServiceUsageV1Beta1(t *testing.T) {
	cases := []struct {
		method, path, want string
	}{
		{"POST", "/v1beta1/projects/p/services/secretmanager.googleapis.com:generateServiceIdentity", "serviceusage"},
		{"POST", "/v1beta1/projects/" + "123456789012" + "/services/pubsub.googleapis.com:generateServiceIdentity", "serviceusage"},
		{"GET", "/v1beta1/operations/op-1", "serviceusage"},
		// Not claimed: v1 has no such method, and other v1beta1 Service Usage
		// methods keep their v1 owner.
		{"POST", "/v1/projects/p/services/secretmanager.googleapis.com:generateServiceIdentity", "serviceusage"},
		{"GET", "/v1beta1/projects/p/services", ""},
		{"POST", "/v1beta1/projects/p/services/pubsub.googleapis.com:enable", ""},
		{"GET", "/v1beta1/projects/p/services/pubsub.googleapis.com", ""},
	}
	for _, tc := range cases {
		got, _ := DetectService(httptest.NewRequest(tc.method, tc.path, nil))
		if got != tc.want {
			t.Errorf("DetectService(%s %q) = %q, want %q", tc.method, tc.path, got, tc.want)
		}
	}
}
