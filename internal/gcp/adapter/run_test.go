package gcp

import (
	"net/http"
	"net/url"
	"testing"
)

// TestDetectRunService locks the Cloud Run v2 routing decision. Cloud Run shares
// the /v2/projects/{p}/locations/{l} namespace with Cloud Functions and Cloud
// Tasks on one origin, so it is claimed by resource segment (services/revisions)
// and by run-prefixed operation ids; the bare operations family stays with
// functions. A synthesized *.run.app host is a data-plane invocation.
func TestDetectRunService(t *testing.T) {
	cases := []struct {
		host string
		path string
		want string
		src  DetectionSource
	}{
		// Control plane, canonical paths.
		{"localhost:8080", "/v2/projects/p/locations/us-central1/services", "run", SourcePath},
		{"localhost:8080", "/v2/projects/p/locations/us-central1/services/s1", "run", SourcePath},
		{"localhost:8080", "/v2/projects/p/locations/us-central1/services/s1:getIamPolicy", "run", SourcePath},
		{"localhost:8080", "/v2/projects/p/locations/us-central1/services/s1/revisions", "run", SourcePath},
		{"localhost:8080", "/v2/projects/p/locations/us-central1/services/s1/revisions/r1", "run", SourcePath},
		{"localhost:8080", "/v2/projects/p/locations/us-central1/operations/operation-run-abc", "run", SourcePath},
		// Path mode (Terraform/gcloud custom endpoint).
		{"localhost:8080", "/run/v2/projects/p/locations/us-central1/services/s1", "run", SourcePath},
		// Host-routed data-plane invocation.
		{"s1-abcdef012345.us-central1.run.app", "/", "run", SourceHost},
		{"s1-abcdef012345.us-central1.run.app:443", "/anything", "run", SourceHost},
		// Shared families keep their existing owners.
		{"localhost:8080", "/v2/projects/p/locations/us-central1/functions/f1", "functions", SourcePath},
		{"localhost:8080", "/v2/projects/p/locations/us-central1/runtimes", "functions", SourcePath},
		{"localhost:8080", "/v2/projects/p/locations/us-central1/operations/op-other", "functions", SourcePath},
		{"localhost:8080", "/v2/projects/p/locations/us-central1/operations", "functions", SourcePath},
		{"localhost:8080", "/v2/projects/p/locations/us-central1/queues/q1", "tasks", SourcePath},
	}
	for _, tc := range cases {
		r := &http.Request{Host: tc.host, URL: &url.URL{Path: tc.path}}
		got, src := DetectService(r)
		if got != tc.want || src != tc.src {
			t.Errorf("DetectService(host=%q path=%q) = (%q,%v), want (%q,%v)",
				tc.host, tc.path, got, src, tc.want, tc.src)
		}
	}
}

// TestRunInvocationHostDoesNotClaimControlHosts verifies the run host token only
// matches *.run.app and never the emulator origin or an admin path.
func TestRunInvocationHostDoesNotClaimControlHosts(t *testing.T) {
	for _, host := range []string{"localhost:8080", "run.app", "example.com"} {
		r := &http.Request{Host: host, Method: http.MethodGet, URL: &url.URL{Path: "/"}}
		if got, _ := DetectService(r); got == "run" {
			t.Errorf("DetectService(host=%q) claimed run for a non-service host", host)
		}
	}
}
