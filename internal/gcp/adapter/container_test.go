package gcp

import (
	"net/http"
	"net/url"
	"testing"
)

// TestDetectContainerService locks the GKE routing decision. GKE shares the
// canonical /v1/projects/{p}/locations/{l}/clusters path with Managed Kafka on
// one origin, so the Host's first DNS label is the discriminator; the default
// host still routes that path to managedkafka. Terraform/gcloud use the
// /container/ path prefix, which the descriptor claims.
func TestDetectContainerService(t *testing.T) {
	cases := []struct {
		host string
		path string
		want string
		src  DetectionSource
	}{
		{"container.localhost:4588", "/v1/projects/p/locations/us-central1/clusters", "container", SourceHost},
		{"container.localhost:4588", "/v1/projects/p/locations/us-central1/clusters/c1", "container", SourceHost},
		{"container.localhost:4588", "/v1/projects/p/locations/us-central1/operations", "container", SourceHost},
		{"container.googleapis.com", "/v1/projects/p/locations/us-central1/clusters/c1", "container", SourceHost},
		// Default host: the canonical path stays Managed Kafka (the pre-existing
		// behavior — do not regress the collision).
		{"localhost:8080", "/v1/projects/p/locations/us-central1/clusters", "managedkafka", SourcePath},
		// Path mode (Terraform/gcloud custom endpoint).
		{"localhost:8080", "/container/v1/projects/p/locations/us-central1/clusters", "container", SourcePath},
		// The host token only rewrites /v1/, so a non-/v1/ path is not GKE.
		{"container.localhost:4588", "/v2/projects/p/locations/us-central1/functions", "functions", SourcePath},
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

// TestContainerHostTokenOnlyV1 verifies the host token never claims a GCS raw
// media path or an admin path even when the Host is a container host.
func TestContainerHostTokenOnlyV1(t *testing.T) {
	for _, path := range []string{"/bucket/object.txt", "/_jaiscloud/health"} {
		r := &http.Request{Host: "container.localhost:4588", Method: http.MethodGet, URL: &url.URL{Path: path}}
		if got, _ := DetectService(r); got == "container" {
			t.Errorf("DetectService(%q) claimed container for a non-/v1/ path", path)
		}
	}
}
