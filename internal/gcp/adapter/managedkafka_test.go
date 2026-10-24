package gcp

import (
	"net/http"
	"net/url"
	"testing"
)

// TestDetectManagedKafkaServiceDoesNotClaimOperations locks the
// operations-routing decision: the shared locations/{l}/operations/{id} LRO
// path is path-ambiguous with Workflows on a single host, so it stays routed to
// workflows. Managed Kafka returns its operations inline (done:true), and only
// the "clusters" segment is claimed.
func TestDetectManagedKafkaServiceDoesNotClaimOperations(t *testing.T) {
	cases := map[string]string{
		"/v1/projects/p/locations/us-central1/clusters":     "managedkafka",
		"/v1/projects/p/locations/us-central1/clusters/c":   "managedkafka",
		"/v1/projects/p/locations/us-central1/operations":   "workflows",
		"/v1/projects/p/locations/us-central1/operations/o": "workflows",
	}
	for path, want := range cases {
		if got := detectV1Service(path); got != want {
			t.Errorf("detectV1Service(%q) = %q, want %q", path, got, want)
		}
	}

	if got := detectManagedKafkaResourceType([]string{"locations", "us-central1", "operations", "op"}); got != "" {
		t.Errorf("detectManagedKafkaResourceType claimed the shared operations path: %q", got)
	}
	if got := detectManagedKafkaResourceType([]string{"locations", "us-central1", "clusters"}); got != "clusters" {
		t.Errorf("detectManagedKafkaResourceType should claim clusters, got %q", got)
	}
}

// TestDetectManagedKafkaOperationsHost locks the host discriminator for the
// shared operations path. Managed Kafka and Cloud Workflows both expose
// locations/{l}/operations on one origin; the default host keeps it on
// Workflows (path detection), while the managedkafka host token routes it to
// Managed Kafka so its own GetOperation/ListOperations become reachable.
func TestDetectManagedKafkaOperationsHost(t *testing.T) {
	cases := []struct {
		host string
		path string
		want string
		src  DetectionSource
	}{
		{"managedkafka.localhost:4588", "/v1/projects/p/locations/us-central1/operations", "managedkafka", SourceHost},
		{"managedkafka.localhost:4588", "/v1/projects/p/locations/us-central1/operations/op", "managedkafka", SourceHost},
		{"managedkafka.googleapis.com", "/v1/projects/p/locations/us-central1/operations/op", "managedkafka", SourceHost},
		// Non-operations paths are left to path detection (the clusters segment
		// already owns them); the host token does not shadow it.
		{"managedkafka.localhost:4588", "/v1/projects/p/locations/us-central1/clusters", "managedkafka", SourcePath},
		// The default host keeps the shared operations path on Workflows.
		{"localhost:8080", "/v1/projects/p/locations/us-central1/operations", "workflows", SourcePath},
		// A sibling service's host token never claims Managed Kafka's operations.
		{"metastore.localhost:4588", "/v1/projects/p/locations/us-central1/operations", "metastore", SourceHost},
		// The host token only rewrites /v1/ paths.
		{"managedkafka.localhost:4588", "/v2/projects/p/locations/us-central1/operations", "functions", SourcePath},
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
