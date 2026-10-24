package gcp

import (
	"net/http"
	"net/url"
	"testing"
)

func TestDetectMetastoreService(t *testing.T) {
	cases := map[string]string{
		"/v1/projects/p/locations/us/services":                        "metastore",
		"/v1/projects/p/locations/us/services/s":                      "metastore",
		"/v1/projects/p/locations/us/services/s/backups":              "metastore",
		"/v1/projects/p/locations/us/services/s/backups/b":            "metastore",
		"/v1/projects/p/locations/us/services/s/metadataImports/m":    "metastore",
		"/v1/projects/p/locations/us/federations":                     "metastore",
		"/v1/projects/p/locations/us/federations/f":                   "metastore",
		"/v1/projects/p/locations/us/services/s/databases/d":          "metastore",
		"/v1/projects/p/locations/us/services/s/databases/d/tables/t": "metastore",
		// Shared operations path stays on workflows (path-ambiguous on one host).
		"/v1/projects/p/locations/us/operations/op": "workflows",
		// Unrelated locations/{l}/... services are unaffected.
		"/v1/projects/p/locations/us/clusters/c":           "managedkafka",
		"/v1/projects/p/locations/us-central1/functions/f": "functions",
		"/v1/projects/p/locations/us/workflows/w":          "workflows",
	}
	for path, want := range cases {
		if got := detectV1Service(path); got != want {
			t.Errorf("detectV1Service(%q) = %q, want %q", path, got, want)
		}
	}
}

// TestDetectMetastoreResourceTypeDoesNotClaimOperations locks the
// operations-routing decision: the shared locations/{l}/operations/{id} LRO
// path is path-ambiguous with Workflows on a single host, so
// detectMetastoreResourceType must NOT claim it (it stays routed to workflows).
// Only the "services" segment is claimed.
func TestDetectMetastoreResourceTypeDoesNotClaimOperations(t *testing.T) {
	if got := detectMetastoreResourceType([]string{"locations", "us", "operations", "op"}); got != "" {
		t.Errorf("detectMetastoreResourceType claimed the shared operations path: %q", got)
	}
	if got := detectMetastoreResourceType([]string{"locations", "us", "services"}); got != "services" {
		t.Errorf("detectMetastoreResourceType should claim services, got %q", got)
	}
}

// TestDetectMetastoreOperationsHost locks the host discriminator for the shared
// operations path. Dataproc Metastore and Cloud Workflows both expose
// locations/{l}/operations on one origin; the default host keeps it on
// Workflows (path detection), while the metastore host token routes it to
// Metastore so its own GetOperation/ListOperations become reachable.
func TestDetectMetastoreOperationsHost(t *testing.T) {
	cases := []struct {
		host string
		path string
		want string
		src  DetectionSource
	}{
		{"metastore.localhost:4588", "/v1/projects/p/locations/us/operations", "metastore", SourceHost},
		{"metastore.localhost:4588", "/v1/projects/p/locations/us/operations/op", "metastore", SourceHost},
		{"metastore.googleapis.com", "/v1/projects/p/locations/us/operations/op", "metastore", SourceHost},
		// Non-operations paths are left to path detection (the services segment
		// already owns them).
		{"metastore.localhost:4588", "/v1/projects/p/locations/us/services", "metastore", SourcePath},
		// The default host keeps the shared operations path on Workflows.
		{"localhost:8080", "/v1/projects/p/locations/us/operations", "workflows", SourcePath},
		// A sibling service's host token never claims Metastore's operations.
		{"managedkafka.localhost:4588", "/v1/projects/p/locations/us/operations", "managedkafka", SourceHost},
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
