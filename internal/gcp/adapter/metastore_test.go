package gcp

import "testing"

func TestDetectMetastoreService(t *testing.T) {
	cases := map[string]string{
		"/v1/projects/p/locations/us/services":                     "metastore",
		"/v1/projects/p/locations/us/services/s":                   "metastore",
		"/v1/projects/p/locations/us/services/s/backups":           "metastore",
		"/v1/projects/p/locations/us/services/s/backups/b":         "metastore",
		"/v1/projects/p/locations/us/services/s/metadataImports/m": "metastore",
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
