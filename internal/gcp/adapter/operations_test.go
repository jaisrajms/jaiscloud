package gcp

import "testing"

// TestOperationsHostService locks the host→service mapping for the shared
// locations/{location}/operations LRO family that Managed Kafka and Dataproc
// Metastore share with Cloud Workflows on the single emulator origin. Only the
// owning service's host token claims it; every other host (and any other path)
// falls through to path detection.
func TestOperationsHostService(t *testing.T) {
	cases := []struct {
		host string
		path string
		want string
	}{
		{"managedkafka", "/v1/projects/p/locations/l/operations", "managedkafka"},
		{"managedkafka", "/v1/projects/p/locations/l/operations/op", "managedkafka"},
		{"metastore", "/v1/projects/p/locations/l/operations", "metastore"},
		{"metastore", "/v1/projects/p/locations/l/operations/op", "metastore"},
		// Non-operations paths are never claimed by the host token.
		{"managedkafka", "/v1/projects/p/locations/l/clusters", ""},
		{"metastore", "/v1/projects/p/locations/l/services", ""},
		// Other hosts are not owners.
		{"container", "/v1/projects/p/locations/l/operations", ""},
		{"workflows", "/v1/projects/p/locations/l/operations", ""},
		{"localhost", "/v1/projects/p/locations/l/operations", ""},
		// The guard is /v1/ only (a /v2 functions operations path is not this family).
		{"managedkafka", "/v2/projects/p/locations/l/operations", ""},
	}
	for _, tc := range cases {
		if got := operationsHostService(tc.host, tc.path); got != tc.want {
			t.Errorf("operationsHostService(%q, %q) = %q, want %q", tc.host, tc.path, got, tc.want)
		}
	}
}

// TestIsLocationOperationsPath locks the path shape, including the short paths
// that must not panic or match.
func TestIsLocationOperationsPath(t *testing.T) {
	yes := []string{
		"/v1/projects/p/locations/l/operations",
		"/v1/projects/p/locations/l/operations/op",
		"/v1/projects/p/locations/l/operations/op/extra",
	}
	no := []string{
		"/v1/projects/p/locations/l/clusters",
		"/v1/projects/p/locations/l/services",
		"/v1/projects/p/locations",
		"/v1/projects/p",
		"/v1/projects",
		"/v1",
		"",
		"/v2/projects/p/locations/l/operations",
	}
	for _, p := range yes {
		if !isLocationOperationsPath(p) {
			t.Errorf("isLocationOperationsPath(%q) = false, want true", p)
		}
	}
	for _, p := range no {
		if isLocationOperationsPath(p) {
			t.Errorf("isLocationOperationsPath(%q) = true, want false", p)
		}
	}
}
