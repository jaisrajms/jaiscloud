package gcp

import "testing"

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
