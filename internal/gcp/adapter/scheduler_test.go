package gcp

import "testing"

// TestDetectSchedulerService locks the Cloud Scheduler routing decision: the
// locations/{l}/jobs path is claimed by segment detection, and the adjacent
// Managed Kafka locations/{l}/clusters path stays routed to managedkafka.
func TestDetectSchedulerService(t *testing.T) {
	cases := map[string]string{
		"/v1/projects/p/locations/us-central1/jobs":          "scheduler",
		"/v1/projects/p/locations/us-central1/jobs/j1":       "scheduler",
		"/v1/projects/p/locations/us-central1/jobs/j1:pause": "scheduler",
		"/v1/projects/p/locations/us-central1/clusters":      "managedkafka",
		"/v1/projects/p/locations/us-central1/operations":    "workflows",
	}
	for path, want := range cases {
		if got := detectV1Service(path); got != want {
			t.Errorf("detectV1Service(%q) = %q, want %q", path, got, want)
		}
	}
}
