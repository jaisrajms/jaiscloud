package gcp

import (
	"net/http"
	"sort"
	"testing"
)

// TestDetectServiceDatastoreVerbs locks the new REST detection: a Datastore
// project-segment custom verb resolves to "datastore", while Cloud Resource
// Manager's project custom methods still resolve to "resourcemanager".
func TestDetectServiceDatastoreVerbs(t *testing.T) {
	for _, verb := range []string{
		"lookup", "runQuery", "runAggregationQuery", "beginTransaction",
		"commit", "rollback", "allocateIds", "reserveIds",
	} {
		r, _ := http.NewRequest(http.MethodPost, "/v1/projects/p:"+verb, nil)
		if svc, _ := DetectService(r); svc != "datastore" {
			t.Errorf("POST /v1/projects/p:%s detected as %q, want datastore", verb, svc)
		}
	}
	for _, verb := range []string{"getIamPolicy", "setIamPolicy", "testIamPermissions"} {
		r, _ := http.NewRequest(http.MethodPost, "/v1/projects/p:"+verb, nil)
		if svc, _ := DetectService(r); svc != "resourcemanager" {
			t.Errorf("POST /v1/projects/p:%s detected as %q, want resourcemanager", verb, svc)
		}
	}
}

// TestKnownServiceNamesIncludesGRPConly guards the transport-selection set: the
// gRPC-only services must be present, otherwise a default "grpc" selection
// would silently disable their gRPC surface.
func TestKnownServiceNamesIncludesGRPConly(t *testing.T) {
	names := KnownServiceNames()
	set := map[string]bool{}
	for _, n := range names {
		set[n] = true
	}
	for _, want := range []string{"storage", "datastore", "logging", "monitoring"} {
		if !set[want] {
			t.Errorf("KnownServiceNames() missing %q", want)
		}
	}
	if !sort.StringsAreSorted(names) {
		t.Errorf("KnownServiceNames() not sorted: %v", names)
	}
}
