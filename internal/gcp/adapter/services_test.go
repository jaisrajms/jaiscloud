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

// TestDetectServiceLoggingAndFunctionsV2 locks the /v2/ namespace split: Cloud
// Logging's entries/logs/descriptors paths resolve to "logging", while Cloud
// Functions v2's project-location paths still resolve to "functions".
func TestDetectServiceLoggingAndFunctionsV2(t *testing.T) {
	loggingPaths := []struct{ method, path string }{
		{http.MethodPost, "/v2/entries:write"},
		{http.MethodPost, "/v2/entries:list"},
		{http.MethodGet, "/v2/monitoredResourceDescriptors"},
		{http.MethodGet, "/v2/projects/p/logs"},
		{http.MethodGet, "/v2/organizations/123/logs"},
		{http.MethodGet, "/v2/folders/9/logs"},
		{http.MethodGet, "/v2/billingAccounts/b/logs"},
		{http.MethodDelete, "/v2/projects/p/logs/mylog"},
		{http.MethodDelete, "/v2/folders/9/logs/a%2Fb"},
		{http.MethodGet, "/v2/projects/p/metrics"},
		{http.MethodPost, "/v2/projects/p/metrics"},
		{http.MethodGet, "/v2/projects/p/metrics/nginx%2Frequests"},
		{http.MethodPut, "/v2/organizations/12/metrics/m"},
		{http.MethodDelete, "/v2/folders/9/metrics/a/b"},
	}
	for _, tc := range loggingPaths {
		r, _ := http.NewRequest(tc.method, tc.path, nil)
		if svc, _ := DetectService(r); svc != "logging" {
			t.Errorf("%s %s detected as %q, want logging", tc.method, tc.path, svc)
		}
	}

	functionsPaths := []struct{ method, path string }{
		{http.MethodGet, "/v2/projects/p/locations"},
		{http.MethodGet, "/v2/projects/p/locations/us-central1"},
		{http.MethodGet, "/v2/projects/p/locations/us-central1/functions"},
		{http.MethodGet, "/v2/projects/p/locations/us-central1/functions/myfn"},
		{http.MethodGet, "/v2/projects/p/locations/us-central1/runtimes"},
		{http.MethodGet, "/v2/projects/p/locations/us-central1/operations/op"},
	}
	for _, tc := range functionsPaths {
		r, _ := http.NewRequest(tc.method, tc.path, nil)
		if svc, _ := DetectService(r); svc != "functions" {
			t.Errorf("%s %s detected as %q, want functions", tc.method, tc.path, svc)
		}
	}
}

// TestDetectServiceFunctionTriggerHost locks host-based detection of a deployed
// function's HTTPS-trigger URL: the trigger host resolves to functions with
// SourceHost, while the control-plane host stays path-detected.
func TestDetectServiceFunctionTriggerHost(t *testing.T) {
	r, _ := http.NewRequest(http.MethodPost, "/my-func", nil)
	r.Host = "us-central1-proj.cloudfunctions.net"
	if svc, src := DetectService(r); svc != "functions" || src != SourceHost {
		t.Errorf("trigger host = (%q, %v), want (functions, SourceHost)", svc, src)
	}

	// The trigger path alone (no trigger host) is not a functions path; it
	// falls through to the GCS raw-media fallback.
	r2, _ := http.NewRequest(http.MethodGet, "/my-func", nil)
	if svc, _ := DetectService(r2); svc == "functions" {
		t.Errorf("bare path detected as functions without a trigger host")
	}

	// The control-plane host is unaffected (path-detected).
	r3, _ := http.NewRequest(http.MethodGet, "/v1/projects/p/locations/us-central1/functions", nil)
	r3.Host = "cloudfunctions.googleapis.com"
	if svc, src := DetectService(r3); svc != "functions" || src != SourcePath {
		t.Errorf("control-plane host = (%q, %v), want (functions, SourcePath)", svc, src)
	}
}

// TestDetectServiceMonitoringV3 locks the /v3/ namespace to Cloud Monitoring:
// every v3 path (metric descriptors, time series, alert policies, notification
// channels/descriptors, monitored resource descriptors) resolves to
// "monitoring".
func TestDetectServiceMonitoringV3(t *testing.T) {
	paths := []struct{ method, path string }{
		{http.MethodGet, "/v3/projects/p/metricDescriptors"},
		{http.MethodGet, "/v3/projects/p/metricDescriptors/custom.googleapis.com/foo"},
		{http.MethodDelete, "/v3/projects/p/metricDescriptors/custom.googleapis.com/foo"},
		{http.MethodPost, "/v3/projects/p/timeSeries"},
		{http.MethodPost, "/v3/projects/p/timeSeries:createService"},
		{http.MethodGet, "/v3/projects/p/alertPolicies"},
		{http.MethodPatch, "/v3/projects/p/alertPolicies/abc"},
		{http.MethodGet, "/v3/projects/p/notificationChannels"},
		{http.MethodPost, "/v3/projects/p/notificationChannels/abc:verify"},
		{http.MethodGet, "/v3/projects/p/notificationChannelDescriptors"},
		{http.MethodGet, "/v3/projects/p/monitoredResourceDescriptors"},
	}
	for _, tc := range paths {
		r, _ := http.NewRequest(tc.method, tc.path, nil)
		if svc, _ := DetectService(r); svc != "monitoring" {
			t.Errorf("%s %s detected as %q, want monitoring", tc.method, tc.path, svc)
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
	for _, want := range []string{"storage", "datastore", "logging", "monitoring", "firestoreadmin"} {
		if !set[want] {
			t.Errorf("KnownServiceNames() missing %q", want)
		}
	}
	if !sort.StringsAreSorted(names) {
		t.Errorf("KnownServiceNames() not sorted: %v", names)
	}
}
