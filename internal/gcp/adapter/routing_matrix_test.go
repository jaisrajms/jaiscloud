package gcp

import (
	"net/http"
	"net/url"
	"testing"
)

// detect routes a synthetic request through the same entry point the gateway
// uses. Building the request directly (rather than via httptest) lets the cases
// below pin host tokens, raw paths and double slashes exactly.
func detect(host, method, path string) (string, DetectionSource) {
	r := &http.Request{Host: host, Method: method, URL: &url.URL{Path: path}}
	return DetectService(r)
}

// TestRoutingMatrixAdversarial consolidates the collision and adversarial cases
// the REST resolver must decide: shared /v1/ and /v2/ paths, custom verbs,
// host-vs-path conflicts, double-slash endpoint concatenation, and requests
// that must resolve to nothing (unknown host + unknown path) rather than being
// mis-served. The exhaustive per-operation matrix lives in
// tests/gcpconformance/routing_test.go; this table pins the hand-picked
// decision cases and every mis-route fixed by AUD7.
func TestRoutingMatrixAdversarial(t *testing.T) {
	cases := []struct {
		name, host, method, path string
		want                     string
		wantSrc                  DetectionSource
	}{
		// --- AUD7 fixes ---------------------------------------------------
		// IAM Credentials getAllowedLocations is a plain trailing resource
		// segment in the real Discovery documents (not the ":getAllowedLocations"
		// custom verb), sharing the serviceAccounts path with IAM.
		{"iamcredentials allowedLocations", "", http.MethodGet,
			"/v1/projects/p/serviceAccounts/sa@x.com/allowedLocations", "iamcredentials", SourcePath},
		{"iamcredentials workloadIdentityPools allowedLocations", "", http.MethodGet,
			"/v1/projects/p/locations/us-central1/workloadIdentityPools/pool/allowedLocations", "iamcredentials", SourcePath},
		{"iamcredentials allowedLocations verb still works", "", http.MethodGet,
			"/v1/projects/p/serviceAccounts/sa@x.com:getAllowedLocations", "iamcredentials", SourcePath},
		// Double-slash endpoint concatenation must still resolve.
		{"double slash topics", "localhost:8080", http.MethodGet,
			"//v1/projects/p/topics/t", "pubsub", SourcePath},
		{"double slash top-level functions operations", "localhost:8080", http.MethodGet,
			"//v1/operations", "functions", SourcePath},

		// --- shared /v1/ LRO operations: host token vs default owner ------
		{"operations managedkafka host", "managedkafka.googleapis.com", http.MethodGet,
			"/v1/projects/p/locations/l/operations", "managedkafka", SourceHost},
		{"operations managedkafka localhost", "managedkafka.localhost:4588", http.MethodGet,
			"/v1/projects/p/locations/l/operations/op", "managedkafka", SourceHost},
		{"operations metastore host", "metastore.googleapis.com", http.MethodGet,
			"/v1/projects/p/locations/l/operations", "metastore", SourceHost},
		{"operations default owner is workflows", "localhost:8080", http.MethodGet,
			"/v1/projects/p/locations/l/operations", "workflows", SourcePath},
		{"operations unknown host falls to workflows", "example.com", http.MethodGet,
			"/v1/projects/p/locations/l/operations/op", "workflows", SourcePath},
		{"operations host token ignores non-operations path", "managedkafka.googleapis.com", http.MethodGet,
			"/v1/projects/p/locations/l/clusters", "managedkafka", SourcePath},

		// --- GKE vs Managed Kafka on the shared clusters path --------------
		{"clusters container host", "container.googleapis.com", http.MethodGet,
			"/v1/projects/p/locations/l/clusters", "container", SourceHost},
		{"clusters default host is managedkafka", "localhost:8080", http.MethodGet,
			"/v1/projects/p/locations/l/clusters", "managedkafka", SourcePath},
		{"clusters /container/ path", "localhost:8080", http.MethodGet,
			"/container/v1/projects/p/locations/l/clusters", "container", SourcePath},

		// --- project-segment custom verbs: datastore vs resourcemanager ----
		{"datastore lookup", "", http.MethodPost, "/v1/projects/p:lookup", "datastore", SourcePath},
		{"resourcemanager getIamPolicy", "", http.MethodPost, "/v1/projects/p:getIamPolicy", "resourcemanager", SourcePath},
		{"resourcemanager collection", "", http.MethodGet, "/v1/projects", "resourcemanager", SourcePath},
		{"resourcemanager bare project", "", http.MethodGet, "/v1/projects/p", "resourcemanager", SourcePath},
		// Datastore Admin export/import is deliberately not claimed; it falls to
		// the resource-manager project surface rather than being mis-served.
		{"datastore admin export not claimed", "", http.MethodPost, "/v1/projects/p:export", "resourcemanager", SourcePath},

		// --- /v2 logging vs functions vs tasks vs run ----------------------
		{"logging entries", "", http.MethodPost, "/v2/entries:write", "logging", SourcePath},
		{"functions locations", "", http.MethodGet, "/v2/projects/p/locations/us-central1/functions", "functions", SourcePath},
		{"tasks queues", "", http.MethodGet, "/v2/projects/p/locations/l/queues", "tasks", SourcePath},
		{"run services", "", http.MethodGet, "/v2/projects/p/locations/l/services", "run", SourcePath},
		{"functions operations default", "", http.MethodGet, "/v2/projects/p/locations/l/operations", "functions", SourcePath},
		{"run-prefixed operation", "", http.MethodGet, "/v2/projects/p/locations/l/operations/operation-run-123", "run", SourcePath},

		// --- serviceusage vs metastore on the "services" segment ----------
		{"serviceusage services", "", http.MethodGet, "/v1/projects/p/services", "serviceusage", SourcePath},
		{"metastore services", "", http.MethodGet, "/v1/projects/p/locations/l/services", "metastore", SourcePath},

		// --- raw GCS media fallback vs unknown ----------------------------
		// A two-segment GET/PUT/HEAD path is the raw XML object form.
		{"raw media GET is storage", "localhost:8080", http.MethodGet, "/bucket/object.txt", "storage", SourcePath},
		{"raw media PUT is storage", "localhost:8080", http.MethodPut, "/bucket/object.txt", "storage", SourcePath},
		{"raw media form via POST is not storage", "localhost:8080", http.MethodPost, "/bucket/object.txt", "", SourceUnknown},
		// A reserved API prefix is never raw media.
		{"storage api path is not raw media", "localhost:8080", http.MethodGet, "/storage/v1/b/bkt/o/obj", "storage", SourcePath},

		// --- unknown must stay unknown (no mis-serve) ----------------------
		{"unknown host and path", "example.com", http.MethodPost, "/nope/nothing", "", SourceUnknown},
		{"unknown v1 resource", "", http.MethodGet, "/v1/projects/p/unknowns/x", "", SourceUnknown},
		{"unknown host v2 resource", "example.com", http.MethodGet, "/v2/projects/p/other/x", "", SourceUnknown},
	}
	for _, tc := range cases {
		got, src := detect(tc.host, tc.method, tc.path)
		if got != tc.want || src != tc.wantSrc {
			t.Errorf("%s: DetectService(host=%q %s %q) = (%q,%v), want (%q,%v)",
				tc.name, tc.host, tc.method, tc.path, got, src, tc.want, tc.wantSrc)
		}
	}
}

// TestAllowedLocationsDecode locks the codec half of the AUD7 iamcredentials
// fix: the real Discovery plain-segment path derives the action the
// iamcredentials provider registers (the ":getAllowedLocations" verb form
// remains supported).
func TestAllowedLocationsDecode(t *testing.T) {
	c := &JSONCodec{Service: "iamcredentials"}
	cases := []struct{ method, path string }{
		{http.MethodGet, "/v1/projects/p/serviceAccounts/sa@x.com/allowedLocations"},
		{http.MethodGet, "/v1/projects/p/locations/us-central1/workloadIdentityPools/pool/allowedLocations"},
		{http.MethodGet, "/v1/projects/p/serviceAccounts/sa@x.com:getAllowedLocations"},
	}
	for _, tc := range cases {
		r := &http.Request{Host: "iamcredentials.googleapis.com", Method: tc.method, URL: &url.URL{Path: tc.path}}
		nr, err := c.Decode(r, nil)
		if err != nil {
			t.Errorf("%s: Decode: %v", tc.path, err)
			continue
		}
		if nr.Action != "GetAllowedLocations" {
			t.Errorf("%s: action = %q, want GetAllowedLocations", tc.path, nr.Action)
		}
	}
}
