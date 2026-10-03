package gcp

import (
	"net/http/httptest"
	"testing"
)

func TestMemorystoreJSONCodecDecode(t *testing.T) {
	cases := []struct {
		method, path, action string
	}{
		{"GET", "/v1/projects/p/locations/us-central1/instances", "ListInstances"},
		{"POST", "/v1/projects/p/locations/us-central1/instances?instanceId=i", "CreateInstance"},
		{"GET", "/v1/projects/p/locations/us-central1/instances/i", "GetInstance"},
		{"PATCH", "/v1/projects/p/locations/us-central1/instances/i?updateMask=displayName", "UpdateInstance"},
		{"DELETE", "/v1/projects/p/locations/us-central1/instances/i", "DeleteInstance"},
		{"POST", "/v1/projects/p/locations/us-central1/instances/i:upgrade", "UpgradeInstance"},
		{"GET", "/v1/projects/p/locations", "ListLocations"},
		{"GET", "/v1/projects/p/locations/us-central1", "GetLocation"},
	}
	for _, tc := range cases {
		codec := &JSONCodec{Service: "redis"}
		r := httptest.NewRequest(tc.method, tc.path, nil)
		nr, err := codec.Decode(r, nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
		if nr.Service != "redis" {
			t.Errorf("%s %s: service = %q, want redis", tc.method, tc.path, nr.Service)
		}
		if nr.Params["project"] != "p" {
			t.Errorf("%s %s: project = %v", tc.method, tc.path, nr.Params["project"])
		}
	}
}

func TestMemorystoreJSONCodecParams(t *testing.T) {
	codec := &JSONCodec{Service: "redis"}

	nr, err := codec.Decode(httptest.NewRequest("GET", "/v1/projects/p/locations/us-central1/instances/i", nil), nil)
	if err != nil {
		t.Fatalf("decode get: %v", err)
	}
	if nr.Params["location"] != "us-central1" {
		t.Errorf("location = %v", nr.Params["location"])
	}
	if nr.Params["instanceId"] != "i" {
		t.Errorf("instanceId = %v", nr.Params["instanceId"])
	}
	if nr.Params["name"] != "locations/us-central1/instances/i" {
		t.Errorf("name = %v", nr.Params["name"])
	}

	nr, err = codec.Decode(httptest.NewRequest("GET", "/v1/projects/p/locations/us-central1", nil), nil)
	if err != nil {
		t.Fatalf("decode get location: %v", err)
	}
	if nr.Params["location"] != "us-central1" {
		t.Errorf("location = %v", nr.Params["location"])
	}
}

func TestDetectMemorystoreService(t *testing.T) {
	cases := map[string]string{
		"/v1/projects/p/locations/us-central1/instances":   "redis",
		"/v1/projects/p/locations/us-central1/instances/i": "redis",
		"/v1/projects/p/locations":                         "redis",
		"/v1/projects/p/locations/us-central1":             "redis",
		// Other location-scoped services must not be stolen by Memorystore.
		"/v1/projects/p/locations/us-central1/functions/f": "functions",
		"/v1/projects/p/locations/us-central1/clusters":    "managedkafka",
		"/v1/projects/p/locations/us/services":             "metastore",
		"/v1/projects/p/locations/us/keyRings/kr":          "kms",
		"/v1/projects/p/locations/us-central1/workflows/w": "workflows",
		"/v1/projects/p/locations/us-central1/triggers/t":  "eventarc",
	}
	for path, want := range cases {
		if got := detectV1Service(path); got != want {
			t.Errorf("detectV1Service(%q) = %q, want %q", path, got, want)
		}
	}
}

// TestDetectServiceMemorystore exercises the full path-prefix detector, not just
// the /v1/ segment resolver, to confirm the GCS raw-media fallback does not
// swallow Memorystore paths.
func TestDetectServiceMemorystore(t *testing.T) {
	for _, path := range []string{
		"/v1/projects/p/locations/us-central1/instances",
		"/v1/projects/p/locations",
	} {
		svc, src := DetectService(httptest.NewRequest("GET", path, nil))
		if svc != "redis" {
			t.Errorf("DetectService(%q) = %q, want redis", path, svc)
		}
		if src != SourcePath {
			t.Errorf("DetectService(%q) source = %v, want SourcePath", path, src)
		}
	}
}
