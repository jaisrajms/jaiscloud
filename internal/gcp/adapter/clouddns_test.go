package gcp

import (
	"net/http/httptest"
	"testing"
)

func TestCloudDNSCodecDecode(t *testing.T) {
	cases := []struct {
		method, path, action string
	}{
		{"GET", "/dns/v1/projects/p", "ProjectGet"},
		{"GET", "/dns/v1/projects/p/managedZones", "ManagedZoneList"},
		{"POST", "/dns/v1/projects/p/managedZones", "ManagedZoneCreate"},
		{"GET", "/dns/v1/projects/p/managedZones/z", "ManagedZoneGet"},
		{"PATCH", "/dns/v1/projects/p/managedZones/z", "ManagedZonePatch"},
		{"PUT", "/dns/v1/projects/p/managedZones/z", "ManagedZoneUpdate"},
		{"DELETE", "/dns/v1/projects/p/managedZones/z", "ManagedZoneDelete"},
		{"GET", "/dns/v1/projects/p/managedZones/z/rrsets", "ResourceRecordSetList"},
		{"POST", "/dns/v1/projects/p/managedZones/z/rrsets", "ResourceRecordSetCreate"},
		{"DELETE", "/dns/v1/projects/p/managedZones/z/rrsets?name=www.example.com.&type=A", "ResourceRecordSetDelete"},
		{"GET", "/dns/v1/projects/p/managedZones/z/rrsets/www.example.com./A", "ResourceRecordSetGet"},
		{"PATCH", "/dns/v1/projects/p/managedZones/z/rrsets/www.example.com./A", "ResourceRecordSetPatch"},
		{"DELETE", "/dns/v1/projects/p/managedZones/z/rrsets/www.example.com./A", "ResourceRecordSetDelete"},
		{"GET", "/dns/v1/projects/p/managedZones/z/changes", "ChangeList"},
		{"POST", "/dns/v1/projects/p/managedZones/z/changes", "ChangeCreate"},
		{"GET", "/dns/v1/projects/p/managedZones/z/changes/c1", "ChangeGet"},
		// Deferred surfaces fail loud rather than 404-ing.
		{"POST", "/dns/v1/projects/p/managedZones/z:getIamPolicy", "Unimplemented"},
		{"GET", "/dns/v1/projects/p/managedZones/z/dnsKeys", "Unimplemented"},
		{"GET", "/dns/v1/projects/p/policies", "Unimplemented"},
		{"GET", "/dns/v1/projects/p/responsePolicies/rp", "Unimplemented"},
	}
	for _, tc := range cases {
		codec := &CloudDNSCodec{Service: "dns"}
		r := httptest.NewRequest(tc.method, tc.path, nil)
		nr, err := codec.Decode(r, nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
		if nr.Service != "dns" {
			t.Errorf("%s %s: service = %q, want dns", tc.method, tc.path, nr.Service)
		}
		if nr.Params["project"] != "p" {
			t.Errorf("%s %s: project = %v", tc.method, tc.path, nr.Params["project"])
		}
	}
}

func TestCloudDNSCodecParams(t *testing.T) {
	codec := &CloudDNSCodec{Service: "dns"}

	// Path-segment name/type form.
	nr, err := codec.Decode(httptest.NewRequest("DELETE", "/dns/v1/projects/p/managedZones/z/rrsets/www.example.com./A", nil), nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Params["managedZone"] != "z" {
		t.Errorf("managedZone = %v", nr.Params["managedZone"])
	}
	if nr.Params["rrsetName"] != "www.example.com." {
		t.Errorf("rrsetName = %v", nr.Params["rrsetName"])
	}
	if nr.Params["rrsetType"] != "A" {
		t.Errorf("rrsetType = %v", nr.Params["rrsetType"])
	}

	// Query-parameter delete form.
	nr, err = codec.Decode(httptest.NewRequest("DELETE", "/dns/v1/projects/p/managedZones/z/rrsets?name=mail.example.com.&type=MX", nil), nil)
	if err != nil {
		t.Fatalf("decode query delete: %v", err)
	}
	if nr.Params["rrsetName"] != "mail.example.com." || nr.Params["rrsetType"] != "MX" {
		t.Errorf("query delete params = %v / %v", nr.Params["rrsetName"], nr.Params["rrsetType"])
	}

	// Change id.
	nr, err = codec.Decode(httptest.NewRequest("GET", "/dns/v1/projects/p/managedZones/z/changes/c1", nil), nil)
	if err != nil {
		t.Fatalf("decode change: %v", err)
	}
	if nr.Params["changeId"] != "c1" {
		t.Errorf("changeId = %v", nr.Params["changeId"])
	}
}

func TestDetectCloudDNSService(t *testing.T) {
	r := httptest.NewRequest("GET", "/dns/v1/projects/p/managedZones", nil)
	svc, src := DetectService(r)
	if svc != "dns" {
		t.Fatalf("DetectService = %q, want dns", svc)
	}
	if src != SourcePath {
		t.Fatalf("detection source = %v, want SourcePath", src)
	}
}
