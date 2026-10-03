package gcp

import (
	"net/http/httptest"
	"testing"
)

func TestComputeCodecDecode(t *testing.T) {
	cases := []struct {
		method, path, action string
	}{
		{"POST", "/compute/v1/projects/p/zones/us-central1-a/instances", "InstancesInsert"},
		{"GET", "/compute/v1/projects/p/zones/us-central1-a/instances", "InstancesList"},
		{"GET", "/compute/v1/projects/p/zones/us-central1-a/instances/i", "InstancesGet"},
		{"DELETE", "/compute/v1/projects/p/zones/us-central1-a/instances/i", "InstancesDelete"},
		{"POST", "/compute/v1/projects/p/zones/us-central1-a/instances/i/start", "InstancesStart"},
		{"POST", "/compute/v1/projects/p/zones/us-central1-a/instances/i/stop", "InstancesStop"},
		{"POST", "/compute/v1/projects/p/zones/us-central1-a/instances/i/reset", "InstancesReset"},
		{"GET", "/compute/v1/projects/p/aggregated/instances", "InstancesAggregatedList"},
		{"POST", "/compute/v1/projects/p/zones/us-central1-a/disks", "DisksInsert"},
		{"GET", "/compute/v1/projects/p/zones/us-central1-a/disks", "DisksList"},
		{"GET", "/compute/v1/projects/p/zones/us-central1-a/disks/d", "DisksGet"},
		{"DELETE", "/compute/v1/projects/p/zones/us-central1-a/disks/d", "DisksDelete"},
		{"POST", "/compute/v1/projects/p/global/networks", "NetworksInsert"},
		{"GET", "/compute/v1/projects/p/global/networks", "NetworksList"},
		{"GET", "/compute/v1/projects/p/global/networks/n", "NetworksGet"},
		{"DELETE", "/compute/v1/projects/p/global/networks/n", "NetworksDelete"},
		{"POST", "/compute/v1/projects/p/global/firewalls", "FirewallsInsert"},
		{"GET", "/compute/v1/projects/p/global/firewalls", "FirewallsList"},
		{"GET", "/compute/v1/projects/p/global/firewalls/f", "FirewallsGet"},
		{"DELETE", "/compute/v1/projects/p/global/firewalls/f", "FirewallsDelete"},
		{"POST", "/compute/v1/projects/p/regions/us-central1/subnetworks", "SubnetworksInsert"},
		{"GET", "/compute/v1/projects/p/regions/us-central1/subnetworks", "SubnetworksList"},
		{"GET", "/compute/v1/projects/p/regions/us-central1/subnetworks/s", "SubnetworksGet"},
		{"DELETE", "/compute/v1/projects/p/regions/us-central1/subnetworks/s", "SubnetworksDelete"},
		{"GET", "/compute/v1/projects/p/zones/us-central1-a/machineTypes", "MachineTypesList"},
		{"GET", "/compute/v1/projects/p/zones/us-central1-a/machineTypes/e2-micro", "MachineTypesGet"},
		{"GET", "/compute/v1/projects/p/zones", "ZonesList"},
		{"GET", "/compute/v1/projects/p/zones/us-central1-a", "ZonesGet"},
		{"GET", "/compute/v1/projects/p/regions", "RegionsList"},
		{"GET", "/compute/v1/projects/p/regions/us-central1", "RegionsGet"},
		{"GET", "/compute/v1/projects/p/zones/us-central1-a/operations", "OperationsList"},
		{"GET", "/compute/v1/projects/p/zones/us-central1-a/operations/op", "OperationsGet"},
		{"GET", "/compute/v1/projects/p/regions/us-central1/operations", "OperationsList"},
		{"GET", "/compute/v1/projects/p/regions/us-central1/operations/op", "OperationsGet"},
		{"GET", "/compute/v1/projects/p/global/operations", "OperationsList"},
		{"GET", "/compute/v1/projects/p/global/operations/op", "OperationsGet"},
		// Deferred surfaces fail loud rather than 404-ing.
		{"POST", "/compute/v1/projects/p/zones/us-central1-a/instances/i/setMetadata", "Unimplemented"},
		{"POST", "/compute/v1/projects/p/zones/us-central1-a/instances/i/attachDisk", "Unimplemented"},
		{"GET", "/compute/v1/projects/p/global/addresses", "Unimplemented"},
		{"GET", "/compute/v1/projects/p/global/images", "Unimplemented"},
		{"GET", "/compute/v1/projects/p/aggregated/operations", "Unimplemented"},
		{"GET", "/compute/v1/projects/p/zones/us-central1-a/operations/op/wait", "Unimplemented"},
	}
	for _, tc := range cases {
		codec := &ComputeCodec{Service: "compute"}
		r := httptest.NewRequest(tc.method, tc.path, nil)
		nr, err := codec.Decode(r, nil)
		if err != nil {
			t.Errorf("%s %s: %v", tc.method, tc.path, err)
			continue
		}
		if nr.Action != tc.action {
			t.Errorf("%s %s: action = %q, want %q", tc.method, tc.path, nr.Action, tc.action)
		}
		if nr.Service != "compute" {
			t.Errorf("%s %s: service = %q, want compute", tc.method, tc.path, nr.Service)
		}
	}
}

func TestComputeCodecParams(t *testing.T) {
	codec := &ComputeCodec{Service: "compute"}

	nr, err := codec.Decode(httptest.NewRequest("GET", "/compute/v1/projects/p/zones/us-central1-a/instances/i", nil), nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if nr.Params["project"] != "p" {
		t.Errorf("project = %v", nr.Params["project"])
	}
	if nr.Params["scope"] != "us-central1-a" || nr.Params["scopeType"] != "zones" {
		t.Errorf("zone scope = %v / %v", nr.Params["scope"], nr.Params["scopeType"])
	}
	if nr.Params["instance"] != "i" || nr.Params["zone"] != "us-central1-a" {
		t.Errorf("instance params = %v", nr.Params)
	}

	nr, err = codec.Decode(httptest.NewRequest("GET", "/compute/v1/projects/p/regions/us-central1/subnetworks/s", nil), nil)
	if err != nil {
		t.Fatalf("decode subnetwork: %v", err)
	}
	if nr.Params["scope"] != "us-central1" || nr.Params["scopeType"] != "regions" || nr.Params["subnetwork"] != "s" {
		t.Errorf("subnetwork params = %v", nr.Params)
	}

	nr, err = codec.Decode(httptest.NewRequest("GET", "/compute/v1/projects/p/global/networks/n", nil), nil)
	if err != nil {
		t.Fatalf("decode network: %v", err)
	}
	if nr.Params["scope"] != "global" || nr.Params["scopeType"] != "global" || nr.Params["network"] != "n" {
		t.Errorf("network params = %v", nr.Params)
	}

	nr, err = codec.Decode(httptest.NewRequest("GET", "/compute/v1/projects/p/zones/us-central1-a/operations/op", nil), nil)
	if err != nil {
		t.Fatalf("decode operation: %v", err)
	}
	if nr.Params["scope"] != "us-central1-a" || nr.Params["operation"] != "op" {
		t.Errorf("operation params = %v", nr.Params)
	}
}

func TestDetectComputeService(t *testing.T) {
	r := httptest.NewRequest("GET", "/compute/v1/projects/p/zones/us-central1-a/instances", nil)
	svc, src := DetectService(r)
	if svc != "compute" {
		t.Fatalf("DetectService = %q, want compute", svc)
	}
	if src != SourcePath {
		t.Fatalf("detection source = %v, want SourcePath", src)
	}
}
