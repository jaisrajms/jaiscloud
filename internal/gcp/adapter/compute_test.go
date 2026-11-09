package gcp

import (
	"net/http"
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
		// The project resource itself: projects.get is the one served method on
		// the bare projects/{project} path; a non-GET request there is not a
		// Discovery method and must fail loud, and the POST siblings
		// (setCommonInstanceMetadata, setDefaultNetworkTier, moveDisk, ...) stay
		// deferred.
		{"GET", "/compute/v1/projects/p", "ProjectsGet"},
		{"POST", "/compute/v1/projects/p/setCommonInstanceMetadata", "Unimplemented"},
		{"POST", "/compute/v1/projects/p", "Unimplemented"},
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

	// The project resource is addressed by the bare path: the codec records the
	// project segment and leaves the scope unset.
	nr, err = codec.Decode(httptest.NewRequest("GET", "/compute/v1/projects/p", nil), nil)
	if err != nil {
		t.Fatalf("decode project: %v", err)
	}
	if nr.Action != "ProjectsGet" || nr.Params["project"] != "p" {
		t.Errorf("project decode = %v / %v", nr.Action, nr.Params["project"])
	}
	if _, ok := nr.Params["scope"]; ok {
		t.Errorf("project decode must not record a scope: %v", nr.Params)
	}

	// A path with no project is a 404, like every other compute resource path:
	// an empty segment must not fall back to the caller's default project.
	if _, err := codec.Decode(httptest.NewRequest("GET", "/compute/v1/projects/", nil), nil); err == nil {
		t.Fatal("expected an error for an empty project segment")
	}
}

func TestComputeCodecImages(t *testing.T) {
	codec := &ComputeCodec{Service: "compute"}

	nr, err := codec.Decode(httptest.NewRequest("GET", "/compute/v1/projects/p/global/images/debian-12-bookworm-v20241009", nil), nil)
	if err != nil {
		t.Fatalf("decode image: %v", err)
	}
	if nr.Action != "ImagesGet" {
		t.Errorf("image action = %q, want ImagesGet", nr.Action)
	}
	if nr.Params["image"] != "debian-12-bookworm-v20241009" || nr.Params["scope"] != "global" {
		t.Errorf("image params = %v", nr.Params)
	}
	if _, ok := nr.Params["family"]; ok {
		t.Errorf("images.get must not record a family: %v", nr.Params)
	}

	nr, err = codec.Decode(httptest.NewRequest("GET", "/compute/v1/projects/p/global/images/family/debian-12", nil), nil)
	if err != nil {
		t.Fatalf("decode image family: %v", err)
	}
	if nr.Action != "ImagesGetFromFamily" || nr.Params["family"] != "debian-12" {
		t.Errorf("image family decode = %q / %v", nr.Action, nr.Params)
	}
	if _, ok := nr.Params["image"]; ok {
		t.Errorf("images.getFromFamily must not record an image name: %v", nr.Params)
	}

	// The rest of the images resource is not served: list, insert, delete and
	// patch resolve to the fail-loud Unimplemented action rather than a 404.
	for _, req := range []*http.Request{
		httptest.NewRequest("GET", "/compute/v1/projects/p/global/images", nil),
		httptest.NewRequest("POST", "/compute/v1/projects/p/global/images", nil),
		httptest.NewRequest("DELETE", "/compute/v1/projects/p/global/images/i", nil),
		httptest.NewRequest("PATCH", "/compute/v1/projects/p/global/images/i", nil),
	} {
		nr, err := codec.Decode(req, nil)
		if err != nil {
			t.Fatalf("decode %s %s: %v", req.Method, req.URL.Path, err)
		}
		if nr.Action != "Unimplemented" {
			t.Errorf("%s %s action = %q, want Unimplemented", req.Method, req.URL.Path, nr.Action)
		}
	}

	// Images are global: the zonal/regional spellings are not served.
	nr, err = codec.Decode(httptest.NewRequest("GET", "/compute/v1/projects/p/zones/us-central1-a/images/i", nil), nil)
	if err != nil {
		t.Fatalf("decode zonal image: %v", err)
	}
	if nr.Action != "Unimplemented" {
		t.Errorf("zonal image action = %q, want Unimplemented", nr.Action)
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
