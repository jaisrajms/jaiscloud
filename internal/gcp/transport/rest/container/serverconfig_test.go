package container

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	core "jaiscloud/internal/gcp/service/container"
	containerstore "jaiscloud/internal/gcp/store/container"
)

// TestCodecRoutesGetServerConfig locks both documented paths:
// projects.locations.getServerConfig and the legacy
// projects.zones.getServerconfig (Discovery container.json).
func TestCodecRoutesGetServerConfig(t *testing.T) {
	codec := NewCodec()
	cases := []struct {
		path     string
		location string
	}{
		{"/v1/projects/p/locations/us-central1/serverConfig", "us-central1"},
		{"/container/v1/projects/p/locations/europe-west1/serverConfig", "europe-west1"},
		{"/v1/projects/p/zones/us-central1-a/serverconfig", "us-central1-a"},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			nr, err := codec.Decode(httptest.NewRequest(http.MethodGet, tc.path, nil), nil)
			if err != nil {
				t.Fatalf("Decode(%s): %v", tc.path, err)
			}
			if nr.Action != "GetServerConfig" {
				t.Fatalf("action = %q, want GetServerConfig", nr.Action)
			}
			if nr.Params["project"] != "p" || nr.Params["location"] != tc.location {
				t.Fatalf("params = %v", nr.Params)
			}
		})
	}
}

// TestCodecRejectsUnsupportedZonePaths keeps the legacy branch narrow: only the
// documented zones.getServerconfig is served, so the other deprecated
// zone-scoped methods stay 404 instead of becoming accidental aliases.
func TestCodecRejectsUnsupportedZonePaths(t *testing.T) {
	codec := NewCodec()
	for _, path := range []string{
		"/v1/projects/p/zones/us-central1-a/clusters",
		"/v1/projects/p/zones/us-central1-a/clusters/c1",
		// The locations spelling of an unknown method is not a zone alias.
		"/v1/projects/p/locations/us-central1/serverconfig",
	} {
		if _, err := codec.Decode(httptest.NewRequest(http.MethodGet, path, nil), nil); err == nil {
			t.Fatalf("Decode(%s) succeeded, want unsupported", path)
		}
	}
}

func TestProviderGetServerConfig(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	resp, err := p.GetServerConfig(ctx, req(map[string]any{"project": "p", "location": "us-central1"}))
	if err != nil {
		t.Fatalf("GetServerConfig: %v", err)
	}
	if resp.Data["defaultClusterVersion"] != core.DefaultMasterVersion {
		t.Fatalf("defaultClusterVersion = %v", resp.Data["defaultClusterVersion"])
	}
	if resp.Data["defaultImageType"] != "COS_CONTAINERD" {
		t.Fatalf("defaultImageType = %v", resp.Data["defaultImageType"])
	}
	images, ok := resp.Data["validImageTypes"].([]string)
	if !ok || len(images) == 0 || images[0] != "COS_CONTAINERD" {
		t.Fatalf("validImageTypes = %v", resp.Data["validImageTypes"])
	}
	masters, ok := resp.Data["validMasterVersions"].([]string)
	if !ok || len(masters) != 1 || masters[0] != core.DefaultMasterVersion {
		t.Fatalf("validMasterVersions = %v", resp.Data["validMasterVersions"])
	}
	channels, ok := resp.Data["channels"].([]any)
	if !ok || len(channels) != 4 {
		t.Fatalf("channels = %v", resp.Data["channels"])
	}
	first := channels[0].(map[string]any)
	if first["channel"] != "RAPID" || first["defaultVersion"] != core.DefaultMasterVersion {
		t.Fatalf("channels[0] = %v", first)
	}
	if _, err := p.GetServerConfig(ctx, req(map[string]any{"project": "", "location": ""})); err == nil {
		t.Fatal("empty project/location accepted")
	}
}

// TestProviderNodeConfigAcceleratorsRoundTrip is the REST half of the DPG25
// round-trip: accelerators and bootDiskKmsKey are decoded, stored and echoed with
// acceleratorCount spelled as an int64 string, as Discovery types it.
func TestProviderNodeConfigAcceleratorsRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	const kmsKey = "projects/p/locations/global/keyRings/ring/cryptoKeys/boot"
	body := map[string]any{"cluster": map[string]any{
		"name":             "gpu",
		"initialNodeCount": float64(1),
		"nodeConfig": map[string]any{
			"machineType": "n1-standard-1",
			"accelerators": []any{map[string]any{
				"acceleratorCount": "1",
				"acceleratorType":  "nvidia-tesla-t4",
				"gpuPartitionSize": "1g.5gb",
			}},
			"bootDiskKmsKey": kmsKey,
		},
	}}
	if _, err := p.CreateCluster(ctx, req(map[string]any{"project": "p", "location": "us-central1", "body": body})); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	resp, err := p.GetCluster(ctx, req(map[string]any{"project": "p", "location": "us-central1", "cluster": "gpu"}))
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	cfg, ok := resp.Data["nodeConfig"].(map[string]any)
	if !ok {
		t.Fatalf("nodeConfig = %v", resp.Data["nodeConfig"])
	}
	if cfg["bootDiskKmsKey"] != kmsKey {
		t.Fatalf("bootDiskKmsKey = %v", cfg["bootDiskKmsKey"])
	}
	accels, ok := cfg["accelerators"].([]any)
	if !ok || len(accels) != 1 {
		t.Fatalf("accelerators = %v", cfg["accelerators"])
	}
	accel := accels[0].(map[string]any)
	if accel["acceleratorType"] != "nvidia-tesla-t4" || accel["gpuPartitionSize"] != "1g.5gb" {
		t.Fatalf("accelerator = %v", accel)
	}
	if accel["acceleratorCount"] != strconv.Itoa(1) {
		t.Fatalf("acceleratorCount = %v (%T), want the int64 string \"1\"", accel["acceleratorCount"], accel["acceleratorCount"])
	}
}

func TestProviderCreateNodePoolRejectsMalformedKmsKey(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	if _, err := p.CreateCluster(ctx, req(map[string]any{"project": "p", "location": "us-central1", "body": clusterBody("c1")})); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	body := map[string]any{"nodePool": map[string]any{
		"name":             "bad",
		"initialNodeCount": float64(1),
		"config":           map[string]any{"bootDiskKmsKey": "short-key"},
	}}
	_, err := p.CreateNodePool(ctx, req(map[string]any{"project": "p", "location": "us-central1", "cluster": "c1", "body": body}))
	if err == nil {
		t.Fatal("malformed bootDiskKmsKey accepted")
	}
}

// TestNodeConfigToJSONOmitsEmptyFields guards the encoder against emitting an
// empty accelerators list or an empty bootDiskKmsKey for a plain pool (a real GKE
// read omits both).
func TestNodeConfigToJSONOmitsEmptyFields(t *testing.T) {
	out := nodeConfigToJSON(&containerstore.NodeConfig{MachineType: "e2-medium"})
	if _, ok := out["accelerators"]; ok {
		t.Fatalf("empty accelerators rendered: %v", out)
	}
	if _, ok := out["bootDiskKmsKey"]; ok {
		t.Fatalf("empty bootDiskKmsKey rendered: %v", out)
	}
}
