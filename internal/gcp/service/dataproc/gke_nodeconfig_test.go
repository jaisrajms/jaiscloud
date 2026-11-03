package dataproc

import (
	"context"
	"encoding/json"
	"testing"

	dataprocstore "jaiscloud/internal/gcp/store/dataproc"
	"jaiscloud/internal/store"
)

// gkeKmsKey is a boot-disk CMEK key name in the form Discovery documents.
const gkeKmsKey = "projects/proj/locations/global/keyRings/ring/cryptoKeys/boot"

// acceleratorsGKE builds a gkeClusterTarget plus one nodePoolTarget with the
// DPG25 node-config fields. acceleratorCount is a JSON string, as Discovery types
// the container int64.
func acceleratorsGKE(pool string, config map[string]any) map[string]any {
	return map[string]any{
		"gkeClusterTarget": gkeClusterPath,
		"nodePoolTarget":   []any{poolTarget(pool, []string{"DEFAULT"}, map[string]any{"config": config})},
	}
}

// TestGKEPoolConfigCarriesAcceleratorsAndBootDiskKmsKey is the Dataproc half of
// the DPG25 round-trip: GkeNodePoolConfig.config.accelerators and
// bootDiskKmsKey are parsed, carried on the provisioner spec, and not dropped.
func TestGKEPoolConfigCarriesAcceleratorsAndBootDiskKmsKey(t *testing.T) {
	raw, err := json.Marshal(vccWith(acceleratorsGKE("gpu", map[string]any{
		"machineType": "n1-standard-1",
		"accelerators": []any{map[string]any{
			"acceleratorCount": "2",
			"acceleratorType":  "nvidia-tesla-t4",
			"gpuPartitionSize": "1g.5gb",
		}},
		"bootDiskKmsKey": gkeKmsKey,
	})))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	v, err := validateVirtualClusterConfig(raw, "dp-gke")
	if err != nil {
		t.Fatalf("validateVirtualClusterConfig: %v", err)
	}
	spec := gkeNodePoolSpec(v.poolTargets[0], "dp-gke", "uuid", "us-central1", nil)
	if len(spec.Accelerators) != 1 {
		t.Fatalf("spec accelerators = %+v", spec.Accelerators)
	}
	a := spec.Accelerators[0]
	if a.AcceleratorType != "nvidia-tesla-t4" || a.AcceleratorCount != 2 || a.GpuPartitionSize != "1g.5gb" {
		t.Fatalf("spec accelerator = %+v", a)
	}
	if spec.BootDiskKmsKey != gkeKmsKey {
		t.Fatalf("spec bootDiskKmsKey = %q", spec.BootDiskKmsKey)
	}
}

// TestGKEPoolConfigWithoutAcceleratorsStaysEmpty keeps the fields absent when the
// caller named none, so the verify path does not assert a shape nobody asked for.
func TestGKEPoolConfigWithoutAcceleratorsStaysEmpty(t *testing.T) {
	raw, err := json.Marshal(vccWith(acceleratorsGKE("plain", map[string]any{"machineType": "e2-medium"})))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	v, err := validateVirtualClusterConfig(raw, "dp-gke")
	if err != nil {
		t.Fatalf("validateVirtualClusterConfig: %v", err)
	}
	spec := gkeNodePoolSpec(v.poolTargets[0], "dp-gke", "uuid", "us-central1", nil)
	if spec.Accelerators != nil || spec.BootDiskKmsKey != "" {
		t.Fatalf("unexpected pool shape: %+v", spec)
	}
}

// TestReconcilePassesAcceleratorsToProvisioner proves the parsed fields reach the
// provisioner for the pool a virtual cluster reconciles.
func TestReconcilePassesAcceleratorsToProvisioner(t *testing.T) {
	stub := &fakeGKENodePoolProvisioner{gke: GKECluster{WorkloadIdentityPool: "proj.svc.id.goog"}}
	p := NewService(dataprocstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithGKENodePoolProvisioner(stub))
	raw, err := json.Marshal(vccWith(acceleratorsGKE("gpu", map[string]any{
		"accelerators":   []any{map[string]any{"acceleratorCount": "1", "acceleratorType": "nvidia-tesla-t4"}},
		"bootDiskKmsKey": gkeKmsKey,
	})))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, _, err := p.CreateCluster(context.Background(), "proj", "us-central1", "dp-gke", ClusterInput{VirtualClusterConfig: raw}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if len(stub.specs) != 1 {
		t.Fatalf("provisioner calls = %d", len(stub.specs))
	}
	spec := stub.specs[0]
	if len(spec.Accelerators) != 1 || spec.Accelerators[0].AcceleratorType != "nvidia-tesla-t4" {
		t.Fatalf("spec accelerators = %+v", spec.Accelerators)
	}
	if spec.BootDiskKmsKey != gkeKmsKey {
		t.Fatalf("spec bootDiskKmsKey = %q", spec.BootDiskKmsKey)
	}
}
