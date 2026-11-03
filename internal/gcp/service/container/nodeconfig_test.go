package container

import (
	"context"
	"testing"

	containerstore "jaiscloud/internal/gcp/store/container"
)

// TestNodePoolStoresAcceleratorsAndBootDiskKmsKey is the DPG25 round-trip: the
// fields are accepted, validated, stored and echoed on read.
func TestNodePoolStoresAcceleratorsAndBootDiskKmsKey(t *testing.T) {
	ctx := context.Background()
	svc := newTestService()
	const kmsKey = "projects/p/locations/global/keyRings/ring/cryptoKeys/boot"
	cfg := &containerstore.NodeConfig{
		MachineType:    "n1-standard-1",
		Accelerators:   []containerstore.AcceleratorConfig{{AcceleratorCount: 1, AcceleratorType: "nvidia-tesla-t4", GpuPartitionSize: "1g.5gb"}},
		BootDiskKmsKey: kmsKey,
	}
	pool := containerstore.NodePool{Name: "gpu-pool", InitialNodeCount: 1, Config: cfg}

	if _, err := svc.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1", NodePools: []containerstore.NodePool{pool}}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	got, err := svc.GetNodePool(ctx, "p", "us-central1", "c1", "gpu-pool")
	if err != nil {
		t.Fatalf("GetNodePool: %v", err)
	}
	if len(got.Config.Accelerators) != 1 {
		t.Fatalf("accelerators = %+v", got.Config.Accelerators)
	}
	a := got.Config.Accelerators[0]
	if a.AcceleratorType != "nvidia-tesla-t4" || a.AcceleratorCount != 1 || a.GpuPartitionSize != "1g.5gb" {
		t.Fatalf("accelerator = %+v", a)
	}
	if got.Config.BootDiskKmsKey != kmsKey {
		t.Fatalf("bootDiskKmsKey = %q, want %q", got.Config.BootDiskKmsKey, kmsKey)
	}
	// The cluster read (and the cluster-level nodeConfig echo derived from the
	// first pool) renders the same fields.
	c, err := svc.GetCluster(ctx, "p", "us-central1", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if len(c.NodePools) != 1 || c.NodePools[0].Config.BootDiskKmsKey != kmsKey {
		t.Fatalf("cluster nodePools = %+v", c.NodePools)
	}
	if c.NodeConfig == nil || len(c.NodeConfig.Accelerators) != 1 {
		t.Fatalf("cluster nodeConfig = %+v", c.NodeConfig)
	}
}

// TestCreateClusterDefaultPoolCarriesAccelerators covers the legacy create shape:
// nodeConfig (with accelerators + bootDiskKmsKey) and initialNodeCount generate
// the default-pool, which must keep both fields.
func TestCreateClusterDefaultPoolCarriesAccelerators(t *testing.T) {
	ctx := context.Background()
	svc := newTestService()
	const kmsKey = "projects/p/locations/us-central1/keyRings/r/cryptoKeys/k"
	_, err := svc.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{
		Name:             "legacy",
		InitialNodeCount: 1,
		NodeConfig: &containerstore.NodeConfig{
			Accelerators:   []containerstore.AcceleratorConfig{{AcceleratorCount: 2, AcceleratorType: "nvidia-tesla-a100"}},
			BootDiskKmsKey: kmsKey,
		},
	})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	got, err := svc.GetNodePool(ctx, "p", "us-central1", "legacy", DefaultNodePoolName)
	if err != nil {
		t.Fatalf("GetNodePool: %v", err)
	}
	if len(got.Config.Accelerators) != 1 || got.Config.Accelerators[0].AcceleratorCount != 2 {
		t.Fatalf("accelerators = %+v", got.Config.Accelerators)
	}
	if got.Config.BootDiskKmsKey != kmsKey {
		t.Fatalf("bootDiskKmsKey = %q", got.Config.BootDiskKmsKey)
	}
}

func TestNodePoolRejectsMalformedAcceleratorsAndKmsKey(t *testing.T) {
	ctx := context.Background()
	svc := newTestService()
	if _, err := svc.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1", InitialNodeCount: 1}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	cases := []struct {
		name string
		cfg  *containerstore.NodeConfig
	}{
		{"bootDiskKmsKey not a resource name", &containerstore.NodeConfig{BootDiskKmsKey: "boot-key"}},
		{"bootDiskKmsKey missing the key segment", &containerstore.NodeConfig{BootDiskKmsKey: "projects/p/locations/global/keyRings/r"}},
		{"accelerator without a type", &containerstore.NodeConfig{Accelerators: []containerstore.AcceleratorConfig{{AcceleratorCount: 1}}}},
		{"accelerator without a count", &containerstore.NodeConfig{Accelerators: []containerstore.AcceleratorConfig{{AcceleratorType: "nvidia-tesla-t4"}}}},
		{"accelerator with a negative count", &containerstore.NodeConfig{Accelerators: []containerstore.AcceleratorConfig{{AcceleratorType: "nvidia-tesla-t4", AcceleratorCount: -1}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.CreateNodePool(ctx, "p", "us-central1", "c1", containerstore.NodePool{
				Name: "bad-pool", InitialNodeCount: 1, Config: tc.cfg,
			})
			providerErr(t, err, "InvalidArgument")
			// Nothing was created: the rejected pool is not stored.
			if _, err := svc.GetNodePool(ctx, "p", "us-central1", "c1", "bad-pool"); err == nil {
				t.Fatal("rejected node pool was stored")
			}
		})
	}
}

// TestUpdateNodePoolAppliesAcceleratorsAndKmsKey covers the merge path: an update
// that sets the fields replaces the stored ones, and a malformed value is
// rejected by the same validation the create uses.
func TestUpdateNodePoolAppliesAcceleratorsAndKmsKey(t *testing.T) {
	ctx := context.Background()
	svc := newTestService()
	if _, err := svc.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1", InitialNodeCount: 1}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := svc.CreateNodePool(ctx, "p", "us-central1", "c1", containerstore.NodePool{
		Name: "pool", InitialNodeCount: 1, Config: &containerstore.NodeConfig{MachineType: "e2-medium"},
	}); err != nil {
		t.Fatalf("CreateNodePool: %v", err)
	}
	const kmsKey = "projects/p/locations/global/keyRings/ring/cryptoKeys/boot"
	if _, err := svc.UpdateNodePool(ctx, "p", "us-central1", "c1", containerstore.NodePool{
		Name: "pool",
		Config: &containerstore.NodeConfig{
			Accelerators:   []containerstore.AcceleratorConfig{{AcceleratorCount: 1, AcceleratorType: "nvidia-tesla-t4"}},
			BootDiskKmsKey: kmsKey,
		},
	}); err != nil {
		t.Fatalf("UpdateNodePool: %v", err)
	}
	got, err := svc.GetNodePool(ctx, "p", "us-central1", "c1", "pool")
	if err != nil {
		t.Fatalf("GetNodePool: %v", err)
	}
	if len(got.Config.Accelerators) != 1 || got.Config.BootDiskKmsKey != kmsKey {
		t.Fatalf("config after update = %+v", got.Config)
	}
	if got.Config.MachineType != "e2-medium" {
		t.Fatalf("merge dropped machineType: %q", got.Config.MachineType)
	}

	// The same validation guards an update.
	_, err = svc.UpdateNodePool(ctx, "p", "us-central1", "c1", containerstore.NodePool{
		Name: "pool", Config: &containerstore.NodeConfig{BootDiskKmsKey: "not-a-key"},
	})
	providerErr(t, err, "InvalidArgument")
}
