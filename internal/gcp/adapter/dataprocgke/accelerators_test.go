package dataprocgke

import (
	"context"
	"errors"
	"testing"

	dataproccore "jaiscloud/internal/gcp/service/dataproc"
	containerstore "jaiscloud/internal/gcp/store/container"
)

const testKmsKey = "projects/proj/locations/global/keyRings/ring/cryptoKeys/boot"

// TestEnsureGKENodePool_MaterializesAccelerators is the DPG25 fix at the seam:
// the accelerators and boot-disk CMEK key the Dataproc core parsed are written
// onto the GKE pool it creates, so the pool read (the client-observable surface)
// carries them.
func TestEnsureGKENodePool_MaterializesAccelerators(t *testing.T) {
	a, core := newAdapter(t)
	createCluster(t, core, containerstore.Cluster{Name: "gke-1"})
	ctx := context.Background()
	cluster := dataproccore.GKECluster{Project: "proj", Location: "us-central1", Name: "gke-1"}

	s := spec()
	s.Accelerators = []dataproccore.GKENodePoolAccelerator{{
		AcceleratorType:  "nvidia-tesla-t4",
		AcceleratorCount: 1,
		GpuPartitionSize: "1g.5gb",
	}}
	s.BootDiskKmsKey = testKmsKey
	if _, err := a.EnsureGKENodePool(ctx, cluster, s); err != nil {
		t.Fatalf("EnsureGKENodePool: %v", err)
	}
	pool, err := core.GetNodePool(ctx, "proj", "us-central1", "gke-1", "spark-pool")
	if err != nil {
		t.Fatalf("GetNodePool: %v", err)
	}
	if len(pool.Config.Accelerators) != 1 {
		t.Fatalf("accelerators not materialized: %+v", pool.Config)
	}
	got := pool.Config.Accelerators[0]
	if got.AcceleratorType != "nvidia-tesla-t4" || got.AcceleratorCount != 1 || got.GpuPartitionSize != "1g.5gb" {
		t.Fatalf("accelerator = %+v", got)
	}
	if pool.Config.BootDiskKmsKey != testKmsKey {
		t.Fatalf("bootDiskKmsKey = %q", pool.Config.BootDiskKmsKey)
	}

	// Verifying the same shape against the pool it created succeeds.
	if _, err := a.EnsureGKENodePool(ctx, cluster, s); err != nil {
		t.Fatalf("verifying the created shape: %v", err)
	}
}

// TestEnsureGKENodePool_VerifiesAccelerators pins the verify half: an existing
// pool whose accelerators or boot-disk key differ is a mismatch naming the field,
// and an existing pool without them is a mismatch when the caller named some.
func TestEnsureGKENodePool_VerifiesAccelerators(t *testing.T) {
	a, core := newAdapter(t)
	createCluster(t, core, containerstore.Cluster{
		Name: "gke-1",
		NodePools: []containerstore.NodePool{{
			Name: "existing",
			Config: &containerstore.NodeConfig{
				MachineType:    "n1-standard-4",
				BootDiskKmsKey: "projects/proj/locations/global/keyRings/ring/cryptoKeys/other",
				Accelerators: []containerstore.AcceleratorConfig{{
					AcceleratorType: "nvidia-tesla-t4", AcceleratorCount: 1,
				}},
			},
		}},
	})
	ctx := context.Background()
	cluster := dataproccore.GKECluster{Project: "proj", Location: "us-central1", Name: "gke-1"}

	s := spec()
	s.Name = "existing"
	s.Locations = nil
	s.LocalSsdCount = 0
	s.ServiceAccount = ""
	s.Autoscaling = nil
	s.Accelerators = []dataproccore.GKENodePoolAccelerator{{
		AcceleratorType: "nvidia-tesla-t4", AcceleratorCount: 1,
	}}
	s.BootDiskKmsKey = testKmsKey

	_, err := a.EnsureGKENodePool(ctx, cluster, s)
	var mismatch *dataproccore.GKENodePoolMismatchError
	if !errors.As(err, &mismatch) || mismatch.Field != "bootDiskKmsKey" {
		t.Fatalf("expected a bootDiskKmsKey mismatch, got %v", err)
	}

	// The caller naming no key leaves the existing one unasserted.
	s.BootDiskKmsKey = ""
	if _, err := a.EnsureGKENodePool(ctx, cluster, s); err != nil {
		t.Fatalf("matching shape rejected: %v", err)
	}

	// A different accelerator count is a mismatch on the accelerator list.
	s.Accelerators = []dataproccore.GKENodePoolAccelerator{{AcceleratorType: "nvidia-tesla-t4", AcceleratorCount: 2}}
	_, err = a.EnsureGKENodePool(ctx, cluster, s)
	if !errors.As(err, &mismatch) || mismatch.Field != "accelerators" {
		t.Fatalf("expected an accelerators mismatch, got %v", err)
	}
}

// TestEnsureGKENodePool_RejectsMalformedKmsKey proves the GKE core's validation
// still guards a pool the Dataproc reconciler creates: the create fails instead
// of storing a key nobody can use.
func TestEnsureGKENodePool_RejectsMalformedKmsKey(t *testing.T) {
	a, core := newAdapter(t)
	createCluster(t, core, containerstore.Cluster{Name: "gke-1"})
	ctx := context.Background()
	cluster := dataproccore.GKECluster{Project: "proj", Location: "us-central1", Name: "gke-1"}

	s := spec()
	s.BootDiskKmsKey = "short-key"
	if _, err := a.EnsureGKENodePool(ctx, cluster, s); err == nil {
		t.Fatal("malformed bootDiskKmsKey accepted")
	}
	if _, err := core.GetNodePool(ctx, "proj", "us-central1", "gke-1", "spark-pool"); err == nil {
		t.Fatal("rejected pool was stored")
	}
}
