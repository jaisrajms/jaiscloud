package dataprocgke

import (
	"context"
	"errors"
	"testing"

	containercore "jaiscloud/internal/gcp/service/container"
	dataproccore "jaiscloud/internal/gcp/service/dataproc"
	containerstore "jaiscloud/internal/gcp/store/container"
	"jaiscloud/internal/model"
)

// newAdapter returns an adapter over a GKE core with one cluster created.
func newAdapter(t *testing.T) (*Adapter, *containercore.Service) {
	t.Helper()
	core := containercore.NewService(containerstore.NewMemoryStore())
	return New(core), core
}

func createCluster(t *testing.T, core *containercore.Service, c containerstore.Cluster) {
	t.Helper()
	if _, err := core.CreateCluster(context.Background(), "proj", "us-central1", c); err != nil {
		t.Fatalf("create GKE cluster: %v", err)
	}
}

func TestResolveGKETarget(t *testing.T) {
	a, core := newAdapter(t)
	createCluster(t, core, containerstore.Cluster{
		Name:                   "gke-1",
		Autopilot:              &containerstore.Autopilot{Enabled: true},
		WorkloadIdentityConfig: &containerstore.WorkloadIdentityConfig{WorkloadPool: "proj.svc.id.goog"},
	})
	got, err := a.ResolveGKETarget(context.Background(), "proj", "us-central1", "gke-1")
	if err != nil {
		t.Fatalf("ResolveGKETarget: %v", err)
	}
	if !got.Autopilot {
		t.Fatal("Autopilot flag not reported")
	}
	if got.WorkloadIdentityPool != "proj.svc.id.goog" {
		t.Fatalf("workload pool not reported: %q", got.WorkloadIdentityPool)
	}
	if got.Project != "proj" || got.Location != "us-central1" || got.Name != "gke-1" {
		t.Fatalf("identity wrong: %+v", got)
	}
}

func TestResolveGKETarget_NoWorkloadIdentity(t *testing.T) {
	a, core := newAdapter(t)
	createCluster(t, core, containerstore.Cluster{Name: "gke-plain"})
	got, err := a.ResolveGKETarget(context.Background(), "proj", "us-central1", "gke-plain")
	if err != nil {
		t.Fatalf("ResolveGKETarget: %v", err)
	}
	if got.WorkloadIdentityPool != "" {
		t.Fatalf("expected no workload pool, got %q", got.WorkloadIdentityPool)
	}
}

func TestResolveGKETarget_NotFound(t *testing.T) {
	a, _ := newAdapter(t)
	_, err := a.ResolveGKETarget(context.Background(), "proj", "us-central1", "missing")
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.Code != "NotFound" {
		t.Fatalf("expected NotFound, got %v", err)
	}
}

func spec() dataproccore.GKENodePoolSpec {
	return dataproccore.GKENodePoolSpec{
		Name:           "spark-pool",
		MachineType:    "n1-standard-4",
		LocalSsdCount:  2,
		Autoscaling:    &dataproccore.GKENodePoolAutoscaling{MinNodeCount: 1, MaxNodeCount: 5},
		Locations:      []string{"us-central1-a"},
		ServiceAccount: "sa@proj.iam.gserviceaccount.com",
		Labels:         map[string]string{"goog-dataproc-cluster-name": "dp-gke"},
		Verify:         true,
	}
}

func TestEnsureGKENodePool_CreatesWithShape(t *testing.T) {
	a, core := newAdapter(t)
	createCluster(t, core, containerstore.Cluster{Name: "gke-1"})
	ctx := context.Background()
	cluster := dataproccore.GKECluster{Project: "proj", Location: "us-central1", Name: "gke-1"}

	created, err := a.EnsureGKENodePool(ctx, cluster, spec())
	if err != nil {
		t.Fatalf("EnsureGKENodePool: %v", err)
	}
	if !created {
		t.Fatal("expected the pool to be created")
	}
	pool, err := core.GetNodePool(ctx, "proj", "us-central1", "gke-1", "spark-pool")
	if err != nil {
		t.Fatalf("GetNodePool: %v", err)
	}
	if pool.Config == nil || pool.Config.MachineType != "n1-standard-4" || pool.Config.LocalSsdCount != 2 {
		t.Fatalf("pool config not applied: %+v", pool.Config)
	}
	if pool.Config.ServiceAccount != "sa@proj.iam.gserviceaccount.com" {
		t.Fatalf("service account not applied: %+v", pool.Config)
	}
	if pool.Config.Labels["goog-dataproc-cluster-name"] != "dp-gke" {
		t.Fatalf("auto labels not applied: %v", pool.Config.Labels)
	}
	if pool.Autoscaling == nil || !pool.Autoscaling.Enabled || pool.Autoscaling.MaxNodeCount != 5 {
		t.Fatalf("autoscaling not applied: %+v", pool.Autoscaling)
	}
	if len(pool.Locations) != 1 || pool.Locations[0] != "us-central1-a" {
		t.Fatalf("locations not applied: %v", pool.Locations)
	}

	// A second call now verifies the existing pool instead of creating another.
	created, err = a.EnsureGKENodePool(ctx, cluster, spec())
	if err != nil {
		t.Fatalf("second EnsureGKENodePool: %v", err)
	}
	if created {
		t.Fatal("expected the existing pool to be reused")
	}
	pools, err := core.ListNodePools(ctx, "proj", "us-central1", "gke-1")
	if err != nil {
		t.Fatalf("ListNodePools: %v", err)
	}
	if len(pools) != 2 { // the GKE mock's default-pool + the reconciled pool
		t.Fatalf("unexpected pool count: %d", len(pools))
	}
}

func TestEnsureGKENodePool_VerifiesExistingShape(t *testing.T) {
	a, core := newAdapter(t)
	createCluster(t, core, containerstore.Cluster{
		Name: "gke-1",
		NodePools: []containerstore.NodePool{{
			Name:   "existing",
			Config: &containerstore.NodeConfig{MachineType: "e2-medium"},
		}},
	})
	ctx := context.Background()
	cluster := dataproccore.GKECluster{Project: "proj", Location: "us-central1", Name: "gke-1"}
	bad := spec()
	bad.Name = "existing"
	bad.Locations = nil
	bad.LocalSsdCount = 0
	bad.ServiceAccount = ""
	bad.Autoscaling = nil

	_, err := a.EnsureGKENodePool(ctx, cluster, bad)
	var mismatch *dataproccore.GKENodePoolMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("expected a mismatch error, got %v", err)
	}
	if mismatch.Pool != "existing" || mismatch.Field != "machineType" {
		t.Fatalf("mismatch names the wrong pool/field: %+v", mismatch)
	}
	if mismatch.Want != "n1-standard-4" || mismatch.Got != "e2-medium" {
		t.Fatalf("mismatch values wrong: %+v", mismatch)
	}

	// A shape that matches is accepted.
	good := bad
	good.MachineType = "e2-medium"
	if _, err := a.EnsureGKENodePool(ctx, cluster, good); err != nil {
		t.Fatalf("matching shape rejected: %v", err)
	}
}

// TestEnsureGKENodePool_ComparesScalarsAndAutoscalerEnabled pins the verify
// contract (DPG54): proto3 scalars have no presence, so a caller's zero value is
// part of the requested shape and is always compared, and an autoscaled request
// requires the existing pool's autoscaler to be enabled, not just to carry bounds.
func TestEnsureGKENodePool_ComparesScalarsAndAutoscalerEnabled(t *testing.T) {
	a, core := newAdapter(t)
	createCluster(t, core, containerstore.Cluster{
		Name: "gke-1",
		NodePools: []containerstore.NodePool{{
			Name:        "scalar",
			Config:      &containerstore.NodeConfig{MachineType: "n1-standard-4", LocalSsdCount: 2, Spot: true},
			Autoscaling: &containerstore.NodePoolAutoscaling{Enabled: false, MinNodeCount: 1, MaxNodeCount: 5},
		}},
	})
	ctx := context.Background()
	cluster := dataproccore.GKECluster{Project: "proj", Location: "us-central1", Name: "gke-1"}

	// spot:false (the caller omitted it) is compared against the spot pool.
	s := spec()
	s.Name = "scalar"
	_, err := a.EnsureGKENodePool(ctx, cluster, s)
	var mismatch *dataproccore.GKENodePoolMismatchError
	if !errors.As(err, &mismatch) || mismatch.Field != "spot" {
		t.Fatalf("expected a spot mismatch, got %v", err)
	}

	// With the scalar shape agreed, a disabled autoscaler is still a mismatch.
	s.Spot = true
	s.ServiceAccount = ""
	_, err = a.EnsureGKENodePool(ctx, cluster, s)
	if !errors.As(err, &mismatch) || mismatch.Field != "autoscaling" {
		t.Fatalf("expected an autoscaling mismatch for a disabled autoscaler, got %v", err)
	}
}

func TestEnsureGKENodePool_AdoptsWithoutConfig(t *testing.T) {
	a, core := newAdapter(t)
	createCluster(t, core, containerstore.Cluster{
		Name:      "gke-1",
		NodePools: []containerstore.NodePool{{Name: "adopt-me", Config: &containerstore.NodeConfig{MachineType: "n2-standard-8"}}},
	})
	ctx := context.Background()
	cluster := dataproccore.GKECluster{Project: "proj", Location: "us-central1", Name: "gke-1"}
	created, err := a.EnsureGKENodePool(ctx, cluster, dataproccore.GKENodePoolSpec{
		Name: "adopt-me",
	})
	if err != nil {
		t.Fatalf("adopting an existing pool must not verify it: %v", err)
	}
	if created {
		t.Fatal("expected adoption, not creation")
	}
}

func TestEnsureGKENodePool_CreatesDefaultShapeWhenAbsent(t *testing.T) {
	a, core := newAdapter(t)
	createCluster(t, core, containerstore.Cluster{Name: "gke-1"})
	ctx := context.Background()
	cluster := dataproccore.GKECluster{Project: "proj", Location: "us-central1", Name: "gke-1"}
	created, err := a.EnsureGKENodePool(ctx, cluster, dataproccore.GKENodePoolSpec{Name: "service-made"})
	if err != nil {
		t.Fatalf("EnsureGKENodePool: %v", err)
	}
	if !created {
		t.Fatal("expected creation")
	}
	pool, err := core.GetNodePool(ctx, "proj", "us-central1", "gke-1", "service-made")
	if err != nil {
		t.Fatalf("GetNodePool: %v", err)
	}
	if pool.InitialNodeCount < 1 {
		t.Fatalf("created pool must have a size: %+v", pool)
	}
}

func TestEnsureGKENodePool_UnknownCluster(t *testing.T) {
	a, _ := newAdapter(t)
	_, err := a.EnsureGKENodePool(context.Background(),
		dataproccore.GKECluster{Project: "proj", Location: "us-central1", Name: "missing"}, spec())
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.Code != "NotFound" {
		t.Fatalf("expected NotFound for an unknown cluster, got %v", err)
	}
}
