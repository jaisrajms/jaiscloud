package container

import (
	"context"
	"errors"
	"strings"
	"testing"

	containerstore "jaiscloud/internal/gcp/store/container"
	"jaiscloud/internal/model"
)

func newTestService() *Service {
	return NewService(containerstore.NewMemoryStore())
}

func providerErr(t *testing.T, err error, wantCode string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want %s error, got nil", wantCode)
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("want *model.ProviderError, got %T (%v)", err, err)
	}
	if pe.Code != wantCode {
		t.Fatalf("error code = %q, want %q (%v)", pe.Code, wantCode, err)
	}
}

func TestCreateClusterReturnsOperationAndStoresCluster(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	op, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1", InitialNodeCount: 3})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if op.OperationType != containerstore.OperationCreateCluster || op.Status != containerstore.OperationStatusDone {
		t.Fatalf("operation = %+v", op)
	}
	if !strings.HasPrefix(op.Name, "operation-") {
		t.Fatalf("operation name = %q, want operation- prefix", op.Name)
	}
	if op.SelfLink != "projects/p/locations/us-central1/operations/"+op.Name {
		t.Fatalf("operation selfLink = %q", op.SelfLink)
	}

	c, err := s.GetCluster(ctx, "p", "us-central1", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if c.Status != containerstore.StatusRunning {
		t.Fatalf("status = %q, want RUNNING", c.Status)
	}
	if c.CurrentMasterVersion != DefaultMasterVersion {
		t.Fatalf("master version = %q, want %q", c.CurrentMasterVersion, DefaultMasterVersion)
	}
	if c.Network != "default" || c.Subnetwork != "default" {
		t.Fatalf("network/subnetwork = %q/%q", c.Network, c.Subnetwork)
	}
	if len(c.NodePools) != 1 || c.NodePools[0].Name != "default-pool" || c.NodePools[0].InitialNodeCount != 3 {
		t.Fatalf("nodePools = %+v", c.NodePools)
	}
	if c.SelfLink != "projects/p/locations/us-central1/clusters/c1" {
		t.Fatalf("cluster selfLink = %q", c.SelfLink)
	}
}

func TestCreateClusterMaterializesCallerNodePools(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	_, err := s.CreateCluster(ctx, "p", "l", containerstore.Cluster{
		Name: "c1",
		NodePools: []containerstore.NodePool{
			{Name: "pool-a", InitialNodeCount: 2},
			{Name: "pool-b", InitialNodeCount: 1, NodeCount: 5},
		},
	})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	c, err := s.GetCluster(ctx, "p", "l", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	// The caller's pools replace the default-pool real GKE only generates when
	// the request carries no nodePools.
	if len(c.NodePools) != 2 || c.NodePools[0].Name != "pool-a" || c.NodePools[1].Name != "pool-b" {
		t.Fatalf("nodePools = %+v", c.NodePools)
	}
	a, b := c.NodePools[0], c.NodePools[1]
	if a.Status != containerstore.StatusRunning || b.Status != containerstore.StatusRunning {
		t.Fatalf("pool status = %q/%q, want RUNNING", a.Status, b.Status)
	}
	if a.SelfLink != "projects/p/locations/l/clusters/c1/nodePools/pool-a" {
		t.Fatalf("pool-a selfLink = %q", a.SelfLink)
	}
	if a.Version != DefaultMasterVersion || b.Version != DefaultMasterVersion {
		t.Fatalf("pool versions = %q/%q, want %q", a.Version, b.Version, DefaultMasterVersion)
	}
	if a.NodeCount != 2 || b.NodeCount != 5 {
		t.Fatalf("node counts = %d/%d, want 2/5", a.NodeCount, b.NodeCount)
	}
}

func TestCreateClusterRejectsDefaultPoolFieldsWithNodePools(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	// Discovery: nodePools "should not be set if node_config or
	// initial_node_count are specified"; real GKE answers each combination with
	// a 400 naming the offending field, and node_config wins when both are set.
	cases := []struct {
		name    string
		cluster containerstore.Cluster
		want    string
	}{
		{
			name:    "initialNodeCount",
			cluster: containerstore.Cluster{Name: "c1", InitialNodeCount: 1, NodePools: []containerstore.NodePool{{Name: "pool-a", InitialNodeCount: 1}}},
			want:    initialNodeCountPoolConflict,
		},
		{
			name:    "nodeConfig",
			cluster: containerstore.Cluster{Name: "c1", NodeConfig: &containerstore.NodeConfig{MachineType: "e2-micro"}, NodePools: []containerstore.NodePool{{Name: "pool-a", InitialNodeCount: 1}}},
			want:    nodeConfigPoolConflict,
		},
		{
			name:    "both prefer nodeConfig",
			cluster: containerstore.Cluster{Name: "c1", InitialNodeCount: 1, NodeConfig: &containerstore.NodeConfig{MachineType: "e2-micro"}, NodePools: []containerstore.NodePool{{Name: "pool-a", InitialNodeCount: 1}}},
			want:    nodeConfigPoolConflict,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.CreateCluster(ctx, "p", "us-central1", tc.cluster)
			providerErr(t, err, "InvalidArgument")
			var pe *model.ProviderError
			if !errors.As(err, &pe) {
				t.Fatalf("want *model.ProviderError, got %T (%v)", err, err)
			}
			if pe.Message != tc.want {
				t.Fatalf("message = %q, want %q", pe.Message, tc.want)
			}
			if _, err := s.GetCluster(ctx, "p", "us-central1", "c1"); err == nil {
				t.Fatal("rejected create must persist no cluster")
			}
		})
	}
}

// TestCreateClusterNegativeInitialNodeCount pins the DPG49 boundary captured
// against real GKE: a negative cluster-level initialNodeCount is a 400 when the
// request carries no nodePools, but is accepted (and ignored) alongside them.
func TestCreateClusterNegativeInitialNodeCount(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	_, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1", InitialNodeCount: -1})
	providerErr(t, err, "InvalidArgument")
	var pe *model.ProviderError
	if !errors.As(err, &pe) || pe.Message != negativeInitialNodeCount {
		t.Fatalf("message = %v, want %q", err, negativeInitialNodeCount)
	}
	if _, err := s.GetCluster(ctx, "p", "us-central1", "c1"); err == nil {
		t.Fatal("rejected create must persist no cluster")
	}

	// With pools the count is ignored rather than rejected, so the create
	// succeeds and the named pool is the only one materialized.
	if _, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{
		Name:             "c2",
		InitialNodeCount: -1,
		NodePools:        []containerstore.NodePool{{Name: "pool-a", InitialNodeCount: 1}},
	}); err != nil {
		t.Fatalf("create with pools and a negative count: %v", err)
	}
	c, err := s.GetCluster(ctx, "p", "us-central1", "c2")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if len(c.NodePools) != 1 || c.NodePools[0].Name != "pool-a" {
		t.Fatalf("nodePools = %+v, want the caller's pool only", c.NodePools)
	}
}

// TestCreateClusterAcceptsZeroInitialNodeCountWithNodePools pins the real-GCP
// boundary: a zero cluster-level initialNodeCount means "no default node pool",
// so it does not conflict with nodePools (captured against a live project).
func TestCreateClusterAcceptsZeroInitialNodeCountWithNodePools(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	_, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{
		Name:      "c1",
		NodePools: []containerstore.NodePool{{Name: "pool-a", InitialNodeCount: 1}},
	})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	c, err := s.GetCluster(ctx, "p", "us-central1", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if len(c.NodePools) != 1 || c.NodePools[0].Name != "pool-a" {
		t.Fatalf("nodePools = %+v", c.NodePools)
	}
	if c.InitialNodeCount != 0 {
		t.Fatalf("initialNodeCount = %d, want 0", c.InitialNodeCount)
	}
}

// TestCreateClusterAppliesNodeConfigToDefaultPool pins the DPG42 behaviour:
// nodeConfig is the request shape for the generated default pool (Discovery on
// Cluster.nodeConfig), so the pool materializes from it with the same
// selfLink/version/node count a caller-supplied pool gets, and the cluster read
// echoes the pool's config (Discovery: "For responses, this field will be
// populated with the node configuration of the first node pool").
func TestCreateClusterAppliesNodeConfigToDefaultPool(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	_, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{
		Name:             "c1",
		InitialNodeCount: 2,
		NodeConfig: &containerstore.NodeConfig{
			MachineType: "e2-micro",
			DiskSizeGb:  50,
			OauthScopes: []string{"scope-a", "scope-b"},
		},
	})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	c, err := s.GetCluster(ctx, "p", "us-central1", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if len(c.NodePools) != 1 || c.NodePools[0].Name != DefaultNodePoolName {
		t.Fatalf("nodePools = %+v", c.NodePools)
	}
	pool := c.NodePools[0]
	if pool.Status != containerstore.StatusRunning {
		t.Fatalf("pool status = %q, want RUNNING", pool.Status)
	}
	if pool.SelfLink != NodePoolName("p", "us-central1", "c1", DefaultNodePoolName) {
		t.Fatalf("pool selfLink = %q", pool.SelfLink)
	}
	if pool.Version != DefaultMasterVersion {
		t.Fatalf("pool version = %q, want %q", pool.Version, DefaultMasterVersion)
	}
	if pool.InitialNodeCount != 2 || pool.NodeCount != 2 {
		t.Fatalf("pool node counts = %d/%d, want 2/2", pool.InitialNodeCount, pool.NodeCount)
	}
	if pool.Config == nil {
		t.Fatal("default-pool has no config: the create's nodeConfig was not applied")
	}
	if pool.Config.MachineType != "e2-micro" || pool.Config.DiskSizeGb != 50 || len(pool.Config.OauthScopes) != 2 {
		t.Fatalf("pool config = %+v", pool.Config)
	}
	if c.NodeConfig == nil || c.NodeConfig.MachineType != "e2-micro" {
		t.Fatalf("cluster nodeConfig echo = %+v", c.NodeConfig)
	}
}

// TestGetClusterEchoesFirstNodePoolConfig pins the read-shape rule Discovery
// states for Cluster.nodeConfig: the response is populated from the first node
// pool, so a cluster created with inline pools echoes pool-a's config (and none
// at all when no pool has one).
func TestGetClusterEchoesFirstNodePoolConfig(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	_, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{
		Name: "c1",
		NodePools: []containerstore.NodePool{
			{Name: "pool-a", InitialNodeCount: 1, Config: &containerstore.NodeConfig{MachineType: "e2-small"}},
			{Name: "pool-b", InitialNodeCount: 1, Config: &containerstore.NodeConfig{MachineType: "e2-large"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	c, err := s.GetCluster(ctx, "p", "us-central1", "c1")
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if c.NodeConfig == nil || c.NodeConfig.MachineType != "e2-small" {
		t.Fatalf("cluster nodeConfig echo = %+v, want pool-a's config", c.NodeConfig)
	}

	// A cluster whose pools carry no config still echoes a config: real GKE
	// renders the defaulted node config for a pool the caller did not shape
	// (DPG46, docs/gcp/gke-node-pool-config-defaults.md).
	_, err = s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{
		Name:      "c2",
		NodePools: []containerstore.NodePool{{Name: "pool-a", InitialNodeCount: 1}},
	})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if c, err := s.GetCluster(ctx, "p", "us-central1", "c2"); err != nil {
		t.Fatalf("GetCluster: %v", err)
	} else if c.NodeConfig == nil || c.NodeConfig.MachineType != DefaultNodeMachineType {
		t.Fatalf("cluster nodeConfig = %+v, want the defaulted pool config", c.NodeConfig)
	}
}

func TestCreateClusterRejectsBadNodePools(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	cases := map[string][]containerstore.NodePool{
		"empty name":   {{Name: ""}},
		"invalid name": {{Name: "Bad_Name"}},
		"duplicate":    {{Name: "pool-a"}, {Name: "pool-a"}},
	}
	for name, pools := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := s.CreateCluster(ctx, "p", "l", containerstore.Cluster{Name: "c1", NodePools: pools})
			providerErr(t, err, "InvalidArgument")
		})
	}
}

func TestCreateClusterErrors(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	if _, err := s.CreateCluster(ctx, "", "l", containerstore.Cluster{Name: "c1"}); err == nil {
		t.Fatal("want InvalidArgument for empty project")
	}
	_, err := s.CreateCluster(ctx, "p", "l", containerstore.Cluster{})
	providerErr(t, err, "InvalidArgument")

	_, err = s.CreateCluster(ctx, "p", "l", containerstore.Cluster{Name: "Bad_Name"})
	providerErr(t, err, "InvalidArgument")

	if _, err := s.CreateCluster(ctx, "p", "l", containerstore.Cluster{Name: "c1"}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err = s.CreateCluster(ctx, "p", "l", containerstore.Cluster{Name: "c1"})
	providerErr(t, err, "AlreadyExists")
}

func TestGetListDeleteCluster(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	_, err := s.GetCluster(ctx, "p", "l", "missing")
	providerErr(t, err, "NotFound")

	for _, name := range []string{"b", "a", "c"} {
		if _, err := s.CreateCluster(ctx, "p", "l", containerstore.Cluster{Name: name}); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	list, err := s.ListClusters(ctx, "p", "l")
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}
	if len(list) != 3 || list[0].Name != "a" || list[1].Name != "b" || list[2].Name != "c" {
		t.Fatalf("list order = %+v", list)
	}

	op, err := s.DeleteCluster(ctx, "p", "l", "a")
	if err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if op.OperationType != containerstore.OperationDeleteCluster || op.Status != containerstore.OperationStatusDone {
		t.Fatalf("delete operation = %+v", op)
	}
	if _, err := s.GetCluster(ctx, "p", "l", "a"); err == nil {
		t.Fatal("cluster still present after delete")
	}
	_, err = s.DeleteCluster(ctx, "p", "l", "a")
	providerErr(t, err, "NotFound")
}

func TestOperations(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	if _, err := s.GetOperation(ctx, "p", "l", "missing"); err == nil {
		t.Fatal("want NotFound for missing operation")
	}
	createOp, err := s.CreateCluster(ctx, "p", "l", containerstore.Cluster{Name: "c1"})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := s.DeleteCluster(ctx, "p", "l", "c1"); err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	got, err := s.GetOperation(ctx, "p", "l", createOp.Name)
	if err != nil || got.OperationType != containerstore.OperationCreateCluster {
		t.Fatalf("GetOperation = %+v, %v", got, err)
	}
	ops, err := s.ListOperations(ctx, "p", "l")
	if err != nil || len(ops) != 2 {
		t.Fatalf("ListOperations = %d, %v", len(ops), err)
	}
}

func TestResetClearsState(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateCluster(ctx, "p", "l", containerstore.Cluster{Name: "c1"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	s.Reset(ctx)
	if list, err := s.ListClusters(ctx, "p", "l"); err != nil || len(list) != 0 {
		t.Fatalf("ListClusters after reset = %d, %v", len(list), err)
	}
	if ops, err := s.ListOperations(ctx, "p", "l"); err != nil || len(ops) != 0 {
		t.Fatalf("ListOperations after reset = %d, %v", len(ops), err)
	}
}
