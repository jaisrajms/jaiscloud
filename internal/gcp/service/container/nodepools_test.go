package container

import (
	"context"
	"errors"
	"testing"

	containerstore "jaiscloud/internal/gcp/store/container"
	"jaiscloud/internal/model"
)

func TestNodePoolCRUD(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	op, err := s.CreateNodePool(ctx, "p", "us-central1", "c1", containerstore.NodePool{
		Name:             "pool-a",
		InitialNodeCount: 2,
		Config:           &containerstore.NodeConfig{MachineType: "e2-medium"},
	})
	if err != nil {
		t.Fatalf("CreateNodePool: %v", err)
	}
	if op.OperationType != containerstore.OperationCreateNodePool || op.Status != containerstore.OperationStatusDone {
		t.Fatalf("create op = %+v", op)
	}
	if op.TargetLink != "projects/p/locations/us-central1/clusters/c1/nodePools/pool-a" {
		t.Fatalf("targetLink = %q", op.TargetLink)
	}

	got, err := s.GetNodePool(ctx, "p", "us-central1", "c1", "pool-a")
	if err != nil {
		t.Fatalf("GetNodePool: %v", err)
	}
	if got.Status != containerstore.StatusRunning {
		t.Fatalf("status = %q, want RUNNING", got.Status)
	}
	if got.Version == "" {
		t.Fatal("version should default to the cluster node version")
	}
	if got.Config == nil || got.Config.MachineType != "e2-medium" {
		t.Fatalf("config = %+v", got.Config)
	}

	if _, err := s.CreateNodePool(ctx, "p", "us-central1", "c1", containerstore.NodePool{Name: "pool-a"}); err == nil {
		t.Fatal("duplicate node pool should error")
	} else {
		providerErr(t, err, "AlreadyExists")
	}

	list, err := s.ListNodePools(ctx, "p", "us-central1", "c1")
	if err != nil {
		t.Fatalf("ListNodePools: %v", err)
	}
	// The create default materialized a default-pool, so there are two.
	if len(list) != 2 || list[0].Name != "default-pool" || list[1].Name != "pool-a" {
		t.Fatalf("list = %+v", list)
	}

	// UpdateNodePool merges the provided fields.
	upd, err := s.UpdateNodePool(ctx, "p", "us-central1", "c1", containerstore.NodePool{Name: "pool-a", Version: "1.31.0-gke.100"})
	if err != nil {
		t.Fatalf("UpdateNodePool: %v", err)
	}
	if upd.OperationType != containerstore.OperationUpdateNodePool {
		t.Fatalf("update op type = %q", upd.OperationType)
	}
	got, _ = s.GetNodePool(ctx, "p", "us-central1", "c1", "pool-a")
	if got.Version != "1.31.0-gke.100" || got.Config.MachineType != "e2-medium" {
		t.Fatalf("after update = %+v", got)
	}

	if op, err := s.SetNodePoolSize(ctx, "p", "us-central1", "c1", "pool-a", 3); err != nil {
		t.Fatalf("SetNodePoolSize: %v", err)
	} else if op.OperationType != containerstore.OperationSetNodePoolSize {
		t.Fatalf("setSize op type = %q", op.OperationType)
	}
	if _, err := s.SetNodePoolAutoscaling(ctx, "p", "us-central1", "c1", "pool-a", containerstore.NodePoolAutoscaling{Enabled: true, MinNodeCount: 1, MaxNodeCount: 4}); err != nil {
		t.Fatalf("SetNodePoolAutoscaling: %v", err)
	}
	got, _ = s.GetNodePool(ctx, "p", "us-central1", "c1", "pool-a")
	if got.Autoscaling == nil || !got.Autoscaling.Enabled || got.Autoscaling.MaxNodeCount != 4 {
		t.Fatalf("autoscaling = %+v", got.Autoscaling)
	}
	if op, err := s.SetNodePoolManagement(ctx, "p", "us-central1", "c1", "pool-a", containerstore.NodeManagement{AutoUpgrade: true}); err != nil {
		t.Fatalf("SetNodePoolManagement: %v", err)
	} else if op.OperationType != containerstore.OperationSetNodePoolManagement {
		t.Fatalf("setManagement op type = %q", op.OperationType)
	}
	if _, err := s.RollbackNodePoolUpgrade(ctx, "p", "us-central1", "c1", "pool-a"); err != nil {
		t.Fatalf("RollbackNodePoolUpgrade: %v", err)
	}

	del, err := s.DeleteNodePool(ctx, "p", "us-central1", "c1", "pool-a")
	if err != nil {
		t.Fatalf("DeleteNodePool: %v", err)
	}
	if del.OperationType != containerstore.OperationDeleteNodePool {
		t.Fatalf("delete op type = %q", del.OperationType)
	}
	providerErr(t, mustErr(s.GetNodePool(ctx, "p", "us-central1", "c1", "pool-a")), "NotFound")
}

func TestNodePoolErrors(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateCluster(ctx, "p", "l", containerstore.Cluster{Name: "c1"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	providerErr(t, opErr(s.CreateNodePool(ctx, "p", "l", "c1", containerstore.NodePool{})), "InvalidArgument")
	providerErr(t, opErr(s.CreateNodePool(ctx, "p", "l", "c1", containerstore.NodePool{Name: "Bad_Name"})), "InvalidArgument")
	providerErr(t, opErr(s.CreateNodePool(ctx, "p", "l", "missing", containerstore.NodePool{Name: "np"})), "NotFound")
	providerErr(t, mustErr(s.GetNodePool(ctx, "p", "l", "c1", "missing")), "NotFound")
	providerErr(t, mustErr(s.GetNodePool(ctx, "p", "l", "missing", "np")), "NotFound")
	if _, err := s.ListNodePools(ctx, "p", "l", "missing"); err != nil {
		providerErr(t, err, "NotFound")
	} else {
		t.Fatal("ListNodePools on a missing cluster should error")
	}
}

func mustErr(_ containerstore.NodePool, err error) error { return err }
func opErr(_ containerstore.Operation, err error) error  { return err }

// TestNodePoolDuplicateNameStory pins the one duplicate-name rule both paths
// share through materializeNodePool: a name already taken on the cluster is a
// 409 conflict (CreateNodePool on an existing pool), while the same name twice
// inside one create request is a 400 — the request is invalid before anything is
// stored, and no pool is created.
func TestNodePoolDuplicateNameStory(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	// Inline duplicates in one create body: 400, and the cluster is not stored.
	providerErr(t, opErr(s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{
		Name: "c-inline",
		NodePools: []containerstore.NodePool{
			{Name: "pool-a", InitialNodeCount: 1},
			{Name: "pool-a", InitialNodeCount: 1},
		},
	})), "InvalidArgument")
	if _, err := s.GetCluster(ctx, "p", "us-central1", "c-inline"); err == nil {
		t.Fatal("a rejected create must not store the cluster")
	}

	// The equivalent CreateNodePool against an existing pool: 409.
	if _, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := s.CreateNodePool(ctx, "p", "us-central1", "c1", containerstore.NodePool{Name: "pool-b", InitialNodeCount: 1}); err != nil {
		t.Fatalf("CreateNodePool: %v", err)
	}
	providerErr(t, opErr(s.CreateNodePool(ctx, "p", "us-central1", "c1", containerstore.NodePool{Name: "pool-b"})), "AlreadyExists")
}

// TestMaterializeNodePoolParity verifies the inline and CreateNodePool paths
// assign the same selfLink, RUNNING status, node version and node count through
// the shared materialiser, and reject a bad name with the same message.
func TestMaterializeNodePoolParity(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if _, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{
		Name: "inline",
		NodePools: []containerstore.NodePool{
			{Name: "pool-a", InitialNodeCount: 3},
		},
	}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	inline, err := s.GetNodePool(ctx, "p", "us-central1", "inline", "pool-a")
	if err != nil {
		t.Fatalf("GetNodePool: %v", err)
	}

	if _, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := s.CreateNodePool(ctx, "p", "us-central1", "c1", containerstore.NodePool{Name: "pool-a", InitialNodeCount: 3}); err != nil {
		t.Fatalf("CreateNodePool: %v", err)
	}
	created, err := s.GetNodePool(ctx, "p", "us-central1", "c1", "pool-a")
	if err != nil {
		t.Fatalf("GetNodePool: %v", err)
	}

	if inline.Status != created.Status || inline.Version != created.Version || inline.NodeCount != created.NodeCount {
		t.Fatalf("inline %+v != created %+v", inline, created)
	}
	if inline.SelfLink != "projects/p/locations/us-central1/clusters/inline/nodePools/pool-a" ||
		created.SelfLink != "projects/p/locations/us-central1/clusters/c1/nodePools/pool-a" {
		t.Fatalf("selfLinks = %q / %q", inline.SelfLink, created.SelfLink)
	}
	if inline.NodeCount != 3 {
		t.Fatalf("nodeCount = %d, want initialNodeCount 3", inline.NodeCount)
	}

	// Name grammar failures answer identically on both paths.
	var msg string
	for _, err := range []error{
		opErr(s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{
			Name:      "bad-inline",
			NodePools: []containerstore.NodePool{{Name: "Bad_Name"}},
		})),
		opErr(s.CreateNodePool(ctx, "p", "us-central1", "c1", containerstore.NodePool{Name: "Bad_Name"})),
	} {
		var pe *model.ProviderError
		if !errors.As(err, &pe) {
			t.Fatalf("error is %T, want *model.ProviderError", err)
		}
		if msg == "" {
			msg = pe.Message
			continue
		}
		if pe.Message != msg {
			t.Fatalf("messages differ: %q vs %q", msg, pe.Message)
		}
	}
	if msg != "invalid node pool name: Bad_Name" {
		t.Fatalf("message = %q", msg)
	}
}
