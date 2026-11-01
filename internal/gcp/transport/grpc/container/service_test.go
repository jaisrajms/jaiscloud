package container

import (
	"context"
	"testing"

	containerpb "cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	core "jaiscloud/internal/gcp/service/container"
	containerstore "jaiscloud/internal/gcp/store/container"
)

func newGRPCService(t *testing.T) (*Service, *core.Service) {
	t.Helper()
	c := core.NewService(containerstore.NewMemoryStore())
	return NewService(c, "p"), c
}

func pbCluster(name string) *containerpb.Cluster {
	return &containerpb.Cluster{Name: name, InitialNodeCount: 1}
}

// TestCreateClusterRejectsDefaultPoolFieldsWithNodePools proves the gRPC path
// enforces the same conflict the REST path does: Discovery's nodePools "should
// not be set if node_config or initial_node_count are specified".
func TestCreateClusterRejectsDefaultPoolFieldsWithNodePools(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)
	parent := "projects/p/locations/us-central1"

	cases := []struct {
		name    string
		cluster *containerpb.Cluster
		want    string
	}{
		{
			name:    "initialNodeCount",
			cluster: &containerpb.Cluster{Name: "c1", InitialNodeCount: 1, NodePools: []*containerpb.NodePool{{Name: "pool-a", InitialNodeCount: 1}}},
			want:    "It's invalid to specify both cluster.initial_node_count and a node pool. Please only provide a node pool.",
		},
		{
			name:    "nodeConfig",
			cluster: &containerpb.Cluster{Name: "c1", NodeConfig: &containerpb.NodeConfig{MachineType: "e2-micro"}, NodePools: []*containerpb.NodePool{{Name: "pool-a", InitialNodeCount: 1}}},
			want:    "It's invalid to specify both cluster.node_config and a node pool. Please only provide a node pool.",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.CreateCluster(ctx, &containerpb.CreateClusterRequest{Parent: parent, Cluster: tc.cluster})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code = %v, want InvalidArgument (%v)", status.Code(err), err)
			}
			if msg := status.Convert(err).Message(); msg != tc.want {
				t.Fatalf("message = %q, want %q", msg, tc.want)
			}
			if _, err := svc.GetCluster(ctx, &containerpb.GetClusterRequest{Name: parent + "/clusters/c1"}); status.Code(err) != codes.NotFound {
				t.Fatalf("rejected create persisted a cluster: %v", err)
			}
		})
	}
}

// TestCreateClusterInlineNodePools proves the gRPC create materializes the
// caller's node_pools the way REST does, instead of substituting a default-pool.
func TestCreateClusterInlineNodePools(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)
	parent := "projects/p/locations/us-central1"

	cluster := &containerpb.Cluster{
		Name: "c1",
		NodePools: []*containerpb.NodePool{
			{Name: "pool-a", InitialNodeCount: 2, Config: &containerpb.NodeConfig{MachineType: "e2-medium"}},
			{Name: "pool-b", InitialNodeCount: 1},
		},
	}
	if _, err := svc.CreateCluster(ctx, &containerpb.CreateClusterRequest{Parent: parent, Cluster: cluster}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	c, err := svc.GetCluster(ctx, &containerpb.GetClusterRequest{Name: parent + "/clusters/c1"})
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	pools := c.GetNodePools()
	if len(pools) != 2 || pools[0].GetName() != "pool-a" || pools[1].GetName() != "pool-b" {
		t.Fatalf("nodePools = %v", pools)
	}
	if pools[0].GetStatus() != containerpb.NodePool_RUNNING {
		t.Fatalf("pool-a status = %v, want RUNNING", pools[0].GetStatus())
	}
	if pools[0].GetVersion() != c.GetCurrentMasterVersion() {
		t.Fatalf("pool-a version = %q, want %q", pools[0].GetVersion(), c.GetCurrentMasterVersion())
	}
	if pools[0].GetConfig().GetMachineType() != "e2-medium" {
		t.Fatalf("pool-a machineType = %q", pools[0].GetConfig().GetMachineType())
	}
}

// TestCreateClusterAppliesNodeConfigToDefaultPool proves the gRPC path applies
// a legacy cluster-level node_config to the generated default-pool and echoes it
// on read, matching REST (Discovery: on read the field holds the first pool's
// node configuration).
func TestCreateClusterAppliesNodeConfigToDefaultPool(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)
	parent := "projects/p/locations/us-central1"

	cluster := &containerpb.Cluster{
		Name:             "c1",
		InitialNodeCount: 2,
		NodeConfig:       &containerpb.NodeConfig{MachineType: "e2-micro", DiskSizeGb: 50},
	}
	if _, err := svc.CreateCluster(ctx, &containerpb.CreateClusterRequest{Parent: parent, Cluster: cluster}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	c, err := svc.GetCluster(ctx, &containerpb.GetClusterRequest{Name: parent + "/clusters/c1"})
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if c.GetNodeConfig().GetMachineType() != "e2-micro" || c.GetNodeConfig().GetDiskSizeGb() != 50 {
		t.Fatalf("cluster nodeConfig = %v", c.GetNodeConfig())
	}
	pools := c.GetNodePools()
	if len(pools) != 1 || pools[0].GetName() != core.DefaultNodePoolName {
		t.Fatalf("nodePools = %v", pools)
	}
	if pools[0].GetSelfLink() != parent+"/clusters/c1/nodePools/"+core.DefaultNodePoolName {
		t.Fatalf("default-pool selfLink = %q", pools[0].GetSelfLink())
	}
	if pools[0].GetVersion() != core.DefaultMasterVersion {
		t.Fatalf("default-pool version = %q", pools[0].GetVersion())
	}
	if pools[0].GetConfig().GetMachineType() != "e2-micro" || pools[0].GetConfig().GetDiskSizeGb() != 50 {
		t.Fatalf("default-pool config = %v", pools[0].GetConfig())
	}
}

// TestCanonicalCRUD exercises the canonical name/parent form the newer clients
// and the Java suite use.
func TestCanonicalCRUD(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)
	parent := "projects/p/locations/us-central1"
	name := parent + "/clusters/c1"

	op, err := svc.CreateCluster(ctx, &containerpb.CreateClusterRequest{Parent: parent, Cluster: pbCluster("c1")})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if op.GetOperationType() != containerpb.Operation_CREATE_CLUSTER || op.GetStatus() != containerpb.Operation_DONE {
		t.Fatalf("CreateCluster op = %v", op)
	}
	if op.GetName() == "" || op.GetSelfLink() == "" {
		t.Fatalf("CreateCluster op missing name/selfLink: %v", op)
	}

	c, err := svc.GetCluster(ctx, &containerpb.GetClusterRequest{Name: name})
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	if c.GetName() != "c1" || c.GetLocation() != "us-central1" {
		t.Fatalf("GetCluster = %v", c)
	}
	if c.GetStatus() != containerpb.Cluster_RUNNING {
		t.Fatalf("GetCluster status = %v, want RUNNING", c.GetStatus())
	}
	if c.GetCurrentMasterVersion() == "" {
		t.Fatalf("GetCluster missing currentMasterVersion")
	}
	if c.GetMasterAuth().GetClusterCaCertificate() != "" {
		t.Fatalf("GetCluster caCertificate should be empty")
	}
	if len(c.GetNodePools()) != 1 || c.GetNodePools()[0].GetStatus() != containerpb.NodePool_RUNNING {
		t.Fatalf("GetCluster nodePools = %v", c.GetNodePools())
	}

	list, err := svc.ListClusters(ctx, &containerpb.ListClustersRequest{Parent: parent})
	if err != nil || len(list.GetClusters()) != 1 {
		t.Fatalf("ListClusters = %v, %v", list, err)
	}

	op, err = svc.DeleteCluster(ctx, &containerpb.DeleteClusterRequest{Name: name})
	if err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if op.GetOperationType() != containerpb.Operation_DELETE_CLUSTER {
		t.Fatalf("DeleteCluster op = %v", op)
	}
	if _, err := svc.GetCluster(ctx, &containerpb.GetClusterRequest{Name: name}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetCluster after delete code = %v, want NotFound", status.Code(err))
	}
}

// TestLegacyProjectZoneClusterID exercises the legacy project_id/zone/cluster_id
// fields the stock Java/Go client getters use.
func TestLegacyProjectZoneClusterID(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)

	op, err := svc.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		ProjectId: "p", Zone: "us-central1-a", Cluster: pbCluster("c2"),
	})
	if err != nil {
		t.Fatalf("CreateCluster legacy: %v", err)
	}
	if op.GetLocation() != "us-central1-a" {
		t.Fatalf("legacy op location = %q", op.GetLocation())
	}

	c, err := svc.GetCluster(ctx, &containerpb.GetClusterRequest{ProjectId: "p", Zone: "us-central1-a", ClusterId: "c2"})
	if err != nil {
		t.Fatalf("GetCluster legacy: %v", err)
	}
	if c.GetName() != "c2" || c.GetLocation() != "us-central1-a" {
		t.Fatalf("GetCluster legacy = %v", c)
	}

	list, err := svc.ListClusters(ctx, &containerpb.ListClustersRequest{ProjectId: "p", Zone: "us-central1-a"})
	if err != nil || len(list.GetClusters()) != 1 {
		t.Fatalf("ListClusters legacy = %v, %v", list, err)
	}

	del, err := svc.DeleteCluster(ctx, &containerpb.DeleteClusterRequest{
		ProjectId: "p", Zone: "us-central1-a", ClusterId: "c2",
	})
	if err != nil || del.GetOperationType() != containerpb.Operation_DELETE_CLUSTER {
		t.Fatalf("DeleteCluster legacy = %v, %v", del, err)
	}
}

// TestOperations covers GetOperation/ListOperations over both name forms.
func TestOperations(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)
	parent := "projects/p/locations/us-central1"

	op, err := svc.CreateCluster(ctx, &containerpb.CreateClusterRequest{Parent: parent, Cluster: pbCluster("c3")})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	canonical := core.OperationName("p", "us-central1", op.GetName())
	got, err := svc.GetOperation(ctx, &containerpb.GetOperationRequest{Name: canonical})
	if err != nil {
		t.Fatalf("GetOperation canonical: %v", err)
	}
	if got.GetName() != op.GetName() || got.GetOperationType() != containerpb.Operation_CREATE_CLUSTER {
		t.Fatalf("GetOperation = %v", got)
	}

	legacy, err := svc.GetOperation(ctx, &containerpb.GetOperationRequest{
		ProjectId: "p", Zone: "us-central1", OperationId: op.GetName(),
	})
	if err != nil || legacy.GetName() != op.GetName() {
		t.Fatalf("GetOperation legacy = %v, %v", legacy, err)
	}

	list, err := svc.ListOperations(ctx, &containerpb.ListOperationsRequest{Parent: parent})
	if err != nil || len(list.GetOperations()) != 1 {
		t.Fatalf("ListOperations = %v, %v", list, err)
	}
}

// TestErrorCodes pins the core-error to gRPC-status mapping.
func TestErrorCodes(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)
	parent := "projects/p/locations/us-central1"

	if _, err := svc.CreateCluster(ctx, &containerpb.CreateClusterRequest{Parent: parent, Cluster: pbCluster("dup")}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if _, err := svc.CreateCluster(ctx, &containerpb.CreateClusterRequest{Parent: parent, Cluster: pbCluster("dup")}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate CreateCluster code = %v, want AlreadyExists", status.Code(err))
	}
	if _, err := svc.CreateCluster(ctx, &containerpb.CreateClusterRequest{Parent: parent, Cluster: pbCluster("Bad_Name")}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid name code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := svc.GetCluster(ctx, &containerpb.GetClusterRequest{Name: parent + "/clusters/missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing GetCluster code = %v, want NotFound", status.Code(err))
	}
	if _, err := svc.GetOperation(ctx, &containerpb.GetOperationRequest{Name: parent + "/operations/missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing GetOperation code = %v, want NotFound", status.Code(err))
	}
}

// TestInvalidRefs rejects malformed or incomplete addressing.
func TestInvalidRefs(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)

	cases := []struct {
		name string
		err  error
	}{
		{"list bad parent", func() error {
			_, err := svc.ListClusters(ctx, &containerpb.ListClustersRequest{Parent: "bogus"})
			return err
		}()},
		{"list no addressing", func() error {
			_, err := svc.ListClusters(ctx, &containerpb.ListClustersRequest{})
			return err
		}()},
		{"get bad name", func() error {
			_, err := svc.GetCluster(ctx, &containerpb.GetClusterRequest{Name: "projects/p/locations/l"})
			return err
		}()},
		{"get no addressing", func() error {
			_, err := svc.GetCluster(ctx, &containerpb.GetClusterRequest{})
			return err
		}()},
		{"get operation bad name", func() error {
			_, err := svc.GetOperation(ctx, &containerpb.GetOperationRequest{Name: "bogus"})
			return err
		}()},
		{"get operation no addressing", func() error {
			_, err := svc.GetOperation(ctx, &containerpb.GetOperationRequest{})
			return err
		}()},
	}
	for _, tc := range cases {
		if status.Code(tc.err) != codes.InvalidArgument {
			t.Fatalf("%s code = %v, want InvalidArgument", tc.name, status.Code(tc.err))
		}
	}
}

// TestCreateClusterNoName rejects a nameless cluster with InvalidArgument, and
// maps a canonical cluster path in name to its short id.
func TestCreateClusterNoName(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)
	parent := "projects/p/locations/us-central1"

	if _, err := svc.CreateCluster(ctx, &containerpb.CreateClusterRequest{Parent: parent, Cluster: &containerpb.Cluster{}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nameless CreateCluster code = %v, want InvalidArgument", status.Code(err))
	}

	op, err := svc.CreateCluster(ctx, &containerpb.CreateClusterRequest{Parent: parent, Cluster: pbCluster(parent + "/clusters/c4")})
	if err != nil {
		t.Fatalf("canonical-name CreateCluster: %v", err)
	}
	if _, err := svc.GetCluster(ctx, &containerpb.GetClusterRequest{Name: parent + "/clusters/c4"}); err != nil {
		t.Fatalf("GetCluster after canonical name create: %v", err)
	}
	_ = op
}

// TestInvalidNameMessagesMatchREST pins the messages the gRPC surface answers
// for a malformed canonical cluster or node-pool name. Both codecs now parse
// with the core's parsers, so the message — including the offending value — is
// identical to the REST surface; provider_test.go pins the same strings.
func TestInvalidNameMessagesMatchREST(t *testing.T) {
	ctx := context.Background()
	svc, _ := newGRPCService(t)
	parent := "projects/p/locations/us-central1"

	const badCluster = "projects/p/locations/us-central1/clusters/bad-name/extra"
	_, err := svc.CreateCluster(ctx, &containerpb.CreateClusterRequest{Parent: parent, Cluster: pbCluster(badCluster)})
	if status.Code(err) != codes.InvalidArgument || status.Convert(err).Message() != "invalid cluster name: "+badCluster {
		t.Fatalf("create error = %v (%q)", err, status.Convert(err).Message())
	}

	if _, err := svc.CreateCluster(ctx, &containerpb.CreateClusterRequest{Parent: parent, Cluster: pbCluster("c1")}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	const badPool = "projects/p/locations/us-central1/clusters/c1/nodePools/bad/extra"
	_, err = svc.CreateNodePool(ctx, &containerpb.CreateNodePoolRequest{
		Parent:   parent + "/clusters/c1",
		NodePool: &containerpb.NodePool{Name: badPool, InitialNodeCount: 1},
	})
	if status.Code(err) != codes.InvalidArgument || status.Convert(err).Message() != "invalid node pool name: "+badPool {
		t.Fatalf("create node pool error = %v (%q)", err, status.Convert(err).Message())
	}
}
