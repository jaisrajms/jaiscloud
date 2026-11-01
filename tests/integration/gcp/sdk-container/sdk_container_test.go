// Package sdk_container_test exercises the jaiscloud-gcp emulator's Google
// Kubernetes Engine (GKE) v1 metadata control plane
// (container.googleapis.com/v1) through the official Google REST apiary client.
// Real GKE's native transport is gRPC, but the emulator deliberately exposes
// the REST metadata surface (see docs/gcp/wire-contract.md §5), so the apiary client is the
// transport-matching official REST client here.
//
// The canonical /v1/projects/{project}/locations/{location}/clusters path is
// shared with Managed Kafka on the single emulator origin, so GKE is
// disambiguated by host (container.googleapis.com) or by the "/container/" path
// prefix Terraform/gcloud use. This suite drives the client through the
// "/container/" prefix (see containerEndpoint), which is the path form the
// emulator documents.
//
// It pins the documented behaviors: cluster create/get/list/delete, node-pool
// create/get/list/update/delete, the :setAutoscaling/:setSize/:setManagement
// setters, and the GKE Operation records those mutations return — each created
// already DONE (the emulator is metadata-only, with no asynchronous lifecycle),
// plus the InvalidArgument/AlreadyExists/NotFound error contracts.
//
// Run with the GCP binary running and GCP_EMULATOR_ENDPOINT set:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test ./...
package sdk_container_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	container "google.golang.org/api/container/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// endpoint is the emulator's REST root. GKE's control plane is reached through
// the "/container/" path prefix so the shared clusters path is routed to GKE
// rather than Managed Kafka.
func endpoint() string {
	base := "http://localhost:8080/"
	if e := os.Getenv("GCP_EMULATOR_ENDPOINT"); e != "" {
		base = e
	}
	return strings.TrimRight(base, "/") + "/container/"
}

func baseURL() string {
	base := "http://localhost:8080/"
	if e := os.Getenv("GCP_EMULATOR_ENDPOINT"); e != "" {
		base = e
	}
	return strings.TrimRight(base, "/")
}

func projectID() string {
	if p := os.Getenv("GCP_EMULATOR_PROJECT"); p != "" {
		return p
	}
	return "proj"
}

func locationID() string { return "us-central1" }

func opts() []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(endpoint()), option.WithoutAuthentication()}
}

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// resetState wipes emulator state between tests (the suite shares one server).
func resetState(t *testing.T) {
	t.Helper()
	resp, err := http.Post(baseURL()+"/_jaiscloud/reset", "", nil) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func newService(t *testing.T) *container.Service {
	t.Helper()
	svc, err := container.NewService(context.Background(), opts()...)
	require.NoError(t, err)
	return svc
}

func parent() string {
	return fmt.Sprintf("projects/%s/locations/%s", projectID(), locationID())
}

func clusterName(id string) string { return parent() + "/clusters/" + id }

func nodePoolName(cluster, pool string) string {
	return clusterName(cluster) + "/nodePools/" + pool
}

func operationName(id string) string { return parent() + "/operations/" + id }

// requireAPIError asserts err is a googleapi.Error with the given status.
func requireAPIError(t *testing.T, err error, code int) {
	t.Helper()
	require.Error(t, err)
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, code, apiErr.Code)
}

// TestSDKContainerClusterLifecycle covers cluster CRUD and the CREATE_CLUSTER /
// DELETE_CLUSTER operation records.
func TestSDKContainerClusterLifecycle(t *testing.T) {
	resetState(t)
	svc := newService(t)

	id := unique("c")
	op, err := svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{
		Cluster: &container.Cluster{Name: id, InitialNodeCount: 1},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "CREATE_CLUSTER", op.OperationType)
	require.Equal(t, "DONE", op.Status)
	require.NotEmpty(t, op.Name)
	require.Equal(t, locationID(), op.Location)
	require.Equal(t, clusterName(id), op.TargetLink)
	require.NotEmpty(t, op.SelfLink)
	require.NotEmpty(t, op.StartTime)
	require.NotEmpty(t, op.EndTime)

	got, err := svc.Projects.Locations.Clusters.Get(clusterName(id)).Do()
	require.NoError(t, err)
	require.Equal(t, id, got.Name)
	require.Equal(t, "RUNNING", got.Status)
	require.Equal(t, locationID(), got.Location)
	require.Equal(t, "default", got.Network)
	require.Equal(t, "default", got.Subnetwork)
	require.NotEmpty(t, got.CurrentMasterVersion)
	require.NotEmpty(t, got.CurrentNodeVersion)
	require.NotEmpty(t, got.Endpoint)
	require.NotEmpty(t, got.CreateTime)
	require.Equal(t, clusterName(id), got.SelfLink)
	require.Len(t, got.NodePools, 1)
	require.Equal(t, "default-pool", got.NodePools[0].Name)
	require.Equal(t, "RUNNING", got.NodePools[0].Status)

	// DPG37: real GKE always renders these nested messages, and clients (the
	// hashicorp/google provider dereferences all four) need them present.
	require.NotNil(t, got.NetworkConfig)
	require.Equal(t, "projects/"+projectID()+"/global/networks/default", got.NetworkConfig.Network)
	require.Equal(t, "projects/"+projectID()+"/regions/"+locationID()+"/subnetworks/default", got.NetworkConfig.Subnetwork)
	require.NotNil(t, got.ShieldedNodes)
	require.True(t, got.ShieldedNodes.Enabled)
	require.NotNil(t, got.DefaultMaxPodsConstraint)
	require.EqualValues(t, 110, got.DefaultMaxPodsConstraint.MaxPodsPerNode)
	require.NotNil(t, got.LegacyAbac)
	require.False(t, got.LegacyAbac.Enabled)
	require.Equal(t, "logging.googleapis.com/kubernetes", got.LoggingService)
	require.Equal(t, "monitoring.googleapis.com/kubernetes", got.MonitoringService)

	list, err := svc.Projects.Locations.Clusters.List(parent()).Do()
	require.NoError(t, err)
	require.Len(t, list.Clusters, 1)
	require.Equal(t, id, list.Clusters[0].Name)

	// A duplicate cluster is rejected.
	_, err = svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{
		Cluster: &container.Cluster{Name: id},
	}).Do()
	requireAPIError(t, err, 409)

	// Delete returns a DELETE_CLUSTER operation, and the cluster is gone.
	delOp, err := svc.Projects.Locations.Clusters.Delete(clusterName(id)).Do()
	require.NoError(t, err)
	require.Equal(t, "DELETE_CLUSTER", delOp.OperationType)
	require.Equal(t, "DONE", delOp.Status)
	_, err = svc.Projects.Locations.Clusters.Get(clusterName(id)).Do()
	requireAPIError(t, err, 404)
}

// TestSDKContainerClusterCreateWithInlineNodePools pins create-time pools: a
// Cluster body that carries node_pools materializes them in the same call
// (real GKE behavior) instead of silently getting a lone default-pool.
func TestSDKContainerClusterCreateWithInlineNodePools(t *testing.T) {
	resetState(t)
	svc := newService(t)

	id := unique("c")
	_, err := svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{
		Cluster: &container.Cluster{
			Name: id,
			NodePools: []*container.NodePool{
				{Name: "pool-a", InitialNodeCount: 2, Config: &container.NodeConfig{MachineType: "e2-medium"}},
				{Name: "pool-b", InitialNodeCount: 1},
			},
		},
	}).Do()
	require.NoError(t, err)

	got, err := svc.Projects.Locations.Clusters.Get(clusterName(id)).Do()
	require.NoError(t, err)
	require.Len(t, got.NodePools, 2)
	require.Equal(t, "pool-a", got.NodePools[0].Name)
	require.Equal(t, "pool-b", got.NodePools[1].Name)
	for _, p := range got.NodePools {
		require.Equal(t, "RUNNING", p.Status)
		require.Equal(t, nodePoolName(id, p.Name), p.SelfLink)
		require.Equal(t, got.CurrentMasterVersion, p.Version)
	}
	require.Equal(t, "e2-medium", got.NodePools[0].Config.MachineType)

	// The materialized pools are full members of the cluster's node-pool
	// collection: listable and individually addressable afterwards.
	pools, err := svc.Projects.Locations.Clusters.NodePools.List(clusterName(id)).Do()
	require.NoError(t, err)
	require.Len(t, pools.NodePools, 2)
	pool, err := svc.Projects.Locations.Clusters.NodePools.Get(nodePoolName(id, "pool-b")).Do()
	require.NoError(t, err)
	require.Equal(t, int64(1), pool.InitialNodeCount)

	// A pool name supplied twice is rejected, as real GKE does.
	dup := unique("c")
	_, err = svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{
		Cluster: &container.Cluster{
			Name: dup,
			NodePools: []*container.NodePool{
				{Name: "pool-a", InitialNodeCount: 1},
				{Name: "pool-a", InitialNodeCount: 1},
			},
		},
	}).Do()
	requireAPIError(t, err, 400)
}

// TestSDKContainerClusterCreateAppliesNodeConfigToDefaultPool covers the legacy
// create shape through the official client: a cluster-level nodeConfig with
// initialNodeCount and no nodePools is applied to the generated default-pool and
// echoed on the cluster read (Discovery: the response's nodeConfig holds the
// first pool's node configuration).
func TestSDKContainerClusterCreateAppliesNodeConfigToDefaultPool(t *testing.T) {
	resetState(t)
	svc := newService(t)

	id := unique("c")
	_, err := svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{
		Cluster: &container.Cluster{
			Name:             id,
			InitialNodeCount: 2,
			NodeConfig:       &container.NodeConfig{MachineType: "e2-micro", DiskSizeGb: 50},
		},
	}).Do()
	require.NoError(t, err)

	got, err := svc.Projects.Locations.Clusters.Get(clusterName(id)).Do()
	require.NoError(t, err)
	require.NotNil(t, got.NodeConfig)
	require.Equal(t, "e2-micro", got.NodeConfig.MachineType)
	require.Equal(t, int64(50), got.NodeConfig.DiskSizeGb)
	require.Len(t, got.NodePools, 1)
	pool := got.NodePools[0]
	require.Equal(t, "default-pool", pool.Name)
	require.Equal(t, "RUNNING", pool.Status)
	require.Equal(t, nodePoolName(id, "default-pool"), pool.SelfLink)
	require.Equal(t, got.CurrentMasterVersion, pool.Version)
	require.Equal(t, int64(2), pool.InitialNodeCount)
	require.Equal(t, "e2-micro", pool.Config.MachineType)
	require.Equal(t, int64(50), pool.Config.DiskSizeGb)

	// The generated pool is addressable as a node pool, like an inline one.
	gotPool, err := svc.Projects.Locations.Clusters.NodePools.Get(nodePoolName(id, "default-pool")).Do()
	require.NoError(t, err)
	require.Equal(t, "e2-micro", gotPool.Config.MachineType)
}

// TestSDKContainerNodePools covers node-pool CRUD, the update/setter custom
// methods, and their operation records.
func TestSDKContainerNodePools(t *testing.T) {
	resetState(t)
	svc := newService(t)

	cluster := unique("c")
	_, err := svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{
		Cluster: &container.Cluster{Name: cluster},
	}).Do()
	require.NoError(t, err)

	pool := unique("p")
	createOp, err := svc.Projects.Locations.Clusters.NodePools.Create(clusterName(cluster), &container.CreateNodePoolRequest{
		NodePool: &container.NodePool{
			Name:             pool,
			InitialNodeCount: 2,
			Config:           &container.NodeConfig{MachineType: "e2-medium"},
		},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "CREATE_NODE_POOL", createOp.OperationType)
	require.Equal(t, "DONE", createOp.Status)
	require.Equal(t, nodePoolName(cluster, pool), createOp.TargetLink)

	got, err := svc.Projects.Locations.Clusters.NodePools.Get(nodePoolName(cluster, pool)).Do()
	require.NoError(t, err)
	require.Equal(t, pool, got.Name)
	require.Equal(t, "RUNNING", got.Status)
	require.Equal(t, int64(2), got.InitialNodeCount)
	require.NotEmpty(t, got.Version)
	require.Equal(t, nodePoolName(cluster, pool), got.SelfLink)
	require.NotNil(t, got.Config)
	require.Equal(t, "e2-medium", got.Config.MachineType)

	pools, err := svc.Projects.Locations.Clusters.NodePools.List(clusterName(cluster)).Do()
	require.NoError(t, err)
	require.Len(t, pools.NodePools, 2, "default-pool plus the created pool")
	names := map[string]bool{}
	for _, p := range pools.NodePools {
		names[p.Name] = true
	}
	require.True(t, names["default-pool"])
	require.True(t, names[pool])

	// A duplicate node pool is rejected.
	_, err = svc.Projects.Locations.Clusters.NodePools.Create(clusterName(cluster), &container.CreateNodePoolRequest{
		NodePool: &container.NodePool{Name: pool, InitialNodeCount: 1},
	}).Do()
	requireAPIError(t, err, 409)

	// Update (PUT) applies the desired node version and returns UPGRADE_NODES.
	updOp, err := svc.Projects.Locations.Clusters.NodePools.Update(nodePoolName(cluster, pool), &container.UpdateNodePoolRequest{
		NodeVersion: "1.31.0-gke.100",
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "UPGRADE_NODES", updOp.OperationType)
	upd, err := svc.Projects.Locations.Clusters.NodePools.Get(nodePoolName(cluster, pool)).Do()
	require.NoError(t, err)
	require.Equal(t, "1.31.0-gke.100", upd.Version)

	// :setAutoscaling records the autoscaling config.
	asOp, err := svc.Projects.Locations.Clusters.NodePools.SetAutoscaling(nodePoolName(cluster, pool), &container.SetNodePoolAutoscalingRequest{
		Autoscaling: &container.NodePoolAutoscaling{Enabled: true, MinNodeCount: 1, MaxNodeCount: 5},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "UPGRADE_NODES", asOp.OperationType)
	as, err := svc.Projects.Locations.Clusters.NodePools.Get(nodePoolName(cluster, pool)).Do()
	require.NoError(t, err)
	require.NotNil(t, as.Autoscaling)
	require.True(t, as.Autoscaling.Enabled)
	require.Equal(t, int64(1), as.Autoscaling.MinNodeCount)
	require.Equal(t, int64(5), as.Autoscaling.MaxNodeCount)

	// :setSize records the node count and returns SET_NODE_POOL_SIZE. The stored
	// nodeCount is intentionally not echoed on the NodePool wire object: real
	// GKE's google.container.v1.NodePool schema has no nodeCount field (the
	// requested size is carried only by SetNodePoolSizeRequest and observed via
	// the operation), so the API client has nothing to read back.
	sizeOp, err := svc.Projects.Locations.Clusters.NodePools.SetSize(nodePoolName(cluster, pool), &container.SetNodePoolSizeRequest{
		NodeCount: 3,
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "SET_NODE_POOL_SIZE", sizeOp.OperationType)
	require.Equal(t, "DONE", sizeOp.Status)
	sized, err := svc.Projects.Locations.Clusters.NodePools.Get(nodePoolName(cluster, pool)).Do()
	require.NoError(t, err)
	require.Equal(t, "RUNNING", sized.Status, "setSize must leave the pool usable")

	// :setManagement records the node-management options.
	mgmtOp, err := svc.Projects.Locations.Clusters.NodePools.SetManagement(nodePoolName(cluster, pool), &container.SetNodePoolManagementRequest{
		Management: &container.NodeManagement{AutoUpgrade: true, AutoRepair: true},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "SET_NODE_POOL_MANAGEMENT", mgmtOp.OperationType)
	mgmt, err := svc.Projects.Locations.Clusters.NodePools.Get(nodePoolName(cluster, pool)).Do()
	require.NoError(t, err)
	require.NotNil(t, mgmt.Management)
	require.True(t, mgmt.Management.AutoUpgrade)
	require.True(t, mgmt.Management.AutoRepair)

	// Delete returns DELETE_NODE_POOL and removes the pool.
	delOp, err := svc.Projects.Locations.Clusters.NodePools.Delete(nodePoolName(cluster, pool)).Do()
	require.NoError(t, err)
	require.Equal(t, "DELETE_NODE_POOL", delOp.OperationType)
	_, err = svc.Projects.Locations.Clusters.NodePools.Get(nodePoolName(cluster, pool)).Do()
	requireAPIError(t, err, 404)
}

// TestSDKContainerOperations covers the GKE Operation records get/list/cancel.
func TestSDKContainerOperations(t *testing.T) {
	resetState(t)
	svc := newService(t)

	cluster := unique("c")
	createOp, err := svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{
		Cluster: &container.Cluster{Name: cluster},
	}).Do()
	require.NoError(t, err)

	poolCreateOp, err := svc.Projects.Locations.Clusters.NodePools.Create(clusterName(cluster), &container.CreateNodePoolRequest{
		NodePool: &container.NodePool{Name: unique("p"), InitialNodeCount: 1},
	}).Do()
	require.NoError(t, err)

	fetched, err := svc.Projects.Locations.Operations.Get(operationName(createOp.Name)).Do()
	require.NoError(t, err)
	require.Equal(t, createOp.Name, fetched.Name)
	require.Equal(t, "CREATE_CLUSTER", fetched.OperationType)
	require.Equal(t, "DONE", fetched.Status)
	require.NotEmpty(t, fetched.StartTime)
	require.NotEmpty(t, fetched.EndTime)

	ops, err := svc.Projects.Locations.Operations.List(parent()).Do()
	require.NoError(t, err)
	require.Len(t, ops.Operations, 2)
	seen := map[string]bool{}
	for _, op := range ops.Operations {
		seen[op.Name] = true
	}
	require.True(t, seen[createOp.Name])
	require.True(t, seen[poolCreateOp.Name])

	// Cancel is a no-op success for an existing (already DONE) operation.
	_, err = svc.Projects.Locations.Operations.Cancel(operationName(createOp.Name), &container.CancelOperationRequest{}).Do()
	require.NoError(t, err)

	_, err = svc.Projects.Locations.Operations.Get(operationName("operation-does-not-exist")).Do()
	requireAPIError(t, err, 404)
}

// TestSDKContainerErrors pins the InvalidArgument / NotFound / not-routed
// contracts.
func TestSDKContainerErrors(t *testing.T) {
	resetState(t)
	svc := newService(t)

	// A missing cluster is NotFound.
	_, err := svc.Projects.Locations.Clusters.Get(clusterName("c-missing")).Do()
	requireAPIError(t, err, 404)

	// Deleting a missing cluster is NotFound.
	_, err = svc.Projects.Locations.Clusters.Delete(clusterName("c-missing")).Do()
	requireAPIError(t, err, 404)

	// An invalid cluster name is InvalidArgument.
	_, err = svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{
		Cluster: &container.Cluster{Name: "Bad_Name"},
	}).Do()
	requireAPIError(t, err, 400)

	// A missing cluster name is InvalidArgument.
	_, err = svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{
		Cluster: &container.Cluster{InitialNodeCount: 1},
	}).Do()
	requireAPIError(t, err, 400)

	// A node pool on a missing cluster is NotFound.
	_, err = svc.Projects.Locations.Clusters.NodePools.Create(clusterName("c-missing"), &container.CreateNodePoolRequest{
		NodePool: &container.NodePool{Name: "p1", InitialNodeCount: 1},
	}).Do()
	requireAPIError(t, err, 404)

	// :setSize with a negative node count is InvalidArgument.
	cluster := unique("c")
	_, err = svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{
		Cluster: &container.Cluster{Name: cluster},
	}).Do()
	require.NoError(t, err)
	pool := unique("p")
	_, err = svc.Projects.Locations.Clusters.NodePools.Create(clusterName(cluster), &container.CreateNodePoolRequest{
		NodePool: &container.NodePool{Name: pool, InitialNodeCount: 1},
	}).Do()
	require.NoError(t, err)
	_, err = svc.Projects.Locations.Clusters.NodePools.SetSize(nodePoolName(cluster, pool), &container.SetNodePoolSizeRequest{
		NodeCount: -1,
	}).Do()
	requireAPIError(t, err, 400)
}

// TestSDKContainerClusterCreateRejectsDefaultPoolFieldsWithNodePools drives the
// official client through the conflict real GKE 400s on (Discovery:
// "This field should not be set if node_config or initial_node_count are
// specified"), with the verbatim server messages captured against real GKE.
func TestSDKContainerClusterCreateRejectsDefaultPoolFieldsWithNodePools(t *testing.T) {
	resetState(t)
	svc := newService(t)

	cases := []struct {
		name    string
		cluster *container.Cluster
		want    string
	}{
		{
			name: "initialNodeCount",
			cluster: &container.Cluster{
				Name:             unique("c"),
				InitialNodeCount: 1,
				NodePools:        []*container.NodePool{{Name: "pool-a", InitialNodeCount: 1}},
			},
			want: "It's invalid to specify both cluster.initial_node_count and a node pool. Please only provide a node pool.",
		},
		{
			name: "nodeConfig",
			cluster: &container.Cluster{
				Name:       unique("c"),
				NodeConfig: &container.NodeConfig{MachineType: "e2-micro"},
				NodePools:  []*container.NodePool{{Name: "pool-a", InitialNodeCount: 1}},
			},
			want: "It's invalid to specify both cluster.node_config and a node pool. Please only provide a node pool.",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{Cluster: tc.cluster}).Do()
			requireAPIError(t, err, 400)
			var apiErr *googleapi.Error
			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, tc.want, apiErr.Message)
			// The rejected create persists nothing.
			_, err = svc.Projects.Locations.Clusters.Get(clusterName(tc.cluster.Name)).Do()
			requireAPIError(t, err, 404)
		})
	}
}

// TestSDKContainerClusterNegativeInitialNodeCount pins the DPG49 boundary
// captured against real GKE: the official client's negative cluster-level count
// is rejected with the verbatim message when the request carries no nodePools,
// and accepted (ignored) when it does.
func TestSDKContainerClusterNegativeInitialNodeCount(t *testing.T) {
	resetState(t)
	svc := newService(t)

	neg := unique("cn")
	_, err := svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{Cluster: &container.Cluster{
		Name:             neg,
		InitialNodeCount: -1,
	}}).Do()
	requireAPIError(t, err, 400)
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "Cluster.initial_node_count must be zero or higher.", apiErr.Message)
	_, err = svc.Projects.Locations.Clusters.Get(clusterName(neg)).Do()
	requireAPIError(t, err, 404)

	withPools := unique("cn")
	_, err = svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{Cluster: &container.Cluster{
		Name:             withPools,
		InitialNodeCount: -1,
		NodePools:        []*container.NodePool{{Name: "pool-a", InitialNodeCount: 1}},
	}}).Do()
	require.NoError(t, err)
	got, err := svc.Projects.Locations.Clusters.Get(clusterName(withPools)).Do()
	require.NoError(t, err)
	require.Len(t, got.NodePools, 1)
	require.Equal(t, "pool-a", got.NodePools[0].Name)
}

// TestSDKContainerClusterCreatePoolConflictOnTheWire pins the encoded REST
// contract the apiary client hides: the google.rpc envelope's status string, and
// the one case the client cannot express — an explicit `initialNodeCount: 0`,
// which its `omitempty` tag would drop. A zero cluster-level count means "no
// default node pool", so real GKE accepts it alongside nodePools.
func TestSDKContainerClusterCreatePoolConflictOnTheWire(t *testing.T) {
	resetState(t)

	post := func(t *testing.T, body string) (*http.Response, map[string]any) {
		t.Helper()
		resp, err := http.Post(endpoint()+"v1/"+parent()+"/clusters", "application/json", strings.NewReader(body)) //nolint:noctx
		require.NoError(t, err)
		defer resp.Body.Close()
		var decoded map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&decoded))
		return resp, decoded
	}

	// The 400 keeps GKE's envelope: {"error":{"code","message","status"}}.
	resp, body := post(t, fmt.Sprintf(`{"cluster":{"name":%q,"initialNodeCount":1,"nodePools":[{"name":"pool-a","initialNodeCount":1}]}}`, unique("cw")))
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	apiErr, ok := body["error"].(map[string]any)
	require.True(t, ok, "error envelope = %v", body)
	require.Equal(t, "INVALID_ARGUMENT", apiErr["status"])
	require.Equal(t, "It's invalid to specify both cluster.initial_node_count and a node pool. Please only provide a node pool.", apiErr["message"])

	// An explicit zero count is accepted, and the inline pool materializes.
	id := unique("cz")
	resp, body = post(t, fmt.Sprintf(`{"cluster":{"name":%q,"initialNodeCount":0,"nodePools":[{"name":"pool-a","initialNodeCount":1}]}}`, id))
	require.Equal(t, http.StatusOK, resp.StatusCode, "create with initialNodeCount 0: %v", body)
	require.Equal(t, "CREATE_CLUSTER", body["operationType"])
	got, err := newService(t).Projects.Locations.Clusters.Get(clusterName(id)).Do()
	require.NoError(t, err)
	require.Len(t, got.NodePools, 1)
	require.Equal(t, "pool-a", got.NodePools[0].Name)
}

// TestSDKContainerClusterNestedMessagesOnCreate pins the create-time decode of
// the two nested settings a caller may set — Shielded Nodes can be disabled and
// the pod-per-node constraint chosen — because a create body that is silently
// dropped would read back a cluster the caller never asked for (DPG37).
func TestSDKContainerClusterNestedMessagesOnCreate(t *testing.T) {
	resetState(t)
	svc := newService(t)

	id := unique("cn")
	_, err := svc.Projects.Locations.Clusters.Create(parent(), &container.CreateClusterRequest{
		Cluster: &container.Cluster{
			Name:             id,
			InitialNodeCount: 1,
			ShieldedNodes: &container.ShieldedNodes{
				Enabled:         false,
				ForceSendFields: []string{"Enabled"},
			},
			DefaultMaxPodsConstraint: &container.MaxPodsConstraint{MaxPodsPerNode: 64},
		},
	}).Do()
	require.NoError(t, err)

	got, err := svc.Projects.Locations.Clusters.Get(clusterName(id)).Do()
	require.NoError(t, err)
	require.NotNil(t, got.ShieldedNodes)
	require.False(t, got.ShieldedNodes.Enabled)
	require.NotNil(t, got.DefaultMaxPodsConstraint)
	require.EqualValues(t, 64, got.DefaultMaxPodsConstraint.MaxPodsPerNode)
}
