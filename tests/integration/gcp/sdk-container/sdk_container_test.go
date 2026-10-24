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
