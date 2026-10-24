package grpcconformance

import (
	"context"
	"fmt"

	container "cloud.google.com/go/container/apiv1"
	containerpb "cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// containerLocation is a canonical GKE location; the emulator does not validate
// the location list.
const containerLocation = "us-central1"

// containerChecks covers the GKE v1 surface (google.container.v1.ClusterManager)
// via the official generated cloud.google.com/go/container/apiv1 client:
// ListClusters, GetCluster, CreateCluster, DeleteCluster, GetOperation,
// ListOperations. GKE CreateCluster/DeleteCluster return the
// google.container.v1.Operation inline (not a google.longrunning.Operation).
//
// Every probe is self-contained and run-unique (cfg.ResourceName).
func containerChecks() []Check {
	return []Check{
		{Service: "container", RPC: "CreateCluster", Method: "CreateCluster", KeyField: "CREATE_CLUSTER operation DONE", Run: checkContainerCreateCluster},
		{Service: "container", RPC: "GetCluster", Method: "GetCluster", KeyField: "name/status/currentMasterVersion round-trip", Run: checkContainerGetCluster},
		{Service: "container", RPC: "GetCluster (legacy project/zone)", Method: "GetCluster", KeyField: "legacy project_id/zone/cluster_id addressing", Run: checkContainerGetClusterLegacy},
		{Service: "container", RPC: "ListClusters", Method: "ListClusters", KeyField: "created cluster present", Run: checkContainerListClusters},
		{Service: "container", RPC: "GetOperation", Method: "GetOperation", KeyField: "operation name/type round-trip", Run: checkContainerGetOperation},
		{Service: "container", RPC: "ListOperations", Method: "ListOperations", KeyField: "created operation present", Run: checkContainerListOperations},
		{Service: "container", RPC: "DeleteCluster", Method: "DeleteCluster", KeyField: "DELETE_CLUSTER DONE + NotFound after", Run: checkContainerDeleteCluster},
	}
}

// containerNodePoolChecks covers the GKE v1 node-pool CRUD and the cluster/
// node-pool update setters (google.container.v1.ClusterManager) through the
// official generated client. Each probe creates its own run-unique cluster so it
// is self-contained.
func containerNodePoolChecks() []Check {
	return []Check{
		{Service: "container", RPC: "CreateNodePool", Method: "CreateNodePool", KeyField: "CREATE_NODE_POOL operation DONE", Run: checkCreateNodePool},
		{Service: "container", RPC: "GetNodePool", Method: "GetNodePool", KeyField: "node pool name/status round-trip", Run: checkGetNodePool},
		{Service: "container", RPC: "ListNodePools", Method: "ListNodePools", KeyField: "created node pool present", Run: checkListNodePools},
		{Service: "container", RPC: "DeleteNodePool", Method: "DeleteNodePool", KeyField: "DELETE_NODE_POOL DONE + NotFound after", Run: checkDeleteNodePool},
		{Service: "container", RPC: "UpdateNodePool", Method: "UpdateNodePool", KeyField: "UPGRADE_NODES DONE + version applied", Run: checkUpdateNodePool},
		{Service: "container", RPC: "SetNodePoolAutoscaling", Method: "SetNodePoolAutoscaling", KeyField: "operation DONE + autoscaling applied", Run: checkSetNodePoolAutoscaling},
		{Service: "container", RPC: "SetNodePoolManagement", Method: "SetNodePoolManagement", KeyField: "SET_NODE_POOL_MANAGEMENT DONE", Run: checkSetNodePoolManagement},
		{Service: "container", RPC: "SetNodePoolSize", Method: "SetNodePoolSize", KeyField: "SET_NODE_POOL_SIZE DONE", Run: checkSetNodePoolSize},
		{Service: "container", RPC: "RollbackNodePoolUpgrade", Method: "RollbackNodePoolUpgrade", KeyField: "operation DONE", Run: checkRollbackNodePoolUpgrade},
		{Service: "container", RPC: "SetAddonsConfig", Method: "SetAddonsConfig", KeyField: "operation DONE + addons applied", Run: checkSetAddonsConfig},
		{Service: "container", RPC: "SetLegacyAbac", Method: "SetLegacyAbac", KeyField: "operation DONE", Run: checkSetLegacyAbac},
		{Service: "container", RPC: "SetLocations", Method: "SetLocations", KeyField: "operation DONE + locations applied", Run: checkSetLocations},
		{Service: "container", RPC: "SetLoggingService", Method: "SetLoggingService", KeyField: "operation DONE + logging applied", Run: checkSetLoggingService},
		{Service: "container", RPC: "SetMonitoringService", Method: "SetMonitoringService", KeyField: "operation DONE + monitoring applied", Run: checkSetMonitoringService},
		{Service: "container", RPC: "SetNetworkPolicy", Method: "SetNetworkPolicy", KeyField: "operation DONE", Run: checkSetNetworkPolicy},
		{Service: "container", RPC: "SetMaintenancePolicy", Method: "SetMaintenancePolicy", KeyField: "operation DONE", Run: checkSetMaintenancePolicy},
		{Service: "container", RPC: "SetMasterAuth", Method: "SetMasterAuth", KeyField: "operation DONE", Run: checkSetMasterAuth},
		{Service: "container", RPC: "SetLabels", Method: "SetLabels", KeyField: "operation DONE + labels applied", Run: checkSetLabels},
		{Service: "container", RPC: "UpdateCluster", Method: "UpdateCluster", KeyField: "UPDATE_CLUSTER DONE + field applied", Run: checkUpdateCluster},
		{Service: "container", RPC: "UpdateMaster", Method: "UpdateMaster", KeyField: "UPGRADE_MASTER DONE + version applied", Run: checkUpdateMaster},
		{Service: "container", RPC: "StartIPRotation", Method: "StartIPRotation", KeyField: "operation DONE", Run: checkStartIPRotation},
		{Service: "container", RPC: "CompleteIPRotation", Method: "CompleteIPRotation", KeyField: "operation DONE", Run: checkCompleteIPRotation},
		{Service: "container", RPC: "CancelOperation", Method: "CancelOperation", KeyField: "cancel of an existing operation succeeds", Run: checkCancelOperation},
	}
}

func nodePoolName(cfg Config, cluster, pool string) string {
	return containerParent(cfg) + "/clusters/" + cluster + "/nodePools/" + pool
}

// containerNameRaw returns the canonical cluster name for an already-resolved
// cluster id (no further run-suffix), so a check that pre-computed
// cfg.ResourceName does not apply the suffix twice.
func containerNameRaw(cfg Config, cluster string) string {
	return containerParent(cfg) + "/clusters/" + cluster
}

// createPoolCluster creates a cluster and a node pool, returning the pool id.
func createPoolCluster(ctx context.Context, client *container.ClusterManagerClient, cfg Config, prefix string) (string, string, error) {
	cluster := cfg.ResourceName(prefix)
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cluster, InitialNodeCount: 1},
	}); err != nil {
		return "", "", fmt.Errorf("CreateCluster: %w", err)
	}
	pool := cfg.ResourceName(prefix + "-np")
	if _, err := client.CreateNodePool(ctx, &containerpb.CreateNodePoolRequest{
		Parent:   containerParent(cfg) + "/clusters/" + cluster,
		NodePool: &containerpb.NodePool{Name: pool, InitialNodeCount: 1},
	}); err != nil {
		return "", "", fmt.Errorf("CreateNodePool: %w", err)
	}
	return cluster, pool, nil
}

func checkCreateNodePool(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	cluster := cfg.ResourceName("gcpc-grpc-np-create")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cluster, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	op, err := client.CreateNodePool(ctx, &containerpb.CreateNodePoolRequest{
		Parent:   containerParent(cfg) + "/clusters/" + cluster,
		NodePool: &containerpb.NodePool{Name: cfg.ResourceName("gcpc-grpc-np-create-pool"), InitialNodeCount: 1},
	})
	if err != nil {
		return fmt.Errorf("CreateNodePool: %w", err)
	}
	if op.GetOperationType() != containerpb.Operation_CREATE_NODE_POOL || op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("CreateNodePool op = %v/%v", op.GetOperationType(), op.GetStatus())
	}
	return nil
}

func checkGetNodePool(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster, pool, err := createPoolCluster(ctx, client, cfg, "gcpc-grpc-np-get")
	if err != nil {
		return err
	}
	got, err := client.GetNodePool(ctx, &containerpb.GetNodePoolRequest{Name: nodePoolName(cfg, cluster, pool)})
	if err != nil {
		return fmt.Errorf("GetNodePool: %w", err)
	}
	if got.GetName() != pool || got.GetStatus() != containerpb.NodePool_RUNNING {
		return fmt.Errorf("GetNodePool = %q/%v", got.GetName(), got.GetStatus())
	}
	return nil
}

func checkListNodePools(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster, pool, err := createPoolCluster(ctx, client, cfg, "gcpc-grpc-np-list")
	if err != nil {
		return err
	}
	resp, err := client.ListNodePools(ctx, &containerpb.ListNodePoolsRequest{Parent: containerParent(cfg) + "/clusters/" + cluster})
	if err != nil {
		return fmt.Errorf("ListNodePools: %w", err)
	}
	for _, p := range resp.GetNodePools() {
		if p.GetName() == pool {
			return nil
		}
	}
	return fmt.Errorf("ListNodePools did not include %q", pool)
}

func checkDeleteNodePool(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster, pool, err := createPoolCluster(ctx, client, cfg, "gcpc-grpc-np-del")
	if err != nil {
		return err
	}
	op, err := client.DeleteNodePool(ctx, &containerpb.DeleteNodePoolRequest{Name: nodePoolName(cfg, cluster, pool)})
	if err != nil {
		return fmt.Errorf("DeleteNodePool: %w", err)
	}
	if op.GetOperationType() != containerpb.Operation_DELETE_NODE_POOL || op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("DeleteNodePool op = %v/%v", op.GetOperationType(), op.GetStatus())
	}
	if _, err := client.GetNodePool(ctx, &containerpb.GetNodePoolRequest{Name: nodePoolName(cfg, cluster, pool)}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetNodePool after delete code = %v, want NotFound", status.Code(err))
	}
	return nil
}

func checkUpdateNodePool(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster, pool, err := createPoolCluster(ctx, client, cfg, "gcpc-grpc-np-upd")
	if err != nil {
		return err
	}
	op, err := client.UpdateNodePool(ctx, &containerpb.UpdateNodePoolRequest{
		Name:        nodePoolName(cfg, cluster, pool),
		NodeVersion: "1.31.0-gke.100",
	})
	if err != nil {
		return fmt.Errorf("UpdateNodePool: %w", err)
	}
	if op.GetOperationType() != containerpb.Operation_UPGRADE_NODES || op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("UpdateNodePool op = %v/%v", op.GetOperationType(), op.GetStatus())
	}
	got, err := client.GetNodePool(ctx, &containerpb.GetNodePoolRequest{Name: nodePoolName(cfg, cluster, pool)})
	if err != nil {
		return fmt.Errorf("GetNodePool: %w", err)
	}
	if got.GetVersion() != "1.31.0-gke.100" {
		return fmt.Errorf("node pool version = %q", got.GetVersion())
	}
	return nil
}

func checkSetNodePoolAutoscaling(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster, pool, err := createPoolCluster(ctx, client, cfg, "gcpc-grpc-np-as")
	if err != nil {
		return err
	}
	op, err := client.SetNodePoolAutoscaling(ctx, &containerpb.SetNodePoolAutoscalingRequest{
		Name:        nodePoolName(cfg, cluster, pool),
		Autoscaling: &containerpb.NodePoolAutoscaling{Enabled: true, MinNodeCount: 1, MaxNodeCount: 5},
	})
	if err != nil {
		return fmt.Errorf("SetNodePoolAutoscaling: %w", err)
	}
	if op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("SetNodePoolAutoscaling status = %v", op.GetStatus())
	}
	got, err := client.GetNodePool(ctx, &containerpb.GetNodePoolRequest{Name: nodePoolName(cfg, cluster, pool)})
	if err != nil {
		return fmt.Errorf("GetNodePool: %w", err)
	}
	if a := got.GetAutoscaling(); a == nil || !a.GetEnabled() || a.GetMaxNodeCount() != 5 {
		return fmt.Errorf("autoscaling = %v", a)
	}
	return nil
}

func checkSetNodePoolManagement(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster, pool, err := createPoolCluster(ctx, client, cfg, "gcpc-grpc-np-mgmt")
	if err != nil {
		return err
	}
	op, err := client.SetNodePoolManagement(ctx, &containerpb.SetNodePoolManagementRequest{
		Name:       nodePoolName(cfg, cluster, pool),
		Management: &containerpb.NodeManagement{AutoUpgrade: true, AutoRepair: true},
	})
	if err != nil {
		return fmt.Errorf("SetNodePoolManagement: %w", err)
	}
	if op.GetOperationType() != containerpb.Operation_SET_NODE_POOL_MANAGEMENT || op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("SetNodePoolManagement op = %v/%v", op.GetOperationType(), op.GetStatus())
	}
	return nil
}

func checkSetNodePoolSize(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster, pool, err := createPoolCluster(ctx, client, cfg, "gcpc-grpc-np-size")
	if err != nil {
		return err
	}
	op, err := client.SetNodePoolSize(ctx, &containerpb.SetNodePoolSizeRequest{
		Name:      nodePoolName(cfg, cluster, pool),
		NodeCount: 3,
	})
	if err != nil {
		return fmt.Errorf("SetNodePoolSize: %w", err)
	}
	if op.GetOperationType() != containerpb.Operation_SET_NODE_POOL_SIZE || op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("SetNodePoolSize op = %v/%v", op.GetOperationType(), op.GetStatus())
	}
	return nil
}

func checkRollbackNodePoolUpgrade(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster, pool, err := createPoolCluster(ctx, client, cfg, "gcpc-grpc-np-rb")
	if err != nil {
		return err
	}
	op, err := client.RollbackNodePoolUpgrade(ctx, &containerpb.RollbackNodePoolUpgradeRequest{Name: nodePoolName(cfg, cluster, pool)})
	if err != nil {
		return fmt.Errorf("RollbackNodePoolUpgrade: %w", err)
	}
	if op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("RollbackNodePoolUpgrade status = %v", op.GetStatus())
	}
	return nil
}

func checkSetAddonsConfig(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster := cfg.ResourceName("gcpc-grpc-set-addons")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cluster, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	op, err := client.SetAddonsConfig(ctx, &containerpb.SetAddonsConfigRequest{
		Name:         containerNameRaw(cfg, cluster),
		AddonsConfig: &containerpb.AddonsConfig{HttpLoadBalancing: &containerpb.HttpLoadBalancing{Disabled: true}},
	})
	if err != nil {
		return fmt.Errorf("SetAddonsConfig: %w", err)
	}
	if op.GetOperationType() != containerpb.Operation_UPDATE_CLUSTER || op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("SetAddonsConfig op = %v/%v", op.GetOperationType(), op.GetStatus())
	}
	got, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{Name: containerNameRaw(cfg, cluster)})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	if h := got.GetAddonsConfig().GetHttpLoadBalancing(); h == nil || !h.GetDisabled() {
		return fmt.Errorf("addonsConfig.httpLoadBalancing = %v", h)
	}
	return nil
}

func checkSetLegacyAbac(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster := cfg.ResourceName("gcpc-grpc-legacy-abac")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cluster, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	op, err := client.SetLegacyAbac(ctx, &containerpb.SetLegacyAbacRequest{Name: containerNameRaw(cfg, cluster), Enabled: true})
	if err != nil {
		return fmt.Errorf("SetLegacyAbac: %w", err)
	}
	if op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("SetLegacyAbac status = %v", op.GetStatus())
	}
	return nil
}

func checkSetLocations(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster := cfg.ResourceName("gcpc-grpc-set-locations")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cluster, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	op, err := client.SetLocations(ctx, &containerpb.SetLocationsRequest{
		Name:      containerNameRaw(cfg, cluster),
		Locations: []string{"us-central1-a", "us-central1-b"},
	})
	if err != nil {
		return fmt.Errorf("SetLocations: %w", err)
	}
	if op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("SetLocations status = %v", op.GetStatus())
	}
	got, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{Name: containerNameRaw(cfg, cluster)})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	if len(got.GetLocations()) != 2 {
		return fmt.Errorf("locations = %v", got.GetLocations())
	}
	return nil
}

func checkSetLoggingService(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster := cfg.ResourceName("gcpc-grpc-set-logging")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cluster, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	op, err := client.SetLoggingService(ctx, &containerpb.SetLoggingServiceRequest{
		Name:           containerNameRaw(cfg, cluster),
		LoggingService: "logging.googleapis.com/kubernetes",
	})
	if err != nil {
		return fmt.Errorf("SetLoggingService: %w", err)
	}
	if op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("SetLoggingService status = %v", op.GetStatus())
	}
	got, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{Name: containerNameRaw(cfg, cluster)})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	if got.GetLoggingService() != "logging.googleapis.com/kubernetes" {
		return fmt.Errorf("loggingService = %q", got.GetLoggingService())
	}
	return nil
}

func checkSetMonitoringService(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster := cfg.ResourceName("gcpc-grpc-set-monitoring")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cluster, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	op, err := client.SetMonitoringService(ctx, &containerpb.SetMonitoringServiceRequest{
		Name:              containerNameRaw(cfg, cluster),
		MonitoringService: "monitoring.googleapis.com/kubernetes",
	})
	if err != nil {
		return fmt.Errorf("SetMonitoringService: %w", err)
	}
	if op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("SetMonitoringService status = %v", op.GetStatus())
	}
	return nil
}

func checkSetNetworkPolicy(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster := cfg.ResourceName("gcpc-grpc-set-netpol")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cluster, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	op, err := client.SetNetworkPolicy(ctx, &containerpb.SetNetworkPolicyRequest{
		Name:          containerNameRaw(cfg, cluster),
		NetworkPolicy: &containerpb.NetworkPolicy{Enabled: true, Provider: containerpb.NetworkPolicy_CALICO},
	})
	if err != nil {
		return fmt.Errorf("SetNetworkPolicy: %w", err)
	}
	if op.GetOperationType() != containerpb.Operation_UPDATE_CLUSTER || op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("SetNetworkPolicy op = %v/%v", op.GetOperationType(), op.GetStatus())
	}
	return nil
}

func checkSetMaintenancePolicy(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster := cfg.ResourceName("gcpc-grpc-set-maint")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cluster, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	op, err := client.SetMaintenancePolicy(ctx, &containerpb.SetMaintenancePolicyRequest{
		Name: containerNameRaw(cfg, cluster),
		MaintenancePolicy: &containerpb.MaintenancePolicy{
			Window: &containerpb.MaintenanceWindow{
				Policy: &containerpb.MaintenanceWindow_DailyMaintenanceWindow{
					DailyMaintenanceWindow: &containerpb.DailyMaintenanceWindow{StartTime: "03:00"},
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("SetMaintenancePolicy: %w", err)
	}
	if op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("SetMaintenancePolicy status = %v", op.GetStatus())
	}
	return nil
}

func checkSetMasterAuth(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster := cfg.ResourceName("gcpc-grpc-set-masterauth")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cluster, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	op, err := client.SetMasterAuth(ctx, &containerpb.SetMasterAuthRequest{
		Name:   containerNameRaw(cfg, cluster),
		Action: containerpb.SetMasterAuthRequest_SET_USERNAME,
		Update: &containerpb.MasterAuth{Username: "admin"},
	})
	if err != nil {
		return fmt.Errorf("SetMasterAuth: %w", err)
	}
	if op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("SetMasterAuth status = %v", op.GetStatus())
	}
	return nil
}

func checkSetLabels(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster := cfg.ResourceName("gcpc-grpc-set-labels")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cluster, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	op, err := client.SetLabels(ctx, &containerpb.SetLabelsRequest{
		Name:           containerNameRaw(cfg, cluster),
		ResourceLabels: map[string]string{"team": "platform"},
	})
	if err != nil {
		return fmt.Errorf("SetLabels: %w", err)
	}
	if op.GetOperationType() != containerpb.Operation_UPDATE_CLUSTER || op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("SetLabels op = %v/%v", op.GetOperationType(), op.GetStatus())
	}
	got, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{Name: containerNameRaw(cfg, cluster)})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	if got.GetResourceLabels()["team"] != "platform" {
		return fmt.Errorf("resourceLabels = %v", got.GetResourceLabels())
	}
	return nil
}

func checkUpdateCluster(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster := cfg.ResourceName("gcpc-grpc-update-cluster")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cluster, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	op, err := client.UpdateCluster(ctx, &containerpb.UpdateClusterRequest{
		Name: containerNameRaw(cfg, cluster),
		Update: &containerpb.ClusterUpdate{
			DesiredMonitoringService: "monitoring.googleapis.com/kubernetes",
		},
	})
	if err != nil {
		return fmt.Errorf("UpdateCluster: %w", err)
	}
	if op.GetOperationType() != containerpb.Operation_UPDATE_CLUSTER || op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("UpdateCluster op = %v/%v", op.GetOperationType(), op.GetStatus())
	}
	got, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{Name: containerNameRaw(cfg, cluster)})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	if got.GetMonitoringService() != "monitoring.googleapis.com/kubernetes" {
		return fmt.Errorf("monitoringService = %q", got.GetMonitoringService())
	}
	return nil
}

func checkUpdateMaster(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster := cfg.ResourceName("gcpc-grpc-update-master")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cluster, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	op, err := client.UpdateMaster(ctx, &containerpb.UpdateMasterRequest{
		Name:          containerNameRaw(cfg, cluster),
		MasterVersion: "1.31.0-gke.100",
	})
	if err != nil {
		return fmt.Errorf("UpdateMaster: %w", err)
	}
	if op.GetOperationType() != containerpb.Operation_UPGRADE_MASTER || op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("UpdateMaster op = %v/%v", op.GetOperationType(), op.GetStatus())
	}
	got, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{Name: containerNameRaw(cfg, cluster)})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	if got.GetCurrentMasterVersion() != "1.31.0-gke.100" {
		return fmt.Errorf("currentMasterVersion = %q", got.GetCurrentMasterVersion())
	}
	return nil
}

func checkStartIPRotation(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster := cfg.ResourceName("gcpc-grpc-ip-start")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cluster, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	op, err := client.StartIPRotation(ctx, &containerpb.StartIPRotationRequest{Name: containerNameRaw(cfg, cluster)})
	if err != nil {
		return fmt.Errorf("StartIPRotation: %w", err)
	}
	if op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("StartIPRotation status = %v", op.GetStatus())
	}
	return nil
}

func checkCompleteIPRotation(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	cluster := cfg.ResourceName("gcpc-grpc-ip-complete")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cluster, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	if _, err := client.StartIPRotation(ctx, &containerpb.StartIPRotationRequest{Name: containerNameRaw(cfg, cluster)}); err != nil {
		return fmt.Errorf("StartIPRotation: %w", err)
	}
	op, err := client.CompleteIPRotation(ctx, &containerpb.CompleteIPRotationRequest{Name: containerNameRaw(cfg, cluster)})
	if err != nil {
		return fmt.Errorf("CompleteIPRotation: %w", err)
	}
	if op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("CompleteIPRotation status = %v", op.GetStatus())
	}
	return nil
}

func checkCancelOperation(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	op, err := createContainerCluster(ctx, client, cfg, "gcpc-grpc-cancel-op")
	if err != nil {
		return err
	}
	if err := client.CancelOperation(ctx, &containerpb.CancelOperationRequest{
		Name: operationName(cfg, containerParent(cfg), op.GetName()),
	}); err != nil {
		return fmt.Errorf("CancelOperation: %w", err)
	}
	return nil
}

// newContainerClient dials the emulator and returns the official generated GKE
// client (gRPC transport).
func newContainerClient(ctx context.Context, cfg Config) (*container.ClusterManagerClient, error) {
	return container.NewClusterManagerClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

func containerParent(cfg Config) string {
	return fmt.Sprintf("projects/%s/locations/%s", cfg.Project, containerLocation)
}

func containerName(cfg Config, prefix string) string {
	return containerParent(cfg) + "/clusters/" + cfg.ResourceName(prefix)
}

func operationName(cfg Config, parent, operationID string) string {
	return parent + "/operations/" + operationID
}

// createContainerCluster creates a run-unique cluster and returns the operation.
func createContainerCluster(ctx context.Context, client *container.ClusterManagerClient, cfg Config, prefix string) (*containerpb.Operation, error) {
	op, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cfg.ResourceName(prefix), InitialNodeCount: 1},
	})
	if err != nil {
		return nil, fmt.Errorf("CreateCluster: %w", err)
	}
	return op, nil
}

// Check 1: CreateCluster returns a DONE CREATE_CLUSTER operation.
func checkContainerCreateCluster(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	op, err := createContainerCluster(ctx, client, cfg, "gcpc-grpc-gke-create")
	if err != nil {
		return err
	}
	if op.GetOperationType() != containerpb.Operation_CREATE_CLUSTER {
		return fmt.Errorf("CreateCluster operationType = %v, want CREATE_CLUSTER", op.GetOperationType())
	}
	if op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("CreateCluster status = %v, want DONE", op.GetStatus())
	}
	if op.GetName() == "" || op.GetSelfLink() == "" {
		return fmt.Errorf("CreateCluster operation missing name/selfLink")
	}
	return nil
}

// Check 2: GetCluster returns the short name, RUNNING status and a master version.
func checkContainerGetCluster(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	name := containerName(cfg, "gcpc-grpc-gke-get")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cfg.ResourceName("gcpc-grpc-gke-get"), InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	got, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{Name: name})
	if err != nil {
		return fmt.Errorf("GetCluster: %w", err)
	}
	if got.GetName() != cfg.ResourceName("gcpc-grpc-gke-get") {
		return fmt.Errorf("GetCluster name = %q", got.GetName())
	}
	if got.GetStatus() != containerpb.Cluster_RUNNING {
		return fmt.Errorf("GetCluster status = %v, want RUNNING", got.GetStatus())
	}
	if got.GetCurrentMasterVersion() == "" {
		return fmt.Errorf("GetCluster currentMasterVersion is empty")
	}
	return nil
}

// Check 3: the legacy project_id/zone/cluster_id addressing reaches the same
// cluster as the canonical name.
func checkContainerGetClusterLegacy(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-gke-legacy")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: id, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	got, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{
		ProjectId: cfg.Project, Zone: containerLocation, ClusterId: id,
	})
	if err != nil {
		return fmt.Errorf("GetCluster (legacy): %w", err)
	}
	if got.GetName() != id {
		return fmt.Errorf("legacy GetCluster name = %q, want %q", got.GetName(), id)
	}
	if got.GetLocation() != containerLocation {
		return fmt.Errorf("legacy GetCluster location = %q", got.GetLocation())
	}
	return nil
}

// Check 4: ListClusters includes the created cluster.
func checkContainerListClusters(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-gke-list")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: id, InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	resp, err := client.ListClusters(ctx, &containerpb.ListClustersRequest{Parent: containerParent(cfg)})
	if err != nil {
		return fmt.Errorf("ListClusters: %w", err)
	}
	for _, c := range resp.GetClusters() {
		if c.GetName() == id {
			return nil
		}
	}
	return fmt.Errorf("ListClusters did not include %q", id)
}

// Check 5: GetOperation returns the operation the create produced.
func checkContainerGetOperation(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	op, err := createContainerCluster(ctx, client, cfg, "gcpc-grpc-gke-op")
	if err != nil {
		return err
	}
	got, err := client.GetOperation(ctx, &containerpb.GetOperationRequest{
		Name: operationName(cfg, containerParent(cfg), op.GetName()),
	})
	if err != nil {
		return fmt.Errorf("GetOperation: %w", err)
	}
	if got.GetName() != op.GetName() || got.GetOperationType() != containerpb.Operation_CREATE_CLUSTER {
		return fmt.Errorf("GetOperation = %q/%v", got.GetName(), got.GetOperationType())
	}
	return nil
}

// Check 6: ListOperations includes the create operation.
func checkContainerListOperations(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	op, err := createContainerCluster(ctx, client, cfg, "gcpc-grpc-gke-ops")
	if err != nil {
		return err
	}
	resp, err := client.ListOperations(ctx, &containerpb.ListOperationsRequest{Parent: containerParent(cfg)})
	if err != nil {
		return fmt.Errorf("ListOperations: %w", err)
	}
	for _, o := range resp.GetOperations() {
		if o.GetName() == op.GetName() {
			return nil
		}
	}
	return fmt.Errorf("ListOperations did not include %q", op.GetName())
}

// Check 7: DeleteCluster returns a DONE DELETE_CLUSTER operation and the cluster
// is subsequently NotFound.
func checkContainerDeleteCluster(ctx context.Context, cfg Config) error {
	client, err := newContainerClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	name := containerName(cfg, "gcpc-grpc-gke-delete")
	if _, err := client.CreateCluster(ctx, &containerpb.CreateClusterRequest{
		Parent:  containerParent(cfg),
		Cluster: &containerpb.Cluster{Name: cfg.ResourceName("gcpc-grpc-gke-delete"), InitialNodeCount: 1},
	}); err != nil {
		return fmt.Errorf("CreateCluster: %w", err)
	}
	op, err := client.DeleteCluster(ctx, &containerpb.DeleteClusterRequest{Name: name})
	if err != nil {
		return fmt.Errorf("DeleteCluster: %w", err)
	}
	if op.GetOperationType() != containerpb.Operation_DELETE_CLUSTER || op.GetStatus() != containerpb.Operation_DONE {
		return fmt.Errorf("DeleteCluster op = %v/%v", op.GetOperationType(), op.GetStatus())
	}
	if _, err := client.GetCluster(ctx, &containerpb.GetClusterRequest{Name: name}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetCluster after delete code = %v, want NotFound", status.Code(err))
	}
	return nil
}
