// Package container is the gRPC transport for Google Kubernetes Engine (GKE) v1
// (google.container.v1.ClusterManager). It is a thin proto adapter over the
// transport-neutral core in internal/gcp/service/container: it transcodes
// between the generated protobuf messages and the core's typed API, and maps
// core errors to gRPC status codes. It owns no business logic and no state
// beyond its default project.
//
// The GKE v1 proto is the legacy API: every request carries the canonical
// name/parent fields AND the older project_id/zone/cluster_id (or operation_id)
// fields, and the Java/Go clients may use either. The adapter resolves both —
// name/parent wins when present, otherwise the legacy fields are folded into
// the core's project + location + resource tuple.
package container

import (
	"context"

	containerpb "cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/protobuf/types/known/emptypb"

	grpcutil "jaiscloud/internal/gcp/grpc"
	core "jaiscloud/internal/gcp/service/container"
	containerstore "jaiscloud/internal/gcp/store/container"
	"jaiscloud/internal/model"
)

// Service implements containerpb.ClusterManagerServer over the shared core.
type Service struct {
	containerpb.UnimplementedClusterManagerServer

	core        *core.Service
	defaultProj string
}

// NewService returns a GKE gRPC service wrapping the core. defaultProj is the
// config-default project used when a request carries none.
func NewService(c *core.Service, defaultProj string) *Service {
	return &Service{core: c, defaultProj: defaultProj}
}

func mapError(err error) error { return grpcutil.GRPCStatus(err) }

func invalid(msg string) error { return model.NewProviderError("InvalidArgument", msg, 400) }

// projectFor resolves the owning project for a request, falling back to the
// gRPC routing metadata then the configured default.
func (s *Service) projectFor(ctx context.Context, project string) string {
	if project != "" {
		return project
	}
	return grpcutil.ProjectFromMetadata(ctx, s.defaultProj)
}

// parentRef resolves the project + location a collection RPC targets. A
// canonical parent (projects/{p}/locations/{l}) wins; otherwise the legacy
// project_id + zone pair is used.
func parentRef(parent, projectID, zone string) (project, location string, err error) {
	if parent != "" {
		p, l, ok := core.ParseParent(parent)
		if !ok {
			return "", "", invalid("invalid parent: " + parent)
		}
		return p, l, nil
	}
	if projectID == "" || zone == "" {
		return "", "", invalid("parent (or project_id and zone) is required")
	}
	return projectID, zone, nil
}

// clusterRef resolves the project + location + cluster a cluster RPC targets. A
// canonical name (projects/{p}/locations/{l}/clusters/{c}) wins; otherwise the
// legacy project_id + zone + cluster_id triple is used.
func clusterRef(name, projectID, zone, clusterID string) (project, location, cluster string, err error) {
	if name != "" {
		p, l, c, ok := core.ParseClusterName(name)
		if !ok {
			return "", "", "", invalid("invalid cluster name: " + name)
		}
		return p, l, c, nil
	}
	if projectID == "" || zone == "" || clusterID == "" {
		return "", "", "", invalid("name (or project_id, zone and cluster_id) is required")
	}
	return projectID, zone, clusterID, nil
}

// operationRef resolves the project + location + operation an operation RPC
// targets. A canonical name (projects/{p}/locations/{l}/operations/{o}) wins;
// otherwise the legacy project_id + zone + operation_id triple is used.
func operationRef(name, projectID, zone, operationID string) (project, location, operation string, err error) {
	if name != "" {
		p, l, o, ok := core.ParseOperationName(name)
		if !ok {
			return "", "", "", invalid("invalid operation name: " + name)
		}
		return p, l, o, nil
	}
	if projectID == "" || zone == "" || operationID == "" {
		return "", "", "", invalid("name (or project_id, zone and operation_id) is required")
	}
	return projectID, zone, operationID, nil
}

// ─── Clusters ─────────────────────────────────────────────────────────────────

func (s *Service) ListClusters(ctx context.Context, req *containerpb.ListClustersRequest) (*containerpb.ListClustersResponse, error) {
	project, location, err := parentRef(req.GetParent(), req.GetProjectId(), req.GetZone())
	if err != nil {
		return nil, mapError(err)
	}
	clusters, err := s.core.ListClusters(ctx, s.projectFor(ctx, project), location)
	if err != nil {
		return nil, mapError(err)
	}
	out := &containerpb.ListClustersResponse{}
	for _, c := range clusters {
		out.Clusters = append(out.Clusters, clusterToProto(c))
	}
	return out, nil
}

func (s *Service) GetCluster(ctx context.Context, req *containerpb.GetClusterRequest) (*containerpb.Cluster, error) {
	project, location, cluster, err := clusterRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	c, err := s.core.GetCluster(ctx, s.projectFor(ctx, project), location, cluster)
	if err != nil {
		return nil, mapError(err)
	}
	return clusterToProto(c), nil
}

func (s *Service) CreateCluster(ctx context.Context, req *containerpb.CreateClusterRequest) (*containerpb.Operation, error) {
	project, location, err := parentRef(req.GetParent(), req.GetProjectId(), req.GetZone())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.CreateCluster(ctx, s.projectFor(ctx, project), location, clusterFromProto(req.GetCluster()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) DeleteCluster(ctx context.Context, req *containerpb.DeleteClusterRequest) (*containerpb.Operation, error) {
	project, location, cluster, err := clusterRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.DeleteCluster(ctx, s.projectFor(ctx, project), location, cluster)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

// ─── Operations ───────────────────────────────────────────────────────────────

func (s *Service) GetOperation(ctx context.Context, req *containerpb.GetOperationRequest) (*containerpb.Operation, error) {
	project, location, operation, err := operationRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetOperationId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.GetOperation(ctx, s.projectFor(ctx, project), location, operation)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) ListOperations(ctx context.Context, req *containerpb.ListOperationsRequest) (*containerpb.ListOperationsResponse, error) {
	project, location, err := parentRef(req.GetParent(), req.GetProjectId(), req.GetZone())
	if err != nil {
		return nil, mapError(err)
	}
	ops, err := s.core.ListOperations(ctx, s.projectFor(ctx, project), location)
	if err != nil {
		return nil, mapError(err)
	}
	out := &containerpb.ListOperationsResponse{}
	for _, op := range ops {
		out.Operations = append(out.Operations, operationToProto(op))
	}
	return out, nil
}

func (s *Service) CancelOperation(ctx context.Context, req *containerpb.CancelOperationRequest) (*emptypb.Empty, error) {
	project, location, operation, err := operationRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetOperationId())
	if err != nil {
		return nil, mapError(err)
	}
	if err := s.core.CancelOperation(ctx, s.projectFor(ctx, project), location, operation); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ─── Node pools ───────────────────────────────────────────────────────────────

// nodePoolRef resolves the project + location + cluster + node pool a node-pool
// RPC targets. A canonical name wins; otherwise the legacy
// project_id + zone + cluster_id + node_pool_id tuple is used.
func nodePoolRef(name, projectID, zone, clusterID, nodePoolID string) (project, location, cluster, pool string, err error) {
	if name != "" {
		p, l, c, np, ok := core.ParseNodePoolName(name)
		if !ok {
			return "", "", "", "", invalid("invalid node pool name: " + name)
		}
		return p, l, c, np, nil
	}
	if projectID == "" || zone == "" || clusterID == "" || nodePoolID == "" {
		return "", "", "", "", invalid("name (or project_id, zone, cluster_id and node_pool_id) is required")
	}
	return projectID, zone, clusterID, nodePoolID, nil
}

func (s *Service) ListNodePools(ctx context.Context, req *containerpb.ListNodePoolsRequest) (*containerpb.ListNodePoolsResponse, error) {
	project, location, cluster, err := clusterParentRef(req.GetParent(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	pools, err := s.core.ListNodePools(ctx, s.projectFor(ctx, project), location, cluster)
	if err != nil {
		return nil, mapError(err)
	}
	out := &containerpb.ListNodePoolsResponse{}
	for _, p := range pools {
		out.NodePools = append(out.NodePools, nodePoolToProto(p))
	}
	return out, nil
}

func (s *Service) GetNodePool(ctx context.Context, req *containerpb.GetNodePoolRequest) (*containerpb.NodePool, error) {
	project, location, cluster, pool, err := nodePoolRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId(), req.GetNodePoolId())
	if err != nil {
		return nil, mapError(err)
	}
	p, err := s.core.GetNodePool(ctx, s.projectFor(ctx, project), location, cluster, pool)
	if err != nil {
		return nil, mapError(err)
	}
	return nodePoolToProto(p), nil
}

func (s *Service) CreateNodePool(ctx context.Context, req *containerpb.CreateNodePoolRequest) (*containerpb.Operation, error) {
	project, location, cluster, err := clusterParentRef(req.GetParent(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.CreateNodePool(ctx, s.projectFor(ctx, project), location, cluster, nodePoolFromProto(req.GetNodePool()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) DeleteNodePool(ctx context.Context, req *containerpb.DeleteNodePoolRequest) (*containerpb.Operation, error) {
	project, location, cluster, pool, err := nodePoolRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId(), req.GetNodePoolId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.DeleteNodePool(ctx, s.projectFor(ctx, project), location, cluster, pool)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) UpdateNodePool(ctx context.Context, req *containerpb.UpdateNodePoolRequest) (*containerpb.Operation, error) {
	project, location, cluster, pool, err := nodePoolRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId(), req.GetNodePoolId())
	if err != nil {
		return nil, mapError(err)
	}
	update := containerstore.NodePool{
		Name:      pool,
		Version:   req.GetNodeVersion(),
		Locations: req.GetLocations(),
	}
	if cfg := nodeConfigFromUpdate(req); cfg != nil {
		update.Config = cfg
	}
	if us := req.GetUpgradeSettings(); us != nil {
		update.UpgradeSettings = &containerstore.NodePoolUpgradeSettings{
			MaxSurge:       us.GetMaxSurge(),
			MaxUnavailable: us.GetMaxUnavailable(),
			Strategy:       us.GetStrategy().String(),
		}
	}
	op, err := s.core.UpdateNodePool(ctx, s.projectFor(ctx, project), location, cluster, update)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) SetNodePoolAutoscaling(ctx context.Context, req *containerpb.SetNodePoolAutoscalingRequest) (*containerpb.Operation, error) {
	project, location, cluster, pool, err := nodePoolRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId(), req.GetNodePoolId())
	if err != nil {
		return nil, mapError(err)
	}
	a := req.GetAutoscaling()
	as := containerstore.NodePoolAutoscaling{
		Enabled:           a.GetEnabled(),
		MinNodeCount:      a.GetMinNodeCount(),
		MaxNodeCount:      a.GetMaxNodeCount(),
		Autoprovisioned:   a.GetAutoprovisioned(),
		LocationPolicy:    a.GetLocationPolicy().String(),
		TotalMinNodeCount: a.GetTotalMinNodeCount(),
		TotalMaxNodeCount: a.GetTotalMaxNodeCount(),
	}
	op, err := s.core.SetNodePoolAutoscaling(ctx, s.projectFor(ctx, project), location, cluster, pool, as)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) SetNodePoolManagement(ctx context.Context, req *containerpb.SetNodePoolManagementRequest) (*containerpb.Operation, error) {
	project, location, cluster, pool, err := nodePoolRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId(), req.GetNodePoolId())
	if err != nil {
		return nil, mapError(err)
	}
	m := req.GetManagement()
	mgmt := containerstore.NodeManagement{AutoUpgrade: m.GetAutoUpgrade(), AutoRepair: m.GetAutoRepair()}
	op, err := s.core.SetNodePoolManagement(ctx, s.projectFor(ctx, project), location, cluster, pool, mgmt)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) SetNodePoolSize(ctx context.Context, req *containerpb.SetNodePoolSizeRequest) (*containerpb.Operation, error) {
	project, location, cluster, pool, err := nodePoolRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId(), req.GetNodePoolId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.SetNodePoolSize(ctx, s.projectFor(ctx, project), location, cluster, pool, req.GetNodeCount())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) RollbackNodePoolUpgrade(ctx context.Context, req *containerpb.RollbackNodePoolUpgradeRequest) (*containerpb.Operation, error) {
	project, location, cluster, pool, err := nodePoolRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId(), req.GetNodePoolId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.RollbackNodePoolUpgrade(ctx, s.projectFor(ctx, project), location, cluster, pool)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

// ─── Cluster setters ──────────────────────────────────────────────────────────

func (s *Service) SetAddonsConfig(ctx context.Context, req *containerpb.SetAddonsConfigRequest) (*containerpb.Operation, error) {
	project, location, cluster, err := clusterRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.SetAddonsConfig(ctx, s.projectFor(ctx, project), location, cluster, addonsConfigFromProto(req.GetAddonsConfig()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) SetLegacyAbac(ctx context.Context, req *containerpb.SetLegacyAbacRequest) (*containerpb.Operation, error) {
	project, location, cluster, err := clusterRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.SetLegacyAbac(ctx, s.projectFor(ctx, project), location, cluster, req.GetEnabled())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) SetLocations(ctx context.Context, req *containerpb.SetLocationsRequest) (*containerpb.Operation, error) {
	project, location, cluster, err := clusterRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.SetLocations(ctx, s.projectFor(ctx, project), location, cluster, req.GetLocations())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) SetLoggingService(ctx context.Context, req *containerpb.SetLoggingServiceRequest) (*containerpb.Operation, error) {
	project, location, cluster, err := clusterRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.SetLoggingService(ctx, s.projectFor(ctx, project), location, cluster, req.GetLoggingService())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) SetMonitoringService(ctx context.Context, req *containerpb.SetMonitoringServiceRequest) (*containerpb.Operation, error) {
	project, location, cluster, err := clusterRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.SetMonitoringService(ctx, s.projectFor(ctx, project), location, cluster, req.GetMonitoringService())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) SetNetworkPolicy(ctx context.Context, req *containerpb.SetNetworkPolicyRequest) (*containerpb.Operation, error) {
	project, location, cluster, err := clusterRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	np := req.GetNetworkPolicy()
	op, err := s.core.SetNetworkPolicy(ctx, s.projectFor(ctx, project), location, cluster, containerstore.NetworkPolicy{
		Provider: np.GetProvider().String(),
		Enabled:  np.GetEnabled(),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) SetMaintenancePolicy(ctx context.Context, req *containerpb.SetMaintenancePolicyRequest) (*containerpb.Operation, error) {
	project, location, cluster, err := clusterRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.SetMaintenancePolicy(ctx, s.projectFor(ctx, project), location, cluster, maintenancePolicyFromProto(req.GetMaintenancePolicy()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) SetMasterAuth(ctx context.Context, req *containerpb.SetMasterAuthRequest) (*containerpb.Operation, error) {
	project, location, cluster, err := clusterRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.SetMasterAuth(ctx, s.projectFor(ctx, project), location, cluster, req.GetUpdate().GetUsername())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) SetLabels(ctx context.Context, req *containerpb.SetLabelsRequest) (*containerpb.Operation, error) {
	project, location, cluster, err := clusterRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.SetLabels(ctx, s.projectFor(ctx, project), location, cluster, req.GetResourceLabels())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) UpdateCluster(ctx context.Context, req *containerpb.UpdateClusterRequest) (*containerpb.Operation, error) {
	project, location, cluster, err := clusterRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.UpdateCluster(ctx, s.projectFor(ctx, project), location, cluster, clusterUpdateFromProto(req.GetUpdate()))
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) UpdateMaster(ctx context.Context, req *containerpb.UpdateMasterRequest) (*containerpb.Operation, error) {
	project, location, cluster, err := clusterRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.UpdateMaster(ctx, s.projectFor(ctx, project), location, cluster, req.GetMasterVersion())
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) StartIPRotation(ctx context.Context, req *containerpb.StartIPRotationRequest) (*containerpb.Operation, error) {
	project, location, cluster, err := clusterRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.StartIPRotation(ctx, s.projectFor(ctx, project), location, cluster)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

func (s *Service) CompleteIPRotation(ctx context.Context, req *containerpb.CompleteIPRotationRequest) (*containerpb.Operation, error) {
	project, location, cluster, err := clusterRef(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, mapError(err)
	}
	op, err := s.core.CompleteIPRotation(ctx, s.projectFor(ctx, project), location, cluster)
	if err != nil {
		return nil, mapError(err)
	}
	return operationToProto(op), nil
}

// clusterParentRef resolves the project + location + cluster a node-pool
// collection RPC targets. A canonical cluster parent
// (projects/{p}/locations/{l}/clusters/{c}) wins; otherwise the legacy
// project_id + zone + cluster_id triple is used.
func clusterParentRef(parent, projectID, zone, clusterID string) (project, location, cluster string, err error) {
	if parent != "" {
		p, l, c, ok := core.ParseClusterName(parent)
		if !ok {
			return "", "", "", invalid("invalid parent: " + parent)
		}
		return p, l, c, nil
	}
	if projectID == "" || zone == "" || clusterID == "" {
		return "", "", "", invalid("parent (or project_id, zone and cluster_id) is required")
	}
	return projectID, zone, clusterID, nil
}

// nodeConfigFromUpdate builds a node config from the UpdateNodePoolRequest's
// flat node-config fields.
func nodeConfigFromUpdate(req *containerpb.UpdateNodePoolRequest) *containerstore.NodeConfig {
	c := &containerstore.NodeConfig{
		ImageType:   req.GetImageType(),
		MachineType: req.GetMachineType(),
		DiskType:    req.GetDiskType(),
		DiskSizeGb:  int32(req.GetDiskSizeGb()),
	}
	if c.ImageType == "" && c.MachineType == "" && c.DiskType == "" && c.DiskSizeGb == 0 {
		return nil
	}
	return c
}

// clusterUpdateFromProto builds the core ClusterUpdate subset.
func clusterUpdateFromProto(u *containerpb.ClusterUpdate) core.ClusterUpdate {
	if u == nil {
		return core.ClusterUpdate{}
	}
	out := core.ClusterUpdate{
		DesiredMasterVersion:     u.GetDesiredMasterVersion(),
		DesiredNodeVersion:       u.GetDesiredNodeVersion(),
		DesiredImageType:         u.GetDesiredImageType(),
		DesiredLocations:         u.GetDesiredLocations(),
		DesiredLoggingService:    u.GetDesiredLoggingService(),
		DesiredMonitoringService: u.GetDesiredMonitoringService(),
		DesiredNodePoolID:        u.GetDesiredNodePoolId(),
	}
	if a := u.GetDesiredAddonsConfig(); a != nil {
		cfg := addonsConfigFromProto(a)
		out.DesiredAddonsConfig = &cfg
	}
	if a := u.GetDesiredNodePoolAutoscaling(); a != nil {
		out.DesiredNodePoolAutoscaling = nodePoolAutoscalingFromProto(a)
	}
	return out
}

// compile-time assertion that Service implements the generated server.
var _ containerpb.ClusterManagerServer = (*Service)(nil)
