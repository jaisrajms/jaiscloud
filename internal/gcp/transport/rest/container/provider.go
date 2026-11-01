package container

import (
	"context"
	"strconv"

	core "jaiscloud/internal/gcp/service/container"
	containerstore "jaiscloud/internal/gcp/store/container"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider handles the GKE v1 REST data plane. It is a thin adapter: every
// handler resolves the NormalizedRequest params into the core's typed API,
// calls the shared core Service, and encodes the result as GKE JSON.
type Provider struct {
	core *core.Service
}

// NewProvider returns a GKE REST provider over the shared core.
func NewProvider(c *core.Service) *Provider { return &Provider{core: c} }

// Routes maps "Container.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Container.CreateCluster":           p.CreateCluster,
		"Container.GetCluster":              p.GetCluster,
		"Container.ListClusters":            p.ListClusters,
		"Container.DeleteCluster":           p.DeleteCluster,
		"Container.GetOperation":            p.GetOperation,
		"Container.ListOperations":          p.ListOperations,
		"Container.CancelOperation":         p.CancelOperation,
		"Container.CreateNodePool":          p.CreateNodePool,
		"Container.GetNodePool":             p.GetNodePool,
		"Container.ListNodePools":           p.ListNodePools,
		"Container.DeleteNodePool":          p.DeleteNodePool,
		"Container.UpdateNodePool":          p.UpdateNodePool,
		"Container.SetNodePoolAutoscaling":  p.SetNodePoolAutoscaling,
		"Container.SetNodePoolManagement":   p.SetNodePoolManagement,
		"Container.SetNodePoolSize":         p.SetNodePoolSize,
		"Container.RollbackNodePoolUpgrade": p.RollbackNodePoolUpgrade,
		"Container.SetAddonsConfig":         p.SetAddonsConfig,
		"Container.SetLabels":               p.SetLabels,
		"Container.SetLegacyAbac":           p.SetLegacyAbac,
		"Container.SetLocations":            p.SetLocations,
		"Container.SetLoggingService":       p.SetLoggingService,
		"Container.SetMonitoringService":    p.SetMonitoringService,
		"Container.SetNetworkPolicy":        p.SetNetworkPolicy,
		"Container.SetMaintenancePolicy":    p.SetMaintenancePolicy,
		"Container.SetMasterAuth":           p.SetMasterAuth,
		"Container.UpdateCluster":           p.UpdateCluster,
		"Container.UpdateMaster":            p.UpdateMaster,
		"Container.StartIPRotation":         p.StartIPRotation,
		"Container.CompleteIPRotation":      p.CompleteIPRotation,
	}
}

// Reset delegates to the core so /_jaiscloud/reset clears GKE state.
func (p *Provider) Reset(ctx context.Context) { p.core.Reset(ctx) }

func (p *Provider) CreateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusterMap, _ := bodyOf(nr)["cluster"].(map[string]any)
	if clusterMap == nil {
		return nil, model.NewProviderError("InvalidArgument", "missing root 'cluster' object", 400)
	}
	cluster, err := clusterFromBody(clusterMap)
	if err != nil {
		return nil, err
	}
	op, err := p.core.CreateCluster(ctx, strParam(nr, "project"), strParam(nr, "location"), cluster)
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) GetCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	c, err := p.core.GetCluster(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"))
	if err != nil {
		return nil, err
	}
	return provider.OK(clusterToJSON(c)), nil
}

func (p *Provider) ListClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	clusters, err := p.core.ListClusters(ctx, strParam(nr, "project"), strParam(nr, "location"))
	if err != nil {
		return nil, err
	}
	page, next := paginate(len(clusters), nr)
	items := make([]any, 0, len(page))
	for _, i := range page {
		items = append(items, clusterToJSON(clusters[i]))
	}
	resp := map[string]any{"clusters": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) DeleteCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.DeleteCluster(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) GetOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.GetOperation(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "operation"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) ListOperations(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	ops, err := p.core.ListOperations(ctx, strParam(nr, "project"), strParam(nr, "location"))
	if err != nil {
		return nil, err
	}
	page, next := paginate(len(ops), nr)
	items := make([]any, 0, len(page))
	for _, i := range page {
		items = append(items, operationToJSON(ops[i]))
	}
	resp := map[string]any{"operations": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

// ─── Node pools ───────────────────────────────────────────────────────────────

func (p *Provider) CreateNodePool(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	poolMap, _ := bodyOf(nr)["nodePool"].(map[string]any)
	if poolMap == nil {
		return nil, model.NewProviderError("InvalidArgument", "missing root 'nodePool' object", 400)
	}
	op, err := p.core.CreateNodePool(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), nodePoolFromBody(poolMap))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) GetNodePool(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	pool, err := p.core.GetNodePool(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), strParam(nr, "nodepool"))
	if err != nil {
		return nil, err
	}
	return provider.OK(nodePoolToJSON(pool)), nil
}

func (p *Provider) ListNodePools(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	pools, err := p.core.ListNodePools(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"))
	if err != nil {
		return nil, err
	}
	page, next := paginate(len(pools), nr)
	items := make([]any, 0, len(page))
	for _, i := range page {
		items = append(items, nodePoolToJSON(pools[i]))
	}
	resp := map[string]any{"nodePools": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) DeleteNodePool(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.DeleteNodePool(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), strParam(nr, "nodepool"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) UpdateNodePool(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	update := containerstore.NodePool{
		Name:      strParam(nr, "nodepool"),
		Version:   str(body, "nodeVersion"),
		Locations: strSlice(body["locations"]),
	}
	if cfg := nodeConfigFromMap(body); cfg != nil && (cfg.ImageType != "" || cfg.MachineType != "" || cfg.DiskType != "" || cfg.DiskSizeGb != 0) {
		update.Config = cfg
	}
	if us, ok := body["upgradeSettings"].(map[string]any); ok {
		update.UpgradeSettings = &containerstore.NodePoolUpgradeSettings{
			MaxSurge:       int32From(us, "maxSurge"),
			MaxUnavailable: int32From(us, "maxUnavailable"),
			Strategy:       str(us, "strategy"),
		}
	}
	op, err := p.core.UpdateNodePool(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), update)
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) SetNodePoolAutoscaling(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	a, _ := bodyOf(nr)["autoscaling"].(map[string]any)
	if a == nil {
		return nil, model.NewProviderError("InvalidArgument", "missing 'autoscaling' object", 400)
	}
	op, err := p.core.SetNodePoolAutoscaling(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), strParam(nr, "nodepool"), *nodePoolAutoscalingFromMap(a))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) SetNodePoolManagement(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	m, _ := bodyOf(nr)["management"].(map[string]any)
	if m == nil {
		return nil, model.NewProviderError("InvalidArgument", "missing 'management' object", 400)
	}
	op, err := p.core.SetNodePoolManagement(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), strParam(nr, "nodepool"),
		containerstore.NodeManagement{AutoUpgrade: boolVal(m, "autoUpgrade"), AutoRepair: boolVal(m, "autoRepair")})
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) SetNodePoolSize(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.SetNodePoolSize(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), strParam(nr, "nodepool"), int32From(bodyOf(nr), "nodeCount"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) RollbackNodePoolUpgrade(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.RollbackNodePoolUpgrade(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), strParam(nr, "nodepool"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

// ─── Cluster setters ──────────────────────────────────────────────────────────

func (p *Provider) CancelOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.CancelOperation(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "operation")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) SetAddonsConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	a, _ := bodyOf(nr)["addonsConfig"].(map[string]any)
	if a == nil {
		return nil, model.NewProviderError("InvalidArgument", "missing 'addonsConfig' object", 400)
	}
	op, err := p.core.SetAddonsConfig(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), addonsConfigFromMap(a))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) SetLabels(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.SetLabels(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), strMap(bodyOf(nr)["resourceLabels"]))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) SetLegacyAbac(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.SetLegacyAbac(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), boolVal(bodyOf(nr), "enabled"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) SetLocations(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.SetLocations(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), strSlice(bodyOf(nr)["locations"]))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) SetLoggingService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.SetLoggingService(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), str(bodyOf(nr), "loggingService"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) SetMonitoringService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.SetMonitoringService(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), str(bodyOf(nr), "monitoringService"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) SetNetworkPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	np, _ := bodyOf(nr)["networkPolicy"].(map[string]any)
	if np == nil {
		return nil, model.NewProviderError("InvalidArgument", "missing 'networkPolicy' object", 400)
	}
	op, err := p.core.SetNetworkPolicy(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), networkPolicyFromMap(np))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) SetMaintenancePolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	mp, _ := bodyOf(nr)["maintenancePolicy"].(map[string]any)
	if mp == nil {
		return nil, model.NewProviderError("InvalidArgument", "missing 'maintenancePolicy' object", 400)
	}
	op, err := p.core.SetMaintenancePolicy(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), maintenancePolicyFromMap(mp))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) SetMasterAuth(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	update, _ := bodyOf(nr)["update"].(map[string]any)
	op, err := p.core.SetMasterAuth(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), str(update, "username"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) UpdateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	u, _ := bodyOf(nr)["update"].(map[string]any)
	op, err := p.core.UpdateCluster(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), clusterUpdateFromMap(u))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) UpdateMaster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.UpdateMaster(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"), str(bodyOf(nr), "masterVersion"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) StartIPRotation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.StartIPRotation(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func (p *Provider) CompleteIPRotation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.CompleteIPRotation(ctx, strParam(nr, "project"), strParam(nr, "location"), strParam(nr, "cluster"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op)), nil
}

func clusterUpdateFromMap(m map[string]any) core.ClusterUpdate {
	if m == nil {
		return core.ClusterUpdate{}
	}
	out := core.ClusterUpdate{
		DesiredMasterVersion:     str(m, "desiredMasterVersion"),
		DesiredNodeVersion:       str(m, "desiredNodeVersion"),
		DesiredImageType:         str(m, "desiredImageType"),
		DesiredLocations:         strSlice(m["desiredLocations"]),
		DesiredLoggingService:    str(m, "desiredLoggingService"),
		DesiredMonitoringService: str(m, "desiredMonitoringService"),
		DesiredNodePoolID:        str(m, "desiredNodePoolId"),
	}
	if a, ok := m["desiredAddonsConfig"].(map[string]any); ok {
		cfg := addonsConfigFromMap(a)
		out.DesiredAddonsConfig = &cfg
	}
	if a, ok := m["desiredNodePoolAutoscaling"].(map[string]any); ok {
		out.DesiredNodePoolAutoscaling = nodePoolAutoscalingFromMap(a)
	}
	if sn, ok := m["desiredShieldedNodes"].(map[string]any); ok {
		out.DesiredShieldedNodes = &containerstore.ShieldedNodes{Enabled: boolFrom(sn["enabled"])}
	}
	return out
}

// paginate applies GKE's pageSize/pageToken (a numeric offset, as the emulator
// has no opaque cursor store) and returns the selected indices plus the next
// token ("" when the page is the last).
//
// The pageSize parse here is deliberately lenient (unparseable → the 500
// default) rather than the int32 binding the other bespoke pagers enforce:
// GKE's Discovery document declares no pageSize/pageToken on clusters.list,
// nodePools.list or operations.list, so real GKE ignores the parameter
// entirely. The emulator's pagination on those methods is a deliberate
// convenience (DC40), and rejecting a stray value would diverge further from
// the real API than ignoring it.
func paginate(total int, nr *model.NormalizedRequest) ([]int, string) {
	pageSize := intFrom(nr.Params["pageSize"])
	if pageSize <= 0 || pageSize > 500 {
		pageSize = 500
	}
	offset := 0
	if t := strParam(nr, "pageToken"); t != "" {
		offset, _ = strconv.Atoi(t)
	}
	if offset < 0 {
		offset = 0
	}
	if offset > total {
		offset = total
	}
	end := offset + pageSize
	if end > total {
		end = total
	}
	idxs := make([]int, 0, end-offset)
	for i := offset; i < end; i++ {
		idxs = append(idxs, i)
	}
	if end < total {
		return idxs, strconv.Itoa(end)
	}
	return idxs, ""
}
