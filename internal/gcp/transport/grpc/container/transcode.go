package container

import (
	"strings"
	"time"

	containerpb "cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	core "jaiscloud/internal/gcp/service/container"
	containerstore "jaiscloud/internal/gcp/store/container"
)

// timeFormat is the RFC3339 timestamp encoding GKE v1 uses for its string
// timestamps (Cluster.createTime, Operation.startTime/endTime).
const timeFormat = time.RFC3339Nano

// stringToTimestamp parses an RFC3339 string into a protobuf Timestamp (nil
// when empty/unparseable).
func stringToTimestamp(s string) *timestamppb.Timestamp {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}
	return timestamppb.New(t)
}

// timestampToString renders a protobuf Timestamp as RFC3339 ("" when nil).
func timestampToString(ts *timestamppb.Timestamp) string {
	if ts == nil || !ts.IsValid() {
		return ""
	}
	return ts.AsTime().UTC().Format(time.RFC3339)
}

// clusterToProto renders a stored cluster as the proto Cluster. Name is the
// short cluster id, matching the REST wire shape (the core stores the short
// name; the canonical path is derived by the caller).
func clusterToProto(c containerstore.Cluster) *containerpb.Cluster {
	out := &containerpb.Cluster{
		Name:                  c.Name,
		Location:              c.Location,
		Status:                clusterStatusToProto(c.Status),
		Endpoint:              c.Endpoint,
		SelfLink:              c.SelfLink,
		InitialClusterVersion: c.InitialClusterVersion,
		CurrentMasterVersion:  c.CurrentMasterVersion,
		CurrentNodeVersion:    c.CurrentNodeVersion,
		Network:               c.Network,
		Subnetwork:            c.Subnetwork,
		InitialNodeCount:      c.InitialNodeCount,
		ResourceLabels:        c.ResourceLabels,
		Locations:             c.Locations,
		LoggingService:        c.LoggingService,
		MonitoringService:     c.MonitoringService,
		MasterAuth:            &containerpb.MasterAuth{Username: c.AdminUsername, ClusterCaCertificate: c.CaCertificate},
	}
	if c.AddonsConfig != nil {
		out.AddonsConfig = addonsConfigToProto(c.AddonsConfig)
	}
	if c.LegacyAbac != nil {
		out.LegacyAbac = &containerpb.LegacyAbac{Enabled: c.LegacyAbac.Enabled}
	}
	if c.NetworkPolicy != nil {
		out.NetworkPolicy = &containerpb.NetworkPolicy{
			Provider: networkPolicyProviderToProto(c.NetworkPolicy.Provider),
			Enabled:  c.NetworkPolicy.Enabled,
		}
	}
	if c.MaintenancePolicy != nil {
		out.MaintenancePolicy = maintenancePolicyToProto(c.MaintenancePolicy)
	}
	if !c.CreateTime.IsZero() {
		out.CreateTime = c.CreateTime.UTC().Format(timeFormat)
	}
	for _, p := range c.NodePools {
		out.NodePools = append(out.NodePools, nodePoolToProto(p))
	}
	return out
}

// addonsConfigToProto renders a stored addons config as the proto AddonsConfig
// (the subset the emulator records).
func addonsConfigToProto(a *containerstore.AddonsConfig) *containerpb.AddonsConfig {
	out := &containerpb.AddonsConfig{}
	if a.HttpLoadBalancing != nil {
		out.HttpLoadBalancing = &containerpb.HttpLoadBalancing{Disabled: a.HttpLoadBalancing.Disabled}
	}
	if a.HorizontalPodAutoscaling != nil {
		out.HorizontalPodAutoscaling = &containerpb.HorizontalPodAutoscaling{Disabled: a.HorizontalPodAutoscaling.Disabled}
	}
	if a.NetworkPolicyConfig != nil {
		out.NetworkPolicyConfig = &containerpb.NetworkPolicyConfig{Disabled: a.NetworkPolicyConfig.Disabled}
	}
	return out
}

// addonsConfigFromProto builds the core's addons-config subset.
func addonsConfigFromProto(pb *containerpb.AddonsConfig) containerstore.AddonsConfig {
	var a containerstore.AddonsConfig
	if pb == nil {
		return a
	}
	if h := pb.GetHttpLoadBalancing(); h != nil {
		a.HttpLoadBalancing = &containerstore.AddonConfig{Disabled: h.GetDisabled()}
	}
	if h := pb.GetHorizontalPodAutoscaling(); h != nil {
		a.HorizontalPodAutoscaling = &containerstore.AddonConfig{Disabled: h.GetDisabled()}
	}
	if n := pb.GetNetworkPolicyConfig(); n != nil {
		a.NetworkPolicyConfig = &containerstore.AddonConfig{Disabled: n.GetDisabled()}
	}
	return a
}

// maintenancePolicyToProto renders a stored maintenance policy as the proto.
func maintenancePolicyToProto(mp *containerstore.MaintenancePolicy) *containerpb.MaintenancePolicy {
	out := &containerpb.MaintenancePolicy{ResourceVersion: mp.ResourceVersion}
	if mp.Window == nil {
		return out
	}
	w := &containerpb.MaintenanceWindow{}
	if mp.Window.DailyMaintenanceWindow != nil {
		w.Policy = &containerpb.MaintenanceWindow_DailyMaintenanceWindow{
			DailyMaintenanceWindow: &containerpb.DailyMaintenanceWindow{
				StartTime: mp.Window.DailyMaintenanceWindow.StartTime,
				Duration:  mp.Window.DailyMaintenanceWindow.Duration,
			},
		}
	} else if mp.Window.RecurringWindow != nil {
		rw := &containerpb.RecurringTimeWindow{Recurrence: mp.Window.RecurringWindow.Recurrence}
		if mp.Window.RecurringWindow.Window != nil {
			rw.Window = &containerpb.TimeWindow{
				StartTime: stringToTimestamp(mp.Window.RecurringWindow.Window.StartTime),
				EndTime:   stringToTimestamp(mp.Window.RecurringWindow.Window.EndTime),
			}
		}
		w.Policy = &containerpb.MaintenanceWindow_RecurringWindow{RecurringWindow: rw}
	}
	if len(mp.Window.MaintenanceExclusions) > 0 {
		w.MaintenanceExclusions = make(map[string]*containerpb.TimeWindow, len(mp.Window.MaintenanceExclusions))
		for k, v := range mp.Window.MaintenanceExclusions {
			if v == nil {
				continue
			}
			w.MaintenanceExclusions[k] = &containerpb.TimeWindow{
				StartTime: stringToTimestamp(v.StartTime),
				EndTime:   stringToTimestamp(v.EndTime),
			}
		}
	}
	out.Window = w
	return out
}

// maintenancePolicyFromProto builds the core's maintenance-policy subset.
func maintenancePolicyFromProto(pb *containerpb.MaintenancePolicy) containerstore.MaintenancePolicy {
	mp := containerstore.MaintenancePolicy{}
	if pb == nil {
		return mp
	}
	mp.ResourceVersion = pb.GetResourceVersion()
	if w := pb.GetWindow(); w != nil {
		switch pol := w.GetPolicy().(type) {
		case *containerpb.MaintenanceWindow_DailyMaintenanceWindow:
			mp.Window = &containerstore.MaintenanceWindow{
				DailyMaintenanceWindow: &containerstore.DailyMaintenanceWindow{
					StartTime: pol.DailyMaintenanceWindow.GetStartTime(),
					Duration:  pol.DailyMaintenanceWindow.GetDuration(),
				},
			}
		case *containerpb.MaintenanceWindow_RecurringWindow:
			rw := &containerstore.RecurringWindow{Recurrence: pol.RecurringWindow.GetRecurrence()}
			if tw := pol.RecurringWindow.GetWindow(); tw != nil {
				rw.Window = &containerstore.TimeWindow{
					StartTime: timestampToString(tw.GetStartTime()),
					EndTime:   timestampToString(tw.GetEndTime()),
				}
			}
			mp.Window = &containerstore.MaintenanceWindow{RecurringWindow: rw}
		}
		if ex := w.GetMaintenanceExclusions(); len(ex) > 0 {
			if mp.Window == nil {
				mp.Window = &containerstore.MaintenanceWindow{}
			}
			mp.Window.MaintenanceExclusions = make(map[string]*containerstore.TimeWindow, len(ex))
			for k, v := range ex {
				mp.Window.MaintenanceExclusions[k] = &containerstore.TimeWindow{
					StartTime: timestampToString(v.GetStartTime()),
					EndTime:   timestampToString(v.GetEndTime()),
				}
			}
		}
	}
	return mp
}

// nodePoolToProto renders a stored node pool as the proto NodePool.
func nodePoolToProto(p containerstore.NodePool) *containerpb.NodePool {
	out := &containerpb.NodePool{
		Name:             p.Name,
		Status:           nodePoolStatusToProto(p.Status),
		InitialNodeCount: p.InitialNodeCount,
		Version:          p.Version,
		Locations:        p.Locations,
		SelfLink:         p.SelfLink,
	}
	if p.Config != nil {
		out.Config = nodeConfigToProto(p.Config)
	}
	if p.Autoscaling != nil {
		out.Autoscaling = &containerpb.NodePoolAutoscaling{
			Enabled:           p.Autoscaling.Enabled,
			MinNodeCount:      p.Autoscaling.MinNodeCount,
			MaxNodeCount:      p.Autoscaling.MaxNodeCount,
			Autoprovisioned:   p.Autoscaling.Autoprovisioned,
			LocationPolicy:    nodePoolLocationPolicyToProto(p.Autoscaling.LocationPolicy),
			TotalMinNodeCount: p.Autoscaling.TotalMinNodeCount,
			TotalMaxNodeCount: p.Autoscaling.TotalMaxNodeCount,
		}
	}
	if p.Management != nil {
		out.Management = &containerpb.NodeManagement{
			AutoUpgrade: p.Management.AutoUpgrade,
			AutoRepair:  p.Management.AutoRepair,
		}
	}
	if p.UpgradeSettings != nil {
		strategy := nodePoolUpdateStrategyToProto(p.UpgradeSettings.Strategy)
		out.UpgradeSettings = &containerpb.NodePool_UpgradeSettings{
			MaxSurge:       p.UpgradeSettings.MaxSurge,
			MaxUnavailable: p.UpgradeSettings.MaxUnavailable,
			Strategy:       &strategy,
		}
	}
	return out
}

// nodePoolAutoscalingFromProto builds the core's autoscaling subset.
func nodePoolAutoscalingFromProto(a *containerpb.NodePoolAutoscaling) *containerstore.NodePoolAutoscaling {
	return &containerstore.NodePoolAutoscaling{
		Enabled:           a.GetEnabled(),
		MinNodeCount:      a.GetMinNodeCount(),
		MaxNodeCount:      a.GetMaxNodeCount(),
		Autoprovisioned:   a.GetAutoprovisioned(),
		LocationPolicy:    a.GetLocationPolicy().String(),
		TotalMinNodeCount: a.GetTotalMinNodeCount(),
		TotalMaxNodeCount: a.GetTotalMaxNodeCount(),
	}
}

// nodeConfigToProto renders a stored node config as the proto NodeConfig.
func nodeConfigToProto(c *containerstore.NodeConfig) *containerpb.NodeConfig {
	return &containerpb.NodeConfig{
		MachineType:    c.MachineType,
		DiskSizeGb:     c.DiskSizeGb,
		DiskType:       c.DiskType,
		ImageType:      c.ImageType,
		OauthScopes:    c.OauthScopes,
		ServiceAccount: c.ServiceAccount,
		Metadata:       c.Metadata,
		Labels:         c.Labels,
		ResourceLabels: c.ResourceLabels,
		Tags:           c.Tags,
		MinCpuPlatform: c.MinCpuPlatform,
		LocalSsdCount:  c.LocalSsdCount,
		Preemptible:    c.Preemptible,
		Spot:           c.Spot,
	}
}

// nodePoolFromProto builds the core's writable NodePool subset from a request
// NodePool. A canonical node-pool path in name is reduced to its short id.
func nodePoolFromProto(pb *containerpb.NodePool) containerstore.NodePool {
	if pb == nil {
		return containerstore.NodePool{}
	}
	p := containerstore.NodePool{
		Name:             pb.GetName(),
		InitialNodeCount: pb.GetInitialNodeCount(),
		Version:          pb.GetVersion(),
		Locations:        pb.GetLocations(),
	}
	if strings.Contains(p.Name, "/") {
		if _, _, _, id, ok := core.ParseNodePoolName(p.Name); ok {
			p.Name = id
		}
	}
	if c := pb.GetConfig(); c != nil {
		p.Config = &containerstore.NodeConfig{
			MachineType:    c.GetMachineType(),
			DiskSizeGb:     c.GetDiskSizeGb(),
			DiskType:       c.GetDiskType(),
			ImageType:      c.GetImageType(),
			OauthScopes:    c.GetOauthScopes(),
			ServiceAccount: c.GetServiceAccount(),
			Metadata:       c.GetMetadata(),
			Labels:         c.GetLabels(),
			ResourceLabels: c.GetResourceLabels(),
			Tags:           c.GetTags(),
			MinCpuPlatform: c.GetMinCpuPlatform(),
			LocalSsdCount:  c.GetLocalSsdCount(),
			Preemptible:    c.GetPreemptible(),
			Spot:           c.GetSpot(),
		}
	}
	if a := pb.GetAutoscaling(); a != nil {
		p.Autoscaling = nodePoolAutoscalingFromProto(a)
	}
	if m := pb.GetManagement(); m != nil {
		p.Management = &containerstore.NodeManagement{
			AutoUpgrade: m.GetAutoUpgrade(),
			AutoRepair:  m.GetAutoRepair(),
		}
	}
	if u := pb.GetUpgradeSettings(); u != nil {
		p.UpgradeSettings = &containerstore.NodePoolUpgradeSettings{
			MaxSurge:       u.GetMaxSurge(),
			MaxUnavailable: u.GetMaxUnavailable(),
			Strategy:       u.GetStrategy().String(),
		}
	}
	return p
}

// operationToProto renders a stored operation as the GKE proto Operation (the
// emulator's own google.container.v1.Operation shape, not
// google.longrunning.Operation).
func operationToProto(op containerstore.Operation) *containerpb.Operation {
	out := &containerpb.Operation{
		Name:          op.Name,
		Location:      op.Location,
		OperationType: operationTypeToProto(op.OperationType),
		Status:        operationStatusToProto(op.Status),
		SelfLink:      op.SelfLink,
		TargetLink:    op.TargetLink,
	}
	if !op.StartTime.IsZero() {
		out.StartTime = op.StartTime.UTC().Format(timeFormat)
	}
	if !op.EndTime.IsZero() {
		out.EndTime = op.EndTime.UTC().Format(timeFormat)
	}
	return out
}

// clusterFromProto builds the core's writable Cluster subset from a request
// Cluster. Input-only/derived fields are ignored; the core fills defaults. A
// canonical cluster path in name is reduced to its short id, mirroring the REST
// codec.
func clusterFromProto(pb *containerpb.Cluster) containerstore.Cluster {
	if pb == nil {
		return containerstore.Cluster{}
	}
	c := containerstore.Cluster{
		Name:                  pb.GetName(),
		InitialClusterVersion: pb.GetInitialClusterVersion(),
		Network:               pb.GetNetwork(),
		Subnetwork:            pb.GetSubnetwork(),
		InitialNodeCount:      pb.GetInitialNodeCount(),
		ResourceLabels:        pb.GetResourceLabels(),
	}
	if strings.Contains(c.Name, "/") {
		if _, _, id, ok := core.ParseClusterName(c.Name); ok {
			c.Name = id
		}
	}
	return c
}

func clusterStatusToProto(status string) containerpb.Cluster_Status {
	switch status {
	case containerstore.StatusProvisioning:
		return containerpb.Cluster_PROVISIONING
	case containerstore.StatusRunning:
		return containerpb.Cluster_RUNNING
	case containerstore.StatusError:
		return containerpb.Cluster_ERROR
	default:
		return containerpb.Cluster_STATUS_UNSPECIFIED
	}
}

func nodePoolStatusToProto(status string) containerpb.NodePool_Status {
	switch status {
	case containerstore.StatusProvisioning:
		return containerpb.NodePool_PROVISIONING
	case containerstore.StatusRunning:
		return containerpb.NodePool_RUNNING
	case containerstore.StatusError:
		return containerpb.NodePool_ERROR
	default:
		return containerpb.NodePool_STATUS_UNSPECIFIED
	}
}

func operationTypeToProto(opType string) containerpb.Operation_Type {
	switch opType {
	case containerstore.OperationCreateCluster:
		return containerpb.Operation_CREATE_CLUSTER
	case containerstore.OperationDeleteCluster:
		return containerpb.Operation_DELETE_CLUSTER
	case containerstore.OperationCreateNodePool:
		return containerpb.Operation_CREATE_NODE_POOL
	case containerstore.OperationDeleteNodePool:
		return containerpb.Operation_DELETE_NODE_POOL
	case containerstore.OperationUpdateNodePool:
		return containerpb.Operation_UPGRADE_NODES
	case containerstore.OperationSetNodePoolSize:
		return containerpb.Operation_SET_NODE_POOL_SIZE
	case containerstore.OperationSetNodePoolManagement:
		return containerpb.Operation_SET_NODE_POOL_MANAGEMENT
	case containerstore.OperationUpdateCluster:
		return containerpb.Operation_UPDATE_CLUSTER
	case containerstore.OperationUpgradeMaster:
		return containerpb.Operation_UPGRADE_MASTER
	default:
		return containerpb.Operation_TYPE_UNSPECIFIED
	}
}

func nodePoolUpdateStrategyToProto(s string) containerpb.NodePoolUpdateStrategy {
	switch s {
	case "SURGE":
		return containerpb.NodePoolUpdateStrategy_SURGE
	case "BLUE_GREEN":
		return containerpb.NodePoolUpdateStrategy_BLUE_GREEN
	case "SHORT_LIVED":
		return containerpb.NodePoolUpdateStrategy_SHORT_LIVED
	default:
		return containerpb.NodePoolUpdateStrategy_NODE_POOL_UPDATE_STRATEGY_UNSPECIFIED
	}
}

func nodePoolLocationPolicyToProto(s string) containerpb.NodePoolAutoscaling_LocationPolicy {
	switch s {
	case "BALANCED":
		return containerpb.NodePoolAutoscaling_BALANCED
	case "ANY":
		return containerpb.NodePoolAutoscaling_ANY
	default:
		return containerpb.NodePoolAutoscaling_LOCATION_POLICY_UNSPECIFIED
	}
}

func networkPolicyProviderToProto(s string) containerpb.NetworkPolicy_Provider {
	switch s {
	case "CALICO":
		return containerpb.NetworkPolicy_CALICO
	default:
		return containerpb.NetworkPolicy_PROVIDER_UNSPECIFIED
	}
}

func operationStatusToProto(status string) containerpb.Operation_Status {
	switch status {
	case containerstore.OperationStatusDone:
		return containerpb.Operation_DONE
	default:
		return containerpb.Operation_STATUS_UNSPECIFIED
	}
}
