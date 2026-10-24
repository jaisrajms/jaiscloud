package container

import (
	"context"

	"jaiscloud/internal/clock"
	containerstore "jaiscloud/internal/gcp/store/container"
)

// ClusterUpdate carries the writable subset of google.container.v1.ClusterUpdate
// applied by UpdateCluster. Nil/empty fields are left unchanged.
type ClusterUpdate struct {
	DesiredAddonsConfig        *containerstore.AddonsConfig
	DesiredMasterVersion       string
	DesiredNodeVersion         string
	DesiredImageType           string
	DesiredLocations           []string
	DesiredLoggingService      string
	DesiredMonitoringService   string
	DesiredNodePoolID          string
	DesiredNodePoolAutoscaling *containerstore.NodePoolAutoscaling
}

// SetAddonsConfig records the cluster's addons config.
func (s *Service) SetAddonsConfig(ctx context.Context, project, location, cluster string, addons containerstore.AddonsConfig) (containerstore.Operation, error) {
	return s.mutateClusterOp(ctx, project, location, cluster, containerstore.OperationUpdateCluster, func(c *containerstore.Cluster) {
		a := addons
		c.AddonsConfig = &a
	})
}

// SetLegacyAbac records whether legacy ABAC is enabled.
func (s *Service) SetLegacyAbac(ctx context.Context, project, location, cluster string, enabled bool) (containerstore.Operation, error) {
	return s.mutateClusterOp(ctx, project, location, cluster, containerstore.OperationUpdateCluster, func(c *containerstore.Cluster) {
		c.LegacyAbac = &containerstore.LegacyAbac{Enabled: enabled}
	})
}

// SetLocations records the cluster's node locations.
func (s *Service) SetLocations(ctx context.Context, project, location, cluster string, locations []string) (containerstore.Operation, error) {
	return s.mutateClusterOp(ctx, project, location, cluster, containerstore.OperationUpdateCluster, func(c *containerstore.Cluster) {
		c.Locations = locations
	})
}

// SetLoggingService records the cluster's logging service.
func (s *Service) SetLoggingService(ctx context.Context, project, location, cluster, service string) (containerstore.Operation, error) {
	return s.mutateClusterOp(ctx, project, location, cluster, containerstore.OperationUpdateCluster, func(c *containerstore.Cluster) {
		c.LoggingService = service
	})
}

// SetMonitoringService records the cluster's monitoring service.
func (s *Service) SetMonitoringService(ctx context.Context, project, location, cluster, service string) (containerstore.Operation, error) {
	return s.mutateClusterOp(ctx, project, location, cluster, containerstore.OperationUpdateCluster, func(c *containerstore.Cluster) {
		c.MonitoringService = service
	})
}

// SetNetworkPolicy records the cluster's network policy.
func (s *Service) SetNetworkPolicy(ctx context.Context, project, location, cluster string, np containerstore.NetworkPolicy) (containerstore.Operation, error) {
	return s.mutateClusterOp(ctx, project, location, cluster, containerstore.OperationUpdateCluster, func(c *containerstore.Cluster) {
		n := np
		c.NetworkPolicy = &n
	})
}

// SetMaintenancePolicy records the cluster's maintenance policy.
func (s *Service) SetMaintenancePolicy(ctx context.Context, project, location, cluster string, mp containerstore.MaintenancePolicy) (containerstore.Operation, error) {
	return s.mutateClusterOp(ctx, project, location, cluster, containerstore.OperationUpdateCluster, func(c *containerstore.Cluster) {
		m := mp
		c.MaintenancePolicy = &m
	})
}

// SetMasterAuth records the admin username (the admin password is never
// echoed, matching real GKE). The password-changing actions are no-ops.
func (s *Service) SetMasterAuth(ctx context.Context, project, location, cluster, username string) (containerstore.Operation, error) {
	return s.mutateClusterOp(ctx, project, location, cluster, containerstore.OperationUpdateCluster, func(c *containerstore.Cluster) {
		if username != "" {
			c.AdminUsername = username
		}
	})
}

// SetLabels replaces the cluster's resource labels.
func (s *Service) SetLabels(ctx context.Context, project, location, cluster string, labels map[string]string) (containerstore.Operation, error) {
	return s.mutateClusterOp(ctx, project, location, cluster, containerstore.OperationUpdateCluster, func(c *containerstore.Cluster) {
		c.ResourceLabels = labels
	})
}

// UpdateCluster applies the desired fields of a ClusterUpdate.
func (s *Service) UpdateCluster(ctx context.Context, project, location, cluster string, u ClusterUpdate) (containerstore.Operation, error) {
	return s.mutateClusterOp(ctx, project, location, cluster, containerstore.OperationUpdateCluster, func(c *containerstore.Cluster) {
		if u.DesiredAddonsConfig != nil {
			a := *u.DesiredAddonsConfig
			c.AddonsConfig = &a
		}
		if u.DesiredMasterVersion != "" {
			c.CurrentMasterVersion = u.DesiredMasterVersion
		}
		if u.DesiredNodeVersion != "" {
			c.CurrentNodeVersion = u.DesiredNodeVersion
		}
		if u.DesiredImageType != "" {
			for i := range c.NodePools {
				if c.NodePools[i].Config == nil {
					c.NodePools[i].Config = &containerstore.NodeConfig{}
				}
				c.NodePools[i].Config.ImageType = u.DesiredImageType
			}
		}
		if len(u.DesiredLocations) > 0 {
			c.Locations = u.DesiredLocations
		}
		if u.DesiredLoggingService != "" {
			c.LoggingService = u.DesiredLoggingService
		}
		if u.DesiredMonitoringService != "" {
			c.MonitoringService = u.DesiredMonitoringService
		}
		// Real GKE targets the node pool named by desiredNodePoolId (there is no
		// "all pools" form), so an empty ID applies nothing.
		if u.DesiredNodePoolAutoscaling != nil && u.DesiredNodePoolID != "" {
			for i := range c.NodePools {
				if c.NodePools[i].Name == u.DesiredNodePoolID {
					a := *u.DesiredNodePoolAutoscaling
					c.NodePools[i].Autoscaling = &a
				}
			}
		}
	})
}

// UpdateMaster records the cluster's master version.
func (s *Service) UpdateMaster(ctx context.Context, project, location, cluster, version string) (containerstore.Operation, error) {
	if version == "" {
		return containerstore.Operation{}, invalidArgument("masterVersion is required")
	}
	return s.mutateClusterOp(ctx, project, location, cluster, containerstore.OperationUpgradeMaster, func(c *containerstore.Cluster) {
		c.CurrentMasterVersion = version
	})
}

// StartIPRotation records that IP rotation is in progress.
func (s *Service) StartIPRotation(ctx context.Context, project, location, cluster string) (containerstore.Operation, error) {
	return s.mutateClusterOp(ctx, project, location, cluster, containerstore.OperationUpdateCluster, func(c *containerstore.Cluster) {
		c.IPRotationEnabled = true
	})
}

// CompleteIPRotation records that IP rotation finished.
func (s *Service) CompleteIPRotation(ctx context.Context, project, location, cluster string) (containerstore.Operation, error) {
	return s.mutateClusterOp(ctx, project, location, cluster, containerstore.OperationUpdateCluster, func(c *containerstore.Cluster) {
		c.IPRotationEnabled = false
	})
}

// CancelOperation is a no-op success for an existing operation (the emulator's
// operations are created already DONE, so there is nothing to cancel).
func (s *Service) CancelOperation(ctx context.Context, project, location, name string) error {
	if _, err := s.store.GetOperation(ctx, project, location, name); err != nil {
		return mapStoreErr(err)
	}
	return nil
}

// mutateClusterOp applies fn to a cluster that must exist and records an
// operation of the given type targeting it.
func (s *Service) mutateClusterOp(ctx context.Context, project, location, cluster, opType string, fn func(*containerstore.Cluster)) (containerstore.Operation, error) {
	now := clock.Now()
	var selfLink string
	err := s.store.MutateCluster(ctx, project, location, cluster, func(c *containerstore.Cluster) error {
		selfLink = c.SelfLink
		fn(c)
		return nil
	})
	if err != nil {
		return containerstore.Operation{}, mapStoreErr(err)
	}
	op := s.newOperation(project, location, opType, selfLink, now)
	if err := s.store.CreateOperation(ctx, project, location, op); err != nil {
		return containerstore.Operation{}, mapStoreErr(err)
	}
	return op, nil
}
