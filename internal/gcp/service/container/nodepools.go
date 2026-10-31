package container

import (
	"context"
	"sort"
	"time"

	"jaiscloud/internal/clock"
	containerstore "jaiscloud/internal/gcp/store/container"
)

// validNodePoolName is GKE's node-pool-id grammar (an RFC 1035 label).
var validNodePoolName = validClusterName

// CreateNodePool stores a new node pool on an existing cluster and returns the
// CREATE_NODE_POOL operation. The mock materializes the pool already RUNNING
// (there is no real node lifecycle).
func (s *Service) CreateNodePool(ctx context.Context, project, location, cluster string, p containerstore.NodePool) (containerstore.Operation, error) {
	now := clock.Now()
	p, err := materializeNodePool(project, location, cluster, p, "")
	if err != nil {
		return containerstore.Operation{}, err
	}
	var clusterNodeVersion string
	err = s.store.MutateCluster(ctx, project, location, cluster, func(c *containerstore.Cluster) error {
		for _, existing := range c.NodePools {
			if existing.Name == p.Name {
				// A name already taken on the cluster is a resource conflict —
				// 409 AlreadyExists — per the duplicate-name rule documented on
				// materializeNodePool.
				return containerstore.ErrAlreadyExists
			}
		}
		clusterNodeVersion = c.CurrentNodeVersion
		if clusterNodeVersion == "" {
			clusterNodeVersion = c.CurrentMasterVersion
		}
		if p.Version == "" {
			p.Version = clusterNodeVersion
		}
		c.NodePools = append(c.NodePools, p)
		return nil
	})
	if err != nil {
		return containerstore.Operation{}, mapStoreErr(err)
	}
	return s.recordNodePoolOperation(ctx, project, location, containerstore.OperationCreateNodePool, p.SelfLink, now)
}

// materializeNodePool validates a pool's name and fills the fields real GKE
// assigns when it materializes the pool: the canonical selfLink, the pool's node
// version (inherited from the cluster when the caller named none) and the node
// count implied by initialNodeCount. The mock has no node lifecycle, so the pool
// is stored RUNNING. Both the cluster create (inline nodePools) and
// CreateNodePool go through this one function, so the two paths cannot drift.
//
// Duplicate names, one story: a name already taken on the cluster is a resource
// conflict (409 AlreadyExists — the answer CreateNodePool gives for an existing
// pool), while the same name appearing twice inside one create request is a
// malformed request (400 InvalidArgument, the answer real GKE gives for an
// invalid cluster body — neither occurrence can be "the existing one", and
// nothing is created). Neither case is a name-grammar failure.
func materializeNodePool(project, location, cluster string, p containerstore.NodePool, nodeVersion string) (containerstore.NodePool, error) {
	if p.Name == "" {
		return p, invalidArgument("node pool name is required")
	}
	if !validNodePoolName.MatchString(p.Name) {
		return p, invalidArgument("invalid node pool name: " + p.Name)
	}
	p.Status = containerstore.StatusRunning
	p.SelfLink = NodePoolName(project, location, cluster, p.Name)
	if p.Version == "" {
		p.Version = nodeVersion
	}
	if p.NodeCount == 0 {
		p.NodeCount = p.InitialNodeCount
	}
	return p, nil
}

// materializeNodePools materializes the pools a cluster create request supplied
// inline through materializeNodePool, and rejects a name that appears twice in
// the same request (400 InvalidArgument — see the duplicate-name rule there).
func materializeNodePools(project, location string, c *containerstore.Cluster) error {
	seen := make(map[string]bool, len(c.NodePools))
	out := make([]containerstore.NodePool, 0, len(c.NodePools))
	for _, p := range c.NodePools {
		p, err := materializeNodePool(project, location, c.Name, p, c.CurrentMasterVersion)
		if err != nil {
			return err
		}
		if seen[p.Name] {
			return invalidArgument("duplicate node pool name: " + p.Name)
		}
		seen[p.Name] = true
		out = append(out, p)
	}
	c.NodePools = out
	return nil
}

// GetNodePool returns a node pool by short name.
func (s *Service) GetNodePool(ctx context.Context, project, location, cluster, pool string) (containerstore.NodePool, error) {
	c, err := s.store.GetCluster(ctx, project, location, cluster)
	if err != nil {
		return containerstore.NodePool{}, mapStoreErr(err)
	}
	for _, p := range c.NodePools {
		if p.Name == pool {
			return p, nil
		}
	}
	return containerstore.NodePool{}, modelNotFound("node pool not found")
}

// ListNodePools lists a cluster's node pools, sorted by short name.
func (s *Service) ListNodePools(ctx context.Context, project, location, cluster string) ([]containerstore.NodePool, error) {
	c, err := s.store.GetCluster(ctx, project, location, cluster)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	return sortNodePools(c.NodePools), nil
}

// DeleteNodePool removes a node pool and returns the DELETE_NODE_POOL operation.
func (s *Service) DeleteNodePool(ctx context.Context, project, location, cluster, pool string) (containerstore.Operation, error) {
	now := clock.Now()
	var selfLink string
	err := s.store.MutateCluster(ctx, project, location, cluster, func(c *containerstore.Cluster) error {
		out := c.NodePools[:0]
		found := false
		for _, p := range c.NodePools {
			if p.Name == pool {
				found = true
				selfLink = p.SelfLink
				continue
			}
			out = append(out, p)
		}
		if !found {
			return containerstore.ErrNoSuchNodePool
		}
		c.NodePools = out
		return nil
	})
	if err != nil {
		return containerstore.Operation{}, mapStoreErr(err)
	}
	if selfLink == "" {
		selfLink = NodePoolName(project, location, cluster, pool)
	}
	return s.recordNodePoolOperation(ctx, project, location, containerstore.OperationDeleteNodePool, selfLink, now)
}

// UpdateNodePool applies the provided (non-empty) fields of a node pool and
// returns the UPGRADE_NODES operation. Fields left unset are preserved.
func (s *Service) UpdateNodePool(ctx context.Context, project, location, cluster string, update containerstore.NodePool) (containerstore.Operation, error) {
	now := clock.Now()
	selfLink := NodePoolName(project, location, cluster, update.Name)
	err := s.store.MutateCluster(ctx, project, location, cluster, func(c *containerstore.Cluster) error {
		for i := range c.NodePools {
			if c.NodePools[i].Name == update.Name {
				selfLink = c.NodePools[i].SelfLink
				mergeNodePool(&c.NodePools[i], update)
				return nil
			}
		}
		return containerstore.ErrNoSuchNodePool
	})
	if err != nil {
		return containerstore.Operation{}, mapStoreErr(err)
	}
	return s.recordNodePoolOperation(ctx, project, location, containerstore.OperationUpdateNodePool, selfLink, now)
}

// SetNodePoolAutoscaling records the pool's autoscaling config.
func (s *Service) SetNodePoolAutoscaling(ctx context.Context, project, location, cluster, pool string, as containerstore.NodePoolAutoscaling) (containerstore.Operation, error) {
	return s.mutateNodePool(ctx, project, location, cluster, pool, containerstore.OperationUpdateNodePool, func(p *containerstore.NodePool) {
		a := as
		p.Autoscaling = &a
	})
}

// SetNodePoolManagement records the pool's node-management options.
func (s *Service) SetNodePoolManagement(ctx context.Context, project, location, cluster, pool string, m containerstore.NodeManagement) (containerstore.Operation, error) {
	return s.mutateNodePool(ctx, project, location, cluster, pool, containerstore.OperationSetNodePoolManagement, func(p *containerstore.NodePool) {
		mgmt := m
		p.Management = &mgmt
	})
}

// SetNodePoolSize records the pool's node count.
func (s *Service) SetNodePoolSize(ctx context.Context, project, location, cluster, pool string, size int32) (containerstore.Operation, error) {
	if size < 0 {
		return containerstore.Operation{}, invalidArgument("nodeCount must be non-negative")
	}
	return s.mutateNodePool(ctx, project, location, cluster, pool, containerstore.OperationSetNodePoolSize, func(p *containerstore.NodePool) {
		p.NodeCount = size
	})
}

// RollbackNodePoolUpgrade is a recorded no-op (there is no real upgrade
// lifecycle to roll back) that returns an UPGRADE_NODES operation.
func (s *Service) RollbackNodePoolUpgrade(ctx context.Context, project, location, cluster, pool string) (containerstore.Operation, error) {
	return s.mutateNodePool(ctx, project, location, cluster, pool, containerstore.OperationUpdateNodePool, func(*containerstore.NodePool) {})
}

// mutateNodePool applies fn to a node pool that must exist and records an
// operation of the given type.
func (s *Service) mutateNodePool(ctx context.Context, project, location, cluster, pool, opType string, fn func(*containerstore.NodePool)) (containerstore.Operation, error) {
	now := clock.Now()
	selfLink := NodePoolName(project, location, cluster, pool)
	err := s.store.MutateCluster(ctx, project, location, cluster, func(c *containerstore.Cluster) error {
		for i := range c.NodePools {
			if c.NodePools[i].Name == pool {
				selfLink = c.NodePools[i].SelfLink
				fn(&c.NodePools[i])
				return nil
			}
		}
		return containerstore.ErrNoSuchNodePool
	})
	if err != nil {
		return containerstore.Operation{}, mapStoreErr(err)
	}
	return s.recordNodePoolOperation(ctx, project, location, opType, selfLink, now)
}

// recordNodePoolOperation persists an operation whose target is a node pool.
func (s *Service) recordNodePoolOperation(ctx context.Context, project, location, opType, targetLink string, now time.Time) (containerstore.Operation, error) {
	op := s.newOperation(project, location, opType, targetLink, now)
	if err := s.store.CreateOperation(ctx, project, location, op); err != nil {
		return containerstore.Operation{}, mapStoreErr(err)
	}
	return op, nil
}

// mergeNodePool copies the non-zero fields of src into dst.
func mergeNodePool(dst *containerstore.NodePool, src containerstore.NodePool) {
	if src.Version != "" {
		dst.Version = src.Version
	}
	if len(src.Locations) > 0 {
		dst.Locations = src.Locations
	}
	if src.NodeCount > 0 {
		dst.NodeCount = src.NodeCount
	}
	if src.Autoscaling != nil {
		dst.Autoscaling = src.Autoscaling
	}
	if src.Management != nil {
		dst.Management = src.Management
	}
	if src.UpgradeSettings != nil {
		dst.UpgradeSettings = src.UpgradeSettings
	}
	if src.Config != nil {
		mergeNodeConfig(dst, src.Config)
	}
}

// mergeNodeConfig merges the non-empty fields of a node config into the pool.
func mergeNodeConfig(dst *containerstore.NodePool, src *containerstore.NodeConfig) {
	if dst.Config == nil {
		cfg := *src
		dst.Config = &cfg
		return
	}
	c := dst.Config
	if src.MachineType != "" {
		c.MachineType = src.MachineType
	}
	if src.DiskType != "" {
		c.DiskType = src.DiskType
	}
	if src.DiskSizeGb != 0 {
		c.DiskSizeGb = src.DiskSizeGb
	}
	if src.ImageType != "" {
		c.ImageType = src.ImageType
	}
	if len(src.OauthScopes) > 0 {
		c.OauthScopes = src.OauthScopes
	}
	if src.ServiceAccount != "" {
		c.ServiceAccount = src.ServiceAccount
	}
	if len(src.Metadata) > 0 {
		c.Metadata = src.Metadata
	}
	if len(src.Labels) > 0 {
		c.Labels = src.Labels
	}
	if len(src.ResourceLabels) > 0 {
		c.ResourceLabels = src.ResourceLabels
	}
	if len(src.Tags) > 0 {
		c.Tags = src.Tags
	}
	if src.MinCpuPlatform != "" {
		c.MinCpuPlatform = src.MinCpuPlatform
	}
	if src.LocalSsdCount != 0 {
		c.LocalSsdCount = src.LocalSsdCount
	}
	if src.Preemptible {
		c.Preemptible = true
	}
	if src.Spot {
		c.Spot = true
	}
}

func sortNodePools(in []containerstore.NodePool) []containerstore.NodePool {
	out := append([]containerstore.NodePool(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
