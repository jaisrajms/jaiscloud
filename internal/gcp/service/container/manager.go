package container

import (
	"context"

	containerstore "jaiscloud/internal/gcp/store/container"
)

// ClusterManager is the seam between the metadata control plane and the
// (currently absent) real Kubernetes control plane. The emulator ships only the
// mock: provisioning materializes the stored record and returns it instantly
// RUNNING. A future real engine (k3s-per-cluster; see docs/GA.md §7) would
// implement this interface to start/stop a control plane; the metadata core
// would not change.
type ClusterManager interface {
	// Provision prepares the backing control plane and returns the cluster
	// record to persist (status, endpoint, versions).
	Provision(ctx context.Context, c containerstore.Cluster) (containerstore.Cluster, error)
	// Deprovision tears down the backing control plane for a cluster.
	Deprovision(ctx context.Context, c containerstore.Cluster) error
}

// MockClusterManager makes a cluster instantly RUNNING with no real control
// plane. It is the only implementation that ships.
type MockClusterManager struct{}

// Provision marks the cluster RUNNING with a loopback endpoint.
func (MockClusterManager) Provision(_ context.Context, c containerstore.Cluster) (containerstore.Cluster, error) {
	c.Status = containerstore.StatusRunning
	c.Endpoint = "127.0.0.1"
	c.CurrentNodeVersion = c.CurrentMasterVersion
	return c, nil
}

// Deprovision is a no-op (there is no control plane to tear down).
func (MockClusterManager) Deprovision(_ context.Context, _ containerstore.Cluster) error {
	return nil
}
