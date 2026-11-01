// Package container is the transport-neutral core for Google Kubernetes Engine
// (GKE) v1 (container.googleapis.com). It implements the metadata-only cluster
// control plane the emulator exposes — cluster CRUD plus the GKE Operation
// records create/delete return — over a Store. Both the REST adapter and any
// future transport share one instance, so they cannot drift.
//
// The emulator deliberately has no real control plane: clusters are stored
// records, not schedulable Kubernetes API endpoints (see docs/gcp/wire-contract.md §5). The
// small ClusterManager seam exists so a real engine can attach later; only the
// mock implementation ships today.
package container

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"regexp"
	"time"

	"jaiscloud/internal/clock"
	containerstore "jaiscloud/internal/gcp/store/container"
	"jaiscloud/internal/model"
)

// DefaultMasterVersion is the currentMasterVersion the mock reports when the
// caller supplies no initialClusterVersion. It mirrors the emulator's mock.
const DefaultMasterVersion = "1.30.5-gke.1014001"

// DefaultNodePoolName is the name GKE gives the node pool it generates from a
// create's cluster-level initialNodeCount/nodeConfig (Discovery on
// Cluster.nodeConfig: "this configuration (along with the initial_node_count)
// will be used to create a NodePool object with an auto-generated name").
const DefaultNodePoolName = "default-pool"

// The cluster-create conflict messages are verbatim real GKE 400s, captured
// against a live project (see docs/gcp/gke-create-pool-conflict.md).
const (
	nodeConfigPoolConflict       = "It's invalid to specify both cluster.node_config and a node pool. Please only provide a node pool."
	initialNodeCountPoolConflict = "It's invalid to specify both cluster.initial_node_count and a node pool. Please only provide a node pool."
	// negativeInitialNodeCount is the verbatim real GKE 400 for a negative
	// cluster-level initialNodeCount on a request that carries no nodePools
	// (docs/gcp/gke-create-pool-conflict.md).
	negativeInitialNodeCount = "Cluster.initial_node_count must be zero or higher."
)

// validClusterName is GKE's cluster-id grammar.
var validClusterName = regexp.MustCompile(`^[a-z](?:[a-z0-9-]{0,38}[a-z0-9])?$`)

// Service is the transport-neutral GKE core.
type Service struct {
	store   containerstore.Store
	manager ClusterManager
}

// NewService returns a GKE core over the store using the mock ClusterManager.
func NewService(s containerstore.Store) *Service {
	return &Service{store: s, manager: MockClusterManager{}}
}

// SetClusterManager overrides the cluster manager seam. A nil manager is a
// no-op, leaving the mock in place (tests rely on the default).
func (s *Service) SetClusterManager(m ClusterManager) {
	if m != nil {
		s.manager = m
	}
}

// Reset clears all clusters and operations (/_jaiscloud/reset).
func (s *Service) Reset(ctx context.Context) { s.store.Reset(ctx) }

// CreateCluster validates and stores a new cluster and returns the
// CREATE_CLUSTER operation. The cluster is created already RUNNING.
func (s *Service) CreateCluster(ctx context.Context, project, location string, c containerstore.Cluster) (containerstore.Operation, error) {
	if project == "" || location == "" {
		return containerstore.Operation{}, invalidArgument("project and location are required")
	}
	if c.Name == "" {
		return containerstore.Operation{}, invalidArgument("cluster name is required")
	}
	if !validClusterName.MatchString(c.Name) {
		return containerstore.Operation{}, invalidArgument("invalid cluster name: " + c.Name)
	}
	// Real GKE rejects a negative cluster-level initialNodeCount only when the
	// request carries no nodePools; with pools the value is ignored (captured in
	// docs/gcp/gke-create-pool-conflict.md, DPG49).
	if len(c.NodePools) == 0 && c.InitialNodeCount < 0 {
		return containerstore.Operation{}, invalidArgument(negativeInitialNodeCount)
	}
	// Discovery on Cluster.nodePools: "This field should not be set if
	// node_config or initial_node_count are specified", and on nodeConfig /
	// initialNodeCount: "Do not use this and a node_pool at the same time". Real
	// GKE answers such a create with a 400 naming the conflicting field (the
	// node_config message wins when both are set); the emulator accepted it and
	// ignored the default-pool fields.
	if len(c.NodePools) > 0 {
		if c.NodeConfig != nil {
			return containerstore.Operation{}, invalidArgument(nodeConfigPoolConflict)
		}
		// A zero initialNodeCount means "no default node pool", which real GKE
		// accepts alongside nodePools, so only a positive count conflicts.
		if c.InitialNodeCount > 0 {
			return containerstore.Operation{}, invalidArgument(initialNodeCountPoolConflict)
		}
	}

	now := clock.Now()
	c.Location = location
	c.Status = containerstore.StatusProvisioning
	if c.CurrentMasterVersion == "" {
		c.CurrentMasterVersion = DefaultMasterVersion
	}
	c.InitialClusterVersion = c.CurrentMasterVersion
	if c.Network == "" {
		c.Network = "default"
	}
	if c.Subnetwork == "" {
		c.Subnetwork = "default"
	}
	// The response messages real GKE always renders are persisted at create so
	// a snapshot round-trip carries them; NormalizeCluster is the one owner of
	// those literals, and WithReadDefaults applies the same function to records
	// written before they were modelled.
	c = NormalizeCluster(c)
	// Workload Identity is required by Dataproc-on-GKE; an empty pool means the
	// project default, exactly as GKE fills it.
	if c.WorkloadIdentityConfig != nil && c.WorkloadIdentityConfig.WorkloadPool == "" {
		c.WorkloadIdentityConfig = &containerstore.WorkloadIdentityConfig{WorkloadPool: project + ".svc.id.goog"}
	}
	if len(c.NodePools) == 0 {
		// Discovery on Cluster.nodeConfig: "For requests, this field should only
		// be used in lieu of a node_pool object, since this configuration (along
		// with the initial_node_count) will be used to create a NodePool object
		// with an auto-generated name" — so the generated default-pool carries
		// the caller's nodeConfig, and the shared materialization gives it the
		// same selfLink/version/node count a caller-supplied pool gets.
		c.NodePools = []containerstore.NodePool{{
			Name:             DefaultNodePoolName,
			InitialNodeCount: c.InitialNodeCount,
			Config:           c.NodeConfig,
		}}
	}
	if err := materializeNodePools(project, location, &c); err != nil {
		// A create request may carry its pools inline (Discovery: Cluster.nodePools);
		// real GKE materializes them in the same call, so they are stored as
		// provisioned pools rather than replaced by a default-pool.
		return containerstore.Operation{}, err
	}
	c.CaCertificate = ""
	c.CreateTime = now
	c.SelfLink = ClusterName(project, location, c.Name)

	provisioned, err := s.manager.Provision(ctx, c)
	if err != nil {
		return containerstore.Operation{}, err
	}
	provisioned.ProjectID = project
	provisioned.Location = location
	if provisioned.SelfLink == "" {
		provisioned.SelfLink = ClusterName(project, location, provisioned.Name)
	}
	if err := s.store.CreateCluster(ctx, project, location, provisioned); err != nil {
		return containerstore.Operation{}, mapStoreErr(err)
	}

	op := s.newOperation(project, location, containerstore.OperationCreateCluster,
		provisioned.SelfLink, now)
	if err := s.store.CreateOperation(ctx, project, location, op); err != nil {
		return containerstore.Operation{}, mapStoreErr(err)
	}
	return op, nil
}

// GetCluster returns a cluster by short name, with the response messages real
// GKE always renders filled in.
func (s *Service) GetCluster(ctx context.Context, project, location, name string) (containerstore.Cluster, error) {
	c, err := s.store.GetCluster(ctx, project, location, name)
	if err != nil {
		return containerstore.Cluster{}, mapStoreErr(err)
	}
	return WithReadDefaults(c), nil
}

// ListClusters lists clusters under a location, sorted by short name, with the
// response messages real GKE always renders filled in.
func (s *Service) ListClusters(ctx context.Context, project, location string) ([]containerstore.Cluster, error) {
	clusters, err := s.store.ListClusters(ctx, project, location)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	for i := range clusters {
		clusters[i] = WithReadDefaults(clusters[i])
	}
	return clusters, nil
}

// DeleteCluster deprovisions and removes a cluster, returning the
// DELETE_CLUSTER operation.
func (s *Service) DeleteCluster(ctx context.Context, project, location, name string) (containerstore.Operation, error) {
	c, err := s.store.GetCluster(ctx, project, location, name)
	if err != nil {
		return containerstore.Operation{}, mapStoreErr(err)
	}
	if err := s.manager.Deprovision(ctx, c); err != nil {
		return containerstore.Operation{}, err
	}
	if err := s.store.DeleteCluster(ctx, project, location, name); err != nil {
		return containerstore.Operation{}, mapStoreErr(err)
	}
	op := s.newOperation(project, location, containerstore.OperationDeleteCluster, c.SelfLink, clock.Now())
	if err := s.store.CreateOperation(ctx, project, location, op); err != nil {
		return containerstore.Operation{}, mapStoreErr(err)
	}
	return op, nil
}

// GetOperation returns an operation by id.
func (s *Service) GetOperation(ctx context.Context, project, location, name string) (containerstore.Operation, error) {
	op, err := s.store.GetOperation(ctx, project, location, name)
	if err != nil {
		return containerstore.Operation{}, mapStoreErr(err)
	}
	return op, nil
}

// ListOperations lists operations under a location, sorted by id.
func (s *Service) ListOperations(ctx context.Context, project, location string) ([]containerstore.Operation, error) {
	ops, err := s.store.ListOperations(ctx, project, location)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	return ops, nil
}

func (s *Service) newOperation(project, location, opType, targetLink string, now time.Time) containerstore.Operation {
	name := newOperationID()
	return containerstore.Operation{
		ProjectID:     project,
		Location:      location,
		Name:          name,
		OperationType: opType,
		Status:        containerstore.OperationStatusDone,
		TargetLink:    targetLink,
		SelfLink:      OperationName(project, location, name),
		StartTime:     now,
		EndTime:       now,
	}
}

func newOperationID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "operation-" + hex.EncodeToString(b[:])
}

func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

func modelNotFound(msg string) error {
	return model.NewProviderError("NotFound", msg, 404)
}

func mapStoreErr(err error) error {
	switch {
	case errors.Is(err, containerstore.ErrNoSuchCluster):
		return model.NewProviderError("NotFound", "cluster not found", 404)
	case errors.Is(err, containerstore.ErrNoSuchNodePool):
		return model.NewProviderError("NotFound", "node pool not found", 404)
	case errors.Is(err, containerstore.ErrNoSuchOperation):
		return model.NewProviderError("NotFound", "operation not found", 404)
	case errors.Is(err, containerstore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "already exists", 409)
	default:
		return err
	}
}
