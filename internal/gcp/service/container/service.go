// Package container is the transport-neutral core for Google Kubernetes Engine
// (GKE) v1 (container.googleapis.com). It implements the metadata-only cluster
// control plane the emulator exposes — cluster CRUD plus the GKE Operation
// records create/delete return — over a Store. Both the REST adapter and any
// future transport share one instance, so they cannot drift.
//
// The emulator deliberately has no real control plane: clusters are stored
// records, not schedulable Kubernetes API endpoints (see docs/GA.md §7). The
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
// caller supplies no initialClusterVersion. It mirrors floci-gcp's mock.
const DefaultMasterVersion = "1.30.5-gke.1014001"

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
	if len(c.NodePools) == 0 {
		c.NodePools = []containerstore.NodePool{{
			Name:             "default-pool",
			Status:           containerstore.StatusRunning,
			InitialNodeCount: c.InitialNodeCount,
		}}
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

// GetCluster returns a cluster by short name.
func (s *Service) GetCluster(ctx context.Context, project, location, name string) (containerstore.Cluster, error) {
	c, err := s.store.GetCluster(ctx, project, location, name)
	if err != nil {
		return containerstore.Cluster{}, mapStoreErr(err)
	}
	return c, nil
}

// ListClusters lists clusters under a location, sorted by short name.
func (s *Service) ListClusters(ctx context.Context, project, location string) ([]containerstore.Cluster, error) {
	clusters, err := s.store.ListClusters(ctx, project, location)
	if err != nil {
		return nil, mapStoreErr(err)
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

func mapStoreErr(err error) error {
	switch {
	case errors.Is(err, containerstore.ErrNoSuchCluster):
		return model.NewProviderError("NotFound", "cluster not found", 404)
	case errors.Is(err, containerstore.ErrNoSuchOperation):
		return model.NewProviderError("NotFound", "operation not found", 404)
	case errors.Is(err, containerstore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "cluster already exists", 409)
	default:
		return err
	}
}
