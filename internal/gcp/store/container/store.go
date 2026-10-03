// Package container provides the Google Kubernetes Engine (GKE) v1 store.
// Clusters are project+location scoped with canonical names
// projects/{project}/locations/{location}/clusters/{cluster}; operations are the
// google.container.v1.Operation records under
// projects/{project}/locations/{location}/operations/{operation}.
//
// The emulator is metadata-only: a cluster is a stored record, not a schedulable
// Kubernetes control plane, and an operation is created already DONE (there is
// no asynchronous lifecycle to observe). The store persists the record the
// caller last observed so read-back is stable across a Postgres restart.
package container

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNoSuchCluster is returned when a cluster name is unknown.
	ErrNoSuchCluster = errors.New("NoSuchCluster")
	// ErrNoSuchOperation is returned when an operation name is unknown.
	ErrNoSuchOperation = errors.New("NoSuchOperation")
	// ErrAlreadyExists is returned when creating a cluster that already exists.
	ErrAlreadyExists = errors.New("AlreadyExists")
)

// Cluster status values (google.container.v1.Cluster.Status).
const (
	StatusRunning      = "RUNNING"
	StatusProvisioning = "PROVISIONING"
	StatusError        = "ERROR"
)

// Operation status values (google.container.v1.Operation.Status).
const (
	OperationStatusDone = "DONE"
)

// Operation type values (google.container.v1.Operation.Type).
const (
	OperationCreateCluster = "CREATE_CLUSTER"
	OperationDeleteCluster = "DELETE_CLUSTER"
)

// NodePool is the persisted subset of a GKE node pool.
type NodePool struct {
	Name             string `json:"name"`
	Status           string `json:"status,omitempty"`
	InitialNodeCount int32  `json:"initialNodeCount,omitempty"`
}

// Cluster is the persisted subset of google.container.v1.Cluster. Name is the
// short cluster id (the last path segment); the canonical resource name is
// derived from ProjectID/Location/Name.
type Cluster struct {
	ProjectID string `json:"projectId"`
	Location  string `json:"location"`
	Name      string `json:"name"`
	Status    string `json:"status"`
	Endpoint  string `json:"endpoint,omitempty"`
	// CaCertificate is the masterAuth.clusterCaCertificate value. The mock
	// mints no real CA, so this is the empty string.
	CaCertificate string `json:"caCertificate,omitempty"`

	CurrentMasterVersion  string `json:"currentMasterVersion,omitempty"`
	CurrentNodeVersion    string `json:"currentNodeVersion,omitempty"`
	InitialClusterVersion string `json:"initialClusterVersion,omitempty"`

	Network    string `json:"network,omitempty"`
	Subnetwork string `json:"subnetwork,omitempty"`

	// InitialNodeCount is the request's cluster-level initialNodeCount; the mock
	// materializes it as the default node pool's size.
	InitialNodeCount int32 `json:"initialNodeCount,omitempty"`

	NodePools      []NodePool        `json:"nodePools,omitempty"`
	ResourceLabels map[string]string `json:"resourceLabels,omitempty"`

	CreateTime time.Time `json:"createTime,omitempty"`
	SelfLink   string    `json:"selfLink,omitempty"`
}

// Operation is the persisted subset of google.container.v1.Operation. Name is
// the operation id ("operation-<uuid>"); the canonical resource name is derived
// from ProjectID/Location/Name. Note this is GKE's own Operation shape
// (operationType/status), not google.longrunning.Operation (done/response).
type Operation struct {
	ProjectID     string    `json:"projectId"`
	Location      string    `json:"location"`
	Name          string    `json:"name"`
	OperationType string    `json:"operationType"`
	Status        string    `json:"status"`
	TargetLink    string    `json:"targetLink,omitempty"`
	SelfLink      string    `json:"selfLink,omitempty"`
	StartTime     time.Time `json:"startTime,omitempty"`
	EndTime       time.Time `json:"endTime,omitempty"`
}

// Store is the GKE cluster/operation store.
type Store interface {
	CreateCluster(ctx context.Context, projectID, location string, c Cluster) error
	GetCluster(ctx context.Context, projectID, location, name string) (Cluster, error)
	DeleteCluster(ctx context.Context, projectID, location, name string) error
	ListClusters(ctx context.Context, projectID, location string) ([]Cluster, error)

	CreateOperation(ctx context.Context, projectID, location string, op Operation) error
	GetOperation(ctx context.Context, projectID, location, name string) (Operation, error)
	ListOperations(ctx context.Context, projectID, location string) ([]Operation, error)

	// Reset clears all clusters and operations (/_jaiscloud/reset).
	Reset(ctx context.Context)
}
