// Package dataproc provides the Cloud Dataproc store (clusters, jobs, and the
// long-running operations returned by create/update/delete/start/stop/submit).
// Resources are project+region scoped with canonical names
// projects/{project}/regions/{region}/clusters/{name} (and .../jobs/{id},
// .../operations/{id}). A cluster is a logical record only — the emulator never
// stands up a real multi-node cluster, exactly as EMR-on-EC2 never stands up a
// real YARN cluster.
package dataproc

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNoSuchCluster   = errors.New("NoSuchCluster")
	ErrNoSuchJob       = errors.New("NoSuchJob")
	ErrNoSuchOperation = errors.New("NoSuchOperation")
	ErrAlreadyExists   = errors.New("AlreadyExists")
)

// ClusterStatus mirrors dataproc.v1.ClusterStatus.
type ClusterStatus struct {
	State          string    `json:"state"` // CREATING|RUNNING|STOPPED|DELETING|ERROR|...
	Detail         string    `json:"detail,omitempty"`
	StateStartTime time.Time `json:"stateStartTime,omitempty"`
}

// Cluster is a logical Dataproc cluster. Config is the full ClusterConfig wire
// object (gceClusterConfig, masterConfig/workerConfig/secondaryWorkerConfig,
// softwareConfig, initializationActions, ...) stored verbatim as JSON so
// read-back matches what the caller sent.
type Cluster struct {
	ProjectID     string            `json:"projectId"`
	Region        string            `json:"region"`
	Name          string            `json:"clusterName"`
	Config        json.RawMessage   `json:"config,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
	Status        ClusterStatus     `json:"status"`
	StatusHistory []ClusterStatus   `json:"statusHistory,omitempty"`
	ClusterUUID   string            `json:"clusterUuid,omitempty"`
	CreateTime    time.Time         `json:"createTime"`
	UpdateTime    time.Time         `json:"updateTime"`
}

// JobStatus mirrors dataproc.v1.JobStatus.
type JobStatus struct {
	State          string    `json:"state"` // DONE|RUNNING|ERROR|CANCELLED|PENDING
	Details        string    `json:"details,omitempty"`
	StateStartTime time.Time `json:"stateStartTime,omitempty"`
}

// Job is a Dataproc job. Type is the oneof field name (sparkJob, pysparkJob,
// sparkSqlJob, sparkRJob, hadoopJob, hiveJob, pigJob) and TypeJob holds the
// per-type job body verbatim.
type Job struct {
	ProjectID               string            `json:"projectId"`
	Region                  string            `json:"region"`
	JobID                   string            `json:"jobId"`
	PlacementClusterName    string            `json:"placementClusterName"`
	Type                    string            `json:"type"`
	TypeJob                 json.RawMessage   `json:"typeJob,omitempty"`
	Labels                  map[string]string `json:"labels,omitempty"`
	Status                  JobStatus         `json:"status"`
	StatusHistory           []JobStatus       `json:"statusHistory,omitempty"`
	DriverOutputResourceURI string            `json:"driverOutputResourceUri,omitempty"`
	DriverControlFilesURI   string            `json:"driverControlFilesUri,omitempty"`
	JobUUID                 string            `json:"jobUuid,omitempty"`
	CreateTime              time.Time         `json:"createTime"`
}

// Operation is a done long-running operation. Metadata and Response are the
// already-rendered JSON wire objects (Metadata carries its @type) stored
// verbatim.
type Operation struct {
	ID         string    `json:"id"`
	ProjectID  string    `json:"projectId"`
	Region     string    `json:"region"`
	Done       bool      `json:"done"`
	Metadata   string    `json:"metadata,omitempty"`
	Response   string    `json:"response,omitempty"`
	Verb       string    `json:"verb"`
	Target     string    `json:"target"`
	CreateTime time.Time `json:"createTime"`
	EndTime    time.Time `json:"endTime"`
}

// Store is the Cloud Dataproc store.
type Store interface {
	CreateCluster(ctx context.Context, projectID, region string, c Cluster) error
	GetCluster(ctx context.Context, projectID, region, name string) (Cluster, error)
	UpdateCluster(ctx context.Context, projectID, region string, c Cluster) error
	// UpdateClusterAtomic performs a locked get-mutate-set cycle: mutate
	// receives the current cluster and returns the version to persist, or an
	// error to abort without writing. Unlike a separate GetCluster followed
	// by UpdateCluster, this is atomic with respect to concurrent updates on
	// the same cluster, so a labels/config PATCH can't race with a concurrent
	// StartCluster/StopCluster status transition and lose one or the other.
	UpdateClusterAtomic(ctx context.Context, projectID, region, name string, mutate func(Cluster) (Cluster, error)) (Cluster, error)
	DeleteCluster(ctx context.Context, projectID, region, name string) error
	ListClusters(ctx context.Context, projectID, region string) ([]Cluster, error)

	CreateJob(ctx context.Context, projectID, region string, j Job) error
	GetJob(ctx context.Context, projectID, region, jobID string) (Job, error)
	UpdateJob(ctx context.Context, projectID, region string, j Job) error
	// UpdateJobAtomic performs a locked get-mutate-set cycle: mutate receives
	// the current job and returns the version to persist, or an error to
	// abort without writing. Used by CancelJob and finishJob so a client
	// cancelling a job can't race with the job's own goroutine reaching a
	// terminal state — whichever acquires the lock first wins, and the other
	// sees the already-terminal state inside its own mutate and no-ops.
	UpdateJobAtomic(ctx context.Context, projectID, region, jobID string, mutate func(Job) (Job, error)) (Job, error)
	DeleteJob(ctx context.Context, projectID, region, jobID string) error
	ListJobs(ctx context.Context, projectID, region string) ([]Job, error)

	CreateOperation(ctx context.Context, projectID, region string, op Operation) error
	GetOperation(ctx context.Context, projectID, region, id string) (Operation, error)
	UpdateOperation(ctx context.Context, projectID, region string, op Operation) error

	Reset(ctx context.Context)
}
