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
	"strings"
	"time"
)

var (
	ErrNoSuchCluster          = errors.New("NoSuchCluster")
	ErrNoSuchJob              = errors.New("NoSuchJob")
	ErrNoSuchOperation        = errors.New("NoSuchOperation")
	ErrNoSuchWorkflowTemplate = errors.New("NoSuchWorkflowTemplate")
	ErrAlreadyExists          = errors.New("AlreadyExists")
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
//
// VirtualClusterConfig is the dataproc.v1.VirtualClusterConfig wire object
// (kubernetesClusterConfig, auxiliaryServicesConfig, stagingBucket, ...) for a
// Dataproc-on-GKE cluster, also stored verbatim. It is mutually exclusive with
// Config in the API: a GKE-backed cluster has a VirtualClusterConfig and no
// GCE ClusterConfig.
type Cluster struct {
	ProjectID            string            `json:"projectId"`
	Region               string            `json:"region"`
	Name                 string            `json:"clusterName"`
	Config               json.RawMessage   `json:"config,omitempty"`
	VirtualClusterConfig json.RawMessage   `json:"virtualClusterConfig,omitempty"`
	Labels               map[string]string `json:"labels,omitempty"`
	Status               ClusterStatus     `json:"status"`
	StatusHistory        []ClusterStatus   `json:"statusHistory,omitempty"`
	ClusterUUID          string            `json:"clusterUuid,omitempty"`
	// Namespace is the effective Kubernetes namespace the cluster's jobs run in
	// (the caller's virtualClusterConfig kubernetesNamespace when supplied, else
	// a derived per-cluster name). Empty in mock execution mode (no executor);
	// on provisioning failure it falls back to the process-wide namespace.
	// NamespaceOwned is true when the emulator created the namespace and may
	// therefore delete it on cluster delete/reset; a pre-existing adopted
	// namespace is left untouched.
	Namespace      string    `json:"namespace,omitempty"`
	NamespaceOwned bool      `json:"namespaceOwned,omitempty"`
	CreateTime     time.Time `json:"createTime"`
	UpdateTime     time.Time `json:"updateTime"`
}

// IsGKEBacked reports whether the cluster was created with a
// virtualClusterConfig (a Dataproc-on-GKE cluster) rather than a GCE
// ClusterConfig. It is derived from the stored config, not persisted.
func (c Cluster) IsGKEBacked() bool {
	s := strings.TrimSpace(string(c.VirtualClusterConfig))
	return s != "" && s != "{}" && s != "null"
}

// JobStatus mirrors dataproc.v1.JobStatus.
type JobStatus struct {
	State          string    `json:"state"` // DONE|RUNNING|ERROR|CANCELLED|PENDING
	Details        string    `json:"details,omitempty"`
	StateStartTime time.Time `json:"stateStartTime,omitempty"`
	// Substate is the agent-reported progress substate (SUBMITTED|QUEUED|
	// STALE_STATUS). Per dataproc.v1.JobStatus every defined substate applies to
	// RUNNING; terminal states carry none.
	Substate string `json:"substate,omitempty"`
}

// JobScheduling mirrors dataproc.v1.JobScheduling: the restart policy for a
// job's driver. Both fields are optional; 0 (the documented default) means
// "no restarts after a failure". The API documents maxima of 10 (per hour)
// and 240 (total).
type JobScheduling struct {
	MaxFailuresPerHour int32 `json:"maxFailuresPerHour,omitempty"`
	MaxFailuresTotal   int32 `json:"maxFailuresTotal,omitempty"`
}

// Job is a Dataproc job. Type is the oneof field name (sparkJob, pysparkJob,
// sparkSqlJob, sparkRJob, hadoopJob, hiveJob, pigJob) and TypeJob holds the
// per-type job body verbatim.
type Job struct {
	ProjectID            string `json:"projectId"`
	Region               string `json:"region"`
	JobID                string `json:"jobId"`
	PlacementClusterName string `json:"placementClusterName"`
	// PlacementClusterUUID is the UUID of the cluster the job was submitted to
	// (dataproc.v1.JobPlacement.cluster_uuid, output-only). Captured at submit so
	// it survives the cluster being deleted before the job is terminal.
	PlacementClusterUUID string `json:"placementClusterUuid,omitempty"`
	// Scheduling is the job's restart policy (dataproc.v1.Job.scheduling). A nil
	// value or zero counters means the driver is not restarted (the API
	// default).
	Scheduling *JobScheduling `json:"scheduling,omitempty"`
	// LongRunning marks an emulator-detected long-running (streaming) job. Real
	// GCP has no such field: a streaming job is simply one whose driver never
	// exits, so real Dataproc reports it RUNNING until the driver terminates.
	// The marker only affects the mock-mode state machine (which must not
	// auto-settle such a job); k8s mode already stays RUNNING while the driver
	// pod lives. See plan_docs/gcp-dataproc-streaming-wave-plan.md §5.
	LongRunning             bool              `json:"longRunning,omitempty"`
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

// WorkflowTemplate is a stored Dataproc workflow template
// (dataproc.v1.WorkflowTemplate). Definition is the full WorkflowTemplate wire
// object (jobs, placement, parameters, labels, dagTimeout, ...) stored verbatim
// as JSON; the create/update timestamps and version are managed by the store.
//
// Only the latest version is retained: a create stores version 1 and every
// update stores the next version in place (real GCP keeps a version history —
// see README-GCP.md). Get/Delete with an explicit non-current version therefore
// report NotFound.
type WorkflowTemplate struct {
	ProjectID  string          `json:"projectId"`
	Region     string          `json:"region"`
	TemplateID string          `json:"id"`
	Version    int32           `json:"version"`
	Definition json.RawMessage `json:"definition"`
	CreateTime time.Time       `json:"createTime"`
	UpdateTime time.Time       `json:"updateTime"`
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
	// ListClustersByProject returns every cluster in a project across all
	// regions, ordered by region then name. It backs the region-optional
	// console list.
	ListClustersByProject(ctx context.Context, projectID string) ([]Cluster, error)

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
	// ListJobsByProject returns every job in a project across all regions,
	// ordered by region then job id. It backs the region-optional console list.
	ListJobsByProject(ctx context.Context, projectID string) ([]Job, error)

	CreateOperation(ctx context.Context, projectID, region string, op Operation) error
	GetOperation(ctx context.Context, projectID, region, id string) (Operation, error)
	UpdateOperation(ctx context.Context, projectID, region string, op Operation) error
	// UpdateOperationAtomic performs a locked get-mutate-set cycle: mutate
	// receives the current operation and returns the version to persist, or an
	// error to abort. Used by the submit-operation poll so refreshing an
	// in-flight operation's metadata can't clobber a concurrent poll that just
	// completed it (which would resurrect a done operation).
	UpdateOperationAtomic(ctx context.Context, projectID, region, id string, mutate func(Operation) (Operation, error)) (Operation, error)
	// DeleteStaleOperations removes completed operations whose CreateTime
	// predates cutoff (across every project/region scope), returning the number
	// deleted. In-flight operations are retained. It is the store half of the
	// Dataproc operation-retention sweep: real operations are GC'd after a TTL,
	// and without this jc_dataproc_operations grows unbounded (the only other
	// delete is Reset).
	DeleteStaleOperations(ctx context.Context, cutoff time.Time) (int, error)

	CreateWorkflowTemplate(ctx context.Context, projectID, region string, t WorkflowTemplate) error
	GetWorkflowTemplate(ctx context.Context, projectID, region, templateID string) (WorkflowTemplate, error)
	// UpdateWorkflowTemplateAtomic performs a locked get-mutate-set cycle over a
	// template, so a version bump (UpdateWorkflowTemplate) can't race a
	// concurrent update into a lost version. mutate receives the current
	// template and returns the version to persist.
	UpdateWorkflowTemplateAtomic(ctx context.Context, projectID, region, templateID string, mutate func(WorkflowTemplate) (WorkflowTemplate, error)) (WorkflowTemplate, error)
	DeleteWorkflowTemplate(ctx context.Context, projectID, region, templateID string) error
	ListWorkflowTemplates(ctx context.Context, projectID, region string) ([]WorkflowTemplate, error)
	// ListWorkflowTemplatesByProject returns every workflow template in a
	// project across all regions, ordered by region then template id. It backs
	// the region-optional console list.
	ListWorkflowTemplatesByProject(ctx context.Context, projectID string) ([]WorkflowTemplate, error)

	Reset(ctx context.Context)
}
