// Package dataprocui serves the Cloud Dataproc console UI API. Handlers call
// the transport-neutral Dataproc core directly (in-process) rather than over the
// wire.
//
// The console is region-optional: lists aggregate every resource in the project
// across all regions (no region picker) and show the region as a read-only
// field. Detail/actions link back with the region carried from the list row, so
// a resource id never has to be disambiguated by hand.
//
// The surface is clusters list/detail with start/stop/create/update/delete,
// jobs list/detail with submit/cancel, and workflow templates list/detail with
// create/update/delete/instantiate. Mutations return the settled-or-in-flight
// record; the core's long-running operation is dropped (the record settles on a
// later read), except instantiate, whose operation the console shows.
package dataprocui

import "encoding/json"

// StatusEvent is one entry of a resource's status history (cluster or job).
type StatusEvent struct {
	State     string `json:"state"`
	Detail    string `json:"detail,omitempty"`
	StartTime string `json:"stateStartTime,omitempty"`
}

// Cluster is the console rendering of a Dataproc cluster, flattened across
// regions for the list page. Config/VirtualClusterConfig and the Kubernetes
// namespace placement (Namespace/NamespaceOwned/KubernetesNamespace) are only
// populated on the detail read, not the list.
type Cluster struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Region        string            `json:"region"`
	Status        string            `json:"status"`
	StatusDetail  string            `json:"statusDetail,omitempty"`
	StatusHistory []StatusEvent     `json:"statusHistory,omitempty"`
	ClusterUUID   string            `json:"clusterUuid,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
	GKEBacked     bool              `json:"gkeBacked,omitempty"`
	// Namespace is the effective Kubernetes namespace the cluster's jobs run in
	// (the requested kubernetesNamespace when supplied, else a derived
	// per-cluster name). Empty only in mock execution mode (no k8s client);
	// when provisioning fails it falls back to the process-wide namespace.
	// Populated on the detail read only.
	Namespace string `json:"namespace,omitempty"`
	// NamespaceOwned is true when the emulator created the namespace and may
	// delete it on cluster delete; a pre-existing adopted namespace is false.
	NamespaceOwned bool `json:"namespaceOwned,omitempty"`
	// KubernetesNamespace is the effective namespace recorded on a GKE
	// VirtualClusterConfig: the caller's value, or the cluster name when the
	// request omitted it ("" for a GCE-backed cluster); populated on the detail
	// read only.
	KubernetesNamespace  string          `json:"kubernetesNamespace,omitempty"`
	Config               json.RawMessage `json:"config,omitempty"`
	VirtualClusterConfig json.RawMessage `json:"virtualClusterConfig,omitempty"`
	CreateTime           string          `json:"createTime,omitempty"`
	UpdateTime           string          `json:"updateTime,omitempty"`
}

// ListClustersResponse is the response for GET /clusters.
type ListClustersResponse struct {
	Clusters []Cluster `json:"clusters"`
	Total    int       `json:"total"`
}

// CreateClusterRequest is the console create-cluster body (POST /clusters).
// Exactly one of Config (a GCE ClusterConfig) or VirtualClusterConfig (a
// Dataproc-on-GKE VirtualClusterConfig carrying kubernetesNamespace) is sent;
// the core stores it verbatim and enforces mutual exclusivity.
type CreateClusterRequest struct {
	Region               string            `json:"region"`
	Name                 string            `json:"name"`
	Labels               map[string]string `json:"labels,omitempty"`
	Config               json.RawMessage   `json:"config,omitempty"`
	VirtualClusterConfig json.RawMessage   `json:"virtualClusterConfig,omitempty"`
}

// UpdateClusterRequest is the console patch body
// (PATCH /clusters/{region}/{cluster}). UpdateMask carries the fields the user
// touched as camelCase paths (config.workerConfig.numInstances, labels); it is
// passed to the core verbatim, and an empty mask applies everything supplied.
type UpdateClusterRequest struct {
	Labels     map[string]string `json:"labels,omitempty"`
	Config     json.RawMessage   `json:"config,omitempty"`
	UpdateMask []string          `json:"updateMask,omitempty"`
}

// Job is the console rendering of a Dataproc job, flattened across regions for
// the list page. TypeJob carries the type-specific body (sparkJob, pysparkJob,
// ...) and is only populated on the detail read.
type Job struct {
	ID                      string            `json:"id"`
	Name                    string            `json:"name"`
	Region                  string            `json:"region"`
	ClusterName             string            `json:"clusterName,omitempty"`
	Type                    string            `json:"type,omitempty"`
	TypeJob                 json.RawMessage   `json:"typeJob,omitempty"`
	Status                  string            `json:"status"`
	StatusDetail            string            `json:"statusDetail,omitempty"`
	Substate                string            `json:"substate,omitempty"`
	StatusHistory           []StatusEvent     `json:"statusHistory,omitempty"`
	Labels                  map[string]string `json:"labels,omitempty"`
	DriverOutputResourceURI string            `json:"driverOutputResourceUri,omitempty"`
	DriverControlFilesURI   string            `json:"driverControlFilesUri,omitempty"`
	JobUUID                 string            `json:"jobUuid,omitempty"`
	CreateTime              string            `json:"createTime,omitempty"`
}

// ListJobsResponse is the response for GET /jobs.
type ListJobsResponse struct {
	Jobs  []Job `json:"jobs"`
	Total int   `json:"total"`
}

// SubmitJobRequest is the console submit body (POST /jobs). The console is
// region-optional, so the region travels in the body alongside the
// dataproc.v1.Job object (reference/placement and exactly one type-job field).
type SubmitJobRequest struct {
	Region string          `json:"region"`
	Job    json.RawMessage `json:"job"`
}

// WorkflowTemplate is the console rendering of a Dataproc workflow template,
// flattened across regions for the list page. Definition carries the full wire
// workflow object and is only populated on the detail read.
type WorkflowTemplate struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	Region     string          `json:"region"`
	Version    int32           `json:"version"`
	Definition json.RawMessage `json:"definition,omitempty"`
	CreateTime string          `json:"createTime,omitempty"`
	UpdateTime string          `json:"updateTime,omitempty"`
}

// ListWorkflowTemplatesResponse is the response for GET /workflow-templates.
type ListWorkflowTemplatesResponse struct {
	Templates []WorkflowTemplate `json:"templates"`
	Total     int                `json:"total"`
}

// WorkflowTemplateRequest is the console create/update body. It mirrors the
// wire WorkflowTemplate (placement, jobs, parameters, labels, dagTimeout, ...)
// with region and version moved out of the definition: the console is
// region-optional on create, and version is the optimistic-concurrency token on
// update. Definition is passed to the core verbatim.
type WorkflowTemplateRequest struct {
	Region     string          `json:"region"`
	ID         string          `json:"id"`
	Version    int32           `json:"version,omitempty"`
	Definition json.RawMessage `json:"definition,omitempty"`
}

// InstantiateWorkflowTemplateRequest is the console instantiate body
// (POST /workflow-templates/{region}/{template}/instantiate), matching
// dataproc.v1.InstantiateWorkflowTemplateRequest without requestId (the
// emulator, like real GCP best-effort, does not deduplicate runs).
type InstantiateWorkflowTemplateRequest struct {
	Version    int32             `json:"version,omitempty"`
	Parameters map[string]string `json:"parameters,omitempty"`
}
