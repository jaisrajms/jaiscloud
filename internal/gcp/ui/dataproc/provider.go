package dataprocui

import (
	"context"

	dataproccore "jaiscloud/internal/gcp/service/dataproc"
	dpstore "jaiscloud/internal/gcp/store/dataproc"
)

// ProviderInterface is the subset of *dataproccore.Service used by the Dataproc
// UI handlers. The core is transport-neutral and addressable directly, so the
// UI reuses its typed API rather than going through a REST adapter. It keeps the
// UI decoupled from the core's concrete type and hides the long-running
// operations the core returns from mutations.
type ProviderInterface interface {
	// ListAllClusters lists every cluster in a project across all regions,
	// sorted by region then name. It backs the region-optional console list.
	ListAllClusters(ctx context.Context, project string) ([]dpstore.Cluster, error)

	// GetCluster returns one cluster.
	GetCluster(ctx context.Context, project, region, name string) (dpstore.Cluster, error)

	// CreateCluster creates a cluster in CREATING state from a GCE config or a
	// GKE virtualClusterConfig. The returned long-running operation is
	// discarded (the cluster settles lazily on a later read), so the console
	// shows CREATING and then RUNNING on refresh.
	CreateCluster(ctx context.Context, project, region, name string, in dataproccore.ClusterInput) (dpstore.Cluster, error)

	// UpdateCluster applies a masked config/labels patch. An empty mask means
	// "apply everything supplied", which is the wire behaviour; the console
	// always sends the fields the caller touched. The returned long-running
	// operation is discarded (the cluster settles on a later read).
	UpdateCluster(ctx context.Context, project, region, name string, in dataproccore.ClusterInput, mask []string) (dpstore.Cluster, error)

	// StartCluster / StopCluster transition a cluster's state. The returned
	// operation is discarded (the transition settles lazily on the next read).
	StartCluster(ctx context.Context, project, region, name string) (dpstore.Cluster, error)
	StopCluster(ctx context.Context, project, region, name string) (dpstore.Cluster, error)

	// DeleteCluster removes a cluster.
	DeleteCluster(ctx context.Context, project, region, name string) error

	// ListAllJobs lists every job in a project across all regions, sorted by
	// region then job id.
	ListAllJobs(ctx context.Context, project string) ([]dpstore.Job, error)

	// GetJob returns one job.
	GetJob(ctx context.Context, project, region, jobID string) (dpstore.Job, error)

	// SubmitJob submits a job to a cluster. The core validates and dispatches
	// the type-specific body; the returned operation-less job is PENDING or
	// RUNNING and settles on a later read.
	SubmitJob(ctx context.Context, project, region string, in dataproccore.JobInput) (dpstore.Job, error)

	// CancelJob transitions a running job to CANCELLED.
	CancelJob(ctx context.Context, project, region, jobID string) (dpstore.Job, error)

	// ListAllWorkflowTemplates lists every workflow template in a project
	// across all regions, sorted by region then template id.
	ListAllWorkflowTemplates(ctx context.Context, project string) ([]dpstore.WorkflowTemplate, error)

	// GetWorkflowTemplate returns one workflow template (its latest version).
	GetWorkflowTemplate(ctx context.Context, project, region, id string) (dpstore.WorkflowTemplate, error)

	// CreateWorkflowTemplate stores a new template at version 1.
	CreateWorkflowTemplate(ctx context.Context, project, region string, in dataproccore.WorkflowTemplateInput) (dpstore.WorkflowTemplate, error)

	// UpdateWorkflowTemplate replaces a definition and bumps the version; a
	// version that no longer matches the server's is ABORTED by the core.
	UpdateWorkflowTemplate(ctx context.Context, project, region string, in dataproccore.WorkflowTemplateInput) (dpstore.WorkflowTemplate, error)

	// DeleteWorkflowTemplate removes a template (version 0 = all versions).
	DeleteWorkflowTemplate(ctx context.Context, project, region, id string, version int32) error

	// InstantiateWorkflowTemplate runs a stored template. The operation is
	// returned to the console so it can show the run's name.
	InstantiateWorkflowTemplate(ctx context.Context, project, region, templateID string, version int32, parameters map[string]string) (dpstore.Operation, error)
}

// Provider adapts the transport-neutral Dataproc core onto ProviderInterface,
// discarding the long-running operations the core returns from mutations.
type Provider struct {
	svc *dataproccore.Service
}

// NewProvider returns a UI provider over the Dataproc core.
func NewProvider(svc *dataproccore.Service) *Provider { return &Provider{svc: svc} }

var _ ProviderInterface = (*Provider)(nil)

// ListAllClusters implements ProviderInterface.
func (p *Provider) ListAllClusters(ctx context.Context, project string) ([]dpstore.Cluster, error) {
	return p.svc.ListAllClusters(ctx, project)
}

// GetCluster implements ProviderInterface.
func (p *Provider) GetCluster(ctx context.Context, project, region, name string) (dpstore.Cluster, error) {
	return p.svc.GetCluster(ctx, project, region, name)
}

// StartCluster implements ProviderInterface; the start operation is discarded.
func (p *Provider) StartCluster(ctx context.Context, project, region, name string) (dpstore.Cluster, error) {
	c, _, err := p.svc.StartCluster(ctx, project, region, name)
	return c, err
}

// StopCluster implements ProviderInterface; the stop operation is discarded.
func (p *Provider) StopCluster(ctx context.Context, project, region, name string) (dpstore.Cluster, error) {
	c, _, err := p.svc.StopCluster(ctx, project, region, name)
	return c, err
}

// DeleteCluster implements ProviderInterface; the delete operation is discarded.
func (p *Provider) DeleteCluster(ctx context.Context, project, region, name string) error {
	_, err := p.svc.DeleteCluster(ctx, project, region, name)
	return err
}

// CreateCluster implements ProviderInterface; the create operation is discarded
// and the cluster is returned in CREATING state.
func (p *Provider) CreateCluster(ctx context.Context, project, region, name string, in dataproccore.ClusterInput) (dpstore.Cluster, error) {
	c, _, err := p.svc.CreateCluster(ctx, project, region, name, in)
	return c, err
}

// UpdateCluster implements ProviderInterface; the update operation is discarded
// and the cluster is returned in UPDATING state.
func (p *Provider) UpdateCluster(ctx context.Context, project, region, name string, in dataproccore.ClusterInput, mask []string) (dpstore.Cluster, error) {
	c, _, err := p.svc.UpdateCluster(ctx, project, region, name, in, mask)
	return c, err
}

// ListAllJobs implements ProviderInterface.
func (p *Provider) ListAllJobs(ctx context.Context, project string) ([]dpstore.Job, error) {
	return p.svc.ListAllJobs(ctx, project)
}

// GetJob implements ProviderInterface.
func (p *Provider) GetJob(ctx context.Context, project, region, jobID string) (dpstore.Job, error) {
	return p.svc.GetJob(ctx, project, region, jobID)
}

// SubmitJob implements ProviderInterface.
func (p *Provider) SubmitJob(ctx context.Context, project, region string, in dataproccore.JobInput) (dpstore.Job, error) {
	return p.svc.SubmitJob(ctx, project, region, in)
}

// CancelJob implements ProviderInterface.
func (p *Provider) CancelJob(ctx context.Context, project, region, jobID string) (dpstore.Job, error) {
	return p.svc.CancelJob(ctx, project, region, jobID)
}

// ListAllWorkflowTemplates implements ProviderInterface.
func (p *Provider) ListAllWorkflowTemplates(ctx context.Context, project string) ([]dpstore.WorkflowTemplate, error) {
	return p.svc.ListAllWorkflowTemplates(ctx, project)
}

// GetWorkflowTemplate implements ProviderInterface; version 0 returns the
// latest.
func (p *Provider) GetWorkflowTemplate(ctx context.Context, project, region, id string) (dpstore.WorkflowTemplate, error) {
	return p.svc.GetWorkflowTemplate(ctx, project, region, id, 0)
}

// CreateWorkflowTemplate implements ProviderInterface.
func (p *Provider) CreateWorkflowTemplate(ctx context.Context, project, region string, in dataproccore.WorkflowTemplateInput) (dpstore.WorkflowTemplate, error) {
	return p.svc.CreateWorkflowTemplate(ctx, project, region, in)
}

// UpdateWorkflowTemplate implements ProviderInterface.
func (p *Provider) UpdateWorkflowTemplate(ctx context.Context, project, region string, in dataproccore.WorkflowTemplateInput) (dpstore.WorkflowTemplate, error) {
	return p.svc.UpdateWorkflowTemplate(ctx, project, region, in)
}

// DeleteWorkflowTemplate implements ProviderInterface.
func (p *Provider) DeleteWorkflowTemplate(ctx context.Context, project, region, id string, version int32) error {
	return p.svc.DeleteWorkflowTemplate(ctx, project, region, id, version)
}

// InstantiateWorkflowTemplate implements ProviderInterface.
func (p *Provider) InstantiateWorkflowTemplate(ctx context.Context, project, region, templateID string, version int32, parameters map[string]string) (dpstore.Operation, error) {
	return p.svc.InstantiateWorkflowTemplate(ctx, project, region, templateID, version, parameters)
}
