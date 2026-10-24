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

	// CancelJob transitions a running job to CANCELLED.
	CancelJob(ctx context.Context, project, region, jobID string) (dpstore.Job, error)

	// ListAllWorkflowTemplates lists every workflow template in a project
	// across all regions, sorted by region then template id.
	ListAllWorkflowTemplates(ctx context.Context, project string) ([]dpstore.WorkflowTemplate, error)

	// GetWorkflowTemplate returns one workflow template (its latest version).
	GetWorkflowTemplate(ctx context.Context, project, region, id string) (dpstore.WorkflowTemplate, error)
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

// ListAllJobs implements ProviderInterface.
func (p *Provider) ListAllJobs(ctx context.Context, project string) ([]dpstore.Job, error) {
	return p.svc.ListAllJobs(ctx, project)
}

// GetJob implements ProviderInterface.
func (p *Provider) GetJob(ctx context.Context, project, region, jobID string) (dpstore.Job, error) {
	return p.svc.GetJob(ctx, project, region, jobID)
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
