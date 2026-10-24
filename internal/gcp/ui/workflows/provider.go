package workflowsui

import (
	"context"

	workflowexecutionscore "jaiscloud/internal/gcp/service/workflowexecutions"
	workflowscore "jaiscloud/internal/gcp/service/workflows"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
)

// workflowManagedMask is the updateMask the UI applies when the client does not
// send one: exactly the fields the workflow edit form manages. The edit form
// round-trips every one of these (it loads the workflow first), so a
// description-only edit leaves sourceContents unchanged and does not bump the
// revision.
const workflowManagedMask = "description,sourceContents,serviceAccount,callLogLevel,labels,userEnvVars"

// ProviderInterface is the subset of the Workflows management and Workflow
// Executions cores the UI handlers consume. The two services share the workflows
// store but are distinct cores, so Provider composes them onto one seam; it
// keeps the handlers decoupled from the concrete cores.
type ProviderInterface interface {
	// ListWorkflowsByProject lists every workflow in a project across all
	// locations, sorted by location then id. It backs the location-optional
	// console list.
	ListWorkflowsByProject(ctx context.Context, project string) ([]workflowsstore.Workflow, error)

	// Workflow CRUD.
	GetWorkflow(ctx context.Context, project, location, id string) (workflowsstore.Workflow, error)
	CreateWorkflow(ctx context.Context, project, location string, in WorkflowInput) (workflowsstore.Workflow, error)
	UpdateWorkflow(ctx context.Context, project, location, id string, in WorkflowUpdateInput) (workflowsstore.Workflow, error)
	DeleteWorkflow(ctx context.Context, project, location, id string) error

	// Executions.
	ListExecutions(ctx context.Context, project, location, workflowID string, pageSize int, pageToken string) ([]workflowsstore.Execution, string, error)
	GetExecution(ctx context.Context, project, location, workflowID, executionID string) (workflowsstore.Execution, error)
	RunWorkflow(ctx context.Context, project, location, workflowID, argument, callLogLevel string, labels map[string]string) (workflowsstore.Execution, error)
	CancelExecution(ctx context.Context, project, location, workflowID, executionID string) (workflowsstore.Execution, error)
}

// Provider composes the transport-neutral Workflows management core and the
// Workflow Executions core onto ProviderInterface. The management core is
// required; the executions core is required in practice because the UI is wired
// only when the workflows service is enabled.
type Provider struct {
	workflows  *workflowscore.Service
	executions *workflowexecutionscore.Service
}

// NewProvider returns a UI provider over the workflows and workflow-executions
// cores.
func NewProvider(workflows *workflowscore.Service, executions *workflowexecutionscore.Service) *Provider {
	return &Provider{workflows: workflows, executions: executions}
}

var _ ProviderInterface = (*Provider)(nil)

// ListWorkflowsByProject implements ProviderInterface.
func (p *Provider) ListWorkflowsByProject(ctx context.Context, project string) ([]workflowsstore.Workflow, error) {
	return p.workflows.ListWorkflowsByProject(ctx, project)
}

// GetWorkflow implements ProviderInterface.
func (p *Provider) GetWorkflow(ctx context.Context, project, location, id string) (workflowsstore.Workflow, error) {
	return p.workflows.GetWorkflow(ctx, project, location, id)
}

// CreateWorkflow implements ProviderInterface; the create LRO is discarded.
func (p *Provider) CreateWorkflow(ctx context.Context, project, location string, in WorkflowInput) (workflowsstore.Workflow, error) {
	w, _, err := p.workflows.CreateWorkflow(ctx, project, location, workflowscore.CreateInput{
		ID:             in.ID,
		Description:    in.Description,
		Labels:         in.Labels,
		ServiceAccount: in.ServiceAccount,
		SourceContents: in.SourceContents,
		CallLogLevel:   in.CallLogLevel,
		UserEnvVars:    in.UserEnvVars,
	})
	return w, err
}

// UpdateWorkflow implements ProviderInterface; the update LRO is discarded.
func (p *Provider) UpdateWorkflow(ctx context.Context, project, location, id string, in WorkflowUpdateInput) (workflowsstore.Workflow, error) {
	mask := in.UpdateMask
	if mask == "" {
		mask = workflowManagedMask
	}
	w, _, err := p.workflows.UpdateWorkflow(ctx, project, location, workflowscore.UpdateInput{
		ID:             id,
		UpdateMask:     mask,
		Description:    in.Description,
		Labels:         in.Labels,
		UserEnvVars:    in.UserEnvVars,
		ServiceAccount: in.ServiceAccount,
		SourceContents: in.SourceContents,
		CallLogLevel:   in.CallLogLevel,
	})
	return w, err
}

// DeleteWorkflow implements ProviderInterface; the delete LRO is discarded.
func (p *Provider) DeleteWorkflow(ctx context.Context, project, location, id string) error {
	_, err := p.workflows.DeleteWorkflow(ctx, project, location, id)
	return err
}

// ListExecutions implements ProviderInterface, always at full view.
func (p *Provider) ListExecutions(ctx context.Context, project, location, workflowID string, pageSize int, pageToken string) ([]workflowsstore.Execution, string, error) {
	return p.executions.ListExecutions(ctx, project, location, workflowID, workflowexecutionscore.ViewFull, "", "", pageSize, pageToken)
}

// GetExecution implements ProviderInterface, always at full view.
func (p *Provider) GetExecution(ctx context.Context, project, location, workflowID, executionID string) (workflowsstore.Execution, error) {
	return p.executions.GetExecution(ctx, project, location, workflowID, executionID, workflowexecutionscore.ViewFull)
}

// RunWorkflow implements ProviderInterface by creating (and synchronously
// running) an execution.
func (p *Provider) RunWorkflow(ctx context.Context, project, location, workflowID, argument, callLogLevel string, labels map[string]string) (workflowsstore.Execution, error) {
	return p.executions.CreateExecution(ctx, project, location, workflowID, workflowexecutionscore.CreateExecutionInput{
		Argument:     argument,
		CallLogLevel: callLogLevel,
		Labels:       labels,
	})
}

// CancelExecution implements ProviderInterface.
func (p *Provider) CancelExecution(ctx context.Context, project, location, workflowID, executionID string) (workflowsstore.Execution, error) {
	return p.executions.CancelExecution(ctx, project, location, workflowID, executionID)
}
