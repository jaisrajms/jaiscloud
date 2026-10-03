package dataproc

import (
	"context"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	core "jaiscloud/internal/gcp/service/dataproc"
)

// Provider handles the Cloud Dataproc v1 REST data plane. It is a thin adapter:
// every handler resolves the NormalizedRequest params into the core's typed API,
// calls the shared core Service, and encodes the result as Discovery-shaped
// JSON. No business logic lives here.
type Provider struct {
	core        *core.Service
	defaultProj string
}

// NewProvider returns a Dataproc REST provider over the shared core.
func NewProvider(c *core.Service, defaultProj string) *Provider {
	return &Provider{core: c, defaultProj: defaultProj}
}

// Routes maps "Dataproc.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Dataproc.CreateCluster":        p.CreateCluster,
		"Dataproc.GetCluster":           p.GetCluster,
		"Dataproc.ListClusters":         p.ListClusters,
		"Dataproc.UpdateCluster":        p.UpdateCluster,
		"Dataproc.DeleteCluster":        p.DeleteCluster,
		"Dataproc.StartCluster":         p.StartCluster,
		"Dataproc.StopCluster":          p.StopCluster,
		"Dataproc.DiagnoseCluster":      p.DiagnoseCluster,
		"Dataproc.SubmitJob":            p.SubmitJob,
		"Dataproc.SubmitJobAsOperation": p.SubmitJobAsOperation,
		"Dataproc.GetJob":               p.GetJob,
		"Dataproc.ListJobs":             p.ListJobs,
		"Dataproc.DeleteJob":            p.DeleteJob,
		"Dataproc.CancelJob":            p.CancelJob,
		"Dataproc.GetOperation":         p.GetOperation,

		"Dataproc.CreateWorkflowTemplate": p.CreateWorkflowTemplate,
		"Dataproc.GetWorkflowTemplate":    p.GetWorkflowTemplate,
		"Dataproc.ListWorkflowTemplates":  p.ListWorkflowTemplates,
		"Dataproc.UpdateWorkflowTemplate": p.UpdateWorkflowTemplate,
		"Dataproc.DeleteWorkflowTemplate": p.DeleteWorkflowTemplate,

		"Dataproc.InstantiateWorkflowTemplate":       p.InstantiateWorkflowTemplate,
		"Dataproc.InstantiateInlineWorkflowTemplate": p.InstantiateInlineWorkflowTemplate,
	}
}

// project resolves the owning project: the path project, else the request's
// account (project) scope, else the configured default.
func (p *Provider) project(nr *model.NormalizedRequest) string {
	if s := strParam(nr, "project"); s != "" {
		return s
	}
	if nr.AccountID != "" {
		return nr.AccountID
	}
	return p.defaultProj
}

// --- Clusters ---

func (p *Provider) CreateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	body := bodyOf(nr)
	_, op, err := p.core.CreateCluster(ctx, project, strParam(nr, "region"), bodyString(body, "clusterName"), core.ClusterInputFromMap(body))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op)), nil
}

func (p *Provider) GetCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	c, err := p.core.GetCluster(ctx, p.project(nr), strParam(nr, "region"), strParam(nr, "clusterName"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.ClusterJSON(c)), nil
}

func (p *Provider) ListClusters(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	page, next, err := p.core.ListClusters(ctx, p.project(nr), strParam(nr, "region"), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, c := range page {
		items = append(items, core.ClusterJSON(c))
	}
	out := map[string]any{"clusters": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) UpdateCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	_, op, err := p.core.UpdateCluster(ctx, project, strParam(nr, "region"), strParam(nr, "clusterName"),
		core.ClusterInputFromMap(bodyOf(nr)), core.ParseMask(strParam(nr, "updateMask")))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op)), nil
}

func (p *Provider) DeleteCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.DeleteCluster(ctx, p.project(nr), strParam(nr, "region"), strParam(nr, "clusterName"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op)), nil
}

func (p *Provider) StartCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	_, op, err := p.core.StartCluster(ctx, p.project(nr), strParam(nr, "region"), strParam(nr, "clusterName"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op)), nil
}

func (p *Provider) StopCluster(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	_, op, err := p.core.StopCluster(ctx, p.project(nr), strParam(nr, "region"), strParam(nr, "clusterName"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op)), nil
}

// DiagnoseCluster is an unimplemented stub: it fails loud rather than silently
// succeeding, so SDK callers observe a real UNIMPLEMENTED error.
func (p *Provider) DiagnoseCluster(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return nil, p.core.DiagnoseCluster()
}

// --- Jobs ---

func (p *Provider) SubmitJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	jobBody := jobBodyOf(nr)
	if jobBody == nil {
		return nil, model.NewProviderError("InvalidArgument", "missing job", 400)
	}
	j, err := p.core.SubmitJob(ctx, p.project(nr), strParam(nr, "region"), core.JobInputFromMap(jobBody))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.JobJSON(j)), nil
}

func (p *Provider) SubmitJobAsOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	jobBody := jobBodyOf(nr)
	if jobBody == nil {
		return nil, model.NewProviderError("InvalidArgument", "missing job", 400)
	}
	op, err := p.core.SubmitJobAsOperation(ctx, p.project(nr), strParam(nr, "region"), core.JobInputFromMap(jobBody))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op)), nil
}

// jobBodyOf extracts the nested "job" object from a SubmitJobRequest body.
func jobBodyOf(nr *model.NormalizedRequest) map[string]any {
	if m, ok := bodyOf(nr)["job"].(map[string]any); ok {
		return m
	}
	return nil
}

func (p *Provider) GetJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	j, err := p.core.GetJob(ctx, p.project(nr), strParam(nr, "region"), strParam(nr, "jobId"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.JobJSON(j)), nil
}

func (p *Provider) ListJobs(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	matcher, err := core.ParseJobStateMatcher(strParam(nr, "jobStateMatcher"))
	if err != nil {
		return nil, err
	}
	page, next, err := p.core.ListJobs(ctx, p.project(nr), strParam(nr, "region"),
		strParam(nr, "clusterName"), strParam(nr, "filter"), matcher,
		intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, j := range page {
		items = append(items, core.JobJSON(j))
	}
	out := map[string]any{"jobs": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) DeleteJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.DeleteJob(ctx, p.project(nr), strParam(nr, "region"), strParam(nr, "jobId")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) CancelJob(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	j, err := p.core.CancelJob(ctx, p.project(nr), strParam(nr, "region"), strParam(nr, "jobId"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.JobJSON(j)), nil
}

// --- Operations ---

func (p *Provider) GetOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.GetOperation(ctx, p.project(nr), strParam(nr, "region"), strParam(nr, "operationId"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op)), nil
}

// --- Workflow templates ---

// CreateWorkflowTemplate stores a workflow template. The Discovery request body
// is the WorkflowTemplate itself.
func (p *Provider) CreateWorkflowTemplate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	t, err := p.core.CreateWorkflowTemplate(ctx, p.project(nr), strParam(nr, "region"),
		core.WorkflowTemplateInput{ID: bodyString(body, "id"), Definition: body})
	if err != nil {
		return nil, err
	}
	return provider.OK(core.WorkflowTemplateJSON(t)), nil
}

// GetWorkflowTemplate returns one workflow template, optionally a specific
// version.
func (p *Provider) GetWorkflowTemplate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	t, err := p.core.GetWorkflowTemplate(ctx, p.project(nr), strParam(nr, "region"),
		strParam(nr, "workflowTemplateId"), int32(intFrom(nr.Params["version"])))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.WorkflowTemplateJSON(t)), nil
}

// ListWorkflowTemplates returns a page of the region's workflow templates.
func (p *Provider) ListWorkflowTemplates(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	page, next, err := p.core.ListWorkflowTemplates(ctx, p.project(nr), strParam(nr, "region"),
		intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, t := range page {
		items = append(items, core.WorkflowTemplateJSON(t))
	}
	out := map[string]any{"templates": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

// UpdateWorkflowTemplate replaces a workflow template. The Discovery request
// body is the WorkflowTemplate itself (with the version to match).
func (p *Provider) UpdateWorkflowTemplate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	id := strParam(nr, "workflowTemplateId")
	if id == "" {
		id = bodyString(body, "id")
	}
	t, err := p.core.UpdateWorkflowTemplate(ctx, p.project(nr), strParam(nr, "region"),
		core.WorkflowTemplateInput{ID: id, Version: int32(intFrom(body["version"])), Definition: body})
	if err != nil {
		return nil, err
	}
	return provider.OK(core.WorkflowTemplateJSON(t)), nil
}

// DeleteWorkflowTemplate removes a workflow template (all versions, or a
// specific version).
func (p *Provider) DeleteWorkflowTemplate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.DeleteWorkflowTemplate(ctx, p.project(nr), strParam(nr, "region"),
		strParam(nr, "workflowTemplateId"), int32(intFrom(nr.Params["version"]))); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// InstantiateWorkflowTemplate runs a stored template. The Discovery request body
// is an InstantiateWorkflowTemplateRequest ({version, parameters, requestId}).
func (p *Provider) InstantiateWorkflowTemplate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	op, err := p.core.InstantiateWorkflowTemplate(ctx, p.project(nr), strParam(nr, "region"),
		strParam(nr, "workflowTemplateId"), int32(intFrom(body["version"])), stringMap(body["parameters"]))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op)), nil
}

// InstantiateInlineWorkflowTemplate runs an inline template. The Discovery
// request body is the WorkflowTemplate itself.
func (p *Provider) InstantiateInlineWorkflowTemplate(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.InstantiateInlineWorkflowTemplate(ctx, p.project(nr), strParam(nr, "region"), bodyOf(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op)), nil
}
