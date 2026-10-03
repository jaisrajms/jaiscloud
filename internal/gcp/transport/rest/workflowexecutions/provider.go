package workflowexecutions

import (
	"context"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	core "jaiscloud/internal/gcp/service/workflowexecutions"
)

// Provider handles the Cloud Workflow Executions v1 REST data plane. It is a
// thin adapter: every handler resolves the NormalizedRequest params into the
// core's typed API, calls the shared core Service, and encodes the result as
// Discovery-shaped JSON. No business logic lives here.
type Provider struct {
	core        *core.Service
	defaultProj string
}

// NewProvider returns a Workflow Executions REST provider over the shared core.
func NewProvider(c *core.Service, defaultProj string) *Provider {
	return &Provider{core: c, defaultProj: defaultProj}
}

// Routes maps "WorkflowExecution.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"WorkflowExecution.CreateExecution": p.CreateExecution,
		"WorkflowExecution.GetExecution":    p.GetExecution,
		"WorkflowExecution.ListExecutions":  p.ListExecutions,
		"WorkflowExecution.CancelExecution": p.CancelExecution,
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

func (p *Provider) CreateExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	body := bodyOf(nr)
	e, err := p.core.CreateExecution(ctx, project, strParam(nr, "location"), strParam(nr, "workflowId"), core.CreateExecutionInput{
		Argument:     strFrom(body["argument"]),
		CallLogLevel: strFrom(body["callLogLevel"]),
		Labels:       stringMapFrom(body["labels"]),
	})
	if err != nil {
		return nil, err
	}
	return provider.OK(executionToJSON(e, project)), nil
}

func (p *Provider) GetExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	e, err := p.core.GetExecution(ctx, project, strParam(nr, "location"), strParam(nr, "workflowId"), strParam(nr, "executionId"),
		viewFromParam(strParam(nr, "view"), core.ViewFull))
	if err != nil {
		return nil, err
	}
	return provider.OK(executionToJSON(e, project)), nil
}

func (p *Provider) ListExecutions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	page, next, err := p.core.ListExecutions(ctx, project, strParam(nr, "location"), strParam(nr, "workflowId"),
		viewFromParam(strParam(nr, "view"), core.ViewBasic), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, e := range page {
		items = append(items, executionToJSON(e, project))
	}
	out := map[string]any{"executions": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) CancelExecution(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	e, err := p.core.CancelExecution(ctx, project, strParam(nr, "location"), strParam(nr, "workflowId"), strParam(nr, "executionId"))
	if err != nil {
		return nil, err
	}
	return provider.OK(executionToJSON(e, project)), nil
}
