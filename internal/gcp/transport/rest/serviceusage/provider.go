package serviceusage

import (
	"context"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	core "jaiscloud/internal/gcp/service/serviceusage"
)

// Provider handles the Service Usage v1 REST data plane. It is a thin adapter:
// every handler resolves the NormalizedRequest params into the core's typed API,
// calls the shared core Service, and encodes the result as Discovery-shaped
// JSON. No business logic lives here.
type Provider struct {
	core        *core.Service
	defaultProj string
}

// NewProvider returns a Service Usage REST provider over the shared core.
func NewProvider(c *core.Service, defaultProj string) *Provider {
	return &Provider{core: c, defaultProj: defaultProj}
}

// Routes maps "ServiceUsage.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"ServiceUsage.ServicesList":        p.ListServices,
		"ServiceUsage.ServicesGet":         p.GetService,
		"ServiceUsage.ServicesBatchEnable": p.BatchEnableServices,
		"ServiceUsage.ServicesEnable":      p.EnableService,
		"ServiceUsage.ServicesDisable":     p.DisableService,
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

func (p *Provider) ListServices(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	filter, err := core.ParseFilter(strParam(nr, "filter"))
	if err != nil {
		return nil, err
	}
	page, next, err := p.core.ListAPIs(ctx, p.project(nr), filter, intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, a := range page {
		items = append(items, serviceToJSON(a))
	}
	resp := map[string]any{"services": items}
	if next != "" {
		resp["nextPageToken"] = next
	}
	return provider.OK(resp), nil
}

func (p *Provider) GetService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	api, err := p.core.GetAPI(ctx, p.project(nr), strParam(nr, "service"))
	if err != nil {
		return nil, err
	}
	return provider.OK(serviceToJSON(api)), nil
}

func (p *Provider) EnableService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	_, op, err := p.core.EnableAPI(ctx, p.project(nr), strParam(nr, "service"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op, operationResponse(op))), nil
}

func (p *Provider) DisableService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	_, op, err := p.core.DisableAPI(ctx, p.project(nr), strParam(nr, "service"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op, operationResponse(op))), nil
}

func (p *Provider) BatchEnableServices(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	_, op, err := p.core.BatchEnableAPIs(ctx, p.project(nr), stringSlice(bodyOf(nr)["serviceIds"]))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op, operationResponse(op))), nil
}

// ResolveOperation resolves a top-level google.longrunning operation name
// (operations/{id}) owned by Service Usage. It is consulted by the Functions
// REST provider, which owns the /v1/operations/{id} route the two services
// share. It returns handled=false for names outside the top-level shape and for
// ids that are not Service Usage operations, so a genuine Functions unknown id
// still 404s through that provider.
func (p *Provider) ResolveOperation(ctx context.Context, project, name string) (map[string]any, bool, error) {
	if !core.IsTopLevelOperationName(name) {
		return nil, false, nil
	}
	op, err := p.core.GetOperation(ctx, project, name)
	if err != nil {
		if core.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, true, err
	}
	return operationToJSON(op, operationResponse(op)), true, nil
}
