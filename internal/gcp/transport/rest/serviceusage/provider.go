package serviceusage

import (
	"context"

	"jaiscloud/internal/gcp/paging"
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
		"ServiceUsage.ServicesBatchGet":    p.BatchGetServices,
		"ServiceUsage.ServicesBatchEnable": p.BatchEnableServices,
		"ServiceUsage.ServicesEnable":      p.EnableService,
		"ServiceUsage.ServicesDisable":     p.DisableService,
		// generateServiceIdentity is a v1beta1-only method (the v1 Discovery
		// declares no equivalent; a v1 call is a 404 on real GCP) and
		// operations.get is its poll path on the same v1beta1 base URL.
		"ServiceUsage.ServicesGenerateServiceIdentity": p.GenerateServiceIdentity,
		"ServiceUsage.OperationsGet":                   p.GetOperation,
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
	// Query binding errors (pageSize) precede semantic validation (filter), as
	// the real API parses the request binding before the handler runs.
	pageSize, err := paging.ParseSize(nr.Params["pageSize"])
	if err != nil {
		return nil, err
	}
	filter, err := core.ParseFilter(strParam(nr, "filter"))
	if err != nil {
		return nil, err
	}
	page, next, err := p.core.ListAPIs(ctx, p.project(nr), filter, pageSize, strParam(nr, "pageToken"))
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

func (p *Provider) BatchGetServices(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	apis, err := p.core.BatchGetAPIs(ctx, p.project(nr), stringSlice(nr.Params["names"]))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(apis))
	for _, a := range apis {
		items = append(items, serviceToJSON(a))
	}
	return provider.OK(map[string]any{"services": items}), nil
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

// GenerateServiceIdentity serves the v1beta1 services.generateServiceIdentity
// method: POST /v1beta1/projects/{project}/services/{service}:generateServiceIdentity.
// The operation carries the service agent's email and uniqueId, which the
// google-beta provider reads before granting it KMS/Pub/Sub roles.
func (p *Provider) GenerateServiceIdentity(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.GenerateServiceIdentity(ctx, p.project(nr), strParam(nr, "service"))
	if err != nil {
		return nil, err
	}
	return provider.OK(operationToJSON(op, operationResponse(op))), nil
}

// GetOperation serves GET /v1beta1/operations/{id}, the v1beta1 poll path for
// the operations generateServiceIdentity returns. The default synchronous mode
// returns a done operation so the provider's waiter never polls; this keeps the
// opt-in async mode (and any client that polls anyway) resolvable.
func (p *Provider) GetOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	op, err := p.core.GetOperation(ctx, p.project(nr), strParam(nr, "name"))
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
