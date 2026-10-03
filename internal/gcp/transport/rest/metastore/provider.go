package metastore

import (
	"context"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	core "jaiscloud/internal/gcp/service/metastore"
)

// Provider handles the Dataproc Metastore v1 REST control plane. It is a thin
// adapter: every handler resolves the NormalizedRequest params into the core's
// typed API, calls the shared core Service, and encodes the result as
// Discovery-shaped JSON. No business logic lives here.
type Provider struct {
	core        *core.Service
	defaultProj string
}

// NewProvider returns a Dataproc Metastore REST provider over the shared core.
func NewProvider(c *core.Service, defaultProj string) *Provider {
	return &Provider{core: c, defaultProj: defaultProj}
}

// Routes maps "Metastore.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Metastore.CreateService":                 p.CreateService,
		"Metastore.GetService":                    p.GetService,
		"Metastore.ListServices":                  p.ListServices,
		"Metastore.UpdateService":                 p.UpdateService,
		"Metastore.DeleteService":                 p.DeleteService,
		"Metastore.CreateBackup":                  p.CreateBackup,
		"Metastore.GetBackup":                     p.GetBackup,
		"Metastore.ListBackups":                   p.ListBackups,
		"Metastore.DeleteBackup":                  p.DeleteBackup,
		"Metastore.CreateMetadataImport":          p.CreateMetadataImport,
		"Metastore.GetMetadataImport":             p.GetMetadataImport,
		"Metastore.ListMetadataImports":           p.ListMetadataImports,
		"Metastore.UpdateMetadataImport":          p.UpdateMetadataImport,
		"Metastore.GetOperation":                  p.GetOperation,
		"Metastore.ListOperations":                p.ListOperations,
		"Metastore.ExportMetadata":                p.unimplemented("ExportMetadata"),
		"Metastore.RestoreService":                p.unimplemented("RestoreService"),
		"Metastore.QueryMetadata":                 p.unimplemented("QueryMetadata"),
		"Metastore.MoveTableToDatabase":           p.unimplemented("MoveTableToDatabase"),
		"Metastore.AlterMetadataResourceLocation": p.unimplemented("AlterMetadataResourceLocation"),
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

// --- Services ---

func (p *Provider) CreateService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	_, op, err := p.core.CreateService(ctx, project, strParam(nr, "location"), strParam(nr, "serviceId"), rawBodyOf(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project)), nil
}

func (p *Provider) GetService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	svc, err := p.core.GetService(ctx, project, strParam(nr, "location"), strParam(nr, "serviceId"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.ServiceJSON(svc, project)), nil
}

func (p *Provider) ListServices(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	page, next, err := p.core.ListServices(ctx, project, strParam(nr, "location"), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, svc := range page {
		items = append(items, core.ServiceJSON(svc, project))
	}
	out := map[string]any{"services": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) UpdateService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	_, op, err := p.core.UpdateService(ctx, project, strParam(nr, "location"), strParam(nr, "serviceId"), rawBodyOf(nr), strParam(nr, "updateMask"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project)), nil
}

func (p *Provider) DeleteService(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	op, err := p.core.DeleteService(ctx, project, strParam(nr, "location"), strParam(nr, "serviceId"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project)), nil
}

// --- Backups ---

func (p *Provider) CreateBackup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	_, op, err := p.core.CreateBackup(ctx, project, strParam(nr, "location"), strParam(nr, "serviceId"), strParam(nr, "backupId"), rawBodyOf(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project)), nil
}

func (p *Provider) GetBackup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	b, err := p.core.GetBackup(ctx, project, strParam(nr, "location"), strParam(nr, "serviceId"), strParam(nr, "backupId"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.BackupJSON(b, project)), nil
}

func (p *Provider) ListBackups(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	page, next, err := p.core.ListBackups(ctx, project, strParam(nr, "location"), strParam(nr, "serviceId"), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, b := range page {
		items = append(items, core.BackupJSON(b, project))
	}
	out := map[string]any{"backups": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) DeleteBackup(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	op, err := p.core.DeleteBackup(ctx, project, strParam(nr, "location"), strParam(nr, "serviceId"), strParam(nr, "backupId"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project)), nil
}

// --- Metadata imports ---

func (p *Provider) CreateMetadataImport(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	_, op, err := p.core.CreateMetadataImport(ctx, project, strParam(nr, "location"), strParam(nr, "serviceId"), strParam(nr, "metadataImportId"), rawBodyOf(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project)), nil
}

func (p *Provider) GetMetadataImport(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	mi, err := p.core.GetMetadataImport(ctx, project, strParam(nr, "location"), strParam(nr, "serviceId"), strParam(nr, "metadataImportId"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.MetadataImportJSON(mi, project)), nil
}

func (p *Provider) ListMetadataImports(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	page, next, err := p.core.ListMetadataImports(ctx, project, strParam(nr, "location"), strParam(nr, "serviceId"), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, mi := range page {
		items = append(items, core.MetadataImportJSON(mi, project))
	}
	out := map[string]any{"metadataImports": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) UpdateMetadataImport(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	_, op, err := p.core.UpdateMetadataImport(ctx, project, strParam(nr, "location"), strParam(nr, "serviceId"), strParam(nr, "metadataImportId"), rawBodyOf(nr), strParam(nr, "updateMask"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project)), nil
}

// --- Operations ---

func (p *Provider) GetOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	op, err := p.core.GetOperation(ctx, project, strParam(nr, "location"), strParam(nr, "operationId"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project)), nil
}

// ResolveOperation resolves a Dataproc Metastore operation by its location-scoped
// name. handled reports whether Metastore owns the name; a name the store does
// not know — including one belonging to another service that shares the
// locations/{location}/operations/{id} path — returns handled=false so the
// caller falls through. It satisfies the Workflows REST provider's
// cross-service OperationResolver, mirroring the gRPC operations.Resolver.
func (p *Provider) ResolveOperation(ctx context.Context, project, location, opID string) (map[string]any, bool, error) {
	if location == "" || opID == "" {
		return nil, false, nil
	}
	op, err := p.core.GetOperation(ctx, project, location, opID)
	if err != nil {
		if core.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return core.OperationJSON(op, project), true, nil
}

func (p *Provider) ListOperations(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	page, next, err := p.core.ListOperations(ctx, project, strParam(nr, "location"), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, op := range page {
		items = append(items, core.OperationJSON(op, project))
	}
	out := map[string]any{"operations": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

// unimplemented returns a handler that fails loud with Unimplemented for the
// deferred control-plane operations.
func (p *Provider) unimplemented(name string) provider.HandlerFunc {
	return func(_ context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
		return nil, model.NewProviderError("Unimplemented", name+" is not supported by the emulator", 501)
	}
}
