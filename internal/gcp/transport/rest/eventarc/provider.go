// Package eventarc is the REST transport for the Eventarc v1 control plane
// (eventarc.googleapis.com/v1).
//
// Real GCP serves the proto-defined v1 API over both gRPC and REST
// (grpc-gateway transcoding), and this REST surface follows the vendored
// Discovery document. The Codec is the generic adapter.JSONCodec{Service:
// "eventarc"} (path → NormalizedRequest); this Provider holds the routes.
// Neither owns business logic — both delegate to the single core Service shared
// with the gRPC transport (see internal/gcp/service/eventarc).
//
// Resources live under
// /v1/projects/{project}/locations/{location}/triggers[/{id}] and
// .../channels[/{id}], plus read-only .../providers[/{id}]. Create/Update/Delete
// return a done google.longrunning.Operation; Get/List return the resource
// inline. The IAM trio is served per-resource as :getIamPolicy /
// :setIamPolicy / :testIamPermissions.
package eventarc

import (
	"context"

	"jaiscloud/internal/gcp/policy"
	core "jaiscloud/internal/gcp/service/eventarc"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// Provider handles the Eventarc v1 REST control plane. It is a thin adapter:
// every handler resolves the NormalizedRequest params into the core's typed API,
// calls the shared core Service, and encodes the result as Discovery-shaped
// JSON. No business logic lives here.
type Provider struct {
	core        *core.Service
	defaultProj string
}

// NewProvider returns an Eventarc REST provider over the shared core.
func NewProvider(c *core.Service, defaultProj string) *Provider {
	return &Provider{core: c, defaultProj: defaultProj}
}

// Reset wipes the core store.
func (p *Provider) Reset(ctx context.Context) { p.core.Reset(ctx) }

// Routes maps "Eventarc.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	routes := map[string]provider.HandlerFunc{
		"Eventarc.CreateTrigger": p.CreateTrigger,
		"Eventarc.GetTrigger":    p.GetTrigger,
		"Eventarc.ListTriggers":  p.ListTriggers,
		"Eventarc.UpdateTrigger": p.UpdateTrigger,
		"Eventarc.DeleteTrigger": p.DeleteTrigger,

		"Eventarc.CreateChannel": p.CreateChannel,
		"Eventarc.GetChannel":    p.GetChannel,
		"Eventarc.ListChannels":  p.ListChannels,
		"Eventarc.UpdateChannel": p.UpdateChannel,
		"Eventarc.DeleteChannel": p.DeleteChannel,

		"Eventarc.ListProviders": p.ListProviders,
		"Eventarc.GetProvider":   p.GetProvider,

		// The shared google.longrunning operations surface, reached only under
		// the eventarc host (see adapter.operationsHostService). Each completed
		// mutation's done operation is persisted by the core, so get/list/delete
		// address a real record.
		"Eventarc.ListOperations":  p.ListOperations,
		"Eventarc.GetOperation":    p.GetOperation,
		"Eventarc.DeleteOperation": p.DeleteOperation,
		"Eventarc.CancelOperation": p.CancelOperation,

		"Eventarc.TriggerGetIamPolicy":       p.TriggerGetIamPolicy,
		"Eventarc.TriggerSetIamPolicy":       p.TriggerSetIamPolicy,
		"Eventarc.TriggerTestIamPermissions": p.TriggerTestIamPermissions,
		"Eventarc.ChannelGetIamPolicy":       p.ChannelGetIamPolicy,
		"Eventarc.ChannelSetIamPolicy":       p.ChannelSetIamPolicy,
		"Eventarc.ChannelTestIamPermissions": p.ChannelTestIamPermissions,
	}

	// The advanced surface (message buses, enrollments, pipelines, Google API
	// sources, channel connections): identical CRUD shape per family, so the
	// routes are derived from the core's kind table rather than hand-listed.
	for _, k := range core.AdvancedKinds {
		routes["Eventarc.Create"+k.Proto] = p.advancedCreate(k)
		routes["Eventarc.Get"+k.Proto] = p.advancedGet(k)
		routes["Eventarc."+k.ListMethod] = p.advancedList(k)
		if !k.NoUpdate {
			routes["Eventarc.Update"+k.Proto] = p.advancedUpdate(k)
		}
		routes["Eventarc.Delete"+k.Proto] = p.advancedDelete(k)
		routes["Eventarc."+k.Proto+"GetIamPolicy"] = p.advancedGetIam(k)
		routes["Eventarc."+k.Proto+"SetIamPolicy"] = p.advancedSetIam(k)
		routes["Eventarc."+k.Proto+"TestIamPermissions"] = p.advancedTestIam(k)
	}
	routes["Eventarc.ListMessageBusEnrollments"] = p.ListMessageBusEnrollments
	routes["Eventarc.GetGoogleChannelConfig"] = p.GetGoogleChannelConfig
	routes["Eventarc.UpdateGoogleChannelConfig"] = p.UpdateGoogleChannelConfig

	return routes
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

// --- Triggers ---

func (p *Provider) CreateTrigger(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	t, op, err := p.core.CreateTrigger(ctx, project, strParam(nr, "location"), strParam(nr, "triggerId"), rawBodyOf(nr), boolParam(nr, "validateOnly"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project, core.OperationResponse(core.TriggerTypeURL, core.TriggerJSON(project, t)))), nil
}

func (p *Provider) GetTrigger(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	t, err := p.core.GetTrigger(ctx, project, strParam(nr, "location"), core.NameID(strParam(nr, "name")))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.TriggerJSON(project, t)), nil
}

func (p *Provider) ListTriggers(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	pageSize, err := pageSizeParam(nr)
	if err != nil {
		return nil, err
	}
	page, next, err := p.core.ListTriggers(ctx, project, strParam(nr, "location"),
		strParam(nr, "filter"), strParam(nr, "orderBy"),
		pageSize, strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, t := range page {
		items = append(items, core.TriggerJSON(project, t))
	}
	out := map[string]any{"triggers": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) UpdateTrigger(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	body := bodyOf(nr)
	t, op, err := p.core.UpdateTrigger(ctx, project, strParam(nr, "location"), core.NameID(strParam(nr, "name")),
		rawBodyOf(nr), strParam(nr, "updateMask"), core.BodyEtag(body), boolParam(nr, "validateOnly"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project, core.OperationResponse(core.TriggerTypeURL, core.TriggerJSON(project, t)))), nil
}

func (p *Provider) DeleteTrigger(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	t, op, err := p.core.DeleteTrigger(ctx, project, strParam(nr, "location"), core.NameID(strParam(nr, "name")), strParam(nr, "etag"), boolParam(nr, "validateOnly"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project, core.OperationResponse(core.TriggerTypeURL, core.TriggerJSON(project, t)))), nil
}

// --- Channels ---

func (p *Provider) CreateChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	c, op, err := p.core.CreateChannel(ctx, project, strParam(nr, "location"), strParam(nr, "channelId"), rawBodyOf(nr), boolParam(nr, "validateOnly"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project, core.OperationResponse(core.ChannelTypeURL, core.ChannelJSON(project, c)))), nil
}

func (p *Provider) GetChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	c, err := p.core.GetChannel(ctx, project, strParam(nr, "location"), core.NameID(strParam(nr, "name")))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.ChannelJSON(project, c)), nil
}

func (p *Provider) ListChannels(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	// channels.list documents only orderBy (no filter), so a `filter` query
	// parameter is not read here.
	pageSize, err := pageSizeParam(nr)
	if err != nil {
		return nil, err
	}
	page, next, err := p.core.ListChannels(ctx, project, strParam(nr, "location"),
		strParam(nr, "orderBy"), pageSize, strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, c := range page {
		items = append(items, core.ChannelJSON(project, c))
	}
	out := map[string]any{"channels": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) UpdateChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	body := bodyOf(nr)
	c, op, err := p.core.UpdateChannel(ctx, project, strParam(nr, "location"), core.NameID(strParam(nr, "name")),
		rawBodyOf(nr), strParam(nr, "updateMask"), core.BodyEtag(body), boolParam(nr, "validateOnly"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project, core.OperationResponse(core.ChannelTypeURL, core.ChannelJSON(project, c)))), nil
}

func (p *Provider) DeleteChannel(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	c, op, err := p.core.DeleteChannel(ctx, project, strParam(nr, "location"), core.NameID(strParam(nr, "name")), strParam(nr, "etag"), boolParam(nr, "validateOnly"))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.OperationJSON(op, project, core.OperationResponse(core.ChannelTypeURL, core.ChannelJSON(project, c)))), nil
}

// --- Long-running operations (the shared google.longrunning surface) ---

// ListOperations serves GET …/locations/{l}/operations over the operations the
// core persists for each completed control-plane mutation.
func (p *Provider) ListOperations(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	pageSize, err := pageSizeParam(nr)
	if err != nil {
		return nil, err
	}
	page, next, err := p.core.ListOperations(ctx, project, strParam(nr, "location"), pageSize, strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, op := range page {
		items = append(items, core.StoredOperationJSON(op, project))
	}
	out := map[string]any{"operations": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

// GetOperation serves GET …/operations/{id} for an operation the core persisted
// when its mutation completed (an unknown name is NotFound, as real GCP).
func (p *Provider) GetOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, location, id := p.operationTarget(nr)
	op, err := p.core.GetOperation(ctx, project, location, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.StoredOperationJSON(op, project)), nil
}

// operationTarget resolves a location-scoped operation's project, location and
// id from its full resource name, falling back to the request params.
func (p *Provider) operationTarget(nr *model.NormalizedRequest) (project, location, id string) {
	name := strParam(nr, "name")
	if pn, pl, pid, ok := core.SplitOperationName(name); ok {
		return pn, pl, pid
	}
	return p.project(nr), strParam(nr, "location"), core.NameID(name)
}

// DeleteOperation serves DELETE …/operations/{id} (an unknown name is NotFound,
// matching the Workflows surface and real GCP).
func (p *Provider) DeleteOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, location, id := p.operationTarget(nr)
	if err := p.core.DeleteOperation(ctx, project, location, id); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// CancelOperation serves POST …/operations/{id}:cancel. Every Eventarc
// operation is already done, so cancel reports the stored operation (a no-op)
// and answers NotFound for an unknown name.
func (p *Provider) CancelOperation(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project, location, id := p.operationTarget(nr)
	op, err := p.core.CancelOperation(ctx, project, location, id)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.StoredOperationJSON(op, project)), nil
}

// --- Providers (read-only discovery) ---

func (p *Provider) ListProviders(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	location := strParam(nr, "location")
	pageSize, err := pageSizeParam(nr)
	if err != nil {
		return nil, err
	}
	page, next, err := p.core.ListProviders(ctx, project, location,
		strParam(nr, "filter"), strParam(nr, "orderBy"), pageSize, strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, d := range page {
		items = append(items, core.ProviderJSON(project, location, d))
	}
	out := map[string]any{"providers": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

func (p *Provider) GetProvider(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	location := strParam(nr, "location")
	d, err := p.core.GetProvider(ctx, project, location, core.NameID(strParam(nr, "name")))
	if err != nil {
		return nil, err
	}
	return provider.OK(core.ProviderJSON(project, location, d)), nil
}

// --- IAM (triggers / channels) ---

func (p *Provider) TriggerGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	pol, err := p.core.TriggerGetIamPolicy(ctx, project, strParam(nr, "location"), core.NameID(strParam(nr, "name")))
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) TriggerSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	pol, err := p.core.TriggerSetIamPolicy(ctx, project, strParam(nr, "location"), core.NameID(strParam(nr, "name")), bodyOf(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) TriggerTestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	perms, err := p.core.TriggerTestIamPermissions(ctx, project, strParam(nr, "location"), core.NameID(strParam(nr, "name")), policy.Permissions(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"permissions": perms}), nil
}

func (p *Provider) ChannelGetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	pol, err := p.core.ChannelGetIamPolicy(ctx, project, strParam(nr, "location"), core.NameID(strParam(nr, "name")))
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) ChannelSetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	pol, err := p.core.ChannelSetIamPolicy(ctx, project, strParam(nr, "location"), core.NameID(strParam(nr, "name")), bodyOf(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) ChannelTestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	perms, err := p.core.ChannelTestIamPermissions(ctx, project, strParam(nr, "location"), core.NameID(strParam(nr, "name")), policy.Permissions(bodyOf(nr)))
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"permissions": perms}), nil
}

// --- Advanced surface (message buses / enrollments / pipelines / Google API
// sources / channel connections / Google channel config) ---

// advancedCreate creates one record of the given kind and returns the done
// create operation wrapping it.
func (p *Provider) advancedCreate(k core.Kind) provider.HandlerFunc {
	return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		project := p.project(nr)
		rec, op, err := p.core.CreateAdvanced(ctx, project, k, strParam(nr, "location"), strParam(nr, k.IDParam), rawBodyOf(nr), boolParam(nr, "validateOnly"))
		if err != nil {
			return nil, err
		}
		return provider.OK(core.OperationJSON(op, project, core.OperationResponse(k.TypeURL(), core.AdvancedJSON(project, k, rec)))), nil
	}
}

func (p *Provider) advancedGet(k core.Kind) provider.HandlerFunc {
	return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		project := p.project(nr)
		id := core.NameID(strParam(nr, "name"))
		if k.Singleton {
			id = ""
		}
		rec, err := p.core.GetAdvanced(ctx, project, k, strParam(nr, "location"), id)
		if err != nil {
			return nil, err
		}
		return provider.OK(core.AdvancedJSON(project, k, rec)), nil
	}
}

func (p *Provider) advancedList(k core.Kind) provider.HandlerFunc {
	return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		project := p.project(nr)
		// Only the kinds whose list method documents filter/orderBy honor them;
		// channelConnections.list carries only pageSize/pageToken.
		filter, orderBy := "", ""
		if k.HasListParams() {
			filter, orderBy = strParam(nr, "filter"), strParam(nr, "orderBy")
		}
		pageSize, err := pageSizeParam(nr)
		if err != nil {
			return nil, err
		}
		page, next, err := p.core.ListAdvanced(ctx, project, k, strParam(nr, "location"),
			filter, orderBy, pageSize, strParam(nr, "pageToken"))
		if err != nil {
			return nil, err
		}
		items := make([]any, 0, len(page))
		for _, r := range page {
			items = append(items, core.AdvancedJSON(project, k, r))
		}
		out := map[string]any{k.ListField: items}
		if next != "" {
			out["nextPageToken"] = next
		}
		return provider.OK(out), nil
	}
}

func (p *Provider) advancedUpdate(k core.Kind) provider.HandlerFunc {
	return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		project := p.project(nr)
		rec, op, err := p.core.UpdateAdvanced(ctx, project, k, strParam(nr, "location"), core.NameID(strParam(nr, "name")),
			rawBodyOf(nr), strParam(nr, "updateMask"), core.BodyEtag(bodyOf(nr)), boolParam(nr, "validateOnly"))
		if err != nil {
			return nil, err
		}
		return provider.OK(core.OperationJSON(op, project, core.OperationResponse(k.TypeURL(), core.AdvancedJSON(project, k, rec)))), nil
	}
}

func (p *Provider) advancedDelete(k core.Kind) provider.HandlerFunc {
	return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		project := p.project(nr)
		rec, op, err := p.core.DeleteAdvanced(ctx, project, k, strParam(nr, "location"), core.NameID(strParam(nr, "name")), strParam(nr, "etag"), boolParam(nr, "validateOnly"))
		if err != nil {
			return nil, err
		}
		return provider.OK(core.OperationJSON(op, project, core.OperationResponse(k.TypeURL(), core.AdvancedJSON(project, k, rec)))), nil
	}
}

func (p *Provider) advancedGetIam(k core.Kind) provider.HandlerFunc {
	return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		project := p.project(nr)
		pol, err := p.core.AdvancedGetIamPolicy(ctx, project, k, strParam(nr, "location"), core.NameID(strParam(nr, "name")))
		if err != nil {
			return nil, err
		}
		return provider.OK(policy.ToMap(pol)), nil
	}
}

func (p *Provider) advancedSetIam(k core.Kind) provider.HandlerFunc {
	return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		project := p.project(nr)
		pol, err := p.core.AdvancedSetIamPolicy(ctx, project, k, strParam(nr, "location"), core.NameID(strParam(nr, "name")), bodyOf(nr))
		if err != nil {
			return nil, err
		}
		return provider.OK(policy.ToMap(pol)), nil
	}
}

func (p *Provider) advancedTestIam(k core.Kind) provider.HandlerFunc {
	return func(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
		project := p.project(nr)
		perms, err := p.core.AdvancedTestIamPermissions(ctx, project, k, strParam(nr, "location"), core.NameID(strParam(nr, "name")), policy.Permissions(bodyOf(nr)))
		if err != nil {
			return nil, err
		}
		return provider.OK(map[string]any{"permissions": perms}), nil
	}
}

// ListMessageBusEnrollments serves the messageBuses/{bus}:listEnrollments custom
// method: the names of the enrollments attached to the named message bus.
func (p *Provider) ListMessageBusEnrollments(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	pageSize, err := pageSizeParam(nr)
	if err != nil {
		return nil, err
	}
	names, next, err := p.core.ListMessageBusEnrollments(ctx, project, strParam(nr, "location"), core.NameID(strParam(nr, "name")),
		pageSize, strParam(nr, "pageToken"))
	if err != nil {
		return nil, err
	}
	out := map[string]any{"enrollments": names}
	if next != "" {
		out["nextPageToken"] = next
	}
	return provider.OK(out), nil
}

// GetGoogleChannelConfig returns the per-location Google channel config
// singleton (an empty default when it has never been updated).
func (p *Provider) GetGoogleChannelConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	rec, err := p.core.GetAdvanced(ctx, project, core.GoogleChannelConfigKind, strParam(nr, "location"), "")
	if err != nil {
		return nil, err
	}
	return provider.OK(core.AdvancedJSON(project, core.GoogleChannelConfigKind, rec)), nil
}

// UpdateGoogleChannelConfig updates the singleton and returns it inline — the
// real method returns the config, not a long-running operation.
func (p *Provider) UpdateGoogleChannelConfig(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	rec, _, err := p.core.UpdateAdvanced(ctx, project, core.GoogleChannelConfigKind, strParam(nr, "location"), "",
		rawBodyOf(nr), strParam(nr, "updateMask"), core.BodyEtag(bodyOf(nr)), false)
	if err != nil {
		return nil, err
	}
	return provider.OK(core.AdvancedJSON(project, core.GoogleChannelConfigKind, rec)), nil
}
