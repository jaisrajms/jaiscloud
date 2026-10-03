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
	return map[string]provider.HandlerFunc{
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

		"Eventarc.TriggerGetIamPolicy":       p.TriggerGetIamPolicy,
		"Eventarc.TriggerSetIamPolicy":       p.TriggerSetIamPolicy,
		"Eventarc.TriggerTestIamPermissions": p.TriggerTestIamPermissions,
		"Eventarc.ChannelGetIamPolicy":       p.ChannelGetIamPolicy,
		"Eventarc.ChannelSetIamPolicy":       p.ChannelSetIamPolicy,
		"Eventarc.ChannelTestIamPermissions": p.ChannelTestIamPermissions,
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
	page, next, err := p.core.ListTriggers(ctx, project, strParam(nr, "location"), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
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
	page, next, err := p.core.ListChannels(ctx, project, strParam(nr, "location"), intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
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

// --- Providers (read-only discovery) ---

func (p *Provider) ListProviders(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	project := p.project(nr)
	location := strParam(nr, "location")
	page, next, err := p.core.ListProviders(ctx, project, location, intFrom(nr.Params["pageSize"]), strParam(nr, "pageToken"))
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
