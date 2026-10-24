package eventarcui

import (
	"context"
	"encoding/json"

	"jaiscloud/internal/gcp/policy"
	eventarccore "jaiscloud/internal/gcp/service/eventarc"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
)

// ProviderInterface is the subset of *eventarc.Service used by the Eventarc UI
// handlers. The core is transport-neutral and addressable directly, so the UI
// reuses its typed API rather than going through a REST adapter. It keeps the
// UI decoupled from the core's concrete type and hides the (always-done)
// long-running operations the core returns from mutations.
type ProviderInterface interface {
	// ListTriggersByProject lists every trigger in a project across all
	// locations, sorted by location then id. It backs the location-optional
	// console list.
	ListTriggersByProject(ctx context.Context, project string) ([]eventarcstore.Trigger, error)

	// Trigger CRUD.
	GetTrigger(ctx context.Context, project, location, id string) (eventarcstore.Trigger, error)
	CreateTrigger(ctx context.Context, project, location string, in TriggerInput) (eventarcstore.Trigger, error)
	UpdateTrigger(ctx context.Context, project, location, id string, in TriggerInput) (eventarcstore.Trigger, error)
	DeleteTrigger(ctx context.Context, project, location, id string) error

	// Trigger IAM.
	TriggerGetIamPolicy(ctx context.Context, project, location, id string) (policy.Policy, error)
	TriggerSetIamPolicy(ctx context.Context, project, location, id string, body map[string]any) (policy.Policy, error)

	// ListChannelsByProject lists every channel in a project across all
	// locations, sorted by location then id. It backs the location-optional
	// console list.
	ListChannelsByProject(ctx context.Context, project string) ([]eventarcstore.Channel, error)

	// Channel CRUD.
	GetChannel(ctx context.Context, project, location, id string) (eventarcstore.Channel, error)
	CreateChannel(ctx context.Context, project, location string, in ChannelInput) (eventarcstore.Channel, error)
	UpdateChannel(ctx context.Context, project, location, id string, in ChannelInput) (eventarcstore.Channel, error)
	DeleteChannel(ctx context.Context, project, location, id string) error

	// Channel IAM.
	ChannelGetIamPolicy(ctx context.Context, project, location, id string) (policy.Policy, error)
	ChannelSetIamPolicy(ctx context.Context, project, location, id string, body map[string]any) (policy.Policy, error)
}

// Provider adapts the transport-neutral Eventarc core onto ProviderInterface,
// discarding the synchronous mutation operations.
type Provider struct {
	svc *eventarccore.Service
}

// NewProvider returns a UI provider over the Eventarc core.
func NewProvider(svc *eventarccore.Service) *Provider { return &Provider{svc: svc} }

var _ ProviderInterface = (*Provider)(nil)

// ListTriggersByProject implements ProviderInterface.
func (p *Provider) ListTriggersByProject(ctx context.Context, project string) ([]eventarcstore.Trigger, error) {
	return p.svc.ListTriggersByProject(ctx, project)
}

// GetTrigger implements ProviderInterface.
func (p *Provider) GetTrigger(ctx context.Context, project, location, id string) (eventarcstore.Trigger, error) {
	return p.svc.GetTrigger(ctx, project, location, id)
}

// CreateTrigger implements ProviderInterface; the create LRO is discarded.
func (p *Provider) CreateTrigger(ctx context.Context, project, location string, in TriggerInput) (eventarcstore.Trigger, error) {
	cfg, err := buildTriggerConfig(in)
	if err != nil {
		return eventarcstore.Trigger{}, err
	}
	t, _, err := p.svc.CreateTrigger(ctx, project, location, in.Name, cfg, false)
	return t, err
}

// UpdateTrigger implements ProviderInterface; the update LRO is discarded. In
// structured mode the form body is merged over the stored body so fields the
// form does not model (destination.path/networkConfig, retryPolicy, transport
// extras) survive; the JSON escape hatch is sent verbatim.
func (p *Provider) UpdateTrigger(ctx context.Context, project, location, id string, in TriggerInput) (eventarcstore.Trigger, error) {
	cfg, err := buildTriggerConfig(in)
	if err != nil {
		return eventarcstore.Trigger{}, err
	}
	if len(in.Config) == 0 {
		if stored, err := p.svc.GetTrigger(ctx, project, location, id); err == nil {
			cfg = mergeTriggerConfig(stored.Config, cfg)
		} else {
			return eventarcstore.Trigger{}, err
		}
	}
	t, _, err := p.svc.UpdateTrigger(ctx, project, location, id, cfg, "", "", false)
	return t, err
}

// DeleteTrigger implements ProviderInterface; the delete LRO is discarded.
func (p *Provider) DeleteTrigger(ctx context.Context, project, location, id string) error {
	_, _, err := p.svc.DeleteTrigger(ctx, project, location, id, "", false)
	return err
}

// TriggerGetIamPolicy implements ProviderInterface.
func (p *Provider) TriggerGetIamPolicy(ctx context.Context, project, location, id string) (policy.Policy, error) {
	return p.svc.TriggerGetIamPolicy(ctx, project, location, id)
}

// TriggerSetIamPolicy implements ProviderInterface.
func (p *Provider) TriggerSetIamPolicy(ctx context.Context, project, location, id string, body map[string]any) (policy.Policy, error) {
	return p.svc.TriggerSetIamPolicy(ctx, project, location, id, body)
}

// ListChannelsByProject implements ProviderInterface.
func (p *Provider) ListChannelsByProject(ctx context.Context, project string) ([]eventarcstore.Channel, error) {
	return p.svc.ListChannelsByProject(ctx, project)
}

// GetChannel implements ProviderInterface.
func (p *Provider) GetChannel(ctx context.Context, project, location, id string) (eventarcstore.Channel, error) {
	return p.svc.GetChannel(ctx, project, location, id)
}

// CreateChannel implements ProviderInterface; the create LRO is discarded.
func (p *Provider) CreateChannel(ctx context.Context, project, location string, in ChannelInput) (eventarcstore.Channel, error) {
	cfg, err := buildChannelConfig(in)
	if err != nil {
		return eventarcstore.Channel{}, err
	}
	c, _, err := p.svc.CreateChannel(ctx, project, location, in.Name, cfg, false)
	return c, err
}

// UpdateChannel implements ProviderInterface; the update LRO is discarded.
func (p *Provider) UpdateChannel(ctx context.Context, project, location, id string, in ChannelInput) (eventarcstore.Channel, error) {
	cfg, err := buildChannelConfig(in)
	if err != nil {
		return eventarcstore.Channel{}, err
	}
	c, _, err := p.svc.UpdateChannel(ctx, project, location, id, cfg, "", "", false)
	return c, err
}

// DeleteChannel implements ProviderInterface; the delete LRO is discarded.
func (p *Provider) DeleteChannel(ctx context.Context, project, location, id string) error {
	_, _, err := p.svc.DeleteChannel(ctx, project, location, id, "", false)
	return err
}

// ChannelGetIamPolicy implements ProviderInterface.
func (p *Provider) ChannelGetIamPolicy(ctx context.Context, project, location, id string) (policy.Policy, error) {
	return p.svc.ChannelGetIamPolicy(ctx, project, location, id)
}

// ChannelSetIamPolicy implements ProviderInterface.
func (p *Provider) ChannelSetIamPolicy(ctx context.Context, project, location, id string, body map[string]any) (policy.Policy, error) {
	return p.svc.ChannelSetIamPolicy(ctx, project, location, id, body)
}

// buildTriggerConfig renders the structured form fields into the Trigger body
// the core expects, or returns the JSON escape hatch verbatim when set.
func buildTriggerConfig(in TriggerInput) (json.RawMessage, error) {
	if len(in.Config) > 0 {
		var probe map[string]any
		if err := json.Unmarshal(in.Config, &probe); err != nil {
			return nil, invalidArgument("config must be a JSON object")
		}
		return in.Config, nil
	}
	body := map[string]any{}
	if in.DestinationType != "" {
		switch in.DestinationType {
		case "cloudFunction", "workflow":
			if in.Destination == "" {
				return nil, invalidArgument("destination is required")
			}
			body["destination"] = map[string]any{in.DestinationType: in.Destination}
		case "cloudRun":
			if in.Destination == "" {
				return nil, invalidArgument("destination is required")
			}
			cloudRun := map[string]any{"service": in.Destination}
			if in.DestinationRegion != "" {
				cloudRun["region"] = in.DestinationRegion
			}
			body["destination"] = map[string]any{"cloudRun": cloudRun}
		default:
			return nil, invalidArgument("unsupported destinationType " + in.DestinationType + "; use the JSON config body")
		}
	}
	if in.ServiceAccount != "" {
		body["serviceAccount"] = in.ServiceAccount
	}
	if in.Channel != "" {
		body["channel"] = in.Channel
	}
	if in.EventDataContentType != "" {
		body["eventDataContentType"] = in.EventDataContentType
	}
	if in.TransportPubsubTopic != "" {
		body["transport"] = map[string]any{"pubsub": map[string]any{"topic": in.TransportPubsubTopic}}
	}
	if len(in.EventFilters) > 0 {
		filters := make([]any, 0, len(in.EventFilters))
		for _, f := range in.EventFilters {
			fm := map[string]any{"attribute": f.Attribute, "value": f.Value}
			if f.Operator != "" {
				fm["operator"] = f.Operator
			}
			filters = append(filters, fm)
		}
		body["eventFilters"] = filters
	}
	if len(in.Labels) > 0 {
		labels := make(map[string]any, len(in.Labels))
		for k, v := range in.Labels {
			labels[k] = v
		}
		body["labels"] = labels
	}
	return json.Marshal(body)
}

// buildChannelConfig renders the structured channel form fields into the
// Channel body, or returns the JSON escape hatch verbatim when set.
func buildChannelConfig(in ChannelInput) (json.RawMessage, error) {
	if len(in.Config) > 0 {
		var probe map[string]any
		if err := json.Unmarshal(in.Config, &probe); err != nil {
			return nil, invalidArgument("config must be a JSON object")
		}
		return in.Config, nil
	}
	body := map[string]any{}
	if in.Provider != "" {
		body["provider"] = in.Provider
	}
	if in.CryptoKeyName != "" {
		body["cryptoKeyName"] = in.CryptoKeyName
	}
	if len(in.Labels) > 0 {
		labels := make(map[string]any, len(in.Labels))
		for k, v := range in.Labels {
			labels[k] = v
		}
		body["labels"] = labels
	}
	return json.Marshal(body)
}

// mergeTriggerConfig overlays a structured-update body onto the stored trigger
// body so unmodeled fields survive. The destination is replaced wholesale
// (Eventarc validates exactly one destination key), except that a same-kind
// nested object keeps the stored sub-fields the form does not model (e.g.
// cloudRun.path/networkConfig).
func mergeTriggerConfig(storedCfg, overlayCfg json.RawMessage) json.RawMessage {
	var stored, overlay map[string]any
	_ = json.Unmarshal(storedCfg, &stored)
	_ = json.Unmarshal(overlayCfg, &overlay)
	if len(stored) == 0 || len(overlay) == 0 {
		return overlayCfg
	}
	if od, ok := overlay["destination"].(map[string]any); ok {
		if sd, ok := stored["destination"].(map[string]any); ok {
			for kind, ov := range od {
				ovm, ok := ov.(map[string]any)
				if !ok {
					continue
				}
				if svm, ok := sd[kind].(map[string]any); ok {
					od[kind] = deepMerge(svm, ovm)
				}
			}
		}
	}
	merged := deepMerge(stored, overlay)
	// Never let the merge produce two destination kinds.
	if dest, ok := overlay["destination"]; ok {
		merged["destination"] = dest
	}
	if b, err := json.Marshal(merged); err == nil {
		return b
	}
	return overlayCfg
}

// deepMerge recursively merges src into dst, with src scalars/arrays winning.
func deepMerge(dst, src map[string]any) map[string]any {
	out := make(map[string]any, len(dst)+len(src))
	for k, v := range dst {
		out[k] = v
	}
	for k, v := range src {
		if sm, ok := v.(map[string]any); ok {
			if dm, ok := out[k].(map[string]any); ok {
				out[k] = deepMerge(dm, sm)
				continue
			}
		}
		out[k] = v
	}
	return out
}
