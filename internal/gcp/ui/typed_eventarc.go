package ui

import (
	"context"
	"encoding/json"

	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/gcp/resource"
	eventarccore "jaiscloud/internal/gcp/service/eventarc"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	eventarcui "jaiscloud/internal/gcp/ui/eventarc"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// typedEventarc serves the Eventarc console through the plugin chain when a plugin
// covers the key (see typed.go).
type typedEventarc struct {
	eventarcui.ProviderInterface
	c typedCaller
}

func routeEventarc(c typedCaller, p eventarcui.ProviderInterface) eventarcui.ProviderInterface {
	return typedEventarc{ProviderInterface: p, c: c}
}

// derivedFields are rendered on read and not part of the stored body.
var triggerDerived = map[string]bool{"name": true, "uid": true, "etag": true, "createTime": true, "updateTime": true, "labels": true}
var channelDerived = map[string]bool{"name": true, "uid": true, "etag": true, "createTime": true, "updateTime": true, "labels": true,
	"activationToken": true, "pubsubTopic": true, "state": true}

func configOf(m map[string]any, derived map[string]bool) json.RawMessage {
	cfg := map[string]any{}
	for k, v := range m {
		if !derived[k] {
			cfg[k] = v
		}
	}
	b, _ := json.Marshal(cfg)
	return b
}

func decodeTrigger(m map[string]any) eventarcstore.Trigger {
	parts := nameParts(strField(m, "name"))
	return eventarcstore.Trigger{
		ProjectID: parts["projects"], Location: parts["locations"], Name: parts["triggers"],
		Config: configOf(m, triggerDerived), Labels: strMapField(m, "labels"),
		UID: strField(m, "uid"), Etag: strField(m, "etag"),
		CreateTime: timeField(m, "createTime"), UpdateTime: timeField(m, "updateTime"),
	}
}

func decodeChannel(m map[string]any) eventarcstore.Channel {
	parts := nameParts(strField(m, "name"))
	return eventarcstore.Channel{
		ProjectID: parts["projects"], Location: parts["locations"], Name: parts["channels"],
		Config: configOf(m, channelDerived), Labels: strMapField(m, "labels"),
		UID: strField(m, "uid"), Etag: strField(m, "etag"), ActivationToken: strField(m, "activationToken"),
		CreateTime: timeField(m, "createTime"), UpdateTime: timeField(m, "updateTime"),
	}
}

func bodyOfConfig(cfg json.RawMessage) map[string]any {
	body := map[string]any{}
	_ = json.Unmarshal(cfg, &body)
	return body
}

func triggerName(project, location, id string) string {
	return resource.ResourceID(project)("eventarc-trigger", location+"/"+id)
}

func channelName(project, location, id string) string {
	return resource.ResourceID(project)("eventarc-channel", location+"/"+id)
}

// ---- triggers ----------------------------------------------------------------

func (p typedEventarc) ListTriggersByProject(ctx context.Context, project string) ([]eventarcstore.Trigger, error) {
	all := func(ctx context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
		ts, err := p.ProviderInterface.ListTriggersByProject(ctx, project)
		if err != nil {
			return nil, err
		}
		out := make([]any, 0, len(ts))
		for _, t := range ts {
			out = append(out, eventarccore.TriggerJSON(project, t))
		}
		return provider.OK(map[string]any{"triggers": out}), nil
	}
	data, covered, err := p.c.do(ctx, "Eventarc.ListTriggers", project, "", map[string]any{"project": project, "location": "-"}, all)
	if !covered {
		return p.ProviderInterface.ListTriggersByProject(ctx, project)
	}
	if err != nil {
		return nil, err
	}
	var out []eventarcstore.Trigger
	for _, m := range items(data, "triggers") {
		out = append(out, decodeTrigger(m))
	}
	return out, nil
}

func (p typedEventarc) GetTrigger(ctx context.Context, project, location, id string) (eventarcstore.Trigger, error) {
	data, covered, err := p.c.do(ctx, "Eventarc.GetTrigger", project, location,
		map[string]any{"project": project, "location": location, "name": triggerName(project, location, id)}, nil)
	if !covered {
		return p.ProviderInterface.GetTrigger(ctx, project, location, id)
	}
	if err != nil {
		return eventarcstore.Trigger{}, err
	}
	return decodeTrigger(data), nil
}

func operationResponse(m map[string]any) map[string]any {
	if r, ok := m["response"].(map[string]any); ok {
		return r
	}
	return m
}

func (p typedEventarc) CreateTrigger(ctx context.Context, project, location string, in eventarcui.TriggerInput) (eventarcstore.Trigger, error) {
	if !p.c.chain.Covers("Eventarc.CreateTrigger") {
		return p.ProviderInterface.CreateTrigger(ctx, project, location, in)
	}
	cfg, err := eventarcui.BuildTriggerConfig(in)
	if err != nil {
		return eventarcstore.Trigger{}, err
	}
	data, covered, err := p.c.do(ctx, "Eventarc.CreateTrigger", project, location,
		map[string]any{"project": project, "location": location, "triggerId": in.Name, "body": bodyOfConfig(cfg)}, nil)
	if !covered {
		return p.ProviderInterface.CreateTrigger(ctx, project, location, in)
	}
	if err != nil {
		return eventarcstore.Trigger{}, err
	}
	return decodeTrigger(operationResponse(data)), nil
}

func (p typedEventarc) UpdateTrigger(ctx context.Context, project, location, id string, in eventarcui.TriggerInput) (eventarcstore.Trigger, error) {
	if !p.c.chain.Covers("Eventarc.UpdateTrigger") {
		return p.ProviderInterface.UpdateTrigger(ctx, project, location, id, in)
	}
	cfg, err := eventarcui.BuildTriggerConfig(in)
	if err != nil {
		return eventarcstore.Trigger{}, err
	}
	if len(in.Config) == 0 {
		stored, err := p.GetTrigger(ctx, project, location, id)
		if err != nil {
			return eventarcstore.Trigger{}, err
		}
		cfg = eventarcui.MergeTriggerConfig(stored.Config, cfg)
	}
	data, covered, err := p.c.do(ctx, "Eventarc.UpdateTrigger", project, location,
		map[string]any{"project": project, "location": location, "name": triggerName(project, location, id), "body": bodyOfConfig(cfg)}, nil)
	if !covered {
		return p.ProviderInterface.UpdateTrigger(ctx, project, location, id, in)
	}
	if err != nil {
		return eventarcstore.Trigger{}, err
	}
	return decodeTrigger(operationResponse(data)), nil
}

func (p typedEventarc) DeleteTrigger(ctx context.Context, project, location, id string) error {
	_, covered, err := p.c.do(ctx, "Eventarc.DeleteTrigger", project, location,
		map[string]any{"project": project, "location": location, "name": triggerName(project, location, id)}, nil)
	if !covered {
		return p.ProviderInterface.DeleteTrigger(ctx, project, location, id)
	}
	return err
}

func (p typedEventarc) TriggerGetIamPolicy(ctx context.Context, project, location, id string) (policy.Policy, error) {
	data, covered, err := p.c.do(ctx, "Eventarc.TriggerGetIamPolicy", project, location,
		map[string]any{"project": project, "location": location, "name": triggerName(project, location, id)}, nil)
	if !covered {
		return p.ProviderInterface.TriggerGetIamPolicy(ctx, project, location, id)
	}
	if err != nil {
		return policy.Policy{}, err
	}
	return decodePolicy(data)
}

func (p typedEventarc) TriggerSetIamPolicy(ctx context.Context, project, location, id string, body map[string]any) (policy.Policy, error) {
	data, covered, err := p.c.do(ctx, "Eventarc.TriggerSetIamPolicy", project, location,
		map[string]any{"project": project, "location": location, "name": triggerName(project, location, id), "body": body}, nil)
	if !covered {
		return p.ProviderInterface.TriggerSetIamPolicy(ctx, project, location, id, body)
	}
	if err != nil {
		return policy.Policy{}, err
	}
	return decodePolicy(data)
}

// ---- channels ----------------------------------------------------------------

func (p typedEventarc) ListChannelsByProject(ctx context.Context, project string) ([]eventarcstore.Channel, error) {
	all := func(ctx context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
		cs, err := p.ProviderInterface.ListChannelsByProject(ctx, project)
		if err != nil {
			return nil, err
		}
		out := make([]any, 0, len(cs))
		for _, c := range cs {
			out = append(out, eventarccore.ChannelJSON(project, c))
		}
		return provider.OK(map[string]any{"channels": out}), nil
	}
	data, covered, err := p.c.do(ctx, "Eventarc.ListChannels", project, "", map[string]any{"project": project, "location": "-"}, all)
	if !covered {
		return p.ProviderInterface.ListChannelsByProject(ctx, project)
	}
	if err != nil {
		return nil, err
	}
	var out []eventarcstore.Channel
	for _, m := range items(data, "channels") {
		out = append(out, decodeChannel(m))
	}
	return out, nil
}

func (p typedEventarc) GetChannel(ctx context.Context, project, location, id string) (eventarcstore.Channel, error) {
	data, covered, err := p.c.do(ctx, "Eventarc.GetChannel", project, location,
		map[string]any{"project": project, "location": location, "name": channelName(project, location, id)}, nil)
	if !covered {
		return p.ProviderInterface.GetChannel(ctx, project, location, id)
	}
	if err != nil {
		return eventarcstore.Channel{}, err
	}
	return decodeChannel(data), nil
}

func (p typedEventarc) CreateChannel(ctx context.Context, project, location string, in eventarcui.ChannelInput) (eventarcstore.Channel, error) {
	if !p.c.chain.Covers("Eventarc.CreateChannel") {
		return p.ProviderInterface.CreateChannel(ctx, project, location, in)
	}
	cfg, err := eventarcui.BuildChannelConfig(in)
	if err != nil {
		return eventarcstore.Channel{}, err
	}
	data, covered, err := p.c.do(ctx, "Eventarc.CreateChannel", project, location,
		map[string]any{"project": project, "location": location, "channelId": in.Name, "body": bodyOfConfig(cfg)}, nil)
	if !covered {
		return p.ProviderInterface.CreateChannel(ctx, project, location, in)
	}
	if err != nil {
		return eventarcstore.Channel{}, err
	}
	return decodeChannel(operationResponse(data)), nil
}

func (p typedEventarc) UpdateChannel(ctx context.Context, project, location, id string, in eventarcui.ChannelInput) (eventarcstore.Channel, error) {
	if !p.c.chain.Covers("Eventarc.UpdateChannel") {
		return p.ProviderInterface.UpdateChannel(ctx, project, location, id, in)
	}
	cfg, err := eventarcui.BuildChannelConfig(in)
	if err != nil {
		return eventarcstore.Channel{}, err
	}
	data, covered, err := p.c.do(ctx, "Eventarc.UpdateChannel", project, location,
		map[string]any{"project": project, "location": location, "name": channelName(project, location, id), "body": bodyOfConfig(cfg)}, nil)
	if !covered {
		return p.ProviderInterface.UpdateChannel(ctx, project, location, id, in)
	}
	if err != nil {
		return eventarcstore.Channel{}, err
	}
	return decodeChannel(operationResponse(data)), nil
}

func (p typedEventarc) DeleteChannel(ctx context.Context, project, location, id string) error {
	_, covered, err := p.c.do(ctx, "Eventarc.DeleteChannel", project, location,
		map[string]any{"project": project, "location": location, "name": channelName(project, location, id)}, nil)
	if !covered {
		return p.ProviderInterface.DeleteChannel(ctx, project, location, id)
	}
	return err
}

func (p typedEventarc) ChannelGetIamPolicy(ctx context.Context, project, location, id string) (policy.Policy, error) {
	data, covered, err := p.c.do(ctx, "Eventarc.ChannelGetIamPolicy", project, location,
		map[string]any{"project": project, "location": location, "name": channelName(project, location, id)}, nil)
	if !covered {
		return p.ProviderInterface.ChannelGetIamPolicy(ctx, project, location, id)
	}
	if err != nil {
		return policy.Policy{}, err
	}
	return decodePolicy(data)
}

func (p typedEventarc) ChannelSetIamPolicy(ctx context.Context, project, location, id string, body map[string]any) (policy.Policy, error) {
	data, covered, err := p.c.do(ctx, "Eventarc.ChannelSetIamPolicy", project, location,
		map[string]any{"project": project, "location": location, "name": channelName(project, location, id), "body": body}, nil)
	if !covered {
		return p.ProviderInterface.ChannelSetIamPolicy(ctx, project, location, id, body)
	}
	if err != nil {
		return policy.Policy{}, err
	}
	return decodePolicy(data)
}
