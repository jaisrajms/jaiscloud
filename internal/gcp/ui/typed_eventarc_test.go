package ui

import (
	"context"
	"encoding/json"
	"testing"

	eventarccore "jaiscloud/internal/gcp/service/eventarc"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	resteventarc "jaiscloud/internal/gcp/transport/rest/eventarc"
	eventarcui "jaiscloud/internal/gcp/ui/eventarc"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

func newEventarc() (eventarcui.ProviderInterface, map[string]provider.HandlerFunc) {
	core := eventarccore.NewService(eventarcstore.NewMemoryStore(), store.NewMemoryResourceStore(), workflowsstore.NewMemoryStore())
	return eventarcui.NewProvider(core), resteventarc.NewProvider(core, "proj").Routes()
}

func TestEventarcConsolePassthroughMatchesDirect(t *testing.T) {
	direct, builtins := newEventarc()
	var seen []seenCall
	wrap := map[string]func(provider.HandlerFunc) provider.HandlerFunc{}
	for k := range builtins {
		wrap[k] = passthrough(&seen, k)
	}
	p := routeEventarc(typedFixture(builtins, wrap), direct)
	ctx := context.Background()

	ch, err := p.CreateChannel(ctx, "proj", "us-central1", eventarcui.ChannelInput{Name: "ch1", Provider: "projects/proj/locations/us-central1/providers/p"})
	if err != nil || ch.Name != "ch1" || ch.Location != "us-central1" || ch.ProjectID != "proj" {
		t.Fatalf("CreateChannel = %+v, %v", ch, err)
	}
	got, err := p.GetChannel(ctx, "proj", "us-central1", "ch1")
	want, werr := direct.GetChannel(ctx, "proj", "us-central1", "ch1")
	if err != nil || werr != nil || got.Name != want.Name || got.UID != want.UID || got.Etag != want.Etag || !got.CreateTime.Equal(want.CreateTime) {
		t.Fatalf("channel through the chain:\n  %+v\ndirect:\n  %+v\n%v %v", got, want, err, werr)
	}
	var gotCfg, wantCfg map[string]any
	_ = json.Unmarshal(got.Config, &gotCfg)
	_ = json.Unmarshal(want.Config, &wantCfg)
	if gotCfg["provider"] != wantCfg["provider"] {
		t.Fatalf("channel config through the chain %v, direct %v", gotCfg, wantCfg)
	}
	if list, err := p.ListChannelsByProject(ctx, "proj"); err != nil || len(list) != 1 || list[0].Name != "ch1" {
		t.Fatalf("ListChannelsByProject = %+v, %v", list, err)
	}
	if pol, err := p.ChannelGetIamPolicy(ctx, "proj", "us-central1", "ch1"); err != nil {
		t.Fatalf("ChannelGetIamPolicy = %+v, %v", pol, err)
	}
	if err := p.DeleteChannel(ctx, "proj", "us-central1", "ch1"); err != nil {
		t.Fatalf("DeleteChannel: %v", err)
	}
	if list, err := p.ListTriggersByProject(ctx, "proj"); err != nil || len(list) != 0 {
		t.Fatalf("ListTriggersByProject = %+v, %v", list, err)
	}
	if len(seen) != 6 {
		t.Fatalf("the plugin saw %d calls (%v), want 6", len(seen), seen)
	}
}

func TestEventarcConsoleShowsWhatThePluginServes(t *testing.T) {
	direct, builtins := newEventarc()
	p := routeEventarc(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"Eventarc.ListTriggers": replaceWith(map[string]any{"triggers": []any{map[string]any{
			"name": "projects/proj/locations/europe-west1/triggers/from-plugin", "uid": "u1",
			"destination": map[string]any{"cloudRun": map[string]any{"service": "svc"}}}}}),
	}), direct)
	ts, err := p.ListTriggersByProject(context.Background(), "proj")
	if err != nil || len(ts) != 1 {
		t.Fatalf("ListTriggersByProject = %+v, %v", ts, err)
	}
	tr := ts[0]
	var cfg map[string]any
	_ = json.Unmarshal(tr.Config, &cfg)
	if tr.Name != "from-plugin" || tr.Location != "europe-west1" || tr.UID != "u1" || cfg["destination"] == nil {
		t.Fatalf("the console must show the plugin's trigger, got %+v (config %v)", tr, cfg)
	}
}

// The trigger calls and the remaining channel calls, run through the chain against the real core.
func TestEventarcConsoleTriggerLifecycleThroughTheChain(t *testing.T) {
	direct, builtins := newEventarc()
	var seen []seenCall
	wrap := map[string]func(provider.HandlerFunc) provider.HandlerFunc{}
	for k := range builtins {
		wrap[k] = passthrough(&seen, k)
	}
	p := routeEventarc(typedFixture(builtins, wrap), direct)
	ctx := context.Background()
	in := eventarcui.TriggerInput{
		Name: "t1", DestinationType: "cloudRun", Destination: "svc", DestinationRegion: "us-central1",
		EventFilters: []eventarcui.EventFilter{{Attribute: "type", Value: "google.cloud.pubsub.topic.v1.messagePublished"}},
	}
	created, err := p.CreateTrigger(ctx, "proj", "us-central1", in)
	if err != nil || created.Name != "t1" {
		t.Fatalf("CreateTrigger = %+v, %v", created, err)
	}
	got, err := p.GetTrigger(ctx, "proj", "us-central1", "t1")
	want, werr := direct.GetTrigger(ctx, "proj", "us-central1", "t1")
	if err != nil || werr != nil || got.Name != want.Name || got.UID != want.UID {
		t.Fatalf("GetTrigger through the chain %+v, direct %+v (%v, %v)", got, want, err, werr)
	}
	// An update with no explicit config merges onto the stored trigger.
	in.Labels = map[string]string{"team": "a"}
	if upd, err := p.UpdateTrigger(ctx, "proj", "us-central1", "t1", in); err != nil || upd.Name != "t1" {
		t.Fatalf("UpdateTrigger = %+v, %v", upd, err)
	}
	if _, err := p.TriggerSetIamPolicy(ctx, "proj", "us-central1", "t1", map[string]any{"policy": map[string]any{"bindings": []any{}}}); err != nil {
		t.Fatalf("TriggerSetIamPolicy: %v", err)
	}
	if _, err := p.TriggerGetIamPolicy(ctx, "proj", "us-central1", "t1"); err != nil {
		t.Fatalf("TriggerGetIamPolicy: %v", err)
	}
	if err := p.DeleteTrigger(ctx, "proj", "us-central1", "t1"); err != nil {
		t.Fatalf("DeleteTrigger: %v", err)
	}
	if _, err := direct.GetTrigger(ctx, "proj", "us-central1", "t1"); err == nil {
		t.Fatal("the trigger must be gone from the built-in after DeleteTrigger through the chain")
	}

	if _, err := p.CreateChannel(ctx, "proj", "us-central1", eventarcui.ChannelInput{Name: "ch1", Provider: "projects/proj/locations/us-central1/providers/p"}); err != nil {
		t.Fatal(err)
	}
	if ch, err := p.UpdateChannel(ctx, "proj", "us-central1", "ch1", eventarcui.ChannelInput{Name: "ch1", Provider: "projects/proj/locations/us-central1/providers/p", Labels: map[string]string{"a": "b"}}); err != nil || ch.Name != "ch1" {
		t.Fatalf("UpdateChannel = %+v, %v", ch, err)
	}
	if _, err := p.ChannelSetIamPolicy(ctx, "proj", "us-central1", "ch1", map[string]any{"policy": map[string]any{"bindings": []any{}}}); err != nil {
		t.Fatalf("ChannelSetIamPolicy: %v", err)
	}
	for _, k := range []string{"Eventarc.CreateTrigger", "Eventarc.UpdateTrigger", "Eventarc.DeleteTrigger", "Eventarc.UpdateChannel", "Eventarc.TriggerSetIamPolicy"} {
		found := false
		for _, s := range seen {
			found = found || s.key == k
		}
		if !found {
			t.Errorf("the plugin never saw %s", k)
		}
	}
}

// With no plugin on the key every trigger call stays on the built-in.
func TestEventarcConsoleTriggerCallsStayDirectWithoutAPlugin(t *testing.T) {
	direct, builtins := newEventarc()
	p := routeEventarc(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{}), direct)
	ctx := context.Background()
	in := eventarcui.TriggerInput{Name: "t1", DestinationType: "cloudRun", Destination: "svc", DestinationRegion: "us-central1",
		EventFilters: []eventarcui.EventFilter{{Attribute: "type", Value: "google.cloud.pubsub.topic.v1.messagePublished"}}}
	if _, err := p.CreateTrigger(ctx, "proj", "us-central1", in); err != nil {
		t.Fatal(err)
	}
	if _, err := p.UpdateTrigger(ctx, "proj", "us-central1", "t1", in); err != nil {
		t.Fatal(err)
	}
	if _, err := p.GetTrigger(ctx, "proj", "us-central1", "t1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.TriggerSetIamPolicy(ctx, "proj", "us-central1", "t1", map[string]any{"policy": map[string]any{"bindings": []any{}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.TriggerGetIamPolicy(ctx, "proj", "us-central1", "t1"); err != nil {
		t.Fatal(err)
	}
	if err := p.DeleteTrigger(ctx, "proj", "us-central1", "t1"); err != nil {
		t.Fatal(err)
	}
}
