package eventarcui

import (
	"context"
	"encoding/json"
	"testing"

	eventarccore "jaiscloud/internal/gcp/service/eventarc"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
)

// newProvider builds a real UI provider over the Eventarc core with an
// in-memory store, so the adapter's body construction is exercised against the
// core's validation.
func newProvider() *Provider {
	return NewProvider(eventarccore.NewService(eventarcstore.NewMemoryStore(), nil, nil))
}

func TestProvider_CreateListGetTrigger(t *testing.T) {
	ctx := context.Background()
	p := newProvider()

	created, err := p.CreateTrigger(ctx, "test-project", "us-central1", TriggerInput{
		Name:              "t1",
		DestinationType:   "cloudRun",
		Destination:       "projects/test-project/locations/us-central1/services/svc",
		DestinationRegion: "us-central1",
		EventFilters: []EventFilter{
			{Attribute: "type", Value: "google.cloud.pubsub.topic.v1.messagePublished"},
		},
		ServiceAccount: "sa@test-project.iam.gserviceaccount.com",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.Name != "t1" || created.Location != "us-central1" {
		t.Fatalf("unexpected created trigger: %+v", created)
	}
	// The structured builder must produce a body the core accepts and that
	// carries the Cloud Run region.
	var cfg map[string]any
	if err := json.Unmarshal(created.Config, &cfg); err != nil {
		t.Fatalf("config not JSON: %v", err)
	}
	dest := cfg["destination"].(map[string]any)["cloudRun"].(map[string]any)
	if dest["service"] != "projects/test-project/locations/us-central1/services/svc" {
		t.Fatalf("service not built: %+v", dest)
	}
	if dest["region"] != "us-central1" {
		t.Fatalf("region not built: %+v", dest)
	}

	got, err := p.GetTrigger(ctx, "test-project", "us-central1", "t1")
	if err != nil || got.Name != "t1" {
		t.Fatalf("get: %v %+v", err, got)
	}

	list, err := p.ListTriggersByProject(ctx, "test-project")
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %v %d", err, len(list))
	}
}

func TestProvider_UpdateTriggerPreservesCloudRunExtras(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	raw := json.RawMessage(`{"destination":{"cloudRun":{"service":"old","region":"us-central1","path":"/p"}},"eventFilters":[{"attribute":"type","value":"seed"}],"retryPolicy":{"maxAttempts":3}}`)
	if _, err := p.CreateTrigger(ctx, "test-project", "us-central1", TriggerInput{Name: "t1", Config: raw}); err != nil {
		t.Fatalf("seed create: %v", err)
	}

	updated, err := p.UpdateTrigger(ctx, "test-project", "us-central1", "t1", TriggerInput{
		Name:              "t1",
		DestinationType:   "cloudRun",
		Destination:       "new",
		DestinationRegion: "europe-west1",
		EventFilters:      []EventFilter{{Attribute: "type", Value: "x"}},
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(updated.Config, &m); err != nil {
		t.Fatalf("config not JSON: %v", err)
	}
	cr := m["destination"].(map[string]any)["cloudRun"].(map[string]any)
	if cr["service"] != "new" || cr["region"] != "europe-west1" {
		t.Fatalf("structured fields not applied: %+v", cr)
	}
	if cr["path"] != "/p" {
		t.Fatalf("unmodeled cloudRun.path dropped: %+v", cr)
	}
	if _, ok := m["retryPolicy"]; !ok {
		t.Fatalf("unmodeled retryPolicy dropped: %+v", m)
	}
}

func TestMergeTriggerConfig_ReplacesDestinationKind(t *testing.T) {
	stored := json.RawMessage(`{"destination":{"cloudRun":{"service":"s"}},"transport":{"pubsub":{"topic":"t"}}}`)
	overlay := json.RawMessage(`{"destination":{"workflow":"projects/p/locations/l/workflows/w"}}`)
	merged := mergeTriggerConfig(stored, overlay)
	var m map[string]any
	if err := json.Unmarshal(merged, &m); err != nil {
		t.Fatalf("config not JSON: %v", err)
	}
	dest := m["destination"].(map[string]any)
	if len(dest) != 1 || dest["workflow"] == nil {
		t.Fatalf("destination kinds not collapsed to one: %+v", dest)
	}
	// The stored transport is preserved by the deep merge.
	if tr, ok := m["transport"].(map[string]any); !ok || tr["pubsub"] == nil {
		t.Fatalf("transport dropped: %+v", m)
	}
}

func TestProvider_ChannelRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := newProvider()
	if _, err := p.CreateChannel(ctx, "test-project", "us-central1", ChannelInput{
		Name:          "c1",
		Provider:      "projects/test-project/locations/us-central1/providers/some.saas",
		CryptoKeyName: "projects/test-project/locations/us-central1/keyRings/kr/cryptoKeys/k",
	}); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	list, err := p.ListChannelsByProject(ctx, "test-project")
	if err != nil || len(list) != 1 || list[0].Name != "c1" {
		t.Fatalf("list channels: %v %+v", err, list)
	}
	if err := p.DeleteChannel(ctx, "test-project", "us-central1", "c1"); err != nil {
		t.Fatalf("delete channel: %v", err)
	}
	if list, err := p.ListChannelsByProject(ctx, "test-project"); err != nil || len(list) != 0 {
		t.Fatalf("channel not deleted: %v %+v", err, list)
	}
}
