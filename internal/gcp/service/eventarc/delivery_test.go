package eventarc

import (
	"context"
	"encoding/json"
	"testing"

	"jaiscloud/internal/gcp/eventing"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/store"
)

// fakeFunctions is a static FunctionExister.
type fakeFunctions struct{ exists map[string]bool }

func (f fakeFunctions) FunctionExists(_ context.Context, _, location, id string) (bool, error) {
	return f.exists[location+"/"+id], nil
}

func seedTopic(t *testing.T, resources store.ResourceStore, id string) {
	t.Helper()
	if err := resources.Upsert(context.Background(), "proj", store.GlobalRegion,
		store.ResourceEntry{Type: rtTopic, ID: id}); err != nil {
		t.Fatalf("seed topic %s: %v", id, err)
	}
}

func TestCreateTriggerCloudFunctionDestination(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	seedTopic(t, resources, "t")
	svc := NewService(eventarcstore.NewMemoryStore(), resources, workflowsstore.NewMemoryStore())
	svc.SetFunctionExister(fakeFunctions{exists: map[string]bool{"us-central1/fn": true}})

	body := json.RawMessage(`{
		"destination":{"cloudFunction":"projects/proj/locations/us-central1/functions/fn"},
		"transport":{"pubsub":{"topic":"projects/proj/topics/t"}},
		"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}],
		"retryPolicy":{"maxAttempts":3}}`)
	if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "cf", body, false); err != nil {
		t.Fatalf("create cloudFunction trigger: %v", err)
	}

	// The named function must exist.
	missing := json.RawMessage(`{
		"destination":{"cloudFunction":"projects/proj/locations/us-central1/functions/missing"},
		"transport":{"pubsub":{"topic":"projects/proj/topics/t"}},
		"eventFilters":[{"attribute":"type","value":"x"}]}`)
	if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "bad", missing, false); err == nil {
		t.Fatal("expected NotFound for a missing destination.cloudFunction")
	}

	// A matching Pub/Sub event routes to the function.
	targets := svc.TargetsForEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/t", Source: eventing.SourcePubSub,
	})
	if len(targets) != 1 || targets[0].FunctionID != "fn" || targets[0].Location != "us-central1" || !targets[0].Retry {
		t.Fatalf("targets = %+v", targets)
	}

	// A different topic does not route.
	if got := svc.TargetsForEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/other", Source: eventing.SourcePubSub,
	}); len(got) != 0 {
		t.Fatalf("unexpected targets for another topic: %+v", got)
	}

	// An event whose type does not match the type filter does not route.
	if got := svc.TargetsForEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypeStorageFinalize,
		Resource: "projects/proj/topics/t", Source: eventing.SourcePubSub,
	}); len(got) != 0 {
		t.Fatalf("unexpected targets for a non-matching type: %+v", got)
	}
}

func TestCreateTriggerCloudFunctionAndCloudRunRejected(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	svc := NewService(eventarcstore.NewMemoryStore(), resources, workflowsstore.NewMemoryStore())
	body := json.RawMessage(`{
		"destination":{
			"cloudFunction":"projects/proj/locations/us-central1/functions/fn",
			"cloudRun":{"service":"projects/proj/locations/us-central1/services/s"}},
		"eventFilters":[{"attribute":"type","value":"x"}]}`)
	if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "both", body, false); err == nil {
		t.Fatal("expected an error when two destination oneof fields are set")
	}
}
