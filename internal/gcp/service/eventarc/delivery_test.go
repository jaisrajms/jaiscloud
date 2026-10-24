package eventarc

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"jaiscloud/internal/gcp/eventing"
	eventarcstore "jaiscloud/internal/gcp/store/eventarc"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
	"jaiscloud/internal/model"
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

func TestTargetsForEventStorageCloudFunction(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	svc := NewService(eventarcstore.NewMemoryStore(), resources, workflowsstore.NewMemoryStore())
	svc.SetFunctionExister(fakeFunctions{exists: map[string]bool{"us-central1/fn": true}})

	// A Cloud Storage trigger has no Pub/Sub transport: its eventFilters
	// (type + bucket) select the object event.
	body := json.RawMessage(`{
		"destination":{"cloudFunction":"projects/proj/locations/us-central1/functions/fn"},
		"eventFilters":[
			{"attribute":"type","value":"google.cloud.storage.object.v1.finalized"},
			{"attribute":"bucket","value":"b"}]}`)
	if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "gcsfn", body, false); err != nil {
		t.Fatalf("create storage cloudFunction trigger: %v", err)
	}

	targets := svc.TargetsForEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypeStorageFinalize,
		Resource: "projects/_/buckets/b", Source: eventing.SourceStorage,
	})
	if len(targets) != 1 || targets[0].FunctionID != "fn" || targets[0].Location != "us-central1" {
		t.Fatalf("targets = %+v", targets)
	}
	// A directly-created storage trigger has no transport, so SyncTriggerSubscription
	// provisions no backing subscription; do not advertise one.
	if targets[0].Subscription != "" {
		t.Fatalf("subscription = %q, want empty for a transport-less storage trigger", targets[0].Subscription)
	}

	// A different bucket does not route.
	if got := svc.TargetsForEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypeStorageFinalize,
		Resource: "projects/_/buckets/other", Source: eventing.SourceStorage,
	}); len(got) != 0 {
		t.Fatalf("unexpected targets for another bucket: %+v", got)
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

// TestCreateTriggerCloudRunValidation covers EV6: a destination.cloudRun must
// carry a service and a region (or a full service name with a location), stay
// in the trigger's project, and not disagree with a full service name's
// location. A rejected destination must fail at create/validateOnly and never
// be persisted.
func TestCreateTriggerCloudRunValidation(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		dest string
		ok   bool
	}{
		{"short service and region", `{"service":"hello-run","region":"us-central1"}`, true},
		{"full name derives region", `{"service":"projects/proj/locations/us-central1/services/s"}`, true},
		{"full name and matching region", `{"service":"projects/proj/locations/us-central1/services/s","region":"us-central1"}`, true},
		{"short service without region", `{"service":"hello-run"}`, false},
		{"empty cloudRun", `{}`, false},
		{"full name disagrees with region", `{"service":"projects/proj/locations/us-central1/services/s","region":"europe-west1"}`, false},
		{"full name in another project", `{"service":"projects/other/locations/us-central1/services/s","region":"us-central1"}`, false},
		{"malformed full name with region", `{"service":"projects/proj/services/s","region":"us-central1"}`, false},
		{"malformed full name without region", `{"service":"projects/proj/services/s"}`, false},
		{"slash name missing project", `{"service":"locations/us-central1/services/s","region":"us-central1"}`, false},
		{"non-resource slash name", `{"service":"foo/bar","region":"us-central1"}`, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			svc := NewService(eventarcstore.NewMemoryStore(), store.NewMemoryResourceStore(), workflowsstore.NewMemoryStore())
			body := json.RawMessage(`{"destination":{"cloudRun":` + tt.dest + `},"eventFilters":[{"attribute":"type","value":"x"}]}`)
			if !tt.ok {
				var pe *model.ProviderError
				if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "run", body, false); !errors.As(err, &pe) || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
					t.Fatalf("CreateTrigger error = %v, want InvalidArgument/400", err)
				}
				if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "run", body, true); err == nil {
					t.Fatal("validateOnly accepted an invalid cloudRun destination")
				}
				if _, err := svc.GetTrigger(ctx, "proj", "us-central1", "run"); err == nil {
					t.Fatal("invalid trigger was persisted")
				}
				return
			}
			// A valid destination passes validateOnly without persisting, then a
			// real create persists it.
			if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "run", body, true); err != nil {
				t.Fatalf("validateOnly CreateTrigger: %v", err)
			}
			if _, err := svc.GetTrigger(ctx, "proj", "us-central1", "run"); err == nil {
				t.Fatal("validateOnly persisted the trigger")
			}
			if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "run", body, false); err != nil {
				t.Fatalf("CreateTrigger: %v", err)
			}
			if _, err := svc.GetTrigger(ctx, "proj", "us-central1", "run"); err != nil {
				t.Fatalf("GetTrigger after create: %v", err)
			}
		})
	}
}
