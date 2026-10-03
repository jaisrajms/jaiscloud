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

// fakeSubs is a recording eventing.SubscriptionProvisioner.
type fakeSubs struct {
	ensured       []string
	topics        []string
	createdTopics []string
	deletedTopics []string
	deleted       []string
	failWith      error
	// resources, when set, mirrors the real provider: EnsureEventarcTopic
	// records a labelled topic entry and DeleteEventarcTopic removes it, so the
	// teardown ownership check and the source-switch reap are exercised.
	resources store.ResourceStore
}

func (f *fakeSubs) EnsureEventarcTopic(ctx context.Context, project, location, triggerID string) (string, error) {
	if f.failWith != nil {
		return "", f.failWith
	}
	name := eventing.EventarcTopicID(location, triggerID)
	for _, t := range f.createdTopics {
		if t == name {
			return name, nil
		}
	}
	f.createdTopics = append(f.createdTopics, name)
	if f.resources != nil {
		meta := map[string]any{
			"name":   "projects/" + project + "/topics/" + name,
			"labels": map[string]string{"goog-eventarc-trigger": triggerID},
		}
		data, _ := json.Marshal(meta)
		_ = f.resources.Upsert(ctx, project, store.GlobalRegion, store.ResourceEntry{Type: rtTopic, ID: name, Data: data})
	}
	return name, nil
}

func (f *fakeSubs) DeleteEventarcTopic(ctx context.Context, project, topic string) error {
	id := lastSegment(topic)
	f.deletedTopics = append(f.deletedTopics, id)
	if f.resources != nil {
		_ = f.resources.Delete(ctx, project, store.GlobalRegion, rtTopic, id)
	}
	return nil
}

func (f *fakeSubs) EnsureEventarcSubscription(_ context.Context, _, location, triggerID, topic string) (string, error) {
	if f.failWith != nil {
		return "", f.failWith
	}
	if topic == "missing" {
		return "", model.NewProviderError("NotFound", "topic not found", 404)
	}
	name := eventing.EventarcSubscriptionID(location, triggerID)
	f.ensured = append(f.ensured, name)
	f.topics = append(f.topics, topic)
	return name, nil
}

func (f *fakeSubs) DeleteEventarcSubscription(_ context.Context, _, subscription string) error {
	f.deleted = append(f.deleted, subscription)
	return nil
}

func (f *fakeSubs) SubscriptionDeadLetter(context.Context, string, string) (string, int, bool, error) {
	return "", 0, false, nil
}

func (f *fakeSubs) PublishDeadLetter(context.Context, string, string, []byte, map[string]string) error {
	return nil
}

// TestEnsureFunctionTrigger covers materializing a function's backing Eventarc
// trigger and its transport subscription (FD9), including the rendered
// output-only transport.pubsub.subscription and target subscription.
func TestEnsureFunctionTrigger(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	seedTopic(t, resources, "t")
	st := eventarcstore.NewMemoryStore()
	svc := NewService(st, resources, workflowsstore.NewMemoryStore())
	subs := &fakeSubs{}
	svc.SetSubscriptionProvisioner(subs)

	name, sub, err := svc.EnsureFunctionTrigger(ctx, eventing.FunctionTriggerSpec{
		Project: "proj", Location: "us-central1", FunctionID: "fn",
		EventType: eventing.TypePubSubPublish, Resource: "t",
	})
	if err != nil {
		t.Fatalf("ensure function trigger: %v", err)
	}
	if want := TriggerName("proj", "us-central1", "functions-fn"); name != want {
		t.Fatalf("trigger name = %q, want %q", name, want)
	}
	if want := eventing.EventarcSubscriptionID("us-central1", "functions-fn"); sub != want {
		t.Fatalf("subscription = %q, want %q", sub, want)
	}

	stored, err := svc.GetTrigger(ctx, "proj", "us-central1", "functions-fn")
	if err != nil {
		t.Fatalf("get trigger: %v", err)
	}
	rendered := TriggerJSON("proj", stored)
	transport, _ := rendered["transport"].(map[string]any)
	pubsub, _ := transport["pubsub"].(map[string]any)
	if got, _ := pubsub["subscription"].(string); got != "projects/proj/subscriptions/"+sub {
		t.Fatalf("rendered subscription = %q", got)
	}

	// A matching Pub/Sub event resolves to the function and carries the backing
	// subscription so the delivery engine can resolve its deadLetterPolicy.
	targets := svc.TargetsForEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/t", Source: eventing.SourcePubSub,
	})
	if len(targets) != 1 || targets[0].FunctionID != "fn" || targets[0].Subscription != sub {
		t.Fatalf("targets = %+v", targets)
	}

	// Re-ensure is idempotent.
	if _, _, err := svc.EnsureFunctionTrigger(ctx, eventing.FunctionTriggerSpec{
		Project: "proj", Location: "us-central1", FunctionID: "fn",
		EventType: eventing.TypePubSubPublish, Resource: "t",
	}); err != nil {
		t.Fatalf("re-ensure: %v", err)
	}

	// Delete removes the trigger and its subscription.
	if err := svc.DeleteFunctionTrigger(ctx, "proj", "us-central1", "fn"); err != nil {
		t.Fatalf("delete function trigger: %v", err)
	}
	if _, err := st.GetTrigger(ctx, "proj", "us-central1", "functions-fn"); !errors.Is(err, eventarcstore.ErrNoSuchTrigger) {
		t.Fatalf("trigger still present after delete: %v", err)
	}
	if len(subs.deleted) != 1 || subs.deleted[0] != sub {
		t.Fatalf("deleted subscriptions = %v", subs.deleted)
	}
}

// TestEnsureFunctionTriggerMissingTopic verifies a missing transport topic does
// not leave a dangling backing trigger behind (the subscription is provisioned
// first so its NotFound aborts the whole materialization).
func TestEnsureFunctionTriggerMissingTopic(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	st := eventarcstore.NewMemoryStore()
	svc := NewService(st, resources, workflowsstore.NewMemoryStore())
	svc.SetSubscriptionProvisioner(&fakeSubs{})
	if _, _, err := svc.EnsureFunctionTrigger(ctx, eventing.FunctionTriggerSpec{
		Project: "proj", Location: "us-central1", FunctionID: "fn",
		EventType: eventing.TypePubSubPublish, Resource: "missing",
	}); err == nil {
		t.Fatal("expected an error for a missing topic")
	}
	if _, err := st.GetTrigger(ctx, "proj", "us-central1", "functions-fn"); !errors.Is(err, eventarcstore.ErrNoSuchTrigger) {
		t.Fatalf("expected no dangling trigger, got %v", err)
	}
}

// TestUpdateTriggerRepointsSubscription verifies a PATCH of a cloudFunction
// trigger's transport topic re-points its backing subscription (FD9).
func TestUpdateTriggerRepointsSubscription(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	seedTopic(t, resources, "a")
	seedTopic(t, resources, "b")
	svc := NewService(eventarcstore.NewMemoryStore(), resources, workflowsstore.NewMemoryStore())
	svc.SetFunctionExister(fakeFunctions{exists: map[string]bool{"us-central1/fn": true}})
	subs := &fakeSubs{}
	svc.SetSubscriptionProvisioner(subs)

	cfg := func(topic string) json.RawMessage {
		return json.RawMessage(`{"destination":{"cloudFunction":"projects/proj/locations/us-central1/functions/fn"},` +
			`"transport":{"pubsub":{"topic":"projects/proj/topics/` + topic + `"}},` +
			`"eventFilters":[{"attribute":"type","value":"google.cloud.pubsub.topic.v1.messagePublished"}]}`)
	}
	if _, _, err := svc.CreateTrigger(ctx, "proj", "us-central1", "t1", cfg("a"), false); err != nil {
		t.Fatalf("create: %v", err)
	}
	if len(subs.topics) != 1 || subs.topics[0] != "a" {
		t.Fatalf("provisioned topics = %v", subs.topics)
	}
	// PATCH the transport topic; the subscription follows.
	if _, _, err := svc.UpdateTrigger(ctx, "proj", "us-central1", "t1", cfg("b"), "transport", "", false); err != nil {
		t.Fatalf("update: %v", err)
	}
	if len(subs.topics) != 2 || subs.topics[1] != "b" {
		t.Fatalf("re-pointed topics = %v", subs.topics)
	}
}

// TestEnsureFunctionTriggerStorage covers materializing a Cloud Storage
// function's backing Eventarc trigger: Eventarc auto-provisions a transport
// topic (FP2), so the trigger has a user-configurable dead-letter subscription
// exactly like a Pub/Sub trigger.
func TestEnsureFunctionTriggerStorage(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	st := eventarcstore.NewMemoryStore()
	svc := NewService(st, resources, workflowsstore.NewMemoryStore())
	subs := &fakeSubs{resources: resources}
	svc.SetSubscriptionProvisioner(subs)

	autoTopic := eventing.EventarcTopicID("us-central1", "functions-gcsfn")
	name, sub, err := svc.EnsureFunctionTrigger(ctx, eventing.FunctionTriggerSpec{
		Project: "proj", Location: "us-central1", FunctionID: "gcsfn",
		EventType: eventing.TypeStorageFinalize, Resource: "projects/_/buckets/bkt",
	})
	if err != nil {
		t.Fatalf("ensure function trigger: %v", err)
	}
	if want := TriggerName("proj", "us-central1", "functions-gcsfn"); name != want {
		t.Fatalf("trigger name = %q, want %q", name, want)
	}
	if want := eventing.EventarcSubscriptionID("us-central1", "functions-gcsfn"); sub != want {
		t.Fatalf("subscription = %q, want %q", sub, want)
	}
	// The Eventarc-managed transport topic was provisioned (same id shape as the
	// subscription).
	if len(subs.createdTopics) != 1 || subs.createdTopics[0] != autoTopic {
		t.Fatalf("created topics = %v, want %q", subs.createdTopics, autoTopic)
	}

	stored, err := svc.GetTrigger(ctx, "proj", "us-central1", "functions-gcsfn")
	if err != nil {
		t.Fatalf("get trigger: %v", err)
	}
	rendered := TriggerJSON("proj", stored)
	pubsub := bodyMap(bodyMap(rendered, "transport"), "pubsub")
	if got, _ := pubsub["topic"].(string); got != "projects/proj/topics/"+autoTopic {
		t.Fatalf("rendered transport topic = %q", got)
	}
	if got, _ := pubsub["subscription"].(string); got != "projects/proj/subscriptions/"+sub {
		t.Fatalf("rendered subscription = %q", got)
	}

	// The event filters name the storage event type (CloudEvent spelling) and
	// the source bucket.
	filters, _ := rendered["eventFilters"].([]any)
	got := map[string]string{}
	for _, f := range filters {
		fm, _ := f.(map[string]any)
		attr, _ := fm["attribute"].(string)
		val, _ := fm["value"].(string)
		got[attr] = val
	}
	if got["type"] != "google.cloud.storage.object.v1.finalized" {
		t.Fatalf("type filter = %q", got["type"])
	}
	if got["bucket"] != "bkt" {
		t.Fatalf("bucket filter = %q", got["bucket"])
	}

	// Re-ensure is idempotent (no second transport topic).
	if _, _, err := svc.EnsureFunctionTrigger(ctx, eventing.FunctionTriggerSpec{
		Project: "proj", Location: "us-central1", FunctionID: "gcsfn",
		EventType: eventing.TypeStorageFinalize, Resource: "projects/_/buckets/bkt",
	}); err != nil {
		t.Fatalf("re-ensure: %v", err)
	}
	if len(subs.createdTopics) != 1 {
		t.Fatalf("re-ensure created topics = %v", subs.createdTopics)
	}

	// Delete tears the trigger, subscription, and auto topic down.
	if err := svc.DeleteFunctionTrigger(ctx, "proj", "us-central1", "gcsfn"); err != nil {
		t.Fatalf("delete function trigger: %v", err)
	}
	if _, err := st.GetTrigger(ctx, "proj", "us-central1", "functions-gcsfn"); !errors.Is(err, eventarcstore.ErrNoSuchTrigger) {
		t.Fatalf("trigger still present after delete: %v", err)
	}
	if len(subs.deleted) != 1 || subs.deleted[0] != sub {
		t.Fatalf("deleted subscriptions = %v", subs.deleted)
	}
	if len(subs.deletedTopics) != 1 || subs.deletedTopics[0] != autoTopic {
		t.Fatalf("deleted topics = %v, want %q", subs.deletedTopics, autoTopic)
	}
}

// TestEnsureFunctionTriggerUnsupportedSource rejects a trigger whose event type
// is neither a Pub/Sub nor a storage source.
func TestEnsureFunctionTriggerUnsupportedSource(t *testing.T) {
	svc := NewService(eventarcstore.NewMemoryStore(), store.NewMemoryResourceStore(), workflowsstore.NewMemoryStore())
	svc.SetSubscriptionProvisioner(&fakeSubs{})
	if _, _, err := svc.EnsureFunctionTrigger(context.Background(), eventing.FunctionTriggerSpec{
		Project: "proj", Location: "us-central1", FunctionID: "fn",
		EventType: "google.firestore.document.v1.written", Resource: "projects/proj/databases/(default)/documents/x",
	}); err == nil {
		t.Fatal("expected an error for an unsupported event source")
	}
}

// TestEnsureFunctionTriggerSourceSwitchReapsAutoTopic verifies that switching a
// function from a Cloud Storage source to a Pub/Sub source reaps the
// Eventarc-managed transport topic the storage trigger provisioned.
func TestEnsureFunctionTriggerSourceSwitchReapsAutoTopic(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	seedTopic(t, resources, "user")
	svc := NewService(eventarcstore.NewMemoryStore(), resources, workflowsstore.NewMemoryStore())
	subs := &fakeSubs{resources: resources}
	svc.SetSubscriptionProvisioner(subs)

	if _, _, err := svc.EnsureFunctionTrigger(ctx, eventing.FunctionTriggerSpec{
		Project: "proj", Location: "us-central1", FunctionID: "fn",
		EventType: eventing.TypeStorageFinalize, Resource: "projects/_/buckets/bkt",
	}); err != nil {
		t.Fatalf("storage ensure: %v", err)
	}
	autoTopic := eventing.EventarcTopicID("us-central1", "functions-fn")
	if _, err := resources.Get(ctx, "proj", store.GlobalRegion, rtTopic, autoTopic); err != nil {
		t.Fatalf("auto topic not provisioned: %v", err)
	}

	if _, _, err := svc.EnsureFunctionTrigger(ctx, eventing.FunctionTriggerSpec{
		Project: "proj", Location: "us-central1", FunctionID: "fn",
		EventType: eventing.TypePubSubPublish, Resource: "projects/proj/topics/user",
	}); err != nil {
		t.Fatalf("pubsub ensure: %v", err)
	}
	if len(subs.deletedTopics) != 1 || subs.deletedTopics[0] != autoTopic {
		t.Fatalf("deleted topics = %v, want %q", subs.deletedTopics, autoTopic)
	}
	if _, err := resources.Get(ctx, "proj", store.GlobalRegion, rtTopic, autoTopic); err == nil {
		t.Fatal("auto topic should have been reaped on the source switch")
	}
}

// TestDeleteFunctionTriggerKeepsUserTopic verifies a user Pub/Sub topic whose id
// coincides with the auto-topic naming scheme is not deleted with the function:
// only a topic carrying the Eventarc ownership label is torn down.
func TestDeleteFunctionTriggerKeepsUserTopic(t *testing.T) {
	ctx := context.Background()
	resources := store.NewMemoryResourceStore()
	userTopic := eventing.EventarcTopicID("us-central1", "functions-fn")
	seedTopic(t, resources, userTopic)
	svc := NewService(eventarcstore.NewMemoryStore(), resources, workflowsstore.NewMemoryStore())
	subs := &fakeSubs{resources: resources}
	svc.SetSubscriptionProvisioner(subs)

	if _, _, err := svc.EnsureFunctionTrigger(ctx, eventing.FunctionTriggerSpec{
		Project: "proj", Location: "us-central1", FunctionID: "fn",
		EventType: eventing.TypePubSubPublish, Resource: "projects/proj/topics/" + userTopic,
	}); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if err := svc.DeleteFunctionTrigger(ctx, "proj", "us-central1", "fn"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := resources.Get(ctx, "proj", store.GlobalRegion, rtTopic, userTopic); err != nil {
		t.Fatalf("user topic was deleted: %v", err)
	}
	if len(subs.deletedTopics) != 0 {
		t.Fatalf("deleted topics = %v, want none", subs.deletedTopics)
	}
}
