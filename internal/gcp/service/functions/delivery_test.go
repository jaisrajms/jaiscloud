package functions

import (
	"context"
	"errors"
	"testing"
	"time"

	"jaiscloud/internal/executor/container"
	"jaiscloud/internal/gcp/eventing"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// failingExecutor always fails an invocation, for retry/dead-letter tests.
type failingExecutor struct{ calls int }

func (e *failingExecutor) Invoke(context.Context, container.Request) (container.Result, error) {
	e.calls++
	return container.Result{}, errors.New("boom")
}
func (e *failingExecutor) DeleteFunction(context.Context, string) {}
func (e *failingExecutor) Reset(context.Context)                  {}
func (e *failingExecutor) Close() error                           { return nil }

// fakeTargets is a static eventing.TargetIndex.
type fakeTargets struct{ targets []eventing.Target }

func (f fakeTargets) TargetsForEvent(context.Context, eventing.Event) []eventing.Target {
	return f.targets
}

// fakeProvisioner records backing-Eventarc-trigger provisioning calls. A
// non-nil err makes EnsureFunctionTrigger fail, to exercise re-provision paths.
type fakeProvisioner struct {
	specs   []eventing.FunctionTriggerSpec
	deleted []string
	err     error
}

func (f *fakeProvisioner) EnsureFunctionTrigger(_ context.Context, spec eventing.FunctionTriggerSpec) (string, string, error) {
	f.specs = append(f.specs, spec)
	if f.err != nil {
		return "", "", f.err
	}
	trigger := "projects/" + spec.Project + "/locations/" + spec.Location + "/triggers/functions-" + spec.FunctionID
	sub := eventing.EventarcSubscriptionID(spec.Location, "functions-"+spec.FunctionID)
	return trigger, sub, nil
}

func (f *fakeProvisioner) DeleteFunctionTrigger(_ context.Context, _, _, functionID string) error {
	f.deleted = append(f.deleted, functionID)
	return nil
}

// fakeSubs is a recording eventing.SubscriptionProvisioner.
type fakeSubs struct {
	dlqTopic    string
	maxAttempts int
	ok          bool
	published   int
	lastAttrs   map[string]string
}

func (f *fakeSubs) EnsureEventarcTopic(context.Context, string, string, string) (string, error) {
	return "topic", nil
}
func (f *fakeSubs) DeleteEventarcTopic(context.Context, string, string) error { return nil }
func (f *fakeSubs) EnsureEventarcSubscription(context.Context, string, string, string, string) (string, error) {
	return "sub", nil
}
func (f *fakeSubs) DeleteEventarcSubscription(context.Context, string, string) error { return nil }
func (f *fakeSubs) SubscriptionDeadLetter(context.Context, string, string) (string, int, bool, error) {
	return f.dlqTopic, f.maxAttempts, f.ok, nil
}
func (f *fakeSubs) PublishDeadLetter(_ context.Context, _, _ string, _ []byte, attrs map[string]string) error {
	f.published++
	f.lastAttrs = attrs
	return nil
}

// setDeliverySubscription records a backing subscription on a stored function so
// the delivery engine can resolve its deadLetterPolicy.
func setDeliverySubscription(t *testing.T, fs *functionsstore.MemoryStore, id, sub string) {
	t.Helper()
	ctx := context.Background()
	f, err := fs.GetFunction(ctx, "proj", "us-central1", id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	if f.EventTrigger == nil {
		t.Fatalf("%s has no event trigger", id)
	}
	f.EventTrigger.Subscription = sub
	if err := fs.UpdateFunction(ctx, "proj", "us-central1", id, f); err != nil {
		t.Fatalf("update %s: %v", id, err)
	}
}

func newDeliveryService(t *testing.T, opts ...Option) (*Service, *functionsstore.MemoryStore) {
	t.Helper()
	fs := functionsstore.NewMemoryStore()
	all := append([]Option{}, opts...)
	s := NewService(fs, store.NewMemoryResourceStore(), all...)
	return s, fs
}

func createEventFunction(t *testing.T, s *Service, id string, trigger map[string]any) {
	t.Helper()
	in := FunctionInputFromMap(map[string]any{
		"runtime": "nodejs20", "entryPoint": "handler", "eventTrigger": trigger,
	}, V1)
	if _, _, err := s.CreateFunction(context.Background(), "proj", "us-central1", id, in, V1); err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
}

func deliveries(t *testing.T, s *Service) []functionsstore.Delivery {
	t.Helper()
	got, err := s.ListDeliveries(context.Background(), "proj", "us-central1")
	if err != nil {
		t.Fatalf("list deliveries: %v", err)
	}
	return got
}

func TestDispatchEventDeliversToMatchingFunction(t *testing.T) {
	ctx := context.Background()
	s, _ := newDeliveryService(t)
	createEventFunction(t, s, "fn", map[string]any{
		"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t",
	})

	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/t", Source: eventing.SourcePubSub, Data: []byte("hello"),
	})

	got := deliveries(t, s)
	if len(got) != 1 {
		t.Fatalf("expected 1 delivery, got %d: %+v", len(got), got)
	}
	d := got[0]
	if d.Status != functionsstore.DeliveryDelivered || d.Attempts != 1 || d.Result != "hello" {
		t.Fatalf("delivery = %+v", d)
	}
	if d.FunctionID != "fn" || d.Source != eventing.SourcePubSub {
		t.Fatalf("delivery metadata = %+v", d)
	}

	// A different topic does not match.
	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/other", Source: eventing.SourcePubSub, Data: []byte("x"),
	})
	if got := deliveries(t, s); len(got) != 1 {
		t.Fatalf("non-matching topic produced a delivery: %+v", got)
	}
}

func TestDispatchEventLegacyStorageMatching(t *testing.T) {
	ctx := context.Background()
	s, _ := newDeliveryService(t)
	// The v1 object.change catch-all subscribes to finalize and delete.
	createEventFunction(t, s, "stor", map[string]any{
		"eventType": "providers/cloud.storage/eventTypes/object.change",
		"resource":  "projects/_/buckets/bkt",
	})

	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypeStorageFinalize,
		Resource: "projects/_/buckets/bkt", Source: eventing.SourceStorage, Data: []byte("{}"),
	})
	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypeStorageDelete,
		Resource: "projects/_/buckets/bkt", Source: eventing.SourceStorage, Data: []byte("{}"),
	})
	// A different bucket does not match.
	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypeStorageFinalize,
		Resource: "projects/_/buckets/other", Source: eventing.SourceStorage, Data: []byte("{}"),
	})

	got := deliveries(t, s)
	if len(got) != 2 {
		t.Fatalf("expected 2 deliveries (finalize+delete), got %d: %+v", len(got), got)
	}
	for _, d := range got {
		if d.Status != functionsstore.DeliveryDelivered {
			t.Fatalf("delivery not delivered: %+v", d)
		}
	}
}

func TestDispatchRetryDeadLetter(t *testing.T) {
	ctx := context.Background()
	fail := &failingExecutor{}
	s, _ := newDeliveryService(t, WithExecutor(fail))
	createEventFunction(t, s, "retry", map[string]any{
		"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t",
		"failurePolicy": map[string]any{"retry": map[string]any{}},
	})
	createEventFunction(t, s, "noretry", map[string]any{
		"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t",
	})

	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/t", Source: eventing.SourcePubSub, Data: []byte("x"),
	})

	got := deliveries(t, s)
	if len(got) != 2 {
		t.Fatalf("expected 2 deliveries, got %d: %+v", len(got), got)
	}
	byFn := map[string]functionsstore.Delivery{}
	for _, d := range got {
		byFn[d.FunctionID] = d
	}
	if d := byFn["retry"]; d.Status != functionsstore.DeliveryDeadLetter || d.Attempts != maxDeliveryAttempts {
		t.Fatalf("retry delivery = %+v", d)
	}
	if d := byFn["noretry"]; d.Status != functionsstore.DeliveryFailed || d.Attempts != 1 {
		t.Fatalf("no-retry delivery = %+v", d)
	}
}

func TestDispatchForwardToDeadLetter(t *testing.T) {
	ctx := context.Background()
	fail := &failingExecutor{}
	// maxAttempts 2 exercises the configured cap without the ~3s a real policy's
	// minimum of 5 attempts would cost; the value is honoured verbatim.
	subs := &fakeSubs{dlqTopic: "dlq", maxAttempts: 2, ok: true}
	s, fs := newDeliveryService(t, WithExecutor(fail), WithSubscriptions(subs))
	createEventFunction(t, s, "fn", map[string]any{
		"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t",
		"failurePolicy": map[string]any{"retry": map[string]any{}},
	})
	setDeliverySubscription(t, fs, "fn", "eventarc-us-central1-functions-fn")

	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/t", Source: eventing.SourcePubSub,
		EventID: "m1", Data: []byte("payload"),
	})

	got := deliveries(t, s)
	if len(got) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(got))
	}
	d := got[0]
	if d.Status != functionsstore.DeliveryDeadLetter || d.Attempts != 2 || d.DeadLetterTopic != "dlq" {
		t.Fatalf("delivery = %+v", d)
	}
	if subs.published != 1 {
		t.Fatalf("published = %d, want 1", subs.published)
	}
	if subs.lastAttrs["CloudPubSubDeadLetterSourceSubscription"] != "eventarc-us-central1-functions-fn" {
		t.Fatalf("attrs = %+v", subs.lastAttrs)
	}
	if subs.lastAttrs["CloudPubSubDeadLetterSourceDeliveryCount"] != "2" {
		t.Fatalf("delivery count attr = %q", subs.lastAttrs["CloudPubSubDeadLetterSourceDeliveryCount"])
	}
}

// TestDispatchForwardToDeadLetterStorage covers FP2: a Cloud Storage event that
// exhausts its retries is forwarded to the dead-letter topic of the function's
// backing (Eventarc-provisioned) subscription, exactly like a Pub/Sub event.
func TestDispatchForwardToDeadLetterStorage(t *testing.T) {
	ctx := context.Background()
	fail := &failingExecutor{}
	subs := &fakeSubs{dlqTopic: "dlq", maxAttempts: 2, ok: true}
	s, fs := newDeliveryService(t, WithExecutor(fail), WithSubscriptions(subs))
	createEventFunction(t, s, "gcsfn", map[string]any{
		"eventType":     "google.storage.object.finalize",
		"resource":      "projects/_/buckets/bkt",
		"failurePolicy": map[string]any{"retry": map[string]any{}},
	})
	setDeliverySubscription(t, fs, "gcsfn", eventing.EventarcSubscriptionID("us-central1", "functions-gcsfn"))

	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypeStorageFinalize,
		Resource: "projects/_/buckets/bkt", Source: eventing.SourceStorage,
		EventID: "obj1", Data: []byte("payload"),
	})

	got := deliveries(t, s)
	if len(got) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(got))
	}
	d := got[0]
	if d.Status != functionsstore.DeliveryDeadLetter || d.Attempts != 2 || d.DeadLetterTopic != "dlq" {
		t.Fatalf("delivery = %+v", d)
	}
	if d.Source != eventing.SourceStorage {
		t.Fatalf("delivery source = %q", d.Source)
	}
	if subs.published != 1 {
		t.Fatalf("published = %d, want 1", subs.published)
	}
	if subs.lastAttrs["CloudPubSubDeadLetterSourceSubscription"] != eventing.EventarcSubscriptionID("us-central1", "functions-gcsfn") {
		t.Fatalf("attrs = %+v", subs.lastAttrs)
	}
}

// TestCreateFunctionProvisionsStorageTrigger covers FP2 provisioning: creating a
// Cloud Storage-triggered function materializes a backing Eventarc trigger and
// stores its subscription (the dead-letter surface) on the function.
func TestCreateFunctionProvisionsStorageTrigger(t *testing.T) {
	ctx := context.Background()
	prov := &fakeProvisioner{}
	s, fs := newDeliveryService(t, WithTriggerProvisioner(prov))
	createEventFunction(t, s, "gcsfn", map[string]any{
		"eventType": "providers/cloud.storage/eventTypes/object.change",
		"resource":  "projects/_/buckets/bkt",
	})

	if len(prov.specs) != 1 {
		t.Fatalf("provision calls = %+v", prov.specs)
	}
	spec := prov.specs[0]
	if !eventing.IsStorageEventType(spec.EventType) || spec.Resource != "projects/_/buckets/bkt" {
		t.Fatalf("spec = %+v", spec)
	}
	stored, err := fs.GetFunction(ctx, "proj", "us-central1", "gcsfn")
	if err != nil {
		t.Fatalf("get function: %v", err)
	}
	if stored.EventTrigger == nil || stored.EventTrigger.Trigger == "" || stored.EventTrigger.Subscription == "" {
		t.Fatalf("backing trigger not persisted: %+v", stored.EventTrigger)
	}
}

// TestUpdateFailedReprovisionPreservesBackingTrigger covers FP10: a PATCH that
// re-materializes an event trigger must not clear the previously provisioned
// backing trigger/subscription when the re-provision fails — otherwise the
// delivery engine loses the dead-letter surface of the still-existing backing
// subscription.
func TestUpdateFailedReprovisionPreservesBackingTrigger(t *testing.T) {
	ctx := context.Background()
	prov := &fakeProvisioner{}
	fail := &failingExecutor{}
	subs := &fakeSubs{dlqTopic: "dlq", maxAttempts: 2, ok: true}
	s, fs := newDeliveryService(t, WithTriggerProvisioner(prov), WithExecutor(fail), WithSubscriptions(subs))
	createEventFunction(t, s, "fn", map[string]any{
		"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t",
		"failurePolicy": map[string]any{"retry": map[string]any{}},
	})
	before, err := fs.GetFunction(ctx, "proj", "us-central1", "fn")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if before.EventTrigger == nil || before.EventTrigger.Trigger == "" || before.EventTrigger.Subscription == "" {
		t.Fatalf("initial backing trigger not persisted: %+v", before.EventTrigger)
	}

	// A later PATCH changes the source resource while the provisioner is down.
	prov.err = errors.New("provisioner unavailable")
	in := FunctionInputFromMap(map[string]any{
		"runtime": "nodejs20", "entryPoint": "handler",
		"eventTrigger": map[string]any{
			"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t2",
			"failurePolicy": map[string]any{"retry": map[string]any{}},
		},
	}, V1)
	if _, _, err := s.UpdateFunction(ctx, "proj", "us-central1", "fn", in, nil, V1); err != nil {
		t.Fatalf("UpdateFunction: %v", err)
	}

	after, err := fs.GetFunction(ctx, "proj", "us-central1", "fn")
	if err != nil {
		t.Fatalf("get after: %v", err)
	}
	if after.EventTrigger == nil {
		t.Fatal("event trigger lost on update")
	}
	if after.EventTrigger.Resource != "projects/proj/topics/t2" {
		t.Fatalf("event trigger metadata not updated: %+v", after.EventTrigger)
	}
	if after.EventTrigger.Trigger != before.EventTrigger.Trigger || after.EventTrigger.Subscription != before.EventTrigger.Subscription {
		t.Fatalf("failed re-provision cleared the backing trigger: before=%+v after=%+v",
			before.EventTrigger, after.EventTrigger)
	}

	// The preserved backing subscription still resolves its dead-letter surface:
	// an exhausted delivery on the updated source still forwards to the topic.
	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/t2", Source: eventing.SourcePubSub,
		EventID: "m1", Data: []byte("payload"),
	})
	got := deliveries(t, s)
	if len(got) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(got))
	}
	if got[0].Status != functionsstore.DeliveryDeadLetter || got[0].DeadLetterTopic != "dlq" {
		t.Fatalf("preserved subscription did not resolve its dead-letter surface: %+v", got[0])
	}
}

// TestCreateHTTPFunctionDoesNotProvision verifies only eventarc-backed event
// triggers (Pub/Sub, Cloud Storage) are provisioned; an HTTP-triggered function
// has no backing Eventarc trigger.
func TestCreateHTTPFunctionDoesNotProvision(t *testing.T) {
	prov := &fakeProvisioner{}
	s, _ := newDeliveryService(t, WithTriggerProvisioner(prov))
	in := FunctionInputFromMap(map[string]any{"runtime": "nodejs20", "entryPoint": "handler"}, V1)
	if _, _, err := s.CreateFunction(context.Background(), "proj", "us-central1", "httpfn", in, V1); err != nil {
		t.Fatalf("create httpfn: %v", err)
	}
	if len(prov.specs) != 0 {
		t.Fatalf("provisioned an HTTP function: %+v", prov.specs)
	}
}

// TestV2StorageEventTriggerShape covers the Gen2 Cloud Storage trigger shape: the
// source bucket arrives as an eventFilters bucket (pubsubTopic is Pub/Sub-only),
// is rendered back the same way, and is provisioned like any eventarc-backed
// trigger (FP2).
func TestV2StorageEventTriggerShape(t *testing.T) {
	in := FunctionInputFromMap(map[string]any{
		"buildConfig": map[string]any{"runtime": "nodejs20"},
		"eventTrigger": map[string]any{
			"eventType":    "google.cloud.storage.object.v1.finalized",
			"eventFilters": []any{map[string]any{"attribute": "bucket", "value": "bkt"}},
			"retryPolicy":  "RETRY_POLICY_RETRY",
		},
	}, V2)
	if in.EventTrigger == nil || in.EventTrigger.Resource != "projects/_/buckets/bkt" {
		t.Fatalf("v2 storage source not parsed: %+v", in.EventTrigger)
	}

	rendered := functionJSONV2("proj", functionsstore.Function{
		ID: "f", Location: "us-central1", EventTrigger: in.EventTrigger,
	})
	et, _ := rendered["eventTrigger"].(map[string]any)
	if _, ok := et["pubsubTopic"]; ok {
		t.Fatalf("v2 storage trigger must not render pubsubTopic: %+v", et)
	}
	filters, _ := et["eventFilters"].([]any)
	if len(filters) != 1 {
		t.Fatalf("eventFilters = %+v", filters)
	}
	fm, _ := filters[0].(map[string]any)
	if fm["attribute"] != "bucket" || fm["value"] != "bkt" {
		t.Fatalf("bucket filter = %+v", fm)
	}

	// The parsed bucket source is provisioned like any eventarc-backed trigger.
	prov := &fakeProvisioner{}
	s, _ := newDeliveryService(t, WithTriggerProvisioner(prov))
	if _, _, err := s.CreateFunction(context.Background(), "proj", "us-central1", "gcsfn", in, V2); err != nil {
		t.Fatalf("create: %v", err)
	}
	if len(prov.specs) != 1 || !eventing.IsStorageEventType(prov.specs[0].EventType) || prov.specs[0].Resource != "projects/_/buckets/bkt" {
		t.Fatalf("provision calls = %+v", prov.specs)
	}
}

func TestDispatchNoDeadLetterWithoutPolicy(t *testing.T) {
	ctx := context.Background()
	fail := &failingExecutor{}
	subs := &fakeSubs{ok: false}
	s, fs := newDeliveryService(t, WithExecutor(fail), WithSubscriptions(subs))
	createEventFunction(t, s, "fn", map[string]any{
		"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t",
		"failurePolicy": map[string]any{"retry": map[string]any{}},
	})
	setDeliverySubscription(t, fs, "fn", "sub")

	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/t", Source: eventing.SourcePubSub, Data: []byte("x"),
	})

	d := deliveries(t, s)[0]
	if d.Status != functionsstore.DeliveryDeadLetter || d.Attempts != maxDeliveryAttempts || d.DeadLetterTopic != "" {
		t.Fatalf("delivery = %+v", d)
	}
	if subs.published != 0 {
		t.Fatalf("published without a policy = %d", subs.published)
	}
}

func TestDispatchEventarcTarget(t *testing.T) {
	ctx := context.Background()
	s, _ := newDeliveryService(t, WithEventTargets(fakeTargets{targets: []eventing.Target{
		{Project: "proj", Location: "us-central1", FunctionID: "target"},
	}}))
	// The target function exists but has no eventTrigger of its own.
	in := FunctionInputFromMap(map[string]any{"runtime": "nodejs20", "entryPoint": "handler"}, V1)
	if _, _, err := s.CreateFunction(ctx, "proj", "us-central1", "target", in, V1); err != nil {
		t.Fatalf("create target: %v", err)
	}

	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/triggered", Source: eventing.SourcePubSub, Data: []byte("via-eventarc"),
	})

	got := deliveries(t, s)
	if len(got) != 1 || got[0].FunctionID != "target" || got[0].Result != "via-eventarc" {
		t.Fatalf("eventarc delivery = %+v", got)
	}
}

func TestDispatchDedupesOwnAndEventarcTarget(t *testing.T) {
	ctx := context.Background()
	s, _ := newDeliveryService(t, WithEventTargets(fakeTargets{targets: []eventing.Target{
		{Project: "proj", Location: "us-central1", FunctionID: "fn"},
	}}))
	createEventFunction(t, s, "fn", map[string]any{
		"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t",
	})

	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/t", Source: eventing.SourcePubSub, Data: []byte("once"),
	})
	if got := deliveries(t, s); len(got) != 1 {
		t.Fatalf("expected a single deduplicated delivery, got %d: %+v", len(got), got)
	}
}

func TestRetryPolicyRoundTrip(t *testing.T) {
	v1 := FunctionInputFromMap(map[string]any{
		"runtime": "nodejs20",
		"eventTrigger": map[string]any{
			"eventType": "google.pubsub.topic.publish", "resource": "t",
			"failurePolicy": map[string]any{"retry": map[string]any{}},
		},
	}, V1)
	if v1.EventTrigger == nil || !v1.EventTrigger.Retry {
		t.Fatalf("v1 retry not parsed: %+v", v1.EventTrigger)
	}
	renderedV1 := functionJSONV1("proj", functionsstore.Function{
		ID: "f", Location: "us-central1", EventTrigger: v1.EventTrigger,
	})
	if fp, _ := renderedV1["eventTrigger"].(map[string]any)["failurePolicy"].(map[string]any); fp == nil || fp["retry"] == nil {
		t.Fatalf("v1 failurePolicy not rendered: %+v", renderedV1["eventTrigger"])
	}

	v2 := FunctionInputFromMap(map[string]any{
		"buildConfig":  map[string]any{"runtime": "nodejs22"},
		"eventTrigger": map[string]any{"eventType": "google.pubsub.topic.publish", "pubsubTopic": "t", "retryPolicy": "RETRY_POLICY_RETRY"},
	}, V2)
	if v2.EventTrigger == nil || !v2.EventTrigger.Retries() {
		t.Fatalf("v2 retryPolicy not parsed: %+v", v2.EventTrigger)
	}
	renderedV2 := functionJSONV2("proj", functionsstore.Function{
		ID: "f", Location: "us-central1", EventTrigger: v2.EventTrigger,
	})
	if rp, _ := renderedV2["eventTrigger"].(map[string]any)["retryPolicy"].(string); rp != "RETRY_POLICY_RETRY" {
		t.Fatalf("v2 retryPolicy not rendered: %+v", renderedV2["eventTrigger"])
	}
}

func TestDispatchWithWorkerPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, _ := newDeliveryService(t)
	s.Start(ctx)
	createEventFunction(t, s, "fn", map[string]any{
		"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t",
	})

	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/t", Source: eventing.SourcePubSub, Data: []byte("async"),
	})

	deadline := time.Now().Add(5 * time.Second)
	for {
		got := deliveries(t, s)
		if len(got) == 1 && got[0].Status == functionsstore.DeliveryDelivered {
			if got[0].Result != "async" {
				t.Fatalf("result = %q", got[0].Result)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivery did not complete: %+v", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFunctionExists(t *testing.T) {
	ctx := context.Background()
	s, _ := newDeliveryService(t)
	createEventFunction(t, s, "fn", map[string]any{
		"eventType": "google.pubsub.topic.publish", "resource": "projects/proj/topics/t",
	})
	ok, err := s.FunctionExists(ctx, "proj", "us-central1", "fn")
	if err != nil || !ok {
		t.Fatalf("FunctionExists = %v, %v", ok, err)
	}
	ok, err = s.FunctionExists(ctx, "proj", "us-central1", "missing")
	if err != nil || ok {
		t.Fatalf("FunctionExists missing = %v, %v", ok, err)
	}
}

// TestIsThrottle verifies the typed back-pressure predicate: only the admission
// gate's RESOURCE_EXHAUSTED / 429 is a throttle; a nil, a plain error, an
// INTERNAL ProviderError, and (per the FH2 contract) UNAVAILABLE are not.
func TestIsThrottle(t *testing.T) {
	if !isThrottle(concurrencyExceeded("function", "fn")) {
		t.Fatal("admission 429 not classified as throttle")
	}
	if isThrottle(nil) {
		t.Fatal("nil classified as throttle")
	}
	if isThrottle(errors.New("boom")) {
		t.Fatal("plain error classified as throttle")
	}
	if isThrottle(model.NewProviderError("Internal", "boom", 500)) {
		t.Fatal("INTERNAL classified as throttle")
	}
	if isThrottle(model.NewProviderError("Unavailable", "down", 503)) {
		t.Fatal("UNAVAILABLE classified as throttle (only ResourceExhausted is)")
	}
}

// createCapacityFn creates a v2 event function with maxInstanceCount 1 — a
// single in-flight invocation slot — so a second concurrent delivery is refused
// by the admission gate. retry sets the trigger's failure policy (RETRY_POLICY).
func createCapacityFn(t *testing.T, s *Service, id string, retry bool) {
	t.Helper()
	in := FunctionInput{
		Runtime:          "nodejs20",
		EntryPoint:       "handler",
		MaxInstanceCount: 1,
		EventTrigger: &functionsstore.EventTrigger{
			EventType: "google.pubsub.topic.publish",
			Resource:  "projects/proj/topics/t",
			Retry:     retry,
		},
	}
	if _, _, err := s.CreateFunction(context.Background(), "proj", "us-central1", id, in, V2); err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
}

// dispatchToTopic dispatches a Pub/Sub event to the shared test topic.
func dispatchToTopic(ctx context.Context, s *Service, eventID, data string) {
	s.DispatchEvent(ctx, eventing.Event{
		Project: "proj", EventType: eventing.TypePubSubPublish,
		Resource: "projects/proj/topics/t", Source: eventing.SourcePubSub,
		EventID: eventID, Data: []byte(data),
	})
}

// awaitEvent polls the persisted deliveries for the record of one event.
func awaitEvent(t *testing.T, s *Service, eventID string, ready func(functionsstore.Delivery) bool) functionsstore.Delivery {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, d := range deliveries(t, s) {
			if d.EventID == eventID && ready(d) {
				return d
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivery %q did not reach the expected state: %+v", eventID, deliveries(t, s))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestDispatchThrottleBackPressureDoesNotConsumeAttempt covers FH2: a delivery
// refused by the admission gate (429 RESOURCE_EXHAUSTED) is back-pressure, not
// an invocation, so it is re-polled without advancing its attempt count; once
// the instance frees, it delivers with a single attempt. The trigger has no
// failurePolicy — back-pressure is independent of retry.
func TestDispatchThrottleBackPressureDoesNotConsumeAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exec := newBlockingExecutor()
	s, _ := newDeliveryService(t, WithExecutor(exec))
	createCapacityFn(t, s, "fn", false)
	s.Start(ctx)

	// The first delivery takes the single instance slot and blocks in the
	// executor.
	dispatchToTopic(ctx, s, "first", "first")
	waitEntered(t, exec, 1)

	// The second delivery is refused by the gate while the slot is held. Wait
	// past the first backoff (200ms): if it had been wrongly admitted it would
	// have entered the executor, so enteredCount() staying at 1 confirms the
	// throttle was exercised.
	dispatchToTopic(ctx, s, "second", "second")
	time.Sleep(300 * time.Millisecond)
	if n := exec.enteredCount(); n != 1 {
		t.Fatalf("second delivery reached the executor (entered=%d); the test did not force a throttle", n)
	}

	// Free the slot: the throttled delivery's retry is admitted and succeeds.
	exec.releaseAll()

	d := awaitEvent(t, s, "second", func(d functionsstore.Delivery) bool {
		return d.Status != functionsstore.DeliveryPending
	})
	if d.Status != functionsstore.DeliveryDelivered || d.Attempts != 1 || d.Result != "second" {
		t.Fatalf("throttled delivery = %+v (want delivered with attempts=1)", d)
	}
}

// TestDispatchPersistentThrottleDoesNotDeadLetter covers FH2: a delivery that is
// throttled past the emulator's bounded re-poll leaves the record pending with
// its attempt budget untouched and is never forwarded to the dead-letter topic,
// even under a retry policy with a configured dead-letter surface.
func TestDispatchPersistentThrottleDoesNotDeadLetter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exec := newBlockingExecutor()
	subs := &fakeSubs{dlqTopic: "dlq", maxAttempts: 2, ok: true}
	s, fs := newDeliveryService(t, WithExecutor(exec), WithSubscriptions(subs))
	createCapacityFn(t, s, "fn", true)
	setDeliverySubscription(t, fs, "fn", "sub")
	s.Start(ctx)

	// Hold the slot for the whole test so the second delivery cannot recover.
	dispatchToTopic(ctx, s, "holder", "holder")
	waitEntered(t, exec, 1)
	dispatchToTopic(ctx, s, "throttled", "throttled")

	// The bounded re-poll persists the throttle error when it gives up; the
	// initial pending record carries no error, so that is the settle signal.
	d := awaitEvent(t, s, "throttled", func(d functionsstore.Delivery) bool {
		return d.Error != ""
	})
	if d.Status != functionsstore.DeliveryPending || d.Attempts != 0 {
		t.Fatalf("persistently throttled delivery = %+v (want pending with attempts=0)", d)
	}
	if d.DeadLetterTopic != "" || subs.published != 0 {
		t.Fatalf("throttled delivery was dead-lettered: %+v published=%d", d, subs.published)
	}
	exec.releaseAll()
}
