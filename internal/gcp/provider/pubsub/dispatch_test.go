package pubsub

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/eventing"
)

// recordingDispatcher captures events handed to the functions delivery engine.
type recordingDispatcher struct{ events []eventing.Event }

func (r *recordingDispatcher) DispatchEvent(_ context.Context, ev eventing.Event) {
	r.events = append(r.events, ev)
}

func TestTopicPublishDispatchesFunctionEvent(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	rec := &recordingDispatcher{}
	p.SetFunctionDispatcher(rec)

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/events"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	if _, err := p.TopicPublish(ctx, newNR(map[string]any{
		"name": "topics/events",
		"body": map[string]any{"messages": []any{
			map[string]any{"data": "aGVsbG8=", "attributes": map[string]any{"k": "v"}},
		}},
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if len(rec.events) != 1 {
		t.Fatalf("expected 1 dispatched event, got %d", len(rec.events))
	}
	ev := rec.events[0]
	if ev.EventType != eventing.TypePubSubPublish || ev.Source != eventing.SourcePubSub {
		t.Fatalf("event = %+v", ev)
	}
	if ev.Resource != "projects/proj/topics/events" {
		t.Errorf("resource = %q", ev.Resource)
	}
	if string(ev.Data) != "hello" {
		t.Errorf("data = %q, want hello", ev.Data)
	}
	if ev.Attributes["k"] != "v" || ev.EventID == "" {
		t.Errorf("attributes/id = %+v / %q", ev.Attributes, ev.EventID)
	}
}

func TestPublishEventDispatchesFunctionEvent(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	rec := &recordingDispatcher{}
	p.SetFunctionDispatcher(rec)

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/events"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	// No dispatcher nil-ness, no subscriptions required.
	if _, err := p.PublishEvent(ctx, "proj", "projects/proj/topics/events", []byte("payload"), map[string]string{"a": "b"}); err != nil {
		t.Fatalf("publish event: %v", err)
	}
	if len(rec.events) != 1 || string(rec.events[0].Data) != "payload" {
		t.Fatalf("dispatched = %+v", rec.events)
	}
}
