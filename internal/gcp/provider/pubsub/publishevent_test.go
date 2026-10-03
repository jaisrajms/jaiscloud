package pubsub

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
)

func TestPublishEventFanOut(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/events"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/sub",
		"body": map[string]any{"topic": "projects/proj/topics/events"},
	})); err != nil {
		t.Fatalf("subscription create: %v", err)
	}

	// A long-form topic name (as stored by GCS notificationConfigs) resolves.
	id, err := p.PublishEvent(ctx, "proj", "//pubsub.googleapis.com/projects/proj/topics/events",
		[]byte("hello"), map[string]string{"eventType": "OBJECT_FINALIZE"})
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}
	if id == "" {
		t.Fatal("expected a message id")
	}

	resp, err := p.SubscriptionPull(ctx, newNR(map[string]any{"name": "subscriptions/sub"}))
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	received, _ := resp.Data["receivedMessages"].([]any)
	if len(received) != 1 {
		t.Fatalf("expected 1 received message, got %d (%v)", len(received), resp.Data)
	}
	msg := received[0].(map[string]any)["message"].(map[string]any)
	if got, _ := msg["data"].(string); got != "aGVsbG8=" {
		t.Errorf("data = %q, want aGVsbG8=", got)
	}
	attrs, _ := msg["attributes"].(map[string]string)
	if attrs["eventType"] != "OBJECT_FINALIZE" {
		t.Errorf("eventType attr = %q", attrs["eventType"])
	}
}

func TestPublishEventMissingTopic(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()
	_, err := p.PublishEvent(ctx, "proj", "projects/proj/topics/nope", []byte("x"), nil)
	if err == nil {
		t.Fatal("expected publish to a missing topic to fail")
	}
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 404 {
		t.Fatalf("expected 404, got %v", err)
	}
}

func TestTopicShortIDForms(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"events", "events"},
		{"topics/events", "events"},
		{"projects/proj/topics/events", "events"},
		{"//pubsub.googleapis.com/projects/proj/topics/events", "events"},
	} {
		if got := topicShortID(tc.in); got != tc.want {
			t.Errorf("topicShortID(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
