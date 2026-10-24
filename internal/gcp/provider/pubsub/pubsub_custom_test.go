package pubsub

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
)

// seedSubscription creates a topic + subscription (with a published message) and
// returns the provider.
func seedSubscription(t *testing.T) *Provider {
	t.Helper()
	ctx := context.Background()
	p := newTestProvider()
	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/t"})); err != nil {
		t.Fatalf("TopicCreate: %v", err)
	}
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/s",
		"body": map[string]any{"topic": "projects/proj/topics/t"},
	})); err != nil {
		t.Fatalf("SubscriptionCreate: %v", err)
	}
	return p
}

// TestSubscriptionModifyPushConfig covers the REST subscriptions.modifyPushConfig
// route (the logic was gRPC-only).
func TestSubscriptionModifyPushConfig(t *testing.T) {
	ctx := context.Background()
	p := seedSubscription(t)

	// Set a push config.
	if _, err := p.SubscriptionModifyPushConfig(ctx, newNR(map[string]any{
		"name": "subscriptions/s",
		"body": map[string]any{"pushConfig": map[string]any{
			"pushEndpoint": "https://example.com/push",
			"attributes":   map[string]any{"x-goog-version": "v1"},
		}},
	})); err != nil {
		t.Fatalf("ModifyPushConfig(set): %v", err)
	}
	resp, err := p.SubscriptionGet(ctx, newNR(map[string]any{"name": "subscriptions/s"}))
	if err != nil {
		t.Fatalf("SubscriptionGet: %v", err)
	}
	pc, _ := resp.Data["pushConfig"].(map[string]any)
	if pc["pushEndpoint"] != "https://example.com/push" {
		t.Fatalf("pushConfig = %v, want the endpoint", resp.Data["pushConfig"])
	}

	// An empty pushConfig clears it.
	if _, err := p.SubscriptionModifyPushConfig(ctx, newNR(map[string]any{
		"name": "subscriptions/s",
		"body": map[string]any{"pushConfig": map[string]any{}},
	})); err != nil {
		t.Fatalf("ModifyPushConfig(clear): %v", err)
	}
	resp, err = p.SubscriptionGet(ctx, newNR(map[string]any{"name": "subscriptions/s"}))
	if err != nil {
		t.Fatalf("SubscriptionGet: %v", err)
	}
	if _, ok := resp.Data["pushConfig"]; ok {
		t.Fatalf("pushConfig = %v, want it cleared", resp.Data["pushConfig"])
	}

	// A missing subscription is NotFound.
	if _, err := p.SubscriptionModifyPushConfig(ctx, newNR(map[string]any{
		"name": "subscriptions/missing", "body": map[string]any{},
	})); err == nil {
		t.Fatal("ModifyPushConfig(missing): want an error")
	}
}

// TestSubscriptionSeek covers the REST subscriptions.seek route: a time seek is
// accepted, a snapshot seek validates the target, and a request with neither is
// rejected.
func TestSubscriptionSeek(t *testing.T) {
	ctx := context.Background()
	p := seedSubscription(t)

	if _, err := p.SubscriptionSeek(ctx, newNR(map[string]any{
		"name": "subscriptions/s",
		"body": map[string]any{"time": "2020-01-01T00:00:00Z"},
	})); err != nil {
		t.Fatalf("seek(time): %v", err)
	}

	// A malformed timestamp is 400.
	if _, err := p.SubscriptionSeek(ctx, newNR(map[string]any{
		"name": "subscriptions/s",
		"body": map[string]any{"time": "not-a-time"},
	})); err == nil {
		t.Fatal("seek(bad time): want an error")
	}

	// No target is 400 (the core requires exactly one of time/snapshot).
	if _, err := p.SubscriptionSeek(ctx, newNR(map[string]any{
		"name": "subscriptions/s", "body": map[string]any{},
	})); err == nil {
		t.Fatal("seek(no target): want an error")
	}

	// An unknown snapshot is NotFound.
	_, err := p.SubscriptionSeek(ctx, newNR(map[string]any{
		"name": "subscriptions/s",
		"body": map[string]any{"snapshot": "projects/proj/snapshots/nope"},
	}))
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.HTTPStatus != 404 {
		t.Fatalf("seek(unknown snapshot): err = %v, want 404", err)
	}
}
