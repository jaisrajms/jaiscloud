package pubsub

import (
	"context"
	"testing"
)

// TestSubscriptionExactlyOnceDelivery verifies the REST surface honors
// enableExactlyOnceDelivery: it round-trips, defaults the ack deadline to 60s,
// is omitted when false (matching real Pub/Sub's JSON), and rejects the
// unsupported push + exactly-once combination.
func TestSubscriptionExactlyOnceDelivery(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/eod"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}

	// Exactly-once, no explicit ack deadline → 60-second default.
	resp, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/eod",
		"body": map[string]any{
			"topic":                     "projects/proj/topics/eod",
			"enableExactlyOnceDelivery": true,
		},
	}))
	if err != nil {
		t.Fatalf("subscription create: %v", err)
	}
	if eod, _ := resp.Data["enableExactlyOnceDelivery"].(bool); !eod {
		t.Fatalf("enableExactlyOnceDelivery = %v, want true", resp.Data["enableExactlyOnceDelivery"])
	}
	if ad, _ := resp.Data["ackDeadlineSeconds"].(int); ad != 60 {
		t.Fatalf("ackDeadlineSeconds = %v, want 60 for exactly-once", resp.Data["ackDeadlineSeconds"])
	}

	// Get echoes it.
	got, err := p.SubscriptionGet(ctx, newNR(map[string]any{"name": "subscriptions/eod"}))
	if err != nil {
		t.Fatalf("subscription get: %v", err)
	}
	if eod, _ := got.Data["enableExactlyOnceDelivery"].(bool); !eod {
		t.Fatalf("get enableExactlyOnceDelivery = %v, want true", got.Data["enableExactlyOnceDelivery"])
	}

	// A plain subscription omits the flag entirely (real Pub/Sub drops false
	// booleans; the differential goldens depend on this).
	plain, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/plain",
		"body": map[string]any{"topic": "projects/proj/topics/eod"},
	}))
	if err != nil {
		t.Fatalf("plain subscription create: %v", err)
	}
	if _, present := plain.Data["enableExactlyOnceDelivery"]; present {
		t.Fatal("plain subscription response must omit enableExactlyOnceDelivery")
	}

	// Exactly-once + push is unsupported.
	_, err = p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/eod-push",
		"body": map[string]any{
			"topic":                     "projects/proj/topics/eod",
			"enableExactlyOnceDelivery": true,
			"pushConfig":                map[string]any{"pushEndpoint": "https://example.invalid/push"},
		},
	}))
	if err == nil {
		t.Fatal("exactly-once + push create = nil, want InvalidArgument")
	}
	if status := errStatus(err); status != 400 {
		t.Fatalf("exactly-once + push HTTP status = %d, want 400 (%v)", status, err)
	}
}

// TestSubscriptionAckIDDedup verifies an acked message is not redelivered over
// REST and that the emulator treats a repeat of the same ack id as an
// idempotent no-op (see the gRPC ackRegistry limitation note on superseded ack
// ids).
func TestSubscriptionAckIDDedup(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/ack"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/ack",
		"body": map[string]any{
			"topic":                     "projects/proj/topics/ack",
			"enableExactlyOnceDelivery": true,
		},
	})); err != nil {
		t.Fatalf("subscription create: %v", err)
	}
	if _, err := p.TopicPublish(ctx, newNR(map[string]any{
		"name": "topics/ack",
		"body": map[string]any{"messages": []any{map[string]any{"data": "aGk="}}},
	})); err != nil {
		t.Fatalf("publish: %v", err)
	}

	pull, err := p.SubscriptionPull(ctx, newNR(map[string]any{"name": "subscriptions/ack"}))
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	received, _ := pull.Data["receivedMessages"].([]any)
	if len(received) != 1 {
		t.Fatalf("pull received %d messages, want 1", len(received))
	}
	ackID := received[0].(map[string]any)["ackId"].(string)

	for i := 0; i < 2; i++ {
		if _, err := p.SubscriptionAcknowledge(ctx, newNR(map[string]any{
			"name": "subscriptions/ack",
			"body": map[string]any{"ackIds": []any{ackID}},
		})); err != nil {
			t.Fatalf("ack #%d: %v", i+1, err)
		}
	}
	after, err := p.SubscriptionPull(ctx, newNR(map[string]any{
		"name": "subscriptions/ack",
		"body": map[string]any{"returnImmediately": true},
	}))
	if err != nil {
		t.Fatalf("pull after ack: %v", err)
	}
	if got, _ := after.Data["receivedMessages"].([]any); len(got) != 0 {
		t.Fatalf("acked message redelivered: %d", len(got))
	}
}
