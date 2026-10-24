package pubsub

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
)

// setupLongPoll creates a provider with a topic and subscription, ready for
// publishing/pulling. Returns the provider.
func setupLongPoll(t *testing.T) *Provider {
	t.Helper()
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.TopicCreate(ctx, newNR(map[string]any{"name": "topics/my-topic"})); err != nil {
		t.Fatalf("topic create: %v", err)
	}
	if _, err := p.SubscriptionCreate(ctx, newNR(map[string]any{
		"name": "subscriptions/my-sub",
		"body": map[string]any{"topic": "projects/proj/topics/my-topic"},
	})); err != nil {
		t.Fatalf("subscription create: %v", err)
	}
	return p
}

// TestPubSubLongPollReturnImmediately verifies that returnImmediately=true on an
// empty subscription returns immediately with no messages (no long-poll wait).
func TestPubSubLongPollReturnImmediately(t *testing.T) {
	ctx := context.Background()
	p := setupLongPoll(t)

	start := time.Now()
	resp, err := p.SubscriptionPull(ctx, newNR(map[string]any{
		"name": "subscriptions/my-sub",
		"body": map[string]any{"returnImmediately": true},
	}))
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	elapsed := time.Since(start)
	received, _ := resp.Data["receivedMessages"].([]any)
	if len(received) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(received))
	}
	if elapsed > longPollTimeout {
		t.Fatalf("returnImmediately=true took %v; expected immediate return", elapsed)
	}
}

// TestPubSubLongPollBounded verifies that returnImmediately=false (unset) on an
// empty subscription returns within a bounded time — it waits, but does not hang.
func TestPubSubLongPollBounded(t *testing.T) {
	ctx := context.Background()
	p := setupLongPoll(t)

	start := time.Now()
	resp, err := p.SubscriptionPull(ctx, newNR(map[string]any{"name": "subscriptions/my-sub"}))
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	elapsed := time.Since(start)
	received, _ := resp.Data["receivedMessages"].([]any)
	if len(received) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(received))
	}
	// The point is that it does not hang. Allow generous headroom over the 1s
	// window for slow CI machines.
	if elapsed > 3*time.Second {
		t.Fatalf("empty long-poll took %v; expected bounded return", elapsed)
	}
}

// TestPubSubLongPollBoundedFrozenClock verifies the long-poll window is real
// wall time, not the (possibly frozen) business clock: with the global clock
// frozen, an empty pull must still return within the bounded window rather than
// polling forever (the loop comparison uses clock.RealNow, so a frozen business
// clock cannot stall it). Regression for the clock.Now() deadline (TD15).
func TestPubSubLongPollBoundedFrozenClock(t *testing.T) {
	at := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: at})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })

	p := setupLongPoll(t)

	done := make(chan int, 1)
	go func() {
		resp, err := p.SubscriptionPull(context.Background(), newNR(map[string]any{"name": "subscriptions/my-sub"}))
		if err != nil {
			t.Errorf("pull: %v", err)
			done <- -1
			return
		}
		received, _ := resp.Data["receivedMessages"].([]any)
		done <- len(received)
	}()

	select {
	case n := <-done:
		if n != 0 {
			t.Fatalf("expected 0 messages, got %d", n)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("empty long-poll did not return under a frozen clock; deadline used the business clock")
	}
}

// TestPubSubLongPollDeliversLateMessage verifies that a long-poll waits for a
// message published shortly after the pull starts and delivers it.
func TestPubSubLongPollDeliversLateMessage(t *testing.T) {
	ctx := context.Background()
	p := setupLongPoll(t)

	done := make(chan struct{})
	go func() {
		time.Sleep(200 * time.Millisecond)
		_, err := p.TopicPublish(ctx, newNR(map[string]any{
			"name": "topics/my-topic",
			"body": map[string]any{"messages": []any{map[string]any{"data": "aGVsbG8="}}},
		}))
		if err != nil {
			t.Errorf("publish: %v", err)
		}
		close(done)
	}()

	resp, err := p.SubscriptionPull(ctx, newNR(map[string]any{"name": "subscriptions/my-sub"}))
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	<-done

	received, _ := resp.Data["receivedMessages"].([]any)
	if len(received) != 1 {
		t.Fatalf("expected 1 message delivered via long-poll, got %d", len(received))
	}
	rm := received[0].(map[string]any)
	msg := rm["message"].(map[string]any)
	if msg["data"] != "aGVsbG8=" {
		t.Errorf("unexpected message data: %v", msg["data"])
	}
}
