package sdkpubsub_test

import (
	"context"
	"sync"
	"testing"
	"time"

	pubsub "cloud.google.com/go/pubsub/v2"
)

// TestPubSubStreamingPullReceive drives the official client's high-level
// Subscriber (the StreamingPull machinery) end to end: five messages are
// published, received through Receive, and acked; all five are observed.
func TestPubSubStreamingPullReceive(t *testing.T) {
	c, ctx := newClient(t)
	topic := topicName("stream-topic")
	sub := subName("stream-sub")
	createTopic(t, c, ctx, topic)
	createSub(t, c, ctx, sub, topic, nil)

	want := []string{"s0", "s1", "s2", "s3", "s4"}
	for _, body := range want {
		publish(t, c, ctx, topic, body, nil)
	}

	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	subscriber := c.Subscriber(sub)
	subscriber.ReceiveSettings.NumGoroutines = 1
	subscriber.ReceiveSettings.MaxOutstandingMessages = 10

	// Collect under a mutex and cancel from the callback once everything has
	// arrived, so a duplicate (at-least-once) delivery cannot block the handler.
	var mu sync.Mutex
	seen := map[string]bool{}
	done := make(chan error, 1)
	go func() {
		done <- subscriber.Receive(rctx, func(ctx context.Context, m *pubsub.Message) {
			mu.Lock()
			seen[string(m.Data)] = true
			n := len(seen)
			mu.Unlock()
			m.Ack()
			if n == len(want) {
				cancel()
			}
		})
	}()

	select {
	case <-done:
	case <-time.After(25 * time.Second):
		t.Fatalf("Subscriber.Receive did not observe all messages within 25s")
	}

	mu.Lock()
	defer mu.Unlock()
	for _, body := range want {
		if !seen[body] {
			t.Fatalf("streaming pull never delivered %q (got %v)", body, seen)
		}
	}
}
