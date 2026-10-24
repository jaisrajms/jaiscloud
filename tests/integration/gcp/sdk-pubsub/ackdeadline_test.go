package sdkpubsub_test

import (
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
)

// TestPubSubAckDeadlineAndRedelivery pins the ack-deadline contract: a
// subscription's configured AckDeadlineSeconds round-trips, ModifyAckDeadline(0)
// hands a message straight back so the next pull redelivers it with a fresh ack
// id, and extending the deadline keeps an unacked message from being redelivered.
func TestPubSubAckDeadlineAndRedelivery(t *testing.T) {
	c, ctx := newClient(t)
	topic := topicName("deadline-topic")
	sub := subName("deadline-sub")
	createTopic(t, c, ctx, topic)
	createSub(t, c, ctx, sub, topic, func(s *pubsubpb.Subscription) { s.AckDeadlineSeconds = 30 })

	got, err := c.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: sub})
	if err != nil {
		t.Fatalf("GetSubscription: %v", err)
	}
	if got.GetAckDeadlineSeconds() != 30 {
		t.Fatalf("AckDeadlineSeconds = %d, want the configured 30", got.GetAckDeadlineSeconds())
	}

	// Immediate redelivery via ModifyAckDeadline(0).
	publish(t, c, ctx, topic, "redeliver")
	rm1 := waitForBody(t, c, ctx, sub, "redeliver", 10*time.Second)
	release(t, c, ctx, sub, rm1.GetAckId())
	rm2 := waitForBody(t, c, ctx, sub, "redeliver", 10*time.Second)
	if rm2.GetAckId() == rm1.GetAckId() {
		t.Fatalf("redelivery reused ack id %q, want a fresh id", rm1.GetAckId())
	}
	ack(t, c, ctx, sub, rm2.GetAckId())

	// Extending the deadline holds the message back; releasing it delivers again.
	publish(t, c, ctx, topic, "hold")
	rm3 := waitForBody(t, c, ctx, sub, "hold", 10*time.Second)
	if err := c.SubscriptionAdminClient.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
		Subscription: sub, AckIds: []string{rm3.GetAckId()}, AckDeadlineSeconds: 600,
	}); err != nil {
		t.Fatalf("ModifyAckDeadline(extend): %v", err)
	}
	holdDeadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(holdDeadline) {
		if bodies := pulledBodies(pullOnce(t, c, ctx, sub, 10)); len(bodies) > 0 {
			t.Fatalf("message redelivered while its ack deadline was extended: %v", bodies)
		}
		time.Sleep(25 * time.Millisecond)
	}
	release(t, c, ctx, sub, rm3.GetAckId())
	if rm := waitForBody(t, c, ctx, sub, "hold", 10*time.Second); rm == nil {
		t.Fatalf("message not redelivered after it was released")
	}
}
