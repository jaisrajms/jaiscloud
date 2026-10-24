package sdkpubsub_test

import (
	"context"
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestPubSubRetentionSnapshotSeek pins three retention-side behaviours: the
// subscription's messageRetentionDuration round-trips, a snapshot captures the
// subscription backlog, and seeking back to that snapshot restores messages that
// were acknowledged after the snapshot was taken.
func TestPubSubRetentionSnapshotSeek(t *testing.T) {
	c, ctx := newClient(t)
	topic := topicName("ret-topic")
	sub := subName("ret-sub")
	snap := snapName("ret-snap")
	createTopic(t, c, ctx, topic)
	createSub(t, c, ctx, sub, topic, func(s *pubsubpb.Subscription) {
		s.AckDeadlineSeconds = 30
		s.MessageRetentionDuration = durationpb.New(600 * time.Second)
	})

	got, err := c.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: sub})
	if err != nil {
		t.Fatalf("GetSubscription: %v", err)
	}
	if d := got.GetMessageRetentionDuration(); d == nil || d.AsDuration() != 600*time.Second {
		t.Fatalf("MessageRetentionDuration = %v, want 600s", d)
	}

	// Build an unacked backlog, snapshot it, then ack the messages away.
	publish(t, c, ctx, topic, "r0")
	publish(t, c, ctx, topic, "r1")
	backlog := map[string]*pubsubpb.ReceivedMessage{}
	deadline := time.Now().Add(10 * time.Second)
	for len(backlog) < 2 && time.Now().Before(deadline) {
		for _, rm := range pullOnce(t, c, ctx, sub, 10) {
			body := string(rm.GetMessage().GetData())
			if body == "r0" || body == "r1" {
				backlog[body] = rm
			}
		}
		if len(backlog) < 2 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if backlog["r0"] == nil || backlog["r1"] == nil {
		t.Fatalf("backlog = %v, want r0 and r1 delivered", backlog)
	}

	if _, err := c.SubscriptionAdminClient.CreateSnapshot(ctx, &pubsubpb.CreateSnapshotRequest{
		Name: snap, Subscription: sub,
	}); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	t.Cleanup(func() {
		dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.SubscriptionAdminClient.DeleteSnapshot(dctx, &pubsubpb.DeleteSnapshotRequest{Snapshot: snap})
	})

	ack(t, c, ctx, sub, backlog["r0"].GetAckId())
	ack(t, c, ctx, sub, backlog["r1"].GetAckId())

	// Seek back to the snapshot; both messages are deliverable again.
	if _, err := c.SubscriptionAdminClient.Seek(ctx, &pubsubpb.SeekRequest{
		Subscription: sub,
		Target:       &pubsubpb.SeekRequest_Snapshot{Snapshot: snap},
	}); err != nil {
		t.Fatalf("Seek(snapshot): %v", err)
	}

	seen := map[string]bool{}
	deadline = time.Now().Add(10 * time.Second)
	for len(seen) < 2 && time.Now().Before(deadline) {
		for _, rm := range pullOnce(t, c, ctx, sub, 10) {
			seen[string(rm.GetMessage().GetData())] = true
		}
		if len(seen) < 2 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !seen["r0"] || !seen["r1"] {
		t.Fatalf("after seek, deliverable messages = %v, want r0 and r1", seen)
	}
}

// TestPubSubSeekToTime pins the seek-to-timestamp target: seeking to now
// discards messages published before the seek so they are no longer delivered.
func TestPubSubSeekToTime(t *testing.T) {
	c, ctx := newClient(t)
	topic := topicName("seektime-topic")
	sub := subName("seektime-sub")
	createTopic(t, c, ctx, topic)
	createSub(t, c, ctx, sub, topic, nil)

	publish(t, c, ctx, topic, "before-seek")

	// Prove the message is deliverable first, then hand it back so the seek has
	// a real backlog to discard (otherwise the assertion below passes vacuously).
	rm := waitForBody(t, c, ctx, sub, "before-seek", 10*time.Second)
	release(t, c, ctx, sub, rm.GetAckId())

	// Seek to a moment in the future: every message published before it — i.e.
	// the one above — is discarded.
	if _, err := c.SubscriptionAdminClient.Seek(ctx, &pubsubpb.SeekRequest{
		Subscription: sub,
		Target:       &pubsubpb.SeekRequest_Time{Time: timestamppb.New(time.Now().Add(2 * time.Second))},
	}); err != nil {
		t.Fatalf("Seek(time): %v", err)
	}

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if bodies := pulledBodies(pullOnce(t, c, ctx, sub, 10)); len(bodies) > 0 {
			t.Fatalf("message published before the seek-to-time is still deliverable: %v", bodies)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
