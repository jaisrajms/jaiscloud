package pubsub

import (
	"context"
	"testing"
	"time"
)

// TestMemoryMessagesPullVisibility verifies the ack-deadline redelivery state
// machine: claim → invisible for the deadline → redeliver after it expires.
func TestMemoryMessagesPullVisibility(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryMessages()
	now := time.Now()
	s.Put(ctx, Message{Topic: "t", MessageID: "1", PublishTime: now})

	// Claim.
	msgs, err := s.Pull(ctx, "t", 10, 10, 0, now)
	if err != nil || len(msgs) != 1 || msgs[0].DeliveryAttempt != 1 {
		t.Fatalf("first pull = %+v / %v", msgs, err)
	}

	// Still invisible within the 10s deadline.
	msgs, _ = s.Pull(ctx, "t", 10, 10, 0, now.Add(5*time.Second))
	if len(msgs) != 0 {
		t.Fatalf("expected invisible within deadline, got %+v", msgs)
	}

	// Redelivered after the deadline; delivery attempt increments.
	msgs, _ = s.Pull(ctx, "t", 10, 10, 0, now.Add(11*time.Second))
	if len(msgs) != 1 || msgs[0].DeliveryAttempt != 2 {
		t.Fatalf("expected redelivery with attempt 2, got %+v", msgs)
	}
}

// TestMemoryMessagesBumpDeliveryVersions verifies the Seek contract: bumping a
// message's delivery version invalidates a previously issued ack ID (the version
// is encoded per delivery), without advancing the dead-letter DeliveryAttempt
// counter, and the message stays available for redelivery.
func TestMemoryMessagesBumpDeliveryVersions(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryMessages()
	now := time.Now()
	s.Put(ctx, Message{Topic: "t", MessageID: "1", PublishTime: now})

	msgs, err := s.Pull(ctx, "t", 10, 10, 0, now)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("pull = %+v / %v", msgs, err)
	}
	first := msgs[0]
	if first.DeliveryVersion != 1 || first.DeliveryAttempt != 1 {
		t.Fatalf("first delivery = version %d / attempt %d, want 1/1", first.DeliveryVersion, first.DeliveryAttempt)
	}
	// A Seek makes the message visible and advances only the ack-ID version.
	if err := s.ModifyAckDeadline(ctx, "t", []string{"t/1"}, 0, now); err != nil {
		t.Fatalf("seek visibility reset: %v", err)
	}
	if err := s.BumpDeliveryVersions(ctx, "t", []string{"1"}); err != nil {
		t.Fatalf("BumpDeliveryVersions: %v", err)
	}
	stored, _ := s.List(ctx, "t")
	if len(stored) != 1 || stored[0].DeliveryVersion != 2 || stored[0].DeliveryAttempt != 1 {
		t.Fatalf("after seek = version %d / attempt %d, want 2/1", stored[0].DeliveryVersion, stored[0].DeliveryAttempt)
	}
	// The pre-Seek ack ID no longer names the current delivery.
	if AckIDCurrent(stored[0], first.DeliveryVersion, now) {
		t.Fatal("pre-Seek version still current, want invalidated")
	}
	// The redelivered message gets a fresh version and is current.
	msgs, _ = s.Pull(ctx, "t", 10, 10, 0, now)
	if len(msgs) != 1 || msgs[0].DeliveryVersion != 3 {
		t.Fatalf("redelivery = %+v, want version 3", msgs)
	}
	if !AckIDCurrent(msgs[0], msgs[0].DeliveryVersion, now) {
		t.Fatal("post-Seek redelivery version not current")
	}
}

// TestMemoryMessagesOrderingKey verifies the real-GCP ordered pull contract: a
// key's available messages are delivered together, in publish order, and the
// key is withheld while that batch is outstanding (only one batch at a time).
func TestMemoryMessagesOrderingKey(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryMessages()
	now := time.Now()
	s.Put(ctx, Message{Topic: "t", MessageID: "1", OrderingKey: "k", PublishTime: now})
	s.Put(ctx, Message{Topic: "t", MessageID: "2", OrderingKey: "k", PublishTime: now.Add(time.Second)})

	// First pull returns the whole key batch in publish order.
	msgs, _ := s.Pull(ctx, "t", 10, 10, 0, now.Add(2*time.Second))
	if len(msgs) != 2 || msgs[0].MessageID != "1" || msgs[1].MessageID != "2" {
		t.Fatalf("expected both messages in order, got %+v", msgs)
	}

	// The key is gated while the batch is in-flight.
	if msgs, _ = s.Pull(ctx, "t", 10, 10, 0, now.Add(3*time.Second)); len(msgs) != 0 {
		t.Fatalf("expected the key gated while in-flight, got %+v", msgs)
	}

	// Acking the batch clears the key (acks are retained, not redelivered).
	if err := s.Acknowledge(ctx, "t", "1"); err != nil {
		t.Fatalf("ack 1: %v", err)
	}
	if err := s.Acknowledge(ctx, "t", "2"); err != nil {
		t.Fatalf("ack 2: %v", err)
	}
	if msgs, _ = s.Pull(ctx, "t", 10, 10, 0, now.Add(4*time.Second)); len(msgs) != 0 {
		t.Fatalf("expected empty after ack, got %+v", msgs)
	}
}

// TestMemoryMessagesOrderedAckContract pins the recorded real-GCP behavior:
//   - a later ack is held while an earlier message for the key is unacked, then
//     applied once the earlier message is acked;
//   - a nack/expiry redelivers the message and all subsequent messages for the
//     key, even already-acknowledged ones.
func TestMemoryMessagesOrderedAckContract(t *testing.T) {
	ctx := context.Background()

	newStore := func() (*MemoryMessages, time.Time) {
		s := NewMemoryMessages()
		now := time.Now()
		for i, id := range []string{"1", "2", "3"} {
			s.Put(ctx, Message{Topic: "t", MessageID: id, OrderingKey: "k", PublishTime: now.Add(time.Duration(i) * time.Second)})
		}
		return s, now
	}

	// Held later ack: ack 3 while 2 is unacked, then ack 2 → all acked (empty).
	s, now := newStore()
	_, _ = s.Pull(ctx, "t", 10, 10, 0, now.Add(5*time.Second)) // claim 1,2,3
	s.Acknowledge(ctx, "t", "1")
	s.Acknowledge(ctx, "t", "3") // held behind 2
	if m, _ := s.messages["t"]["3"]; m.Acked {
		t.Fatalf("ack of 3 should be held while 2 is unacked")
	}
	s.Acknowledge(ctx, "t", "2") // applies, cascading the held ack of 3
	if m, _ := s.messages["t"]["3"]; !m.Acked {
		t.Fatalf("held ack of 3 should apply once 2 is acked")
	}
	if msgs, _ := s.Pull(ctx, "t", 10, 10, 0, now.Add(6*time.Second)); len(msgs) != 0 {
		t.Fatalf("expected empty after all acked, got %+v", msgs)
	}

	// Nack of an already-acked earlier message redelivers it and all later ones.
	s, now = newStore()
	_, _ = s.Pull(ctx, "t", 10, 10, 0, now.Add(5*time.Second))
	s.Acknowledge(ctx, "t", "1")
	s.Acknowledge(ctx, "t", "2")
	s.Acknowledge(ctx, "t", "3")
	if err := s.ModifyAckDeadline(ctx, "t", []string{"t/1"}, 0, now.Add(6*time.Second)); err != nil {
		t.Fatalf("nack: %v", err)
	}
	msgs, _ := s.Pull(ctx, "t", 10, 10, 0, now.Add(7*time.Second))
	if len(msgs) != 3 || msgs[0].MessageID != "1" || msgs[1].MessageID != "2" || msgs[2].MessageID != "3" {
		t.Fatalf("nack of an acked message should redeliver all of the key, got %+v", msgs)
	}
}

// TestMemoryMessagesModifyAckDeadline verifies seconds=0 makes a message
// immediately visible and a positive value extends its deadline.
func TestMemoryMessagesModifyAckDeadline(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryMessages()
	now := time.Now()
	s.Put(ctx, Message{Topic: "t", MessageID: "1", PublishTime: now})

	msgs, _ := s.Pull(ctx, "t", 10, 10, 0, now)
	if len(msgs) != 1 {
		t.Fatalf("expected claim, got %+v", msgs)
	}
	// seconds=0 → immediately visible again.
	s.ModifyAckDeadline(ctx, "t", []string{"t/1"}, 0, now)
	msgs, _ = s.Pull(ctx, "t", 10, 10, 0, now)
	if len(msgs) != 1 || msgs[0].DeliveryAttempt != 2 {
		t.Fatalf("expected immediate redelivery, got %+v", msgs)
	}
}
