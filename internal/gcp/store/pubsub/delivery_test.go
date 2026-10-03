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

// TestMemoryMessagesOrderingKey verifies GCP orderingKey FIFO gating: a message
// is not delivered while an earlier message with the same key is in-flight.
func TestMemoryMessagesOrderingKey(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryMessages()
	now := time.Now()
	s.Put(ctx, Message{Topic: "t", MessageID: "1", OrderingKey: "k", PublishTime: now})
	s.Put(ctx, Message{Topic: "t", MessageID: "2", OrderingKey: "k", PublishTime: now.Add(time.Second)})

	// First pull claims only message 1 (message 2 is gated).
	msgs, _ := s.Pull(ctx, "t", 10, 10, 0, now.Add(time.Second))
	if len(msgs) != 1 || msgs[0].MessageID != "1" {
		t.Fatalf("expected only message 1, got %+v", msgs)
	}

	// Message 2 still gated while message 1 is in-flight.
	msgs, _ = s.Pull(ctx, "t", 10, 10, 0, now.Add(2*time.Second))
	if len(msgs) != 0 {
		t.Fatalf("expected message 2 gated, got %+v", msgs)
	}

	// Ack message 1 (delete) unblocks message 2.
	s.Delete(ctx, "t", "1")
	msgs, _ = s.Pull(ctx, "t", 10, 10, 0, now.Add(3*time.Second))
	if len(msgs) != 1 || msgs[0].MessageID != "2" {
		t.Fatalf("expected message 2 after ack, got %+v", msgs)
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
