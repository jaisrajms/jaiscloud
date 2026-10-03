package pubsub

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func TestMemoryMessagesCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryMessages()

	// Put + List (sorted by PublishTime).
	now := time.Now()
	if err := s.Put(ctx, Message{Topic: "t", MessageID: "2", Data: "aGk=", PublishTime: now}); err != nil {
		t.Fatalf("put: %v", err)
	}
	s.Put(ctx, Message{Topic: "t", MessageID: "1", Data: "aGk=", PublishTime: now.Add(-time.Minute)})

	msgs, err := s.List(ctx, "t")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(msgs) != 2 || msgs[0].MessageID != "1" || msgs[1].MessageID != "2" {
		t.Fatalf("unexpected list: %+v", msgs)
	}

	// UpdateDeliveryAttempt.
	if err := s.UpdateDeliveryAttempt(ctx, "t", "1", 7); err != nil {
		t.Fatalf("update delivery attempt: %v", err)
	}
	msgs, _ = s.List(ctx, "t")
	if msgs[0].DeliveryAttempt != 7 {
		t.Fatalf("expected delivery attempt 7, got %d", msgs[0].DeliveryAttempt)
	}

	// UpdateDeliveryAttempt on missing is a no-op (no error).
	if err := s.UpdateDeliveryAttempt(ctx, "t", "missing", 1); err != nil {
		t.Fatalf("update missing: %v", err)
	}

	// Delete.
	if err := s.Delete(ctx, "t", "1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	msgs, _ = s.List(ctx, "t")
	if len(msgs) != 1 || msgs[0].MessageID != "2" {
		t.Fatalf("expected 1 message after delete, got %+v", msgs)
	}

	// Delete missing is a no-op.
	if err := s.Delete(ctx, "t", "missing"); err != nil {
		t.Fatalf("delete missing: %v", err)
	}

	// Reset.
	s.Reset(ctx)
	msgs, _ = s.List(ctx, "t")
	if len(msgs) != 0 {
		t.Fatalf("expected empty after reset, got %+v", msgs)
	}
}

func TestMemoryMessagesNextIDMonotonic(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryMessages()

	prev := int64(0)
	for i := 0; i < 1000; i++ {
		id, err := s.NextID(ctx)
		if err != nil {
			t.Fatalf("NextID: %v", err)
		}
		n, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			t.Fatalf("non-numeric message ID %q", id)
		}
		if n <= prev {
			t.Fatalf("IDs not strictly increasing: %d then %d", prev, n)
		}
		prev = n
	}
}
