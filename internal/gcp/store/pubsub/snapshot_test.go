package pubsub

import (
	"bytes"
	"context"
	"testing"
)

func TestMemoryMessagesSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryMessages()
	s.Put(ctx, Message{Topic: "t", MessageID: "1", Data: "aGk="})

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	dst := NewMemoryMessages()
	if err := dst.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	msgs, _ := dst.List(ctx, "t")
	if len(msgs) != 1 || msgs[0].Data != "aGk=" {
		t.Fatalf("restored messages wrong: %+v", msgs)
	}
}
