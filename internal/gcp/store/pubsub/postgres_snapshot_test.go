//go:build gcp_persistence

package pubsub

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresMessagesSnapshotColumns verifies that ordering_key and a non-zero
// visible_at survive a Postgres Snapshot/Restore round trip (the memory store
// serialises the whole Message struct and therefore already preserves them; the
// Postgres Snapshot previously omitted both columns).
func TestPostgresMessagesSnapshotColumns(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres snapshot test")
	}
	ctx := context.Background()

	pg, err := store.NewPostgresResourceStore(ctx, dsn, "gcp")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pg.Close()
	if err := store.RunMigrations(ctx, pg.Pool(), "gcp", gcpstore.MigrationFS, "gcp"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	s := NewPostgresMessages(pg.Pool())
	topic := "snap-topic"
	// Postgres timestamptz has microsecond precision; truncate the expected value
	// to match what the round trip can preserve.
	visible := clock.Now().Add(5 * time.Minute).Truncate(time.Microsecond)

	if err := s.Put(ctx, Message{
		Topic: topic, MessageID: "m1", Data: "aGk=",
		OrderingKey: "group-1", VisibleAt: visible,
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Restore deletes and re-inserts the whole table; a fresh store reads back.
	if err := s.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	msgs, err := s.List(ctx, topic)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 restored message, got %d", len(msgs))
	}
	got := msgs[0]
	if got.OrderingKey != "group-1" {
		t.Errorf("ordering_key = %q, want group-1", got.OrderingKey)
	}
	if got.VisibleAt.IsZero() {
		t.Error("visible_at was not restored (zero time)")
	}
	if !got.VisibleAt.Equal(visible) {
		t.Errorf("visible_at = %v, want %v", got.VisibleAt, visible)
	}
}
