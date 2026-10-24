//go:build gcp_persistence

package pubsub

import (
	"context"
	"os"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	gcpstore "jaiscloud/internal/gcp/store"
	"jaiscloud/internal/store"
)

// TestPostgresOrderedAckContract verifies the Postgres backend honors the same
// ordered-delivery ack contract as the memory store: a later ack is held behind
// an earlier unacked message and applied once it is acked, and a nack redelivers
// the message and all subsequent messages for the key, even acked ones.
func TestPostgresOrderedAckContract(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres ordered-ack test")
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

	seed := func() (time.Time, string) {
		s.Reset(ctx)
		queue := "ordered-" + clock.Now().Format("150405.000000000")
		now := clock.Now()
		for i, id := range []string{"1", "2", "3"} {
			if err := s.Put(ctx, Message{Topic: "t", Subscription: queue, MessageID: id, OrderingKey: "k", PublishTime: now.Add(time.Duration(i) * time.Second)}); err != nil {
				t.Fatalf("put %s: %v", id, err)
			}
		}
		return now, queue
	}

	// Held later ack: ack 1, ack 3 (held behind 2), ack 2 → all acked (empty).
	now, queue := seed()
	msgs, err := s.Pull(ctx, queue, 10, 10, 0, now.Add(5*time.Second))
	if err != nil || len(msgs) != 3 {
		t.Fatalf("pull = %d msgs / %v, want 3", len(msgs), err)
	}
	for _, id := range []string{"1", "3", "2"} {
		if err := s.Acknowledge(ctx, queue, id); err != nil {
			t.Fatalf("ack %s: %v", id, err)
		}
	}
	if msgs, _ = s.Pull(ctx, queue, 10, 10, 0, now.Add(6*time.Second)); len(msgs) != 0 {
		t.Fatalf("expected empty after all acked, got %d", len(msgs))
	}

	// Nack of an already-acked earlier message redelivers the whole key.
	now, queue = seed()
	if _, err := s.Pull(ctx, queue, 10, 10, 0, now.Add(5*time.Second)); err != nil {
		t.Fatalf("pull: %v", err)
	}
	for _, id := range []string{"1", "2", "3"} {
		if err := s.Acknowledge(ctx, queue, id); err != nil {
			t.Fatalf("ack %s: %v", id, err)
		}
	}
	if err := s.ModifyAckDeadline(ctx, queue, []string{queue + "/1"}, 0, now.Add(6*time.Second)); err != nil {
		t.Fatalf("nack: %v", err)
	}
	msgs, err = s.Pull(ctx, queue, 10, 10, 0, now.Add(7*time.Second))
	if err != nil || len(msgs) != 3 {
		t.Fatalf("nack of an acked message should redeliver the whole key, got %d msgs / %v", len(msgs), err)
	}
	s.Reset(ctx)
}

// TestPostgresBumpDeliveryVersions verifies the Postgres backend honors the Seek
// delivery-version contract: making a message visible again and bumping its
// version invalidates the pre-Seek ack ID without advancing the dead-letter
// delivery attempt, and the next pull mints a fresh version.
func TestPostgresBumpDeliveryVersions(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping Postgres delivery-version test")
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
	s.Reset(ctx)
	queue := "seek-" + clock.Now().Format("150405.000000000")
	now := clock.Now()
	if err := s.Put(ctx, Message{Topic: "t", Subscription: queue, MessageID: "1", PublishTime: now}); err != nil {
		t.Fatalf("put: %v", err)
	}
	msgs, err := s.Pull(ctx, queue, 10, 10, 0, now)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("pull = %d msgs / %v", len(msgs), err)
	}
	if msgs[0].DeliveryVersion != 1 || msgs[0].DeliveryAttempt != 1 {
		t.Fatalf("first delivery = version %d / attempt %d, want 1/1", msgs[0].DeliveryVersion, msgs[0].DeliveryAttempt)
	}
	// Seek's store effect: make the message visible, then advance its version.
	if err := s.ModifyAckDeadline(ctx, queue, []string{queue + "/1"}, 0, now); err != nil {
		t.Fatalf("seek visibility reset: %v", err)
	}
	if err := s.BumpDeliveryVersions(ctx, queue, []string{"1"}); err != nil {
		t.Fatalf("BumpDeliveryVersions: %v", err)
	}
	stored, _ := s.List(ctx, queue)
	if len(stored) != 1 || stored[0].DeliveryVersion != 2 || stored[0].DeliveryAttempt != 1 {
		t.Fatalf("after seek = %+v, want version 2 / attempt 1", stored)
	}
	if AckIDCurrent(stored[0], msgs[0].DeliveryVersion, now) {
		t.Fatal("pre-Seek version still current, want invalidated")
	}
	msgs, err = s.Pull(ctx, queue, 10, 10, 0, now)
	if err != nil || len(msgs) != 1 || msgs[0].DeliveryVersion != 3 {
		t.Fatalf("redelivery = %+v / %v, want version 3", msgs, err)
	}
	s.Reset(ctx)
}
