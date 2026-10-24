package sdkpubsub_test

import (
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
)

// TestPubSubOrderingKeyWithholdAndOrder pins the documented ordered-delivery
// contract: a subscription with enableMessageOrdering delivers every available
// message for an ordering key in one batch, in publish order; a key with an
// outstanding (unacked) batch is withheld until that batch is acked; and keys
// are independent, so a new key is delivered while another key is withheld.
func TestPubSubOrderingKeyWithholdAndOrder(t *testing.T) {
	c, ctx := newClient(t)
	topic := topicName("ord-topic")
	sub := subName("ord-sub")
	createTopic(t, c, ctx, topic)
	createSub(t, c, ctx, sub, topic, func(s *pubsubpb.Subscription) {
		s.EnableMessageOrdering = true
	})

	// Same key: three messages published in order.
	for _, body := range []string{"a0", "a1", "a2"} {
		publish(t, c, ctx, topic, body, func(m *pubsubpb.PubsubMessage) { m.OrderingKey = "k1" })
	}

	// Collect the k1 batch; assert publish order is preserved (dedupe so a
	// redelivery cannot inflate the count).
	var k1 []string
	seenK1 := map[string]bool{}
	deadline := time.Now().Add(8 * time.Second)
	var outstanding []*pubsubpb.ReceivedMessage
	for len(k1) < 3 && time.Now().Before(deadline) {
		rms := pullOnce(t, c, ctx, sub, 10)
		for _, rm := range rms {
			if rm.GetMessage().GetOrderingKey() != "k1" {
				continue
			}
			body := string(rm.GetMessage().GetData())
			if !seenK1[body] {
				seenK1[body] = true
				k1 = append(k1, body)
			}
			outstanding = append(outstanding, rm)
		}
		if len(k1) < 3 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if len(k1) != 3 {
		t.Fatalf("got k1 bodies %v, want the 3 published", k1)
	}
	want := []string{"a0", "a1", "a2"}
	for i := range want {
		if k1[i] != want[i] {
			t.Fatalf("k1 delivered out of publish order: got %v, want %v", k1, want)
		}
	}

	// The k1 batch is outstanding; publish a later k1 message and confirm it is
	// withheld until the batch is acked.
	publish(t, c, ctx, topic, "a3", func(m *pubsubpb.PubsubMessage) { m.OrderingKey = "k1" })
	withheld := true
	pullDeadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(pullDeadline) {
		for _, rm := range pullOnce(t, c, ctx, sub, 10) {
			if string(rm.GetMessage().GetData()) == "a3" {
				withheld = false
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !withheld {
		t.Fatalf("a3 delivered while the k1 batch was unacked, want it withheld")
	}

	// Cross-key independence: a brand-new key is delivered even though k1 is
	// still outstanding.
	publish(t, c, ctx, topic, "b0", func(m *pubsubpb.PubsubMessage) { m.OrderingKey = "k2" })
	gotB0 := false
	bDeadline := time.Now().Add(5 * time.Second)
	for !gotB0 && time.Now().Before(bDeadline) {
		for _, rm := range pullOnce(t, c, ctx, sub, 10) {
			if string(rm.GetMessage().GetData()) == "b0" {
				gotB0 = true
				ack(t, c, ctx, sub, rm.GetAckId())
			}
		}
		if !gotB0 {
			time.Sleep(25 * time.Millisecond)
		}
	}
	if !gotB0 {
		t.Fatalf("k2 message b0 was not delivered while k1 was withheld, want cross-key independence")
	}

	// Ack the withheld batch; the later a3 is now deliverable.
	for _, rm := range outstanding {
		ack(t, c, ctx, sub, rm.GetAckId())
	}
	if rm := waitForBody(t, c, ctx, sub, "a3", 8*time.Second); rm == nil {
		t.Fatalf("a3 not delivered after the k1 batch was acked")
	}
}
