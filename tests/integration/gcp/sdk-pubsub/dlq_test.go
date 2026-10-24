package sdkpubsub_test

import (
	"path"
	"testing"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
)

// TestPubSubDeadLetterForwarding pins the dead-letter contract: after
// maxDeliveryAttempts unacked deliveries, the message is forwarded to the
// dead-letter topic as a new message that preserves the publisher's attributes
// and carries real GCP's CloudPubSubDeadLetterSource* attributes.
func TestPubSubDeadLetterForwarding(t *testing.T) {
	c, ctx := newClient(t)
	srcTopic := topicName("dlq-src-topic")
	dlqTopic := topicName("dlq-topic")
	srcSub := subName("dlq-src-sub")
	dlqSub := subName("dlq-sub")

	createTopic(t, c, ctx, srcTopic)
	createTopic(t, c, ctx, dlqTopic)
	createSub(t, c, ctx, dlqSub, dlqTopic, nil)
	createSub(t, c, ctx, srcSub, srcTopic, func(s *pubsubpb.Subscription) {
		s.AckDeadlineSeconds = 10
		s.DeadLetterPolicy = &pubsubpb.DeadLetterPolicy{
			DeadLetterTopic:     dlqTopic,
			MaxDeliveryAttempts: 5,
		}
	})

	publish(t, c, ctx, srcTopic, "poison", func(m *pubsubpb.PubsubMessage) {
		m.Attributes = map[string]string{"origin": "dlqtest"}
	})

	// Drive the source subscription past maxDeliveryAttempts: each pull is one
	// delivery, immediately released so the next delivery is prompt.
	var forwarded *pubsubpb.ReceivedMessage
	originalMessageID := ""
	deadline := time.Now().Add(30 * time.Second)
	for forwarded == nil && time.Now().Before(deadline) {
		for _, rm := range pullOnce(t, c, ctx, srcSub, 10) {
			if originalMessageID == "" {
				originalMessageID = rm.GetMessage().GetMessageId()
			}
			release(t, c, ctx, srcSub, rm.GetAckId())
		}
		if dlq := pullOnce(t, c, ctx, dlqSub, 10); len(dlq) > 0 {
			forwarded = dlq[0]
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if forwarded == nil {
		t.Fatalf("message was never forwarded to the dead-letter topic")
	}

	if body := string(forwarded.GetMessage().GetData()); body != "poison" {
		t.Fatalf("forwarded body = %q, want %q", body, "poison")
	}
	// The forwarded copy is a NEW message: a fresh server-assigned id.
	if fwdID := forwarded.GetMessage().GetMessageId(); fwdID == "" || fwdID == originalMessageID {
		t.Fatalf("forwarded messageId = %q, want a fresh id distinct from the original %q", fwdID, originalMessageID)
	}
	attrs := forwarded.GetMessage().GetAttributes()
	if attrs["origin"] != "dlqtest" {
		t.Fatalf("forwarded attributes %v lost the publisher attribute origin=dlqtest", attrs)
	}
	wantSub := path.Base(srcSub)
	checks := map[string]bool{
		"CloudPubSubDeadLetterSourceSubscription":        attrs["CloudPubSubDeadLetterSourceSubscription"] == wantSub,
		"CloudPubSubDeadLetterSourceSubscriptionProject": attrs["CloudPubSubDeadLetterSourceSubscriptionProject"] == projectID(),
		"CloudPubSubDeadLetterSourceDeliveryCount":       attrs["CloudPubSubDeadLetterSourceDeliveryCount"] != "",
		"CloudPubSubDeadLetterSourceTopicPublishTime":    attrs["CloudPubSubDeadLetterSourceTopicPublishTime"] != "",
	}
	for name, ok := range checks {
		if !ok {
			t.Fatalf("forwarded message attribute %s = %q is not the documented value (attributes: %v)",
				name, attrs[name], attrs)
		}
	}
}
