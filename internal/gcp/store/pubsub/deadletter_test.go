package pubsub

import (
	"testing"
	"time"
)

func TestDeadLetterAttributes(t *testing.T) {
	// Publish time with a non-UTC instant to prove the attribute is rendered in
	// UTC with real GCP's millisecond + numeric-offset layout.
	published := time.Date(2026, 10, 10, 4, 24, 54, 929_000_000, time.UTC)
	got := DeadLetterAttributes("parity-proj", "src-sub", 5, published)

	want := map[string]string{
		"CloudPubSubDeadLetterSourceSubscription":        "src-sub",
		"CloudPubSubDeadLetterSourceSubscriptionProject": "parity-proj",
		"CloudPubSubDeadLetterSourceDeliveryCount":       "5",
		"CloudPubSubDeadLetterSourceTopicPublishTime":    "2026-10-10T04:24:54.929+00:00",
	}
	if len(got) != len(want) {
		t.Fatalf("attribute count = %d, want %d (%v)", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
}

func TestDeadLetterAttributesUTCConversion(t *testing.T) {
	loc := time.FixedZone("UTC-5", -5*60*60)
	published := time.Date(2026, 10, 10, 4, 24, 54, 0, loc)
	got := DeadLetterAttributes("p", "s", 1, published)
	if want := "2026-10-10T09:24:54.000+00:00"; got[AttrDeadLetterSourceTopicPublishTime] != want {
		t.Errorf("publish time = %q, want %q", got[AttrDeadLetterSourceTopicPublishTime], want)
	}
}

func TestDeadLetterMessageAttributesMergesOriginals(t *testing.T) {
	published := time.Date(2026, 10, 10, 4, 24, 54, 929_000_000, time.UTC)
	original := map[string]string{"k": "v", AttrDeadLetterSourceDeliveryCount: "stale"}
	got := DeadLetterMessageAttributes("proj", "sub", 5, published, original)

	if got["k"] != "v" {
		t.Errorf("original attribute k = %q, want v", got["k"])
	}
	// The source attributes win on collision (the forwarded copy is authoritative).
	if got[AttrDeadLetterSourceDeliveryCount] != "5" {
		t.Errorf("delivery count = %q, want 5", got[AttrDeadLetterSourceDeliveryCount])
	}
	if len(got) != 5 {
		t.Errorf("attribute count = %d, want 5 (%v)", len(got), got)
	}
}

// TestForwardedMessage pins the PSM5 contract: real GCP wraps the undeliverable
// message in a new one, so the forwarded copy gets a fresh caller-minted id and
// a forward-time publishTime, while the source publish time lives only in the
// CloudPubSubDeadLetterSourceTopicPublishTime attribute. The publisher's data,
// attributes, ordering key and key material carry over, and the delivery attempt
// resets.
func TestForwardedMessage(t *testing.T) {
	sourcePublished := time.Date(2026, 10, 10, 4, 24, 54, 929_000_000, time.UTC)
	forwarded := time.Date(2026, 10, 10, 4, 25, 30, 0, time.UTC)
	src := Message{
		MessageID:       "11111111111111111",
		Data:            "Y2lwaGVy",
		Attributes:      map[string]string{"k": "v"},
		PublishTime:     sourcePublished,
		DeliveryAttempt: 6,
		OrderingKey:     "ord-key",
		KmsKeyName:      "key",
		WrappedDEK:      []byte("dek"),
	}

	got := ForwardedMessage(src, "dlq-topic", "dlq-sub", "22222222222222222", forwarded, "proj", "src-sub", 5)

	if got.MessageID != "22222222222222222" {
		t.Errorf("message id = %q, want the fresh id", got.MessageID)
	}
	if got.MessageID == src.MessageID {
		t.Errorf("message id = %q, want a fresh id != source %q", got.MessageID, src.MessageID)
	}
	if !got.PublishTime.Equal(forwarded) {
		t.Errorf("publishTime = %v, want the forward time %v", got.PublishTime, forwarded)
	}
	if got.PublishTime.Equal(src.PublishTime) {
		t.Errorf("publishTime = %v, want the forward time, not the source time %v", got.PublishTime, src.PublishTime)
	}
	if got.Topic != "dlq-topic" || got.Subscription != "dlq-sub" {
		t.Errorf("queue = %s/%s, want dlq-topic/dlq-sub", got.Topic, got.Subscription)
	}
	if got.Data != src.Data || got.KmsKeyName != src.KmsKeyName || string(got.WrappedDEK) != string(src.WrappedDEK) {
		t.Errorf("payload/key material not preserved: %+v", got)
	}
	if got.OrderingKey != "ord-key" {
		t.Errorf("ordering key = %q, want ord-key", got.OrderingKey)
	}
	if got.DeliveryAttempt != 0 {
		t.Errorf("delivery attempt = %d, want 0", got.DeliveryAttempt)
	}
	if got.Attributes["k"] != "v" {
		t.Errorf("publisher attribute k = %q, want v", got.Attributes["k"])
	}
	if base := "2026-10-10T04:24:54.929+00:00"; got.Attributes[AttrDeadLetterSourceTopicPublishTime] != base {
		t.Errorf("source publish time attr = %q, want %q", got.Attributes[AttrDeadLetterSourceTopicPublishTime], base)
	}
	if got.Attributes[AttrDeadLetterSourceSubscription] != "src-sub" {
		t.Errorf("source subscription attr = %q, want src-sub", got.Attributes[AttrDeadLetterSourceSubscription])
	}
	if got.Attributes[AttrDeadLetterSourceDeliveryCount] != "5" {
		t.Errorf("delivery count attr = %q, want 5", got.Attributes[AttrDeadLetterSourceDeliveryCount])
	}
}
