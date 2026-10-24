package pubsub

import (
	"strconv"
	"time"
)

// Dead-letter source attribute keys. Real GCP wraps a forwarded message in a
// new one and adds these attributes identifying the subscription the message
// could not be delivered to. A fifth attribute,
// CloudPubSubDeadLetterSourceDeliveryErrorMessage, exists only for export
// subscriptions and is intentionally not modelled.
const (
	AttrDeadLetterSourceSubscription        = "CloudPubSubDeadLetterSourceSubscription"
	AttrDeadLetterSourceSubscriptionProject = "CloudPubSubDeadLetterSourceSubscriptionProject"
	AttrDeadLetterSourceDeliveryCount       = "CloudPubSubDeadLetterSourceDeliveryCount"
	AttrDeadLetterSourceTopicPublishTime    = "CloudPubSubDeadLetterSourceTopicPublishTime"
)

// deadLetterPublishTimeLayout matches real GCP's attribute format: RFC3339 with
// millisecond precision and a numeric offset (a UTC time renders as "+00:00",
// not "Z"). The recorded differential golden folds the timestamp value, so this
// exact layout is pinned by the unit tests, not the golden.
const deadLetterPublishTimeLayout = "2006-01-02T15:04:05.000-07:00"

// DeadLetterAttributes returns the CloudPubSubDeadLetterSource* attributes real
// GCP attaches when it forwards a message to a subscription's dead-letter topic.
//
//   - subscription is the source subscription's short id (not the full
//     "projects/{p}/subscriptions/{s}" name) — pinned by the recorded golden.
//   - project is the source subscription's project id.
//   - deliveryCount is the number of delivery attempts made to the source
//     subscription before forwarding; real GCP reports the configured
//     maxDeliveryAttempts.
//   - publishedAt is the original message's publish time.
func DeadLetterAttributes(project, subscription string, deliveryCount int, publishedAt time.Time) map[string]string {
	return map[string]string{
		AttrDeadLetterSourceSubscription:        subscription,
		AttrDeadLetterSourceSubscriptionProject: project,
		AttrDeadLetterSourceDeliveryCount:       strconv.Itoa(deliveryCount),
		AttrDeadLetterSourceTopicPublishTime:    publishedAt.UTC().Format(deadLetterPublishTimeLayout),
	}
}

// DeadLetterMessageAttributes merges the original message's attributes with the
// CloudPubSubDeadLetterSource* attributes, mirroring real GCP (which preserves
// the publisher's attributes on the forwarded copy). The source attributes win
// on a key collision.
func DeadLetterMessageAttributes(project, subscription string, deliveryCount int, publishedAt time.Time, original map[string]string) map[string]string {
	out := make(map[string]string, len(original)+4)
	for k, v := range original {
		out[k] = v
	}
	for k, v := range DeadLetterAttributes(project, subscription, deliveryCount, publishedAt) {
		out[k] = v
	}
	return out
}

// ForwardedMessage builds the copy of src that real GCP publishes to a
// subscription's dead-letter topic. Real GCP wraps the undeliverable message in
// a new message (PSM5, verified live 2026-10): the forwarded copy gets a fresh
// server-assigned messageId and a publishTime of the forward instant, while the
// *original* publish time is preserved in the
// CloudPubSubDeadLetterSourceTopicPublishTime attribute. The publisher's data,
// own attributes, ordering key and key material carry onto the copy, and the
// delivery attempt resets to 0 (the source delivery count is reported in the
// CloudPubSubDeadLetterSourceDeliveryCount attribute instead).
//
// messageID is the id the caller allocated via Messages.NextID; topic and
// subscription name the dead-letter topic's delivery queue; forwardTime is the
// forward instant (clock.Now()); project and sourceSubscription identify the
// subscription the message could not be delivered to; deliveryCount is the
// delivery count made to that source subscription.
func ForwardedMessage(src Message, topic, subscription, messageID string, forwardTime time.Time, project, sourceSubscription string, deliveryCount int) Message {
	return Message{
		Topic:           topic,
		Subscription:    subscription,
		MessageID:       messageID,
		Data:            src.Data,
		Attributes:      DeadLetterMessageAttributes(project, sourceSubscription, deliveryCount, src.PublishTime, src.Attributes),
		PublishTime:     forwardTime,
		DeliveryAttempt: 0,
		OrderingKey:     src.OrderingKey,
		KmsKeyName:      src.KmsKeyName,
		WrappedDEK:      src.WrappedDEK,
	}
}
