// Package pubsub provides the Pub/Sub message store. Messages live in the
// dedicated jc_pubsub_messages table; topics and subscriptions remain
// control-plane metadata in the generic ResourceStore (mirroring how SQS queues
// stay in jc_resources while messages get a dedicated table).
package pubsub

import (
	"context"
	"time"
)

// Message is a single published Pub/Sub message.
type Message struct {
	Topic string
	// Subscription is the delivery queue this message was fanned out to. Pub/Sub
	// delivers a copy to every subscription of a topic, so messages are stored
	// per subscription (not once per topic); Ack/ModifyAckDeadline act on this
	// subscription's copy. Empty for legacy/topic-keyed rows.
	Subscription    string
	MessageID       string
	Data            string
	Attributes      map[string]string
	PublishTime     time.Time
	DeliveryAttempt int
	OrderingKey     string    // GCP orderingKey (FIFO group, like SQS MessageGroupId)
	VisibleAt       time.Time // when the message becomes visible again (ack deadline)
	// KmsKeyName is the CMEK key name (empty when server-DEK encrypted). The
	// Data field stores base64(AES-GCM ciphertext) when envelope encryption is
	// active; WrappedDEK is the DEK wrapped by KmsKeyName.
	KmsKeyName string
	WrappedDEK []byte
}

// queueKey is the delivery queue a message belongs to: the subscription ID for
// fanned-out messages, falling back to the logical topic for legacy rows.
func (m Message) queueKey() string {
	if m.Subscription != "" {
		return m.Subscription
	}
	return m.Topic
}

// Messages is the Pub/Sub message store. Every operation is keyed by a delivery
// queue: a subscription ID for fanned-out messages (the normal case), or a
// topic for legacy/topic-keyed rows.
type Messages interface {
	// NextID allocates the next monotonic message ID. The memory backend uses a
	// process-local counter; the Postgres backend uses a database sequence so IDs
	// remain monotonic across restarts (mirrors SQS jc_sqs_fifo_seq).
	NextID(ctx context.Context) (string, error)
	Put(ctx context.Context, m Message) error
	// List returns all messages for a queue, sorted by publish time (no claim).
	List(ctx context.Context, queue string) ([]Message, error)
	// Pull atomically claims up to maxMessages eligible messages, marking each
	// invisible until now+ackDeadlineSec and incrementing its delivery attempt.
	// retentionSec filters out messages older than the topic's retention.
	Pull(ctx context.Context, queue string, maxMessages, ackDeadlineSec, retentionSec int, now time.Time) ([]Message, error)
	Delete(ctx context.Context, queue, messageID string) error
	UpdateDeliveryAttempt(ctx context.Context, queue, messageID string, attempt int) error
	// ModifyAckDeadline resets the visibility deadline for the given ack IDs
	// (each "queue/messageID"); seconds==0 makes them immediately visible.
	ModifyAckDeadline(ctx context.Context, queue string, ackIDs []string, seconds int, now time.Time) error
	Reset(ctx context.Context)
}
