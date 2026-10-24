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
	// DeliveryVersion is the exactly-once acknowledgement-ID version of the
	// message's current delivery, encoded in the wire ack ID. It is deliberately
	// distinct from DeliveryAttempt (which drives dead-letter accounting and must
	// not be advanced by a Seek): a claim advances both, but only a Seek advances
	// DeliveryVersion, so a Seek invalidates outstanding ack IDs without skewing
	// the delivery-attempt count. See store/pubsub/ackid.go.
	DeliveryVersion int
	OrderingKey     string    // GCP orderingKey (FIFO group, like SQS MessageGroupId)
	VisibleAt       time.Time // when the message becomes visible again (ack deadline)
	// Acked marks a keyed message that has been acknowledged. Ordered messages
	// are retained after ack (not deleted) so that a redelivery of an earlier
	// message for the key can redeliver it too — real GCP redelivers all
	// subsequent messages for an ordering key, even acknowledged ones.
	// Unordered messages are deleted on ack and never set this.
	Acked bool
	// AckPending marks a keyed message whose ack arrived while an earlier
	// message for the same key was still unacknowledged. Real GCP accepts the
	// later ack but holds it: it applies only once every earlier message for the
	// key is acked (and is discarded if an earlier message is redelivered).
	AckPending bool
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
	// invisible until now+ackDeadlineSec and advancing its delivery attempt and
	// ack-ID version. retentionSec filters out messages older than the topic's
	// retention.
	Pull(ctx context.Context, queue string, maxMessages, ackDeadlineSec, retentionSec int, now time.Time) ([]Message, error)
	// Acknowledge applies an ack to one message (by message ID). An unordered
	// message is deleted. A keyed message is retained and acked under the GCP
	// ordered-delivery contract: the ack is held (AckPending) while an earlier
	// message for the same ordering key is still unacknowledged, and is applied
	// — cascading through any held later acks — only when every earlier message
	// is acked. Redelivering an earlier message (ModifyAckDeadline 0) clears the
	// held acks so those later messages are redelivered too.
	Acknowledge(ctx context.Context, queue, messageID string) error
	Delete(ctx context.Context, queue, messageID string) error
	// UpdateDeliveryAttempt sets a message's dead-letter delivery attempt. It
	// deliberately does not touch DeliveryVersion (the ack-ID version), which is
	// advanced only by a claim (Pull) or a Seek (BumpDeliveryVersions).
	UpdateDeliveryAttempt(ctx context.Context, queue, messageID string, attempt int) error
	// BumpDeliveryVersions advances each named message's exactly-once ack-ID
	// version by one so a previously issued ack ID no longer names the current
	// delivery. A Seek uses it to invalidate outstanding ack IDs without touching
	// DeliveryAttempt (dead-letter accounting). Unknown IDs are ignored.
	BumpDeliveryVersions(ctx context.Context, queue string, messageIDs []string) error
	// ModifyAckDeadline resets the visibility deadline for the given ack IDs
	// (each "queue/messageID"); seconds==0 makes them immediately visible.
	ModifyAckDeadline(ctx context.Context, queue string, ackIDs []string, seconds int, now time.Time) error
	Reset(ctx context.Context)
}
