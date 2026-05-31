package sqs

import (
	"context"
	"time"
)

// SQSMessage represents a message in a queue (data plane).
type SQSMessage struct {
	MessageID         string
	ReceiptHandle     string
	QueueURL          string
	Body              string
	MD5OfBody         string
	Attributes        map[string]string           // system attributes
	MessageAttributes map[string]MessageAttribute // user-defined attributes
	GroupID           string                      // FIFO: MessageGroupId
	DeduplicationID   string                      // FIFO: MessageDeduplicationId
	DedupScope        string                      // FIFO: "" (queue) or "messageGroup"
	VisibleAt         time.Time                   // when the message becomes visible again
	SentAt            time.Time
	DelayUntil        time.Time // for delayed messages
	ReceiveCount      int
	FirstReceivedAt   *time.Time
	SequenceNumber    string // FIFO sequence
}

// MessageAttribute is a typed user-defined SQS message attribute.
type MessageAttribute struct {
	DataType    string // "String", "Number", "Binary"
	StringValue string
	BinaryValue []byte
}

// SQSMessageStore manages data-plane message storage for SQS.
// Phase 0: MemoryMessageStore. Phase 1: PostgresMessageStore.
type SQSMessageStore interface {
	// Send enqueues a message. Returns (dedupMessageID, sequenceNumber, err).
	// For FIFO queues with deduplication, if a duplicate is detected the original
	// MessageID is returned as dedupMessageID. For FIFO queues, sequenceNumber is
	// the assigned 20-digit sequence number; it is empty for standard queues.
	Send(ctx context.Context, account, region string, msg SQSMessage) (dedupMessageID, sequenceNumber string, err error)
	Receive(ctx context.Context, account, region, queueURL string, maxMessages int, now time.Time) ([]SQSMessage, error)
	Delete(ctx context.Context, account, region, queueURL, receiptHandle string) error
	ChangeVisibility(ctx context.Context, account, region, queueURL, receiptHandle string, timeoutSec int, now time.Time) error
	Purge(ctx context.Context, account, region, queueURL string) error
	GetApproximateCounts(ctx context.Context, account, region, queueURL string, now time.Time) (visible, notVisible, delayed int, err error)

	// SetQueueRetention registers the message retention period for a queue so
	// the store can expire old messages. retentionSecs=0 resets to the 4-day default.
	SetQueueRetention(ctx context.Context, account, region, queueURL string, retentionSecs int) error

	// Peek returns up to limit messages starting at offset WITHOUT changing their state
	// (no visibility timeout update, no receive-count increment). UI-only debugging operation.
	// Also returns the total number of messages in the queue for pagination.
	// limit ≤ 0 defaults to 100.
	Peek(ctx context.Context, account, region, queueURL string, offset, limit int) (msgs []SQSMessage, total int, err error)

	// Reset wipes all messages across all queues.
	Reset(ctx context.Context)
}
