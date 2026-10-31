package esm

import (
	"context"
	"time"

	"jaiscloud/internal/aws/store/stream"
)

const (
	ESMStateCreating  = "Creating"
	ESMStateEnabled   = "Enabled"
	ESMStateDisabled  = "Disabled"
	ESMStateUpdating  = "Updating"
	ESMStateDisabling = "Disabling"
	ESMStateDeleting  = "Deleting"

	ESMSourceSQS             = "sqs"
	ESMSourceDynamoDBStreams = "dynamodb-streams"
)

// DestinationConfig holds the failure/success destination configuration for an ESM.
type DestinationConfig struct {
	OnFailure DestinationDetail `json:"OnFailure,omitempty"`
	OnSuccess DestinationDetail `json:"OnSuccess,omitempty"`
}

// DestinationDetail holds the ARN of the failure/success destination.
type DestinationDetail struct {
	Destination string `json:"Destination,omitempty"`
}

type EventSourceMapping struct {
	UUID                           string            `json:"UUID"`
	FunctionName                   string            `json:"FunctionName"`
	FunctionArn                    string            `json:"FunctionArn"`
	EventSourceArn                 string            `json:"EventSourceArn"`
	BatchSize                      int               `json:"BatchSize"`
	MaximumBatchingWindowInSeconds int               `json:"MaximumBatchingWindowInSeconds"`
	Enabled                        bool              `json:"Enabled"`
	State                          string            `json:"State"`
	StateTransitionReason          string            `json:"StateTransitionReason"`
	LastModified                   time.Time         `json:"LastModified"`
	LastProcessingResult           string            `json:"LastProcessingResult"`
	SourceType                     string            `json:"SourceType"`
	MaximumRetryAttempts           int               `json:"MaximumRetryAttempts"`
	BisectBatchOnFunctionError     bool              `json:"BisectBatchOnFunctionError"`
	FilterCriteria                 string            `json:"FilterCriteria,omitempty"`
	DestinationConfig              DestinationConfig `json:"DestinationConfig,omitempty"`
	Region                         string            `json:"Region"`
	AccountID                      string            `json:"AccountID"`
	Cloud                          string            `json:"Cloud"`

	ConsecutiveErrors  int    `json:"-"`
	QueueName          string `json:"-"`
	TableName          string `json:"-"`
	LastSequenceNumber int    `json:"-"`
	// SourceAccountID and SourceRegion are parsed from EventSourceArn (MA-5b §11.1.6).
	SourceAccountID string `json:"-"`
	SourceRegion    string `json:"-"`
}

// SQSSenderAPI is the SQS interface needed for DLQ delivery.
type SQSSenderAPI interface {
	InternalSend(ctx context.Context, queueARNorURL string, body string, attrs map[string]SQSMessageAttribute, src SQSSourceContext) error
}

// SQSMessageAttribute is a simplified attribute type for cross-package use.
type SQSMessageAttribute struct {
	DataType    string
	StringValue string
}

// SQSSourceContext carries caller identity for internal deliveries.
type SQSSourceContext struct {
	SourceArn        string
	ServicePrincipal string
}

// QueueInternalAPI is the SQS interface needed by ESM pollers. The queue is named
// by its ARN, so the account and region come with it.
type QueueInternalAPI interface {
	InternalReceive(ctx context.Context, queueArn string, maxMessages int, waitTimeSec int) ([]InternalMessage, error)
	InternalDeleteBatch(ctx context.Context, queueArn string, receiptHandles []string) error
}

// InternalMessage is a simplified message for internal ESM use.
type InternalMessage struct {
	MessageID         string
	ReceiptHandle     string
	Body              string
	Attributes        map[string]string
	MessageAttributes map[string]any
	MD5OfBody         string
}

// StreamStoreAPI is the DynamoDB Streams interface needed by ESM pollers. The
// context ends a call the poller no longer wants (shutdown, a deleted mapping);
// an error means the stream could not be read (see model.IsOutage), as opposed to
// there being nothing to read.
type StreamStoreAPI interface {
	GetRecords(ctx context.Context, tableName string, afterSeq int) ([]stream.Record, int, error)
	GetStreamInfo(ctx context.Context, tableName string) (stream.StreamInfo, bool, error)
}
