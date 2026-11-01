package object

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/aws/provider/queue"
)

// S3SQSSender sends a message to an SQS queue for S3 notifications.
type S3SQSSender interface {
	InternalSend(ctx context.Context, queueARNorURL string, body string, attrs map[string]queue.MessageAttribute, src queue.SourceContext) error
}

// S3SNSPublisher publishes to an SNS topic for S3 notifications.
// msgAttrs is nil for plain S3 event notifications.
type S3SNSPublisher interface {
	InternalPublishRaw(ctx context.Context, topicARN string, message string) error
}

// S3LambdaInvoker invokes a Lambda function for S3 notifications.
type S3LambdaInvoker interface {
	InternalInvokeRaw(ctx context.Context, funcARNorName string, payload []byte) error
}

// S3EventBridgeSender sends events to EventBridge for S3 notifications.
type S3EventBridgeSender interface {
	InternalPutEvents(ctx context.Context, entries []map[string]any) error
}

// S3FanoutConfig holds the wired providers for S3 notification fan-out.
type S3FanoutConfig struct {
	SQS         S3SQSSender
	Lambda      S3LambdaInvoker
	EventBridge S3EventBridgeSender
	// Note: SNS is accessed via the InternalSend method of SQS for simplicity,
	// but SNS has its own interface for fan-out.
	SNSPublisher SNSInternalPublisher
}

// SNSInternalPublisher is the narrow interface for SNS fan-out from S3.
type SNSInternalPublisher interface {
	InternalPublishRaw(ctx context.Context, topicARN string, message string) error
}

// SetFanout wires the S3 fan-out config (second-pass wiring).
func (p *ObjectProvider) SetFanout(cfg S3FanoutConfig) {
	p.fanout = cfg
}

// buildS3EventRecord builds the AWS S3 event record JSON for the given operation.
func buildS3EventRecord(bucket, key, eventName, etag string, size int64, region, accountID, configID string, now time.Time) []byte {
	seq := fmt.Sprintf("%016x", rand.Int63())
	record := map[string]any{
		"eventVersion":      "2.1",
		"eventSource":       "aws:s3",
		"awsRegion":         region,
		"eventTime":         now.UTC().Format(time.RFC3339),
		"eventName":         eventName,
		"userIdentity":      map[string]any{"principalId": "EXAMPLE"},
		"requestParameters": map[string]any{"sourceIPAddress": "127.0.0.1"},
		"responseElements": map[string]any{
			"x-amz-request-id": fmt.Sprintf("%016x", rand.Int63()),
			"x-amz-id-2":       fmt.Sprintf("%016x", rand.Int63()),
		},
		"s3": map[string]any{
			"s3SchemaVersion": "1.0",
			"configurationId": configID,
			"bucket": map[string]any{
				"name":          bucket,
				"ownerIdentity": map[string]any{"principalId": accountID},
				"arn":           "arn:aws:s3:::" + bucket, //nolint:hardcoded-arn S3 bucket ARNs have no account/region by AWS spec
			},
			"object": map[string]any{
				"key":       key,
				"size":      size,
				"eTag":      etag,
				"sequencer": seq,
			},
		},
	}
	out := map[string]any{"Records": []any{record}}
	b, _ := json.Marshal(out)
	return b
}

// dispatchS3Notification fans out an S3 event to all configured notification targets.
func (p *ObjectProvider) dispatchS3Notification(_ context.Context, bucket, key, eventName, etag string, size int64, region, accountID string) {
	raw, ok := p.notifStore.Load(bucket)
	if !ok {
		return
	}
	var cfg bucketNotificationConfig
	if err := json.Unmarshal([]byte(raw.(string)), &cfg); err != nil {
		return
	}
	now := clock.Now()

	bgCtx := context.Background()

	for _, q := range cfg.QueueConfigurations {
		if !matchesEvent(q.Events, eventName) || !matchesFilter(q.Filter, key) {
			continue
		}
		payload := buildS3EventRecord(bucket, key, eventName, etag, size, region, accountID, q.Id, now)
		go func(arn string, body []byte) {
			if p.fanout.SQS != nil {
				_ = p.fanout.SQS.InternalSend(bgCtx, arn, string(body), nil, queue.SourceContext{
					SourceArn:        "arn:aws:s3:::" + bucket, //nolint:hardcoded-arn S3 bucket ARNs have no account/region by AWS spec
					ServicePrincipal: "s3.amazonaws.com",
				})
			}
		}(q.QueueArn, payload)
	}

	for _, t := range cfg.TopicConfigurations {
		if !matchesEvent(t.Events, eventName) || !matchesFilter(t.Filter, key) {
			continue
		}
		payload := buildS3EventRecord(bucket, key, eventName, etag, size, region, accountID, t.Id, now)
		go func(arn string, body []byte) {
			if p.fanout.SNSPublisher != nil {
				_ = p.fanout.SNSPublisher.InternalPublishRaw(bgCtx, arn, string(body))
			}
		}(t.TopicArn, payload)
	}

	for _, l := range cfg.LambdaConfigurations {
		if !matchesEvent(l.Events, eventName) || !matchesFilter(l.Filter, key) {
			continue
		}
		payload := buildS3EventRecord(bucket, key, eventName, etag, size, region, accountID, l.Id, now)
		go func(arn string, body []byte) {
			if p.fanout.Lambda != nil {
				_ = p.fanout.Lambda.InternalInvokeRaw(bgCtx, arn, body)
			}
		}(l.LambdaFuncArn, payload)
	}

	// EventBridge delivery is opt-in per bucket (<EventBridgeConfiguration/>).
	if cfg.EventBridge && p.fanout.EventBridge != nil {
		ebPayload := buildEBS3Event(bucket, key, eventName, etag, size, region, accountID, now)
		go func(payload map[string]any) {
			_ = p.fanout.EventBridge.InternalPutEvents(bgCtx, []map[string]any{payload})
		}(ebPayload)
	}
}

func buildEBEventName(eventName string) string {
	if strings.HasPrefix(eventName, "ObjectCreated") {
		return "Object Created"
	}
	if strings.HasPrefix(eventName, "ObjectRemoved") {
		return "Object Deleted"
	}
	return eventName
}

func buildEBS3Event(bucket, key, eventName, etag string, size int64, region, accountID string, now time.Time) map[string]any {
	detail, _ := json.Marshal(map[string]any{
		"version":           "0",
		"bucket":            map[string]any{"name": bucket},
		"object":            map[string]any{"key": key, "size": size, "etag": etag},
		"request-id":        "",
		"requester":         accountID,
		"source-ip-address": "127.0.0.1",
		"reason":            eventName,
		"deletion-type":     "Permanently Deleted",
	})
	return map[string]any{
		"Source":       "aws.s3",
		"DetailType":   buildEBEventName(eventName),
		"EventBusName": "default",
		"Detail":       string(detail),
		"Resources":    []any{"arn:aws:s3:::" + bucket}, //nolint:hardcoded-arn S3 bucket ARNs have no account/region by AWS spec
		"Time":         now.UTC().Format(time.RFC3339),
	}
}
