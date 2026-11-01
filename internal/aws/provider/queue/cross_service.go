package queue

import (
	"context"
	"crypto/md5"
	"fmt"
	"strings"

	awsarn "jaiscloud/internal/aws/arn"
	sqsstore "jaiscloud/internal/aws/store/sqs"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// MessageAttribute is a simplified SQS message attribute for internal cross-service use.
type MessageAttribute struct {
	DataType    string
	StringValue string
}

// SourceContext carries caller identity metadata for internal deliveries.
type SourceContext struct {
	SourceArn        string
	ServicePrincipal string
}

// InternalSendAPI is the interface used by other providers to deliver to SQS without going through the wire codec.
type InternalSendAPI interface {
	InternalSend(ctx context.Context, queueARNorURL string, body string, attrs map[string]MessageAttribute, src SourceContext) error
}

// InternalSend delivers a message to an SQS queue identified by ARN or URL.
// It resolves the target queue and writes directly to the message store.
// The account and region are extracted from the target ARN/URL so that
// cross-account deliveries land in the correct per-(account,region) store.
func (p *QueueProvider) InternalSend(ctx context.Context, queueARNorURL string, body string, attrs map[string]MessageAttribute, src SourceContext) error {
	_, err := p.deliverInternal(ctx, queueARNorURL, body, attrs, src)
	return err
}

// sendMessageInternal serves Queue.SendMessage for an internal origin (SNS, S3
// notifications, Lambda DLQ, ...). The request carries the wire-shaped params
// (QueueUrl may be an ARN) so a plugin can read and edit them; this applies
// whatever the chain left behind.
func (p *QueueProvider) sendMessageInternal(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	target, _ := nr.Params["QueueUrl"].(string)
	body, _ := nr.Params["MessageBody"].(string)
	attrs := map[string]MessageAttribute{}
	if m, ok := nr.Params["MessageAttributes"].(map[string]any); ok {
		for k, raw := range m {
			if a, ok := raw.(map[string]any); ok {
				dt, _ := a["DataType"].(string)
				sv, _ := a["StringValue"].(string)
				attrs[k] = MessageAttribute{DataType: dt, StringValue: sv}
			}
		}
	}
	id, err := p.deliverInternal(ctx, target, body, attrs, SourceContext{SourceArn: nr.SourceArn, ServicePrincipal: nr.ServicePrincipal})
	if err != nil {
		return nil, err
	}
	// The same fields a wire SendMessage answers with, so an after hook sees one shape.
	return provider.OK(map[string]any{"MessageId": id, "MD5OfMessageBody": fmt.Sprintf("%x", md5.Sum([]byte(body)))}), nil
}

// deliverInternal writes a message and returns its id.
func (p *QueueProvider) deliverInternal(ctx context.Context, queueARNorURL string, body string, attrs map[string]MessageAttribute, src SourceContext) (string, error) {
	queueURL, account, region, err := p.resolveQueueURLWithScope(ctx, queueARNorURL)
	if err != nil {
		return "", err
	}
	msgAttrs := make(map[string]sqsstore.MessageAttribute, len(attrs))
	for k, v := range attrs {
		msgAttrs[k] = sqsstore.MessageAttribute{DataType: v.DataType, StringValue: v.StringValue}
	}
	msg := sqsstore.SQSMessage{
		MessageID:         newMessageID(),
		QueueURL:          queueURL,
		Body:              body,
		SentAt:            clock.Now(),
		Attributes:        map[string]string{},
		MessageAttributes: msgAttrs,
	}
	_, _, err = p.messages.Send(ctx, account, region, msg)
	if err != nil {
		return "", err
	}
	p.waiters.Notify(queueURL)
	return msg.MessageID, nil
}

// resolveQueueURLWithScope returns the queue URL plus the account and region
// derived from the target ARN or URL. The account/region are used to route
// the message to the correct per-scope store (multi-account fix §11.1.1).
func (p *QueueProvider) resolveQueueURLWithScope(ctx context.Context, arnOrURL string) (url, account, region string, err error) {
	if strings.HasPrefix(arnOrURL, "http://") || strings.HasPrefix(arnOrURL, "https://") {
		// Region is not encoded in SQS URLs; use cross-scope List to find account+region.
		// Use cross-scope List to find the queue entry and recover its region.
		entries, _ := p.resources.List(ctx, "", "", "sqs_queues", "")
		for _, entry := range entries {
			if entry.ID == arnOrURL {
				return arnOrURL, entry.Account, entry.Region, nil
			}
		}
		return "", "", "", fmt.Errorf("queue: URL not found: %s", arnOrURL)
	}
	if strings.HasPrefix(arnOrURL, "arn:") {
		parsed, e := awsarn.Parse(arnOrURL)
		if e != nil {
			return "", "", "", fmt.Errorf("queue: invalid ARN: %s", arnOrURL)
		}
		account = parsed.AccountID
		region = parsed.Region
		name := parsed.Resource
		if i := strings.LastIndex(name, "/"); i >= 0 {
			name = name[i+1:]
		}
		queueURL, e := p.resolveQueueURLByNameInScope(ctx, account, region, name)
		if e != nil {
			return "", "", "", e
		}
		return queueURL, account, region, nil
	}
	// Bare name — fall back to scanning without account/region scope.
	queueURL, acct, reg, e := p.resolveQueueURLByName(ctx, arnOrURL)
	return queueURL, acct, reg, e
}

// accountRegionFromQueueURL parses account and region from a JaisCloud SQS URL.
// Format: http[s]://host/{account}/{name} — region is not in the URL, use "".
func accountRegionFromQueueURL(u string) (account, region string) {
	// Strip scheme+host: keep /{account}/{name}
	idx := strings.Index(u, "://")
	if idx < 0 {
		return "", ""
	}
	rest := u[idx+3:]
	if slash := strings.Index(rest, "/"); slash >= 0 {
		rest = rest[slash+1:] // strip host
	}
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) >= 1 {
		account = parts[0]
	}
	return account, ""
}

// resolveQueueURLByNameInScope lists queues in the given account+region scope
// and finds the one whose URL ends with the given queue name.
func (p *QueueProvider) resolveQueueURLByNameInScope(ctx context.Context, account, region, queueName string) (string, error) {
	entries, err := p.resources.List(ctx, account, region, "sqs_queues", "")
	if err != nil {
		return "", fmt.Errorf("queue: failed to list queues: %w", err)
	}
	for _, e := range entries {
		url := e.ID
		idx := strings.LastIndex(url, "/")
		if idx >= 0 && url[idx+1:] == queueName {
			return url, nil
		}
	}
	return "", fmt.Errorf("queue: queue not found: %s", queueName)
}
