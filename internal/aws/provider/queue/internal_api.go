package queue

import (
	"context"
	"fmt"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"strings"

	lambdaesm "jaiscloud/internal/aws/provider/lambda/esm"
	"jaiscloud/internal/clock"
)

// InternalReceive is the ESM-internal SQS receive that bypasses the wire layer.
// It resolves the queue URL from the queue name, then calls the message store directly.
func (p *QueueProvider) InternalReceive(ctx context.Context, queueName string, maxMessages int, waitTimeSec int) ([]lambdaesm.InternalMessage, error) {
	queueURL, account, region, err := p.resolveQueueURLByName(ctx, queueName)
	if err != nil {
		return nil, err
	}

	now := clock.Now()
	msgs, err := p.messages.Receive(ctx, account, region, queueURL, maxMessages, now)
	if err != nil {
		return nil, err
	}

	result := make([]lambdaesm.InternalMessage, 0, len(msgs))
	for _, m := range msgs {
		attrs := map[string]string{}
		for k, v := range m.Attributes {
			attrs[k] = v
		}
		// Build message attributes as map[string]any for ESM payload
		msgAttrs := make(map[string]any, len(m.MessageAttributes))
		for k, v := range m.MessageAttributes {
			msgAttrs[k] = map[string]any{
				"DataType":    v.DataType,
				"StringValue": v.StringValue,
			}
		}
		result = append(result, lambdaesm.InternalMessage{
			MessageID:         m.MessageID,
			ReceiptHandle:     m.ReceiptHandle,
			Body:              m.Body,
			Attributes:        attrs,
			MessageAttributes: msgAttrs,
			MD5OfBody:         m.MD5OfBody,
		})
	}
	return result, nil
}

// InternalDeleteBatch deletes a batch of messages by receipt handle for ESM use.
func (p *QueueProvider) InternalDeleteBatch(ctx context.Context, queueName string, receiptHandles []string) error {
	queueURL, account, region, err := p.resolveQueueURLByName(ctx, queueName)
	if err != nil {
		return err
	}
	for _, rh := range receiptHandles {
		// SQS silently succeeds for invalid handles — mirror that behaviour here
		_ = p.messages.Delete(ctx, account, region, queueURL, rh)
	}
	return nil
}

// resolveQueueURLByName scans all SQS queues (cross-scope) and finds the one whose URL
// ends with the given queue name (last path segment). Returns the URL and its stored scope.
func (p *QueueProvider) resolveQueueURLByName(ctx context.Context, queueName string) (url, account, region string, err error) {
	entries, listErr := p.resources.List(ctx, "", "", "sqs_queues", "")
	if listErr != nil {
		return "", "", "", fmt.Errorf("esm: failed to list queues: %w", listErr)
	}
	for _, e := range entries {
		// The resource ID for SQS queues is the queue URL itself
		qURL := e.ID
		// Match last path segment
		idx := strings.LastIndex(qURL, "/")
		if idx >= 0 && qURL[idx+1:] == queueName {
			return qURL, e.Account, e.Region, nil
		}
	}
	return "", "", "", fmt.Errorf("esm: queue not found: %s", queueName)
}

// Ensure QueueProvider satisfies QueueInternalAPI at compile time.
var _ interface {
	InternalReceive(ctx context.Context, queueName string, maxMessages int, waitTimeSec int) ([]lambdaesm.InternalMessage, error)
	InternalDeleteBatch(ctx context.Context, queueName string, receiptHandles []string) error
} = (*QueueProvider)(nil)

// receiveMessageInternal serves Queue.ReceiveMessage for an internal origin
// (the Lambda event-source poller). The queue is named by QueueName, as the
// poller only knows the name. The answer is wire-shaped so a plugin that
// overrides ReceiveMessage can produce it.
func (p *QueueProvider) receiveMessageInternal(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["QueueName"].(string)
	max, _ := toIntParam(nr.Params["MaxNumberOfMessages"])
	wait, _ := toIntParam(nr.Params["WaitTimeSeconds"])
	msgs, err := p.InternalReceive(ctx, name, max, wait)
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, len(msgs))
	for _, m := range msgs {
		attrs := make(map[string]any, len(m.Attributes))
		for k, v := range m.Attributes {
			attrs[k] = v
		}
		out = append(out, map[string]any{
			"MessageId": m.MessageID, "ReceiptHandle": m.ReceiptHandle, "Body": m.Body,
			"MD5OfBody": m.MD5OfBody, "Attributes": attrs, "MessageAttributes": m.MessageAttributes,
		})
	}
	return provider.OK(map[string]any{"Messages": out}), nil
}

// deleteMessageBatchInternal serves Queue.DeleteMessageBatch for an internal origin.
func (p *QueueProvider) deleteMessageBatchInternal(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, _ := nr.Params["QueueName"].(string)
	var handles []string
	if entries, ok := nr.Params["Entries"].([]any); ok {
		for _, e := range entries {
			if m, ok := e.(map[string]any); ok {
				if rh, _ := m["ReceiptHandle"].(string); rh != "" {
					handles = append(handles, rh)
				}
			}
		}
	}
	if err := p.InternalDeleteBatch(ctx, name, handles); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func toIntParam(v any) (int, bool) {
	switch t := v.(type) {
	case int:
		return t, true
	case int64:
		return int(t), true
	case float64:
		return int(t), true
	}
	return 0, false
}
