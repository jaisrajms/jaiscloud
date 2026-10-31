package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewSQSQueueHandler returns a ResourceHandler for AWS::SQS::Queue.
func NewSQSQueueHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "QueueName", logicalID)
			resp, err := call(ctx, "Queue.CreateQueue", child(nr, map[string]any{"QueueName": name}))
			if err != nil {
				return "", nil, err
			}
			url := resp.Data["QueueUrl"].(string)
			arn := nr.ResourceID("sqs-queue", name)
			queueName := name
			return url, map[string]any{"QueueUrl": url, "Arn": arn, "QueueName": queueName}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "QueueName", logicalID) != propStr(newProps, "QueueName", logicalID) {
				return "", nil, true, nil
			}
			if propStr(oldProps, "FifoQueue", "") != propStr(newProps, "FifoQueue", "") {
				return "", nil, true, nil
			}
			// In-place update via SetQueueAttributes
			attrs := map[string]string{}
			if v := propStr(newProps, "VisibilityTimeout", ""); v != "" {
				attrs["VisibilityTimeout"] = v
			}
			if v := propStr(newProps, "MessageRetentionPeriod", ""); v != "" {
				attrs["MessageRetentionPeriod"] = v
			}
			if v := propStr(newProps, "ReceiveMessageWaitTimeSeconds", ""); v != "" {
				attrs["ReceiveMessageWaitTimeSeconds"] = v
			}
			if v, ok := newProps["RedrivePolicy"]; ok {
				attrs["RedrivePolicy"] = propStr(map[string]any{"RedrivePolicy": v}, "RedrivePolicy", "")
			}
			if len(attrs) > 0 {
				if _, err := call(ctx, "Queue.SetQueueAttributes", child(nr, map[string]any{
					"QueueUrl":   physicalID,
					"Attributes": attrs,
				})); err != nil {
					return "", nil, false, err
				}
			}
			name := propStr(newProps, "QueueName", logicalID)
			arn := nr.ResourceID("sqs-queue", name)
			return physicalID, map[string]any{"QueueUrl": physicalID, "Arn": arn, "QueueName": name}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "Queue.DeleteQueue", &model.NormalizedRequest{Params: map[string]any{"QueueUrl": physicalID}})
			return err
		},
		GetAttAttrs: []string{"Arn", "QueueName", "QueueUrl"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"QueueName", "FifoQueue"},
			RequireUpdate:      []string{"VisibilityTimeout", "MessageRetentionPeriod", "ReceiveMessageWaitTimeSeconds", "RedrivePolicy"},
		},
	}
}
