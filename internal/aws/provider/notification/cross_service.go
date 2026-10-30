package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// MessageAttribute is a simplified SNS message attribute for internal cross-service use.
type MessageAttribute struct {
	DataType    string
	StringValue string
}

// InternalPublisher is the interface used by other providers to publish to SNS without going through the wire codec.
type InternalPublisher interface {
	InternalPublish(ctx context.Context, topicARN string, message string, msgAttrs map[string]MessageAttribute) error
}

// InternalPublish delivers a message to an SNS topic, fanning out to all confirmed subscriptions.
func (p *SNSProvider) InternalPublish(ctx context.Context, topicARN string, message string, msgAttrs map[string]MessageAttribute) error {
	wireAttrs := make(map[string]any, len(msgAttrs))
	for k, v := range msgAttrs {
		wireAttrs[k] = map[string]any{
			"DataType":    v.DataType,
			"StringValue": v.StringValue,
		}
	}
	messageID := fmt.Sprintf("%x", rand.Int63())
	entries, _ := p.resources.List(ctx, "", "", "sns_subscriptions", "")
	for _, e := range entries {
		var sd subscriptionData
		if json.Unmarshal(e.Data, &sd) != nil || sd.TopicArn != topicARN {
			continue
		}
		if !sd.Confirmed {
			continue
		}
		if fp := sd.Attributes["FilterPolicy"]; fp != "" {
			if !matchesFilterPolicy(fp, wireAttrs) {
				continue
			}
		}
		rawDelivery := sd.Attributes["RawMessageDelivery"] == "true"
		switch sd.Protocol {
		case "sqs":
			p.deliverToSQS(ctx, sd.Endpoint, topicARN, messageID, message, "", "", "", sd.SubscriptionArn, wireAttrs, rawDelivery)
		case "lambda":
			go p.deliverToLambda(ctx, sd.SubscriptionArn, topicARN, messageID, message, "", sd.Endpoint, wireAttrs)
		case "http", "https":
			go p.deliverToHTTP(topicARN, messageID, message, "", sd.Endpoint, wireAttrs)
		}
	}
	return nil
}

// InternalPublishRaw delivers a plain string message to an SNS topic with no message attributes.
// Satisfies the object.SNSInternalPublisher interface for S3 fan-out.
func (p *SNSProvider) InternalPublishRaw(ctx context.Context, topicARN string, message string) error {
	return p.InternalPublish(ctx, topicARN, message, nil)
}

// internalMsgID generates a message ID for internal use.
func internalMsgID() string {
	return fmt.Sprintf("%x-%x-%x", rand.Int31(), rand.Int31(), clock.Now().UnixNano())
}

// publishInternal serves Notification.Publish for an internal origin (S3
// notifications, CloudWatch alarms, EventBridge targets, ...). The params are
// wire-shaped (TopicArn, Message, MessageAttributes) so a plugin can read and
// edit them; this applies whatever the chain left behind.
func (p *SNSProvider) publishInternal(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	topic, _ := nr.Params["TopicArn"].(string)
	message, _ := nr.Params["Message"].(string)
	var attrs map[string]MessageAttribute
	if m, ok := nr.Params["MessageAttributes"].(map[string]any); ok {
		attrs = make(map[string]MessageAttribute, len(m))
		for k, raw := range m {
			if a, ok := raw.(map[string]any); ok {
				dt, _ := a["DataType"].(string)
				sv, _ := a["StringValue"].(string)
				attrs[k] = MessageAttribute{DataType: dt, StringValue: sv}
			}
		}
	}
	if err := p.InternalPublish(ctx, topic, message, attrs); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}
