package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewSNSSubscriptionHandler returns a ResourceHandler for AWS::SNS::Subscription.
func NewSNSSubscriptionHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			params := copyProps(props)
			resp, err := call(ctx, "Notification.Subscribe", child(nr, params))
			if err != nil {
				return "", nil, err
			}
			subArn, _ := resp.Data["SubscriptionArn"].(string)
			return subArn, map[string]any{"SubscriptionArn": subArn}, nil
		},
		// Subscriptions are immutable on key fields — always replace.
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			return "", nil, true, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "Notification.Unsubscribe", &model.NormalizedRequest{Params: map[string]any{"SubscriptionArn": physicalID}})
			return err
		},
		GetAttAttrs: []string{},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"TopicArn", "Protocol", "Endpoint"},
		},
	}
}
