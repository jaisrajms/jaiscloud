package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewLogsSubscriptionFilterHandler returns a ResourceHandler for AWS::Logs::SubscriptionFilter.
func NewLogsSubscriptionFilterHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			logGroupName := propStr(props, "LogGroupName", "")
			filterName := propStr(props, "FilterName", logicalID)
			filterPattern := propStr(props, "FilterPattern", "")
			destinationArn := propStr(props, "DestinationArn", "")
			if _, err := call(ctx, "CloudWatchLogs.PutSubscriptionFilter", child(nr, map[string]any{
				"logGroupName":   logGroupName,
				"filterName":     filterName,
				"filterPattern":  filterPattern,
				"destinationArn": destinationArn,
			})); err != nil {
				return "", nil, err
			}
			physicalID := logGroupName + "/" + filterName
			return physicalID, map[string]any{}, nil
		},
		// Subscription filters are immutable — always replace.
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			return "", nil, true, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			logGroupName := propStr(props, "LogGroupName", "")
			filterName := propStr(props, "FilterName", "")
			_, err := call(ctx, "CloudWatchLogs.DeleteSubscriptionFilter", &model.NormalizedRequest{
				Params: map[string]any{
					"logGroupName": logGroupName,
					"filterName":   filterName,
				},
			})
			return err
		},
		GetAttAttrs: []string{},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"LogGroupName", "FilterName"},
		},
	}
}
