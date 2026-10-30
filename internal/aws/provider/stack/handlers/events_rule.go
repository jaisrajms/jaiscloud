package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewEventsRuleHandler returns a ResourceHandler for AWS::Events::Rule.
func NewEventsRuleHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "Name", logicalID)
			params := copyProps(props)
			params["Name"] = name
			resp, err := call(ctx, "EventBridge.PutRule", child(nr, params))
			if err != nil {
				return "", nil, err
			}
			ruleArn, _ := resp.Data["RuleArn"].(string)
			return name, map[string]any{"Arn": ruleArn}, nil
		},
		// PutRule is idempotent — re-apply with new params for in-place update.
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			params := copyProps(newProps)
			params["Name"] = physicalID
			resp, err := call(ctx, "EventBridge.PutRule", child(nr, params))
			if err != nil {
				return "", nil, false, err
			}
			ruleArn, _ := resp.Data["RuleArn"].(string)
			return physicalID, map[string]any{"Arn": ruleArn}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			busName := propStr(props, "EventBusName", "default")
			_, err := call(ctx, "EventBridge.DeleteRule", &model.NormalizedRequest{
				Params: map[string]any{"Name": physicalID, "EventBusName": busName},
			})
			return err
		},
		RefAttr:     "Arn",
		GetAttAttrs: []string{"Arn"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"Name", "EventBusName"},
			RequireUpdate:      []string{"Description", "EventPattern", "ScheduleExpression", "State", "Targets", "Tags"},
		},
	}
}
