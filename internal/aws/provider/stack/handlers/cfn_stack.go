package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewCFNStackHandler returns a ResourceHandler for AWS::CloudFormation::Stack (nested stacks).
func NewCFNStackHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "StackName", logicalID)
			params := copyProps(props)
			params["StackName"] = name
			resp, err := call(ctx, "CloudFormation.CreateStack", child(nr, params))
			if err != nil {
				return "", nil, err
			}
			stackID, _ := resp.Data["StackId"].(string)
			return stackID, map[string]any{"StackId": stackID}, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "CloudFormation.DeleteStack", &model.NormalizedRequest{Params: map[string]any{"StackName": physicalID}})
			return err
		},
		// Nested stack outputs are resolved dynamically via Fn::GetAtt Outputs.Key — no static list.
		GetAttAttrs: []string{},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"StackName"},
			RequireUpdate:      []string{"TemplateURL", "Parameters", "TimeoutInMinutes", "NotificationARNs", "Tags"},
		},
	}
}
