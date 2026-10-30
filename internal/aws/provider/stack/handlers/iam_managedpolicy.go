package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewIAMManagedPolicyHandler returns a ResourceHandler for AWS::IAM::ManagedPolicy.
func NewIAMManagedPolicyHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "ManagedPolicyName", logicalID)
			params := copyProps(props)
			params["PolicyName"] = name
			resp, err := call(ctx, "IAM.CreatePolicy", child(nr, params))
			if err != nil {
				return "", nil, err
			}
			arn := ""
			if pm, ok := resp.Data["Policy"].(map[string]any); ok {
				arn, _ = pm["Arn"].(string)
			}
			return arn, map[string]any{"Arn": arn}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "ManagedPolicyName", logicalID) != propStr(newProps, "ManagedPolicyName", logicalID) {
				return "", nil, true, nil
			}
			return "", nil, true, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "IAM.DeletePolicy", &model.NormalizedRequest{Params: map[string]any{"PolicyArn": physicalID}})
			return err
		},
		RefAttr:     "Arn",
		GetAttAttrs: []string{"Arn"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"ManagedPolicyName"},
		},
	}
}
