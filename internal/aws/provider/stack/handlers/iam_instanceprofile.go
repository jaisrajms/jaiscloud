package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewIAMInstanceProfileHandler returns a ResourceHandler for AWS::IAM::InstanceProfile.
func NewIAMInstanceProfileHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "InstanceProfileName", logicalID)
			resp, err := call(ctx, "IAM.CreateInstanceProfile", child(nr, map[string]any{"InstanceProfileName": name}))
			if err != nil {
				return "", nil, err
			}
			arn := ""
			if ipm, ok := resp.Data["InstanceProfile"].(map[string]any); ok {
				arn, _ = ipm["Arn"].(string)
			}
			return name, map[string]any{"Arn": arn}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "InstanceProfileName", logicalID) != propStr(newProps, "InstanceProfileName", logicalID) {
				return "", nil, true, nil
			}
			return "", nil, true, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "IAM.DeleteInstanceProfile", &model.NormalizedRequest{Params: map[string]any{"InstanceProfileName": physicalID}})
			return err
		},
		GetAttAttrs: []string{"Arn"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"InstanceProfileName"},
		},
	}
}
