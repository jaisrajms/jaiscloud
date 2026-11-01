package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewIAMRoleHandler returns a ResourceHandler for AWS::IAM::Role.
func NewIAMRoleHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "RoleName", logicalID)
			params := copyProps(props)
			params["RoleName"] = name
			resp, err := call(ctx, "IAM.CreateRole", child(nr, params))
			if err != nil {
				return "", nil, err
			}
			arn := ""
			if rm, ok := resp.Data["Role"].(map[string]any); ok {
				arn, _ = rm["Arn"].(string)
			}
			return name, map[string]any{"Arn": arn}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "RoleName", logicalID) != propStr(newProps, "RoleName", logicalID) {
				return "", nil, true, nil
			}
			if propStr(oldProps, "Path", "") != propStr(newProps, "Path", "") {
				return "", nil, true, nil
			}
			// Update assume role policy if changed
			if propStr(oldProps, "AssumeRolePolicyDocument", "") != propStr(newProps, "AssumeRolePolicyDocument", "") {
				if _, err := call(ctx, "IAM.UpdateAssumeRolePolicy", child(nr, map[string]any{
					"RoleName":       physicalID,
					"PolicyDocument": propStr(newProps, "AssumeRolePolicyDocument", ""),
				})); err != nil {
					return "", nil, false, err
				}
			}
			arn := nr.ResourceID("iam-role", physicalID)
			return physicalID, map[string]any{"Arn": arn}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "IAM.DeleteRole", &model.NormalizedRequest{
				AccountID: "000000000000",
				Params:    map[string]any{"RoleName": physicalID},
			})
			return err
		},
		GetAttAttrs: []string{"Arn", "RoleId"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"RoleName", "Path"},
			RequireUpdate:      []string{"AssumeRolePolicyDocument", "Description", "ManagedPolicyArns", "Policies", "Tags", "MaxSessionDuration"},
		},
	}
}
