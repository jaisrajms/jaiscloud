package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewEC2SecurityGroupHandler returns a ResourceHandler for AWS::EC2::SecurityGroup.
func NewEC2SecurityGroupHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			groupName := propStr(props, "GroupName", logicalID)
			resp, err := call(ctx, "Compute.CreateSecurityGroup", child(nr, map[string]any{
				"GroupName":        groupName,
				"GroupDescription": propStr(props, "GroupDescription", groupName),
				"VpcId":            propStr(props, "VpcId", ""),
			}))
			if err != nil {
				return "", nil, err
			}
			groupID, _ := resp.Data["GroupId"].(string)
			return groupID, map[string]any{"GroupId": groupID}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "GroupName", logicalID) != propStr(newProps, "GroupName", logicalID) {
				return "", nil, true, nil
			}
			if propStr(oldProps, "VpcId", "") != propStr(newProps, "VpcId", "") {
				return "", nil, true, nil
			}
			return physicalID, map[string]any{"GroupId": physicalID}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "Compute.DeleteSecurityGroup", &model.NormalizedRequest{Params: map[string]any{"GroupId": physicalID}})
			return err
		},
		GetAttAttrs: []string{"GroupId"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"GroupName", "VpcId"},
		},
	}
}
