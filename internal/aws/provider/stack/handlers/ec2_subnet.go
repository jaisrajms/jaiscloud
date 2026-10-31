package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewEC2SubnetHandler returns a ResourceHandler for AWS::EC2::Subnet.
func NewEC2SubnetHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			resp, err := call(ctx, "Compute.CreateSubnet", child(nr, map[string]any{
				"VpcId":            propStr(props, "VpcId", ""),
				"CidrBlock":        propStr(props, "CidrBlock", ""),
				"AvailabilityZone": propStr(props, "AvailabilityZone", ""),
			}))
			if err != nil {
				return "", nil, err
			}
			subnetID := ""
			vpcID := propStr(props, "VpcId", "")
			cidr := propStr(props, "CidrBlock", "")
			az := propStr(props, "AvailabilityZone", "")
			if sm, ok := resp.Data["Subnet"].(map[string]any); ok {
				subnetID, _ = sm["SubnetId"].(string)
			}
			return subnetID, map[string]any{"SubnetId": subnetID, "VpcId": vpcID, "AvailabilityZone": az, "CidrBlock": cidr}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			for _, p := range []string{"VpcId", "CidrBlock", "AvailabilityZone"} {
				if propStr(oldProps, p, "") != propStr(newProps, p, "") {
					return "", nil, true, nil
				}
			}
			return physicalID, map[string]any{
				"SubnetId":         physicalID,
				"VpcId":            propStr(newProps, "VpcId", ""),
				"AvailabilityZone": propStr(newProps, "AvailabilityZone", ""),
				"CidrBlock":        propStr(newProps, "CidrBlock", ""),
			}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "Compute.DeleteSubnet", &model.NormalizedRequest{Params: map[string]any{"SubnetId": physicalID}})
			return err
		},
		GetAttAttrs: []string{"SubnetId", "VpcId", "AvailabilityZone", "CidrBlock"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"VpcId", "CidrBlock", "AvailabilityZone"},
		},
	}
}
