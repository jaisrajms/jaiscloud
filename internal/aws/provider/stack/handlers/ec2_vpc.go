package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewEC2VPCHandler returns a ResourceHandler for AWS::EC2::VPC.
func NewEC2VPCHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			cidr := propStr(props, "CidrBlock", "")
			resp, err := call(ctx, "Compute.CreateVpc", child(nr, map[string]any{
				"CidrBlock": cidr,
			}))
			if err != nil {
				return "", nil, err
			}
			vpcID := ""
			if vm, ok := resp.Data["Vpc"].(map[string]any); ok {
				vpcID, _ = vm["VpcId"].(string)
			}
			return vpcID, map[string]any{"VpcId": vpcID, "CidrBlock": cidr, "DefaultSecurityGroup": "", "DefaultNetworkAcl": ""}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "CidrBlock", "") != propStr(newProps, "CidrBlock", "") {
				return "", nil, true, nil
			}
			return physicalID, map[string]any{"VpcId": physicalID, "CidrBlock": propStr(newProps, "CidrBlock", ""), "DefaultSecurityGroup": "", "DefaultNetworkAcl": ""}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "Compute.DeleteVpc", &model.NormalizedRequest{Params: map[string]any{"VpcId": physicalID}})
			return err
		},
		GetAttAttrs: []string{"VpcId", "CidrBlock", "DefaultNetworkAcl", "DefaultSecurityGroup"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"CidrBlock"},
		},
	}
}
