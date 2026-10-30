package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewEC2RouteTableHandler returns a ResourceHandler for AWS::EC2::RouteTable.
func NewEC2RouteTableHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			resp, err := call(ctx, "Compute.CreateRouteTable", child(nr, map[string]any{
				"VpcId": propStr(props, "VpcId", ""),
			}))
			if err != nil {
				return "", nil, err
			}
			rtID := ""
			if rm, ok := resp.Data["RouteTable"].(map[string]any); ok {
				rtID, _ = rm["RouteTableId"].(string)
			}
			return rtID, map[string]any{"RouteTableId": rtID}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "VpcId", "") != propStr(newProps, "VpcId", "") {
				return "", nil, true, nil
			}
			return physicalID, map[string]any{"RouteTableId": physicalID}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "Compute.DeleteRouteTable", &model.NormalizedRequest{Params: map[string]any{"RouteTableId": physicalID}})
			return err
		},
		GetAttAttrs: []string{"RouteTableId"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"VpcId"},
		},
	}
}
