package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewEC2RouteHandler returns a ResourceHandler for AWS::EC2::Route.
func NewEC2RouteHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			rtID := propStr(props, "RouteTableId", "")
			dest := propStr(props, "DestinationCidrBlock", "")
			gwID := propStr(props, "GatewayId", "")
			if _, err := call(ctx, "Compute.CreateRoute", child(nr, map[string]any{
				"RouteTableId":         rtID,
				"DestinationCidrBlock": dest,
				"GatewayId":            gwID,
			})); err != nil {
				return "", nil, err
			}
			// Composite physical ID: routeTableId/destinationCidrBlock
			physicalID := rtID + "/" + dest
			return physicalID, map[string]any{}, nil
		},
		// Routes are immutable on key fields; always replace.
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			return "", nil, true, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			rtID := propStr(props, "RouteTableId", "")
			dest := propStr(props, "DestinationCidrBlock", "")
			_, err := call(ctx, "Compute.DeleteRoute", &model.NormalizedRequest{
				Params: map[string]any{
					"RouteTableId":         rtID,
					"DestinationCidrBlock": dest,
				},
			})
			return err
		},
		GetAttAttrs: []string{},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"RouteTableId", "DestinationCidrBlock"},
		},
	}
}
