package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewEC2InternetGatewayHandler returns a ResourceHandler for AWS::EC2::InternetGateway.
func NewEC2InternetGatewayHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			resp, err := call(ctx, "Compute.CreateInternetGateway", child(nr, map[string]any{}))
			if err != nil {
				return "", nil, err
			}
			igwID := ""
			if im, ok := resp.Data["InternetGateway"].(map[string]any); ok {
				igwID, _ = im["InternetGatewayId"].(string)
			}
			return igwID, map[string]any{"InternetGatewayId": igwID}, nil
		},
		// InternetGateways have no mutable properties; any tag change is in-place.
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			return physicalID, map[string]any{"InternetGatewayId": physicalID}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "Compute.DeleteInternetGateway", &model.NormalizedRequest{Params: map[string]any{"InternetGatewayId": physicalID}})
			return err
		},
		GetAttAttrs: []string{"InternetGatewayId"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{},
			RequireUpdate:      []string{"Tags"},
		},
	}
}
