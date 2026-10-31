package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewAPIGatewayResourceHandler returns a ResourceHandler for AWS::ApiGateway::Resource.
func NewAPIGatewayResourceHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			restAPIID := propStr(props, "RestApiId", "")
			parentID := propStr(props, "ParentId", "")
			pathPart := propStr(props, "PathPart", logicalID)
			resp, err := call(ctx, "Gateway.CreateResource", child(nr, map[string]any{
				"restApiId": restAPIID,
				"parentId":  parentID,
				"pathPart":  pathPart,
			}))
			if err != nil {
				return "", nil, err
			}
			resourceID, _ := resp.Data["id"].(string)
			return resourceID, map[string]any{"ResourceId": resourceID}, nil
		},
		// Resources are immutable on key fields — always replace.
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			return "", nil, true, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			restAPIID := propStr(props, "RestApiId", "")
			_, err := call(ctx, "Gateway.DeleteResource", &model.NormalizedRequest{
				Params: map[string]any{"restApiId": restAPIID, "resourceId": physicalID},
			})
			return err
		},
		GetAttAttrs: []string{"ResourceId"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"RestApiId", "ParentId", "PathPart"},
		},
	}
}
