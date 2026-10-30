package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewAPIGatewayIntegrationHandler returns a ResourceHandler for AWS::ApiGateway::Integration.
func NewAPIGatewayIntegrationHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			restAPIID := propStr(props, "RestApiId", "")
			resourceID := propStr(props, "ResourceId", "")
			httpMethod := propStr(props, "HttpMethod", "")
			intType := propStr(props, "Type", "")
			uri := propStr(props, "Uri", "")
			if _, err := call(ctx, "Gateway.PutIntegration", child(nr, map[string]any{
				"restApiId":  restAPIID,
				"resourceId": resourceID,
				"httpMethod": httpMethod,
				"type":       intType,
				"uri":        uri,
			})); err != nil {
				return "", nil, err
			}
			physicalID := restAPIID + "/" + resourceID + "/" + httpMethod + "/integration"
			return physicalID, map[string]any{}, nil
		},
		// Integrations are immutable on key fields — always replace.
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			return "", nil, true, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			restAPIID := propStr(props, "RestApiId", "")
			resourceID := propStr(props, "ResourceId", "")
			httpMethod := propStr(props, "HttpMethod", "")
			_, err := call(ctx, "Gateway.DeleteIntegration", &model.NormalizedRequest{
				Params: map[string]any{
					"restApiId":  restAPIID,
					"resourceId": resourceID,
					"httpMethod": httpMethod,
				},
			})
			return err
		},
		GetAttAttrs: []string{},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"RestApiId", "ResourceId", "HttpMethod"},
		},
	}
}
