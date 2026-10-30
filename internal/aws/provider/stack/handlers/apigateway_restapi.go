package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewAPIGatewayRestApiHandler returns a ResourceHandler for AWS::ApiGateway::RestApi.
func NewAPIGatewayRestApiHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "Name", logicalID)
			params := copyProps(props)
			params["name"] = name
			resp, err := call(ctx, "Gateway.CreateRestApi", child(nr, params))
			if err != nil {
				return "", nil, err
			}
			apiID, _ := resp.Data["id"].(string)
			arn := nr.ResourceID("apigateway-restapi", apiID)
			return apiID, map[string]any{"RestApiId": apiID, "RootResourceId": resp.Data["rootResourceId"], "Arn": arn}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "Name", logicalID) != propStr(newProps, "Name", logicalID) {
				return "", nil, true, nil
			}
			resp, err := call(ctx, "Gateway.UpdateRestApi", child(nr, map[string]any{"restApiId": physicalID}))
			if err != nil {
				return "", nil, false, err
			}
			rootResID, _ := resp.Data["rootResourceId"].(string)
			arn := nr.ResourceID("apigateway-restapi", physicalID)
			return physicalID, map[string]any{"RestApiId": physicalID, "RootResourceId": rootResID, "Arn": arn}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "Gateway.DeleteRestApi", &model.NormalizedRequest{Params: map[string]any{"restApiId": physicalID}})
			return err
		},
		GetAttAttrs: []string{"RootResourceId"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"Name"},
		},
	}
}
