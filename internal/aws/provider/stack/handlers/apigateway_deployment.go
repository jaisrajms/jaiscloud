package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewAPIGatewayDeploymentHandler returns a ResourceHandler for AWS::ApiGateway::Deployment.
func NewAPIGatewayDeploymentHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			restAPIID := propStr(props, "RestApiId", "")
			stageName := propStr(props, "StageName", "")
			resp, err := call(ctx, "Gateway.CreateDeployment", child(nr, map[string]any{
				"restApiId": restAPIID,
				"stageName": stageName,
			}))
			if err != nil {
				return "", nil, err
			}
			deploymentID, _ := resp.Data["id"].(string)
			return deploymentID, map[string]any{"DeploymentId": deploymentID}, nil
		},
		// Deployments are immutable — always create new for any update.
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			return "", nil, true, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			restAPIID := propStr(props, "RestApiId", "")
			_, err := call(ctx, "Gateway.DeleteDeployment", &model.NormalizedRequest{
				Params: map[string]any{"restApiId": restAPIID, "deploymentId": physicalID},
			})
			return err
		},
		GetAttAttrs: []string{},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireUpdate: []string{"Description"},
		},
	}
}
