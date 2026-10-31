package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewAPIGatewayStageHandler returns a ResourceHandler for AWS::ApiGateway::Stage.
func NewAPIGatewayStageHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			restAPIID := propStr(props, "RestApiId", "")
			stageName := propStr(props, "StageName", logicalID)
			deploymentID := propStr(props, "DeploymentId", "")
			resp, err := call(ctx, "Gateway.CreateStage", child(nr, map[string]any{
				"restApiId":    restAPIID,
				"stageName":    stageName,
				"deploymentId": deploymentID,
			}))
			if err != nil {
				return "", nil, err
			}
			_ = resp
			physicalID := restAPIID + "/" + stageName
			arn := nr.ResourceID("apigateway-stage", restAPIID+"/stages/"+stageName)
			return physicalID, map[string]any{"StageName": stageName, "StageArn": arn}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			restAPIID := propStr(newProps, "RestApiId", "")
			stageName := propStr(newProps, "StageName", logicalID)
			if _, err := call(ctx, "Gateway.UpdateStage", child(nr, map[string]any{
				"restApiId": restAPIID,
				"stageName": stageName,
			})); err != nil {
				return "", nil, false, err
			}
			arn := nr.ResourceID("apigateway-stage", restAPIID+"/stages/"+stageName)
			return physicalID, map[string]any{"StageName": stageName, "StageArn": arn}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			restAPIID := propStr(props, "RestApiId", "")
			stageName := propStr(props, "StageName", "")
			_, err := call(ctx, "Gateway.DeleteStage", &model.NormalizedRequest{
				Params: map[string]any{"restApiId": restAPIID, "stageName": stageName},
			})
			return err
		},
		GetAttAttrs: []string{"StageArn"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireUpdate: []string{"Description", "Variables"},
		},
	}
}
