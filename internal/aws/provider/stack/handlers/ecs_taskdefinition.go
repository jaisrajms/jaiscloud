package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewECSTaskDefinitionHandler returns a ResourceHandler for AWS::ECS::TaskDefinition.
func NewECSTaskDefinitionHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			params := copyProps(props)
			resp, err := call(ctx, "ECS.RegisterTaskDefinition", child(nr, params))
			if err != nil {
				return "", nil, err
			}
			taskDefArn := ""
			if tdm, ok := resp.Data["taskDefinition"].(map[string]any); ok {
				taskDefArn, _ = tdm["taskDefinitionArn"].(string)
			}
			return taskDefArn, map[string]any{"TaskDefinitionArn": taskDefArn}, nil
		},
		// Task definitions are immutable — create a new revision on update.
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			return "", nil, true, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "ECS.DeregisterTaskDefinition", &model.NormalizedRequest{Params: map[string]any{"taskDefinition": physicalID}})
			return err
		},
		GetAttAttrs: []string{"TaskDefinitionArn"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"Family"},
		},
	}
}
