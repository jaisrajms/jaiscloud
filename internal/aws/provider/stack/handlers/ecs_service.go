package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewECSServiceHandler returns a ResourceHandler for AWS::ECS::Service.
func NewECSServiceHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "ServiceName", logicalID)
			params := copyProps(props)
			params["serviceName"] = name
			resp, err := call(ctx, "ECS.CreateService", child(nr, params))
			if err != nil {
				return "", nil, err
			}
			serviceArn := ""
			if sm, ok := resp.Data["service"].(map[string]any); ok {
				serviceArn, _ = sm["serviceArn"].(string)
			}
			return name, map[string]any{"ServiceArn": serviceArn, "Name": name}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			clusterName := propStr(newProps, "Cluster", "default")
			updateParams := map[string]any{
				"cluster": clusterName,
				"service": physicalID,
			}
			if v, ok := newProps["TaskDefinition"]; ok {
				updateParams["taskDefinition"] = v
			}
			if v, ok := newProps["DesiredCount"]; ok {
				updateParams["desiredCount"] = v
			}
			resp, err := call(ctx, "ECS.UpdateService", child(nr, updateParams))
			if err != nil {
				return "", nil, false, err
			}
			serviceArn := ""
			if sm, ok := resp.Data["service"].(map[string]any); ok {
				serviceArn, _ = sm["serviceArn"].(string)
			}
			return physicalID, map[string]any{"ServiceArn": serviceArn, "Name": physicalID}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			clusterName := propStr(props, "Cluster", "default")
			_, err := call(ctx, "ECS.DeleteService", &model.NormalizedRequest{
				Params: map[string]any{"cluster": clusterName, "service": physicalID},
			})
			return err
		},
		GetAttAttrs: []string{"ServiceArn", "Name"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireUpdate: []string{"TaskDefinition", "DesiredCount"},
		},
	}
}
