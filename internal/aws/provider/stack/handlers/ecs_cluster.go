package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewECSClusterHandler returns a ResourceHandler for AWS::ECS::Cluster.
func NewECSClusterHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "ClusterName", logicalID)
			resp, err := call(ctx, "ECS.CreateCluster", child(nr, map[string]any{"clusterName": name}))
			if err != nil {
				return "", nil, err
			}
			arn := ""
			if cm, ok := resp.Data["cluster"].(map[string]any); ok {
				arn, _ = cm["clusterArn"].(string)
			}
			return name, map[string]any{"Arn": arn}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "ClusterName", logicalID) != propStr(newProps, "ClusterName", logicalID) {
				return "", nil, true, nil
			}
			arn := nr.ResourceID("ecs-cluster", physicalID)
			return physicalID, map[string]any{"Arn": arn}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "ECS.DeleteCluster", &model.NormalizedRequest{Params: map[string]any{"cluster": physicalID}})
			return err
		},
		GetAttAttrs: []string{"Arn"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"ClusterName"},
		},
	}
}
