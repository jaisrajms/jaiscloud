package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewDynamoDBGlobalTableHandler returns a ResourceHandler for AWS::DynamoDB::GlobalTable.
func NewDynamoDBGlobalTableHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "TableName", logicalID)
			replicas := props["Replicas"]
			resp, err := call(ctx, "Table.CreateGlobalTable", child(nr, map[string]any{
				"GlobalTableName":  name,
				"ReplicationGroup": replicas,
			}))
			if err != nil {
				return "", nil, err
			}
			globalTableName := ""
			if gtd, ok := resp.Data["GlobalTableDescription"].(map[string]any); ok {
				globalTableName, _ = gtd["GlobalTableName"].(string)
			}
			arn := nr.ResourceID("dynamodb-table", globalTableName)
			return globalTableName, map[string]any{"TableName": globalTableName, "Arn": arn, "StreamArn": ""}, nil
		},
		// GlobalTable cannot be deleted directly in real AWS; no-op.
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			arn := nr.ResourceID("dynamodb-table", physicalID)
			return physicalID, map[string]any{"TableName": physicalID, "Arn": arn, "StreamArn": ""}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			return nil
		},
		GetAttAttrs: []string{"Arn", "StreamArn"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"TableName"},
			RequireUpdate:      []string{"Replicas", "SSESpecification", "Tags"},
		},
	}
}
