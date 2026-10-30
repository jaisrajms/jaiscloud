package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewLogsLogGroupHandler returns a ResourceHandler for AWS::Logs::LogGroup.
func NewLogsLogGroupHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "LogGroupName", "/cfn/"+logicalID)
			params := copyProps(props)
			params["logGroupName"] = name
			if _, err := call(ctx, "CloudWatchLogs.CreateLogGroup", child(nr, params)); err != nil {
				return "", nil, err
			}
			arn := nr.ResourceID("logs-group", name)
			return name, map[string]any{"Arn": arn}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "LogGroupName", logicalID) != propStr(newProps, "LogGroupName", logicalID) {
				return "", nil, true, nil
			}
			// Update retention if changed
			if v, ok := newProps["RetentionInDays"]; ok {
				if _, err := call(ctx, "CloudWatchLogs.PutRetentionPolicy", child(nr, map[string]any{
					"logGroupName":    physicalID,
					"retentionInDays": v,
				})); err != nil {
					return "", nil, false, err
				}
			}
			arn := nr.ResourceID("logs-group", physicalID)
			return physicalID, map[string]any{"Arn": arn}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "CloudWatchLogs.DeleteLogGroup", &model.NormalizedRequest{Params: map[string]any{"logGroupName": physicalID}})
			return err
		},
		GetAttAttrs: []string{"Arn"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"LogGroupName"},
			RequireUpdate:      []string{"RetentionInDays", "Tags"},
		},
	}
}
