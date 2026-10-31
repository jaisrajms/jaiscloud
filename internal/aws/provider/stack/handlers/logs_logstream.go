package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewLogsLogStreamHandler returns a ResourceHandler for AWS::Logs::LogStream.
func NewLogsLogStreamHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			groupName := propStr(props, "LogGroupName", "")
			streamName := propStr(props, "LogStreamName", logicalID)
			if _, err := call(ctx, "CloudWatchLogs.CreateLogStream", child(nr, map[string]any{
				"logGroupName":  groupName,
				"logStreamName": streamName,
			})); err != nil {
				return "", nil, err
			}
			return streamName, map[string]any{}, nil
		},
		// Log streams are immutable — always replace.
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			return "", nil, true, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			groupName := propStr(props, "LogGroupName", "")
			_, err := call(ctx, "CloudWatchLogs.DeleteLogStream", &model.NormalizedRequest{
				Params: map[string]any{"logGroupName": groupName, "logStreamName": physicalID},
			})
			return err
		},
		GetAttAttrs: []string{},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"LogGroupName", "LogStreamName"},
		},
	}
}
