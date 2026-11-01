package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewCloudWatchAlarmHandler returns a ResourceHandler for AWS::CloudWatch::Alarm.
func NewCloudWatchAlarmHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "AlarmName", logicalID)
			params := copyProps(props)
			params["AlarmName"] = name
			if _, err := call(ctx, "CloudWatch.PutMetricAlarm", child(nr, params)); err != nil {
				return "", nil, err
			}
			arn := nr.ResourceID("cloudwatch-alarm", name)
			return name, map[string]any{"Arn": arn}, nil
		},
		// PutMetricAlarm is idempotent — re-apply with updated params.
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			params := copyProps(newProps)
			params["AlarmName"] = physicalID
			if _, err := call(ctx, "CloudWatch.PutMetricAlarm", child(nr, params)); err != nil {
				return "", nil, false, err
			}
			arn := nr.ResourceID("cloudwatch-alarm", physicalID)
			return physicalID, map[string]any{"Arn": arn}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "CloudWatch.DeleteAlarms", &model.NormalizedRequest{
				Params: map[string]any{"AlarmNames": []any{physicalID}},
			})
			return err
		},
		GetAttAttrs: []string{"Arn"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireUpdate: []string{"Threshold", "ComparisonOperator", "EvaluationPeriods"},
		},
	}
}
