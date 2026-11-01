package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewLambdaEventSourceMappingHandler returns a ResourceHandler for AWS::Lambda::EventSourceMapping.
func NewLambdaEventSourceMappingHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			params := copyProps(props)
			resp, err := call(ctx, "Function.CreateEventSourceMapping", child(nr, params))
			if err != nil {
				return "", nil, err
			}
			uuid, _ := resp.Data["UUID"].(string)
			return uuid, map[string]any{"UUID": uuid}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			// In-place update via UpdateEventSourceMapping
			updateParams := map[string]any{"_esm_uuid": physicalID}
			if v, ok := newProps["BatchSize"]; ok {
				updateParams["BatchSize"] = v
			}
			if v, ok := newProps["Enabled"]; ok {
				updateParams["Enabled"] = v
			}
			if v, ok := newProps["FilterCriteria"]; ok {
				updateParams["FilterCriteria"] = v
			}
			if _, err := call(ctx, "Function.CreateEventSourceMapping", child(nr, map[string]any{
				"_esm_uuid": physicalID,
				"BatchSize": newProps["BatchSize"],
				"Enabled":   newProps["Enabled"],
			})); err != nil {
				// Fall back to replacement if update fails
				return "", nil, true, nil
			}
			return physicalID, map[string]any{"UUID": physicalID}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "Function.DeleteEventSourceMapping", &model.NormalizedRequest{
				Params: map[string]any{"_esm_uuid": physicalID},
			})
			return err
		},
		GetAttAttrs: []string{},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireUpdate: []string{"BatchSize", "Enabled", "FilterCriteria"},
		},
	}
}
