package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewEventsEventBusHandler returns a ResourceHandler for AWS::Events::EventBus.
func NewEventsEventBusHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "Name", logicalID)
			resp, err := call(ctx, "EventBridge.CreateEventBus", child(nr, map[string]any{"Name": name}))
			if err != nil {
				return "", nil, err
			}
			busArn, _ := resp.Data["EventBusArn"].(string)
			return name, map[string]any{"Arn": busArn, "Name": name}, nil
		},
		// EventBus is mostly immutable — in-place update returns existing attrs.
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			arn := nr.ResourceID("events-eventbus", physicalID)
			return physicalID, map[string]any{"Arn": arn, "Name": physicalID}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "EventBridge.DeleteEventBus", &model.NormalizedRequest{Params: map[string]any{"Name": physicalID}})
			return err
		},
		RefAttr:     "Arn",
		GetAttAttrs: []string{"Arn", "Name"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"Name"},
			RequireUpdate:      []string{"Policy", "Tags"},
		},
	}
}
