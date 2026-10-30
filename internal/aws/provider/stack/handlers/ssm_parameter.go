package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewSSMParameterHandler returns a ResourceHandler for AWS::SSM::Parameter.
func NewSSMParameterHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "Name", "/cfn/"+logicalID)
			params := copyProps(props)
			params["Name"] = name
			if _, err := call(ctx, "Parameter.PutParameter", child(nr, params)); err != nil {
				return "", nil, err
			}
			value := propStr(props, "Value", "")
			return name, map[string]any{"Value": value}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "Name", logicalID) != propStr(newProps, "Name", logicalID) {
				return "", nil, true, nil
			}
			if propStr(oldProps, "Type", "") != propStr(newProps, "Type", "") {
				return "", nil, true, nil
			}
			// In-place update — PutParameter with Overwrite=true
			params := copyProps(newProps)
			params["Name"] = physicalID
			params["Overwrite"] = true
			if _, err := call(ctx, "Parameter.PutParameter", child(nr, params)); err != nil {
				return "", nil, false, err
			}
			value := propStr(newProps, "Value", "")
			return physicalID, map[string]any{"Value": value}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "Parameter.DeleteParameter", &model.NormalizedRequest{Params: map[string]any{"Name": physicalID}})
			return err
		},
		GetAttAttrs: []string{"Value"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"Name", "Type"},
		},
	}
}
