package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewStepFunctionsStateMachineHandler returns a ResourceHandler for AWS::StepFunctions::StateMachine.
func NewStepFunctionsStateMachineHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "StateMachineName", logicalID)
			params := copyProps(props)
			params["name"] = name
			resp, err := call(ctx, "StepFunctions.CreateStateMachine", child(nr, params))
			if err != nil {
				return "", nil, err
			}
			smArn, _ := resp.Data["stateMachineArn"].(string)
			return smArn, map[string]any{"Arn": smArn, "Name": name}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			params := copyProps(newProps)
			params["stateMachineArn"] = physicalID
			if _, err := call(ctx, "StepFunctions.UpdateStateMachine", child(nr, params)); err != nil {
				return "", nil, false, err
			}
			name := propStr(newProps, "StateMachineName", logicalID)
			return physicalID, map[string]any{"Arn": physicalID, "Name": name}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "StepFunctions.DeleteStateMachine", &model.NormalizedRequest{Params: map[string]any{"stateMachineArn": physicalID}})
			return err
		},
		RefAttr:     "Arn",
		GetAttAttrs: []string{"Arn", "Name"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"StateMachineName", "StateMachineType"},
			RequireUpdate:      []string{"Definition", "DefinitionString", "RoleArn", "LoggingConfiguration", "TracingConfiguration", "Tags"},
		},
	}
}
