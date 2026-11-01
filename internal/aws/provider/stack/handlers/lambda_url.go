package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewLambdaUrlHandler returns a ResourceHandler for AWS::Lambda::Url.
func NewLambdaUrlHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			targetFunc := propStr(props, "TargetFunctionArn", "")
			authType := propStr(props, "AuthType", "NONE")
			params := map[string]any{
				"_function_name": targetFunc,
				"AuthType":       authType,
			}
			if cors, ok := props["Cors"]; ok {
				params["Cors"] = cors
			}
			resp, err := call(ctx, "Function.CreateFunctionUrlConfig", child(nr, params))
			if err != nil {
				return "", nil, err
			}
			funcURL, _ := resp.Data["FunctionUrl"].(string)
			funcArn, _ := resp.Data["FunctionArn"].(string)
			return funcURL, map[string]any{"FunctionUrl": funcURL, "FunctionArn": funcArn}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			targetFunc := propStr(newProps, "TargetFunctionArn", "")
			params := map[string]any{
				"_function_name": targetFunc,
				"AuthType":       propStr(newProps, "AuthType", "NONE"),
			}
			if cors, ok := newProps["Cors"]; ok {
				params["Cors"] = cors
			}
			resp, err := call(ctx, "Function.UpdateFunctionUrlConfig", child(nr, params))
			if err != nil {
				return "", nil, false, err
			}
			funcURL, _ := resp.Data["FunctionUrl"].(string)
			funcArn, _ := resp.Data["FunctionArn"].(string)
			return physicalID, map[string]any{"FunctionUrl": funcURL, "FunctionArn": funcArn}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			targetFunc := propStr(props, "TargetFunctionArn", "")
			_, err := call(ctx, "Function.DeleteFunctionUrlConfig", &model.NormalizedRequest{
				Params: map[string]any{"_function_name": targetFunc},
			})
			return err
		},
		GetAttAttrs: []string{"FunctionUrl", "FunctionArn"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireUpdate: []string{"AuthType", "Cors"},
		},
	}
}
