package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewLambdaFunctionHandler returns a ResourceHandler for AWS::Lambda::Function.
func NewLambdaFunctionHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "FunctionName", logicalID)
			params := copyProps(props)
			params["FunctionName"] = name
			resp, err := call(ctx, "Function.CreateFunction", child(nr, params))
			if err != nil {
				return "", nil, err
			}
			arn, _ := resp.Data["FunctionArn"].(string)
			return name, map[string]any{"Arn": arn}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "FunctionName", logicalID) != propStr(newProps, "FunctionName", logicalID) {
				return "", nil, true, nil
			}
			// Update function code
			codeParams := map[string]any{"_function_name": physicalID}
			if code, ok := newProps["Code"].(map[string]any); ok {
				for k, v := range code {
					codeParams[k] = v
				}
			}
			if _, err := call(ctx, "Function.UpdateFunctionCode", child(nr, codeParams)); err != nil {
				return "", nil, false, err
			}
			// Update function configuration
			cfgParams := map[string]any{
				"_function_name": physicalID,
				"Description":    propStr(newProps, "Description", ""),
				"Handler":        propStr(newProps, "Handler", ""),
				"Runtime":        propStr(newProps, "Runtime", ""),
				"Role":           propStr(newProps, "Role", ""),
			}
			if v, ok := newProps["MemorySize"]; ok {
				cfgParams["MemorySize"] = v
			}
			if v, ok := newProps["Timeout"]; ok {
				cfgParams["Timeout"] = v
			}
			if v, ok := newProps["Environment"]; ok {
				cfgParams["Environment"] = v
			}
			if v, ok := newProps["Layers"]; ok {
				cfgParams["Layers"] = v
			}
			if v, ok := newProps["VpcConfig"]; ok {
				cfgParams["VpcConfig"] = v
			}
			if _, err := call(ctx, "Function.UpdateFunctionConfiguration", child(nr, cfgParams)); err != nil {
				return "", nil, false, err
			}
			arn := nr.ResourceID("lambda-function", physicalID)
			return physicalID, map[string]any{"Arn": arn}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "Function.DeleteFunction", &model.NormalizedRequest{Params: map[string]any{"_function_name": physicalID}})
			return err
		},
		GetAttAttrs: []string{"Arn"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"FunctionName"},
			RequireUpdate:      []string{"Code", "Description", "Environment", "Handler", "Layers", "MemorySize", "Role", "Runtime", "Tags", "Timeout", "VpcConfig"},
		},
	}
}
