package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewLambdaAliasHandler returns a ResourceHandler for AWS::Lambda::Alias.
func NewLambdaAliasHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			funcName := propStr(props, "FunctionName", "")
			aliasName := propStr(props, "Name", logicalID)
			funcVersion := propStr(props, "FunctionVersion", "$LATEST")
			resp, err := call(ctx, "Function.CreateAlias", child(nr, map[string]any{
				"_function_name":  funcName,
				"Name":            aliasName,
				"FunctionVersion": funcVersion,
				"Description":     propStr(props, "Description", ""),
			}))
			if err != nil {
				return "", nil, err
			}
			arn, _ := resp.Data["AliasArn"].(string)
			return arn, map[string]any{"AliasArn": arn}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "Name", logicalID) != propStr(newProps, "Name", logicalID) {
				return "", nil, true, nil
			}
			if propStr(oldProps, "FunctionName", "") != propStr(newProps, "FunctionName", "") {
				return "", nil, true, nil
			}
			// In-place: update alias function version
			funcName := propStr(newProps, "FunctionName", "")
			aliasName := propStr(newProps, "Name", logicalID)
			resp, err := call(ctx, "Function.UpdateAlias", child(nr, map[string]any{
				"_function_name":  funcName,
				"_alias_name":     aliasName,
				"FunctionVersion": propStr(newProps, "FunctionVersion", "$LATEST"),
				"Description":     propStr(newProps, "Description", ""),
			}))
			if err != nil {
				return "", nil, false, err
			}
			arn, _ := resp.Data["AliasArn"].(string)
			return physicalID, map[string]any{"AliasArn": arn}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			funcName := propStr(props, "FunctionName", "")
			aliasName := propStr(props, "Name", "")
			_, err := call(ctx, "Function.DeleteAlias", &model.NormalizedRequest{
				Params: map[string]any{"_function_name": funcName, "_alias_name": aliasName},
			})
			return err
		},
		GetAttAttrs: []string{"AliasArn"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"Name", "FunctionName"},
		},
	}
}
