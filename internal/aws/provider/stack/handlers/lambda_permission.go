package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewLambdaPermissionHandler returns a ResourceHandler for AWS::Lambda::Permission.
func NewLambdaPermissionHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			funcName := propStr(props, "FunctionName", "")
			stmtID := propStr(props, "StatementId", logicalID)
			params := copyProps(props)
			params["_function_name"] = funcName
			params["StatementId"] = stmtID
			if _, err := call(ctx, "Function.AddPermission", child(nr, params)); err != nil {
				return "", nil, err
			}
			return funcName + "/policy/" + stmtID, map[string]any{}, nil
		},
		// Lambda permissions are immutable — always require replacement.
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			return "", nil, true, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			funcName := propStr(props, "FunctionName", "")
			stmtID := propStr(props, "StatementId", "")
			_, err := call(ctx, "Function.RemovePermission", &model.NormalizedRequest{
				Params: map[string]any{"_function_name": funcName, "_statement_id": stmtID},
			})
			return err
		},
		GetAttAttrs: []string{},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"FunctionName", "StatementId"},
		},
	}
}
