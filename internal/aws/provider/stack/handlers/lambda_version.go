package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewLambdaVersionHandler returns a ResourceHandler for AWS::Lambda::Version.
func NewLambdaVersionHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			funcName := propStr(props, "FunctionName", "")
			desc := propStr(props, "Description", "")
			resp, err := call(ctx, "Function.PublishVersion", child(nr, map[string]any{
				"_function_name": funcName,
				"Description":    desc,
			}))
			if err != nil {
				return "", nil, err
			}
			versionedArn, _ := resp.Data["FunctionArn"].(string)
			version, _ := resp.Data["Version"].(string)
			return versionedArn, map[string]any{"FunctionArn": versionedArn, "Version": version}, nil
		},
		// Lambda versions are immutable — any change requires replacement.
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			return "", nil, true, nil
		},
		// Lambda versions cannot be deleted directly via CloudFormation; no-op.
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			return nil
		},
		GetAttAttrs: []string{"Version"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"FunctionName"},
		},
	}
}
