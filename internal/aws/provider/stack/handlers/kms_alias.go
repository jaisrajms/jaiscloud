package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewKMSAliasHandler returns a ResourceHandler for AWS::KMS::Alias.
func NewKMSAliasHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			aliasName := propStr(props, "AliasName", "alias/"+logicalID)
			targetKeyID := propStr(props, "TargetKeyId", "")
			if _, err := call(ctx, "Key.CreateAlias", child(nr, map[string]any{
				"AliasName":   aliasName,
				"TargetKeyId": targetKeyID,
			})); err != nil {
				return "", nil, err
			}
			return aliasName, map[string]any{}, nil
		},
		// KMS alias names are immutable — always replace.
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			return "", nil, true, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "Key.DeleteAlias", &model.NormalizedRequest{Params: map[string]any{"AliasName": physicalID}})
			return err
		},
		GetAttAttrs: []string{},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"AliasName"},
		},
	}
}
