package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewSecretsManagerSecretHandler returns a ResourceHandler for AWS::SecretsManager::Secret.
func NewSecretsManagerSecretHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			name := propStr(props, "Name", logicalID)
			params := copyProps(props)
			params["Name"] = name
			resp, err := call(ctx, "Secret.CreateSecret", child(nr, params))
			if err != nil {
				return "", nil, err
			}
			arn, _ := resp.Data["ARN"].(string)
			return arn, map[string]any{"Id": name, "Arn": arn}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "Name", logicalID) != propStr(newProps, "Name", logicalID) {
				return "", nil, true, nil
			}
			// In-place update via UpdateSecret
			params := map[string]any{"SecretId": physicalID}
			if v, ok := newProps["Description"]; ok {
				params["Description"] = v
			}
			if v, ok := newProps["SecretString"]; ok {
				params["SecretString"] = v
			}
			if v, ok := newProps["SecretBinary"]; ok {
				params["SecretBinary"] = v
			}
			if _, err := call(ctx, "Secret.UpdateSecret", child(nr, params)); err != nil {
				return "", nil, false, err
			}
			return physicalID, map[string]any{"Id": propStr(newProps, "Name", logicalID), "Arn": physicalID}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "Secret.DeleteSecret", &model.NormalizedRequest{
				ResourceID: func(_, _ string) string { return physicalID },
				Params:     map[string]any{"SecretId": physicalID, "ForceDeleteWithoutRecovery": true},
			})
			return err
		},
		GetAttAttrs: []string{"Arn"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"Name"},
		},
	}
}
