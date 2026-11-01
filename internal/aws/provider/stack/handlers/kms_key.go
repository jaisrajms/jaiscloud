package handlers

import (
	"context"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewKMSKeyHandler returns a ResourceHandler for AWS::KMS::Key.
func NewKMSKeyHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			params := copyProps(props)
			resp, err := call(ctx, "Key.CreateKey", child(nr, params))
			if err != nil {
				return "", nil, err
			}
			keyID, arn := "", ""
			if km, ok := resp.Data["KeyMetadata"].(map[string]any); ok {
				keyID, _ = km["KeyId"].(string)
				arn, _ = km["Arn"].(string)
			}
			return keyID, map[string]any{"Arn": arn, "KeyId": keyID}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			// Update description if changed
			if propStr(oldProps, "Description", "") != propStr(newProps, "Description", "") {
				if _, err := call(ctx, "Key.UpdateKeyDescription", child(nr, map[string]any{
					"KeyId":       physicalID,
					"Description": propStr(newProps, "Description", ""),
				})); err != nil {
					return "", nil, false, err
				}
			}
			// Update key policy if changed
			if propStr(oldProps, "KeyPolicy", "") != propStr(newProps, "KeyPolicy", "") {
				if _, err := call(ctx, "Key.PutKeyPolicy", child(nr, map[string]any{
					"KeyId":      physicalID,
					"PolicyName": "default",
					"Policy":     propStr(newProps, "KeyPolicy", ""),
				})); err != nil {
					return "", nil, false, err
				}
			}
			arn := nr.ResourceID("kms-key", physicalID)
			return physicalID, map[string]any{"Arn": arn, "KeyId": physicalID}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "Key.ScheduleKeyDeletion", &model.NormalizedRequest{
				Params: map[string]any{"KeyId": physicalID, "PendingWindowInDays": float64(7)},
			})
			return err
		},
		GetAttAttrs: []string{"Arn", "KeyId"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireUpdate: []string{"Description", "EnableKeyRotation", "KeyPolicy", "Tags"},
		},
	}
}
