package handlers

import (
	"context"
	"encoding/json"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewS3BucketPolicyHandler returns a ResourceHandler for AWS::S3::BucketPolicy.
func NewS3BucketPolicyHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			bucket := propStr(props, "Bucket", "")
			// PolicyDocument may be a string or a map; normalise to JSON bytes.
			var policyBytes []byte
			switch p := props["PolicyDocument"].(type) {
			case string:
				policyBytes = []byte(p)
			default:
				policyBytes, _ = json.Marshal(p)
			}
			if _, err := call(ctx, "Object.PutBucketPolicy", child(nr, map[string]any{
				"_bucket": bucket,
				"_body":   policyBytes,
			})); err != nil {
				return "", nil, err
			}
			return bucket + "/policy", map[string]any{}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "Bucket", "") != propStr(newProps, "Bucket", "") {
				return "", nil, true, nil
			}
			// Re-apply the policy in place
			bucket := propStr(newProps, "Bucket", "")
			var policyBytes []byte
			switch p := newProps["PolicyDocument"].(type) {
			case string:
				policyBytes = []byte(p)
			default:
				policyBytes, _ = json.Marshal(p)
			}
			if _, err := call(ctx, "Object.PutBucketPolicy", child(nr, map[string]any{
				"_bucket": bucket,
				"_body":   policyBytes,
			})); err != nil {
				return "", nil, false, err
			}
			return physicalID, map[string]any{}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, props map[string]any) error {
			bucket := propStr(props, "Bucket", "")
			_, err := call(ctx, "Object.DeleteBucketPolicy", &model.NormalizedRequest{Params: map[string]any{"_bucket": bucket}})
			return err
		},
		GetAttAttrs: []string{},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"Bucket"},
		},
	}
}
