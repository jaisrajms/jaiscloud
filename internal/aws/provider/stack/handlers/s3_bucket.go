package handlers

import (
	"context"
	"strings"

	stackprovider "jaiscloud/internal/aws/provider/stack"
	"jaiscloud/internal/model"
)

// NewS3BucketHandler returns a ResourceHandler for AWS::S3::Bucket.
func NewS3BucketHandler(call Invoker) stackprovider.ResourceHandler {
	return stackprovider.ResourceHandler{
		Create: func(ctx context.Context, logicalID string, props map[string]any, nr *model.NormalizedRequest) (string, map[string]any, error) {
			stackName, _ := nr.Params["StackName"].(string)
			defaultName := logicalID
			if stackName != "" {
				defaultName = strings.ToLower(stackName + "-" + logicalID)
			}
			name := propStr(props, "BucketName", defaultName)
			if _, err := call(ctx, "Object.CreateBucket", child(nr, map[string]any{"_bucket": name})); err != nil {
				return "", nil, err
			}
			arn := nr.ResourceID("s3-bucket", name)
			return name, map[string]any{"Arn": arn, "DomainName": name + ".s3.amazonaws.com", "WebsiteURL": ""}, nil
		},
		Update: func(ctx context.Context, logicalID, physicalID string, oldProps, newProps map[string]any, nr *model.NormalizedRequest) (string, map[string]any, bool, error) {
			if propStr(oldProps, "BucketName", logicalID) != propStr(newProps, "BucketName", logicalID) {
				return "", nil, true, nil
			}
			// In-place: bucket settings can be updated; no provider-level UpdateBucket call needed
			arn := nr.ResourceID("s3-bucket", physicalID)
			return physicalID, map[string]any{"Arn": arn, "DomainName": physicalID + ".s3.amazonaws.com", "WebsiteURL": ""}, false, nil
		},
		Delete: func(ctx context.Context, physicalID string, _ map[string]any) error {
			_, err := call(ctx, "Object.DeleteBucket", &model.NormalizedRequest{Params: map[string]any{"_bucket": physicalID}})
			return err
		},
		GetAttAttrs: []string{"Arn", "DomainName", "WebsiteURL"},
		ReplacementRules: stackprovider.ReplacementRules{
			RequireReplacement: []string{"BucketName"},
			RequireUpdate:      []string{"Tags", "WebsiteConfiguration", "CorsConfiguration", "LifecycleConfiguration", "NotificationConfiguration"},
		},
	}
}
