package dataproc

import (
	"context"
	"encoding/json"

	"jaiscloud/internal/gcp/reference"
)

// kmsKeyFromConfig returns config.encryptionConfig.kmsKey from a raw
// ClusterConfig, or "" when the caller did not ask for CMEK.
func kmsKeyFromConfig(config json.RawMessage) string {
	cfg := mustJSONMap(config)
	if cfg == nil {
		return ""
	}
	enc, _ := cfg["encryptionConfig"].(map[string]any)
	key, _ := enc["kmsKey"].(string)
	return key
}

// validateClusterKMS resolves a cluster's encryptionConfig.kmsKey against the
// KMS store: the cluster create/update rejects a malformed, cross-location or
// missing key the way real Dataproc does (INVALID_ARGUMENT). A nil KMS store
// checks the name shape and location only.
func (s *Service) validateClusterKMS(ctx context.Context, region string, in ClusterInput) error {
	key := kmsKeyFromConfig(in.Config)
	if key == "" {
		return nil
	}
	return reference.ValidateCryptoKey(ctx, s.kms, "encryptionConfig.kmsKey", key, region)
}

// validateWorkflowKMS is the WorkflowTemplate equivalent: its
// encryptionConfig.kmsKey lives in the stored definition and is resolved when
// the template is created or replaced.
func (s *Service) validateWorkflowKMS(ctx context.Context, region string, def map[string]any) error {
	enc, _ := def["encryptionConfig"].(map[string]any)
	key, _ := enc["kmsKey"].(string)
	if key == "" {
		return nil
	}
	return reference.ValidateCryptoKey(ctx, s.kms, "encryptionConfig.kmsKey", key, region)
}
