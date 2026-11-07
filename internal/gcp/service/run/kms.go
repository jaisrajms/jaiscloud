package run

import (
	"context"

	"jaiscloud/internal/gcp/reference"
)

// encryptionKeyFromService returns Service.template.encryptionKey, or "" when
// the service does not ask for CMEK.
func encryptionKeyFromService(data map[string]any) string {
	tmpl, _ := data["template"].(map[string]any)
	key, _ := tmpl["encryptionKey"].(string)
	return key
}

// validateServiceKMS resolves a service's template.encryptionKey against the
// KMS store: the create/update rejects a malformed, cross-region or missing key
// the way real Cloud Run does (INVALID_ARGUMENT). A nil KMS store checks the
// name shape and location only.
func (s *Service) validateServiceKMS(ctx context.Context, location string, data map[string]any) error {
	key := encryptionKeyFromService(data)
	if key == "" {
		return nil
	}
	return reference.ValidateCryptoKey(ctx, s.kms, "template.encryptionKey", key, location)
}
