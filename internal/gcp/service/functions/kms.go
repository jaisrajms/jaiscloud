package functions

import (
	"context"

	"jaiscloud/internal/gcp/reference"
)

// validateFunctionKMS resolves a function's kmsKeyName against the KMS store
// (FC4-1). Real Cloud Functions rejects a create/update whose CMEK key does not
// exist or is not in the function's region (INVALID_ARGUMENT); a nil store
// checks only the CryptoKey name shape and location.
func (s *Service) validateFunctionKMS(ctx context.Context, location, key string) error {
	if key == "" {
		return nil
	}
	return reference.ValidateCryptoKey(ctx, s.kms, "kmsKeyName", key, location)
}
