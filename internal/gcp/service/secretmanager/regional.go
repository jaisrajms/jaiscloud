// Regional-secret validation shared by the Secret Manager REST provider and
// gRPC service.
//
// A regional secret lives at projects/{p}/locations/{l}/secrets/{s}. Unlike a
// project-level secret it has no replication policy (the location is the
// storage location), and its CMEK configuration is the top-level
// `customerManagedEncryption` field, whose key must reside in the secret's own
// location. See
// https://cloud.google.com/secret-manager/docs/reference/rest/v1/projects.locations.secrets
// and https://cloud.google.com/secret-manager/regional-secrets/create-regional-secret
// ("Replication policies don't apply to regional secrets").
package secretmanager

import (
	"context"
	"fmt"

	"jaiscloud/internal/gcp/crypto"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/model"
)

// RegionalReplicationUnsupported is the 400 for a `replication` policy on a
// regional secret (or a regional write that carries one).
func RegionalReplicationUnsupported() error {
	return model.NewProviderError("InvalidArgument", "replication policies do not apply to regional secrets", 400)
}

// ProjectCMEKUnsupported is the 400 for `customerManagedEncryption` on a
// project-level secret; the field is documented for regionalized secrets only.
func ProjectCMEKUnsupported() error {
	return model.NewProviderError("InvalidArgument", "customerManagedEncryption is only supported on regional secrets", 400)
}

// ValidateRegionalCMEK checks a regional secret's top-level
// customerManagedEncryption shape: the key must be a Cloud KMS CryptoKey in the
// secret's own location. A nil configuration (Google-managed encryption) is
// valid.
func ValidateRegionalCMEK(location string, cme *secretmanagerstore.CustomerManagedEncryption) error {
	if cme == nil {
		return nil
	}
	if cme.KmsKeyName == "" {
		return model.NewProviderError("InvalidArgument", "customerManagedEncryption.kmsKeyName is required", 400)
	}
	keyLocation, ok := cryptoKeyLocation(cme.KmsKeyName)
	if !ok {
		return model.NewProviderError("InvalidArgument",
			fmt.Sprintf("customerManagedEncryption.kmsKeyName %q is not a Cloud KMS CryptoKey name", cme.KmsKeyName), 400)
	}
	if location != "" && keyLocation != location {
		return model.NewProviderError("InvalidArgument",
			fmt.Sprintf("customerManagedEncryption.kmsKeyName must reside in the secret's location %q, got %q", location, keyLocation), 400)
	}
	return nil
}

// ValidateRegionalCMEKKey validates the configuration and resolves the named
// Cloud KMS key, so a missing or unusable key fails at create/patch time as it
// does on real GCP rather than at the next AddSecretVersion.
func ValidateRegionalCMEKKey(ctx context.Context, enc crypto.EnvelopeEncryptor, account, location string, cme *secretmanagerstore.CustomerManagedEncryption) error {
	if err := ValidateRegionalCMEK(location, cme); err != nil {
		return err
	}
	if cme == nil || cme.KmsKeyName == "" {
		return nil
	}
	return enc.ValidateKey(ctx, account, cme.KmsKeyName)
}
