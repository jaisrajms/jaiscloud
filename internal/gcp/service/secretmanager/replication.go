// Replication-policy validation and update semantics shared by the Secret
// Manager REST provider and gRPC service.
//
// Real Secret Manager takes the replication policy as a oneof
// (`automatic` xor `userManaged`) whose non-CMEK shape is immutable: the
// choosing-a-replication-policy and CMEK guides both state that only the CMEK
// configuration can be modified after creation ("Secret Manager does not
// currently support modifying other properties of the replication policy other
// than the CMEK configuration"), and the Discovery document marks `replication`
// Immutable. The emulator used to keep a single key name and re-render the
// automatic arm for every secret, so a caller that set a user-managed policy
// read back a different one. These helpers are transport-neutral so REST and
// gRPC cannot disagree about what a valid policy is or how an update merges.
package secretmanager

import (
	"context"
	"fmt"
	"strings"

	"jaiscloud/internal/gcp/crypto"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/model"
)

// invalidReplication is the 400 INVALID_ARGUMENT every replication-policy
// violation answers with.
func invalidReplication(format string, args ...any) error {
	return model.NewProviderError("InvalidArgument", fmt.Sprintf(format, args...), 400)
}

// ValidateReplication checks a replication policy against the documented
// shape: exactly one arm, non-empty user-managed replicas with distinct
// locations, and a CMEK key (when one is named) that resides in the location
// the arm requires — `global` for automatic replication, the replica's own
// location for user-managed replication. A nil policy is valid (the API's
// default, automatic replication).
func ValidateReplication(r *secretmanagerstore.Replication) error {
	if r == nil {
		return nil
	}
	if r.Automatic != nil && r.UserManaged != nil {
		return invalidReplication("replication must set exactly one of automatic or userManaged")
	}
	if r.Automatic == nil && r.UserManaged == nil {
		return invalidReplication("replication must set exactly one of automatic or userManaged")
	}
	if r.Automatic != nil {
		cme := r.Automatic.CustomerManagedEncryption
		if cme == nil {
			return nil
		}
		if cme.KmsKeyName == "" {
			return invalidReplication("replication.automatic.customerManagedEncryption.kmsKeyName is required")
		}
		location, ok := cryptoKeyLocation(cme.KmsKeyName)
		if !ok {
			return invalidReplication("replication.automatic.customerManagedEncryption.kmsKeyName %q is not a Cloud KMS CryptoKey name", cme.KmsKeyName)
		}
		if location != "global" {
			return invalidReplication("replication.automatic.customerManagedEncryption.kmsKeyName must reside in global, got %q", location)
		}
		return nil
	}

	replicas := r.UserManaged.Replicas
	if len(replicas) == 0 {
		return invalidReplication("replication.userManaged.replicas cannot be empty")
	}
	seen := make(map[string]bool, len(replicas))
	for i, rep := range replicas {
		if rep.Location == "" {
			return invalidReplication("replication.userManaged.replicas[%d].location is required", i)
		}
		if !validReplicaLoc(rep.Location) {
			return invalidReplication("replication.userManaged.replicas[%d].location %q is not a supported location", i, rep.Location)
		}
		if seen[rep.Location] {
			return invalidReplication("replication.userManaged.replicas contains duplicate location %q", rep.Location)
		}
		seen[rep.Location] = true
		cme := rep.CustomerManagedEncryption
		if cme == nil {
			continue
		}
		if cme.KmsKeyName == "" {
			return invalidReplication("replication.userManaged.replicas[%d].customerManagedEncryption.kmsKeyName is required", i)
		}
		location, ok := cryptoKeyLocation(cme.KmsKeyName)
		if !ok {
			return invalidReplication("replication.userManaged.replicas[%d].customerManagedEncryption.kmsKeyName %q is not a Cloud KMS CryptoKey name", i, cme.KmsKeyName)
		}
		if location != rep.Location {
			return invalidReplication("replication.userManaged.replicas[%d].customerManagedEncryption.kmsKeyName must reside in replica location %q, got %q", i, rep.Location, location)
		}
	}
	return nil
}

// ValidateReplicationKeys validates a replication policy's shape and resolves
// every Cloud KMS key it names. Real GCP answers at create/patch time when a
// CMEK key is missing (INVALID_ARGUMENT) or cannot encrypt because its primary
// version is disabled/destroyed (FAILED_PRECONDITION); without this, the
// emulator accepted the policy and only failed later at AddSecretVersion. The
// key is resolved per replica for a user-managed policy, so a policy that names
// two keys fails on either one. Cross-project keys are accepted: Secret Manager
// explicitly supports storing secrets and keys in separate projects.
func ValidateReplicationKeys(ctx context.Context, enc crypto.EnvelopeEncryptor, account string, r *secretmanagerstore.Replication) error {
	if err := ValidateReplication(r); err != nil {
		return err
	}
	for _, binding := range r.CMEKBindings() {
		if err := enc.ValidateKey(ctx, account, binding.KmsKeyName); err != nil {
			return err
		}
	}
	return nil
}

// MergeReplication applies an incoming replication policy to the stored one.
// The oneof arm and the user-managed replica locations are immutable; only the
// CMEK configuration may change, and it is applied only to versions added
// afterwards (the payloads already written keep their wrap). An incoming policy
// without a CMEK block clears the stored key, which is how real GCP disables
// CMEK ("remove CMEK configuration ... by updating the replication policy").
// A nil incoming policy is a no-op, and a nil stored policy (a record written
// before the policy was modelled) is replaced wholesale.
func MergeReplication(stored, incoming *secretmanagerstore.Replication) (*secretmanagerstore.Replication, error) {
	if incoming == nil {
		return stored, nil
	}
	if err := ValidateReplication(incoming); err != nil {
		return nil, err
	}
	if stored == nil {
		return incoming, nil
	}
	if (stored.Automatic != nil) != (incoming.Automatic != nil) {
		return nil, invalidReplication("the replication policy of a secret cannot be changed")
	}
	if incoming.Automatic != nil {
		return &secretmanagerstore.Replication{
			Automatic: &secretmanagerstore.AutomaticReplication{
				CustomerManagedEncryption: incoming.Automatic.CustomerManagedEncryption,
			},
		}, nil
	}

	// User-managed: the replica locations are fixed, so the incoming policy must
	// name the same set. The stored order is preserved in the response. (A stored
	// record with neither arm set cannot be written by either transport; the
	// guard keeps a hand-edited row from panicking.)
	if stored.UserManaged == nil {
		return nil, invalidReplication("the replication policy of a secret cannot be changed")
	}
	incomingByLocation := make(map[string]secretmanagerstore.Replica, len(incoming.UserManaged.Replicas))
	for _, rep := range incoming.UserManaged.Replicas {
		incomingByLocation[rep.Location] = rep
	}
	if len(incomingByLocation) != len(stored.UserManaged.Replicas) {
		return nil, invalidReplication("the replica locations of a secret cannot be changed")
	}
	merged := make([]secretmanagerstore.Replica, 0, len(stored.UserManaged.Replicas))
	for _, rep := range stored.UserManaged.Replicas {
		in, ok := incomingByLocation[rep.Location]
		if !ok {
			return nil, invalidReplication("the replica locations of a secret cannot be changed")
		}
		merged = append(merged, secretmanagerstore.Replica{
			Location:                  rep.Location,
			CustomerManagedEncryption: in.CustomerManagedEncryption,
		})
	}
	return &secretmanagerstore.Replication{UserManaged: &secretmanagerstore.UserManagedReplication{Replicas: merged}}, nil
}

// ParentReplication returns the replication policy a version response renders in
// its replicationStatus. Versions cascade with their secret, so the parent exists
// whenever the version does; a store failure is propagated instead of rendering
// the default automatic arm for what may be a user-managed secret. Shared by
// both transports so they cannot disagree about the version status (DC45).
func ParentReplication(ctx context.Context, secrets secretmanagerstore.Store, project, secretID string) (*secretmanagerstore.Replication, error) {
	sec, err := secrets.GetSecret(ctx, project, secretID)
	if err != nil {
		return nil, err
	}
	return sec.ReplicationPolicy(), nil
}

// cryptoKeyLocation extracts the location segment of a Cloud KMS CryptoKey
// resource name (projects/{p}/locations/{l}/keyRings/{kr}/cryptoKeys/{k}).
func cryptoKeyLocation(name string) (string, bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 8 || parts[0] != "projects" || parts[2] != "locations" || parts[4] != "keyRings" || parts[6] != "cryptoKeys" {
		return "", false
	}
	if parts[1] == "" || parts[3] == "" || parts[5] == "" || parts[7] == "" {
		return "", false
	}
	return parts[3], true
}
