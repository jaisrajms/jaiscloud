// SecretVersion replication status shared by the Secret Manager REST provider
// and gRPC service.
//
// Real Secret Manager reports, per version, the Cloud KMS CryptoKeyVersion that
// actually wrapped that version's payload, as
// `replicationStatus.automatic.customerManagedEncryption.kmsKeyVersionName`
// (the Discovery schema's CustomerManagedEncryptionStatus). The emulator wraps
// each version with the secret's CMEK and tags the wrapped DEK with the
// CryptoKeyVersion it used (kms.EncodeVersionedCiphertext), so the exact
// version — not just the key — is known and can be echoed. It is a per-version
// fact: changing the secret's CMEK, or rotating the key's primary version,
// only affects versions added afterwards, which is precisely what the stored
// wrap-time tag models.
package secretmanager

import (
	"jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
)

// ReplicaStatus is one user-managed replica's view of a SecretVersion: its
// location and, when that replica's CMEK key wrapped the payload, the exact
// CryptoKeyVersion that did.
type ReplicaStatus struct {
	Location          string
	KmsKeyVersionName string
}

// VersionWrap is the version-side wrap data a replication status is rendered
// from: the primary wrap (KmsKeyName/WrappedDEK, what Access unwraps with) and
// the per-replica wraps a user-managed policy records (ReplicaDEKs). Both
// transports build it from the stored version, so REST and gRPC cannot disagree
// about which CryptoKeyVersion a replica reports.
type VersionWrap struct {
	KmsKeyName  string
	WrappedDEK  []byte
	ReplicaDEKs []secretmanagerstore.ReplicaDEK
}

// WrapOf returns v's wrap data.
func WrapOf(v secretmanagerstore.Version) VersionWrap {
	return VersionWrap{KmsKeyName: v.KmsKeyName, WrappedDEK: v.WrappedDEK, ReplicaDEKs: v.ReplicaDEKs}
}

// VersionReplicaStatuses renders the per-replica status of a version whose parent
// secret uses user-managed replication, or nil when the policy is automatic
// (the automatic arm is rendered instead). Every replica location is reported;
// a replica whose key wrapped this version's DEK carries the exact
// kmsKeyVersionName that did. Like real GCP, that status is per-version: it
// keeps naming the key that actually wrapped the payload even after the
// policy's CMEK key is updated, which only affects versions added afterwards.
//
// A version written before per-replica wraps were recorded (no ReplicaDEKs)
// falls back to the single-wrap answer: the replica in the location of the
// version's primary wrap reports it, the others report none. New versions
// record one wrap per CMEK replica, so every CMEK replica reports its own key
// version (DC57).
func VersionReplicaStatuses(w VersionWrap, repl *secretmanagerstore.Replication) []ReplicaStatus {
	if repl == nil || repl.UserManaged == nil {
		return nil
	}
	byLocation := make(map[string]secretmanagerstore.ReplicaDEK, len(w.ReplicaDEKs))
	for _, dek := range w.ReplicaDEKs {
		byLocation[dek.Location] = dek
	}
	wrapLocation, hasWrapLocation := cryptoKeyLocation(w.KmsKeyName)
	replicas := repl.UserManaged.Replicas
	out := make([]ReplicaStatus, 0, len(replicas))
	for _, rep := range replicas {
		status := ReplicaStatus{Location: rep.Location}
		if dek, ok := byLocation[rep.Location]; ok {
			status.KmsKeyVersionName = KmsKeyVersionName(dek.KmsKeyName, dek.WrappedDEK)
		} else if len(w.ReplicaDEKs) == 0 && hasWrapLocation && rep.Location == wrapLocation {
			status.KmsKeyVersionName = KmsKeyVersionName(w.KmsKeyName, w.WrappedDEK)
		}
		out = append(out, status)
	}
	return out
}

// KmsKeyVersionName renders the SecretVersion status's
// `kmsKeyVersionName` from the version's wrapping key and wrapped DEK. It
// returns "" when the version carries no customer-managed encryption, in which
// case the nested status message is omitted (real GCP only populates it when
// CMEK is used).
//
// Note the Secret Manager spelling: `.../cryptoKeys/{key}/versions/{version}`,
// not the Cloud KMS API's `.../cryptoKeyVersions/{version}` (the Discovery
// schema's CustomerManagedEncryptionStatus.kmsKeyVersionName documents the
// former).
func KmsKeyVersionName(kmsKeyName string, wrappedDEK []byte) string {
	if kmsKeyName == "" || len(wrappedDEK) == 0 {
		return ""
	}
	version, _, err := kms.DecodeVersionedCiphertext(wrappedDEK)
	if err != nil || version == "" {
		return ""
	}
	return kmsKeyName + "/versions/" + version
}
