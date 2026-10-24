// Package secret provides the Secret Manager store. Secrets and versions live
// in dedicated jc_sm_secrets / jc_sm_versions tables (mirroring AWS
// jc_sm_secrets / jc_sm_versions).
package secretmanager

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"time"
)

var (
	ErrNoSuchSecret  = errors.New("NoSuchSecret")
	ErrNoSuchVersion = errors.New("NoSuchVersion")
	ErrAlreadyExists = errors.New("AlreadyExists")
	// ErrManagedRotationEnabled is returned by a rotation helper when Cloud SQL
	// managed rotation is already ACTIVE for a secret: the API allows enabling
	// it only once (subsequent rotations go through RotateSecret).
	ErrManagedRotationEnabled = errors.New("managed rotation already enabled")
)

// Rotation is a secret's automatic-rotation schedule. NextRotationTime is an
// RFC3339 timestamp; RotationPeriod is a duration string (e.g. "3600s").
// ManagedRotation carries the Cloud SQL managed-rotation config/status, nil
// until EnableManagedRotation has been called.
type Rotation struct {
	NextRotationTime string           `json:"nextRotationTime"`
	RotationPeriod   string           `json:"rotationPeriod"`
	ManagedRotation  *ManagedRotation `json:"managedRotation,omitempty"`
}

// ManagedRotation is the Cloud SQL managed-rotation state for a secret (GCP's
// typed-secret managed rotation). State is "ACTIVE"/"INACTIVE"; InstanceID and
// Username are the linked Cloud SQL single-user credentials.
type ManagedRotation struct {
	State      string `json:"state"`
	InstanceID string `json:"instanceId,omitempty"`
	Username   string `json:"username,omitempty"`
}

// Replication is a secret's replication policy — the `replication` oneof from
// the API. Exactly one arm is set; a nil Replication on a stored record means
// the policy predates it being modelled, and reads back as the automatic arm
// (see Secret.ReplicationPolicy).
type Replication struct {
	Automatic   *AutomaticReplication   `json:"automatic,omitempty"`
	UserManaged *UserManagedReplication `json:"userManaged,omitempty"`
}

// AutomaticReplication replicates the payload without restrictions.
type AutomaticReplication struct {
	CustomerManagedEncryption *CustomerManagedEncryption `json:"customerManagedEncryption,omitempty"`
}

// UserManagedReplication replicates the payload into the listed locations.
type UserManagedReplication struct {
	Replicas []Replica `json:"replicas"`
}

// Replica is one user-managed replication location. CustomerManagedEncryption
// is per replica, and its key must reside in the replica's location.
type Replica struct {
	Location                  string                     `json:"location"`
	CustomerManagedEncryption *CustomerManagedEncryption `json:"customerManagedEncryption,omitempty"`
}

// CustomerManagedEncryption names the Cloud KMS CryptoKey that encrypts new
// versions. A nil pointer means Google-managed (default) encryption.
type CustomerManagedEncryption struct {
	KmsKeyName string `json:"kmsKeyName"`
}

// CMEKBinding is one replica location bound to the Cloud KMS key that encrypts
// that replica's copy of a version. Automatic replication is a single binding
// whose location is "global" (the CMEK guide requires a global key there); a
// user-managed policy has one per replica that names a key.
type CMEKBinding struct {
	Location   string
	KmsKeyName string
}

// CMEKBindings returns the Cloud KMS keys a version added under this policy is
// wrapped with, one per replica that uses CMEK, in replica order. The
// automatic arm contributes at most one "global" binding; replicas without
// customerManagedEncryption are Google-managed and contribute none.
func (r *Replication) CMEKBindings() []CMEKBinding {
	if r == nil {
		return nil
	}
	if r.Automatic != nil {
		if cme := r.Automatic.CustomerManagedEncryption; cme != nil && cme.KmsKeyName != "" {
			return []CMEKBinding{{Location: "global", KmsKeyName: cme.KmsKeyName}}
		}
		return nil
	}
	if r.UserManaged != nil {
		out := make([]CMEKBinding, 0, len(r.UserManaged.Replicas))
		for _, rep := range r.UserManaged.Replicas {
			if cme := rep.CustomerManagedEncryption; cme != nil && cme.KmsKeyName != "" {
				out = append(out, CMEKBinding{Location: rep.Location, KmsKeyName: cme.KmsKeyName})
			}
		}
		return out
	}
	return nil
}

// WrapKey returns the Cloud KMS key a version added now is wrapped with: the
// automatic arm's key, or the first user-managed replica's. It is the legacy
// single-key view kept for callers that only need one key; new versions record
// one wrap per binding (CMEKBindings / Version.ReplicaDEKs) so every CMEK
// replica can report its own CryptoKeyVersion.
func (r *Replication) WrapKey() string {
	bindings := r.CMEKBindings()
	if len(bindings) == 0 {
		return ""
	}
	return bindings[0].KmsKeyName
}

// GeneratePassword returns a fresh random password for Cloud SQL managed
// rotation. The API only specifies "a random password will be generated", so
// the alphabet is an implementation detail: 24 CSPRNG bytes, base64-encoded
// (32 printable characters).
func GeneratePassword() (string, error) {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b[:]), nil
}

// Secret is Secret Manager secret metadata.
type Secret struct {
	ID          string
	Labels      map[string]string
	Annotations map[string]string
	// Topics are the Pub/Sub topics the secret's control-plane event
	// notifications are published to (`projects/{p}/topics/{t}`, at most 10).
	// Empty when the secret has no event notifications configured.
	Topics         []string
	CreateTime     time.Time
	NextVer        int
	Rotation       *Rotation      // nil when rotation is disabled
	VersionAliases map[string]int // alias name → version number, nil when none
	// Replication is the secret's replication policy. nil on records written
	// before it was modelled (they carry only KmsKeyName); use
	// ReplicationPolicy() to read it.
	Replication *Replication `json:"replication,omitempty"`
	// KmsKeyName is the effective wrapping key of new versions (the automatic
	// arm's key or the first user-managed replica's). Kept alongside the policy
	// because the version-wrap path and records written by older builds read
	// it; it is never the source of truth for the wire shape.
	KmsKeyName string
	// VersionDestroyTTL is the destruction delay applied when a version is
	// destroyed (the API's `versionDestroyTtl`, stored in its canonical
	// google-duration form, e.g. "86400s"). Empty disables delayed destruction.
	VersionDestroyTTL string
	// ExpireTime is when the secret is automatically deleted; zero means the
	// secret does not expire. The API's `ttl` input field is resolved to this
	// instant at write time and never stored.
	ExpireTime time.Time
}

// ReplicationPolicy returns the secret's replication policy, reconstructing the
// automatic arm for records written before the policy was modelled. It never
// returns nil, so both transports render the same shape for every record.
func (s Secret) ReplicationPolicy() *Replication {
	if s.Replication != nil {
		return s.Replication
	}
	auto := &AutomaticReplication{}
	if s.KmsKeyName != "" {
		auto.CustomerManagedEncryption = &CustomerManagedEncryption{KmsKeyName: s.KmsKeyName}
	}
	return &Replication{Automatic: auto}
}

// SetReplication records r as the secret's replication policy and derives the
// effective wrapping key kept in KmsKeyName.
func (s *Secret) SetReplication(r *Replication) {
	s.Replication = r
	s.KmsKeyName = r.WrapKey()
}

// Version is a Secret Manager secret version.
type Version struct {
	SecretID   string
	VersionID  string
	State      string
	CreateTime time.Time
	// DestroyTime is set only when State is DESTROYED (output-only in the API).
	DestroyTime time.Time
	// ScheduledDestroyTime is set when the version was scheduled for delayed
	// destruction (output-only). The version is DISABLED until this instant,
	// then becomes DESTROYED. Enabling or disabling the version cancels it.
	ScheduledDestroyTime time.Time
	Data                 string // base64 payload
	// KmsKeyName/WrappedDEK are the version's primary wrap: the first CMEK
	// binding (automatic arm or first user-managed replica). Access uses them to
	// unwrap the single payload ciphertext. Empty/nil for Google-managed
	// encryption.
	KmsKeyName string
	WrappedDEK []byte
	// ReplicaDEKs holds one wrapped copy of the same DEK per user-managed
	// replica that uses CMEK, so every replica can report its own
	// CryptoKeyVersion. Empty for automatic replication (the primary wrap is
	// the only one) and for records written before it was modelled.
	ReplicaDEKs []ReplicaDEK `json:"replicaDeks,omitempty"`
}

// ReplicaDEK is one replica's wrapped copy of a version's DEK: the replica
// location, the Cloud KMS CryptoKey that wrapped it, and the wrapped blob (the
// blob itself tags the CryptoKeyVersion that encrypted it).
type ReplicaDEK struct {
	Location   string `json:"location"`
	KmsKeyName string `json:"kmsKeyName"`
	WrappedDEK []byte `json:"wrappedDek"`
}

// Store is the Secret Manager store.
type Store interface {
	CreateSecret(ctx context.Context, projectID, id string, s Secret) error
	GetSecret(ctx context.Context, projectID, id string) (Secret, error)
	UpdateSecret(ctx context.Context, projectID, id string, s Secret) error
	// UpdateSecretAtomic atomically reads the current secret, calls mutate to
	// compute the new value, and writes it back — no other GetSecret/
	// UpdateSecret/NextVersion for this secret can be observed or applied in
	// between. Callers doing a read-modify-write (a labels/rotation patch, an
	// automatic-rotation version bump) must use this instead of a separate
	// GetSecret+UpdateSecret pair: the latter has a lost-update window where a
	// concurrent NextVersion() call's counter advance gets silently rolled
	// back by the stale write. Returns ErrNoSuchSecret if the secret doesn't
	// exist (mutate is not called in that case).
	UpdateSecretAtomic(ctx context.Context, projectID, id string, mutate func(current Secret) (Secret, error)) (Secret, error)
	DeleteSecret(ctx context.Context, projectID, id string) error // cascades versions
	// DeleteSecretIf atomically deletes the secret (cascading its versions) only
	// when cond returns nil for the currently stored value. A non-nil cond error
	// aborts the delete and is returned unchanged; ErrNoSuchSecret when absent.
	// Callers use it for a precondition (the API etag) so that a concurrent
	// mutation cannot land between the check and the delete.
	DeleteSecretIf(ctx context.Context, projectID, id string, cond func(Secret) error) error
	ListSecrets(ctx context.Context, projectID string) ([]Secret, error)

	CreateVersion(ctx context.Context, projectID string, v Version) error
	GetVersion(ctx context.Context, projectID, secretID, versionID string) (Version, error)
	ListVersions(ctx context.Context, projectID, secretID string) ([]Version, error)
	UpdateVersion(ctx context.Context, projectID string, v Version) error
	// UpdateVersionIf atomically reads the current version, calls mutate, and
	// writes it back — no other transition can interleave. A non-nil mutate
	// error aborts the write and is returned unchanged; ErrNoSuchVersion when
	// the version is absent. Used for the version etag precondition.
	UpdateVersionIf(ctx context.Context, projectID, secretID, versionID string, mutate func(current Version) (Version, error)) (Version, error)

	// NextVersion atomically allocates the next version number for a secret and
	// advances its counter, so concurrent AddVersion calls never collide.
	NextVersion(ctx context.Context, projectID, secretID string) (int, error)

	Reset(ctx context.Context)
}
