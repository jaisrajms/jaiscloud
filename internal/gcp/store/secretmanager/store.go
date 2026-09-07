// Package secret provides the Secret Manager store. Secrets and versions live
// in dedicated jc_sm_secrets / jc_sm_versions tables (mirroring AWS
// jc_sm_secrets / jc_sm_versions).
package secretmanager

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNoSuchSecret  = errors.New("NoSuchSecret")
	ErrNoSuchVersion = errors.New("NoSuchVersion")
	ErrAlreadyExists = errors.New("AlreadyExists")
)

// Rotation is a secret's automatic-rotation schedule. NextRotationTime is an
// RFC3339 timestamp; RotationPeriod is a duration string (e.g. "3600s").
type Rotation struct {
	NextRotationTime string `json:"nextRotationTime"`
	RotationPeriod   string `json:"rotationPeriod"`
}

// Secret is Secret Manager secret metadata.
type Secret struct {
	ID             string
	Labels         map[string]string
	CreateTime     time.Time
	NextVer        int
	Rotation       *Rotation      // nil when rotation is disabled
	VersionAliases map[string]int // alias name → version number, nil when none
	KmsKeyName     string
}

// Version is a Secret Manager secret version.
type Version struct {
	SecretID   string
	VersionID  string
	State      string
	CreateTime time.Time
	Data       string // base64 payload
	KmsKeyName string
	WrappedDEK []byte
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
	ListSecrets(ctx context.Context, projectID string) ([]Secret, error)

	CreateVersion(ctx context.Context, projectID string, v Version) error
	GetVersion(ctx context.Context, projectID, secretID, versionID string) (Version, error)
	ListVersions(ctx context.Context, projectID, secretID string) ([]Version, error)
	UpdateVersion(ctx context.Context, projectID string, v Version) error

	// NextVersion atomically allocates the next version number for a secret and
	// advances its counter, so concurrent AddVersion calls never collide.
	NextVersion(ctx context.Context, projectID, secretID string) (int, error)

	Reset(ctx context.Context)
}
