// Package kms provides the KMS store. Key rings, crypto keys, and crypto-key
// versions live in dedicated jc_kms_* tables (mirroring AWS jc_kms_*).
package kms

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNoSuchKeyRing   = errors.New("NoSuchKeyRing")
	ErrNoSuchCryptoKey = errors.New("NoSuchCryptoKey")
	ErrNoSuchVersion   = errors.New("NoSuchVersion")
	ErrAlreadyExists   = errors.New("AlreadyExists")
	// ErrNotDestroyable is returned when a version cannot be scheduled for
	// destruction because it is not ENABLED or DISABLED (the only destroyable
	// states in Cloud KMS).
	ErrNotDestroyable = errors.New("NotDestroyable")
	// ErrNotRestorable is returned when a version cannot be restored because it
	// is not DESTROY_SCHEDULED (the only restorable state in Cloud KMS).
	ErrNotRestorable = errors.New("NotRestorable")
)

// KeyRing is a KMS key ring.
type KeyRing struct {
	Location   string
	ID         string
	CreateTime time.Time
}

// CryptoKey is a KMS crypto key.
type CryptoKey struct {
	Location       string
	KeyRingID      string
	ID             string
	Purpose        string
	CreateTime     time.Time
	PrimaryVersion string
	Algorithm      string // version template algorithm (e.g. GOOGLE_SYMMETRIC_ENCRYPTION, RSA_SIGN_PKCS1_2048_SHA256)

	// Labels is the user-supplied key metadata. Nil/empty means none.
	Labels map[string]string
	// RotationPeriod is the manual rotation schedule. Zero means rotation is
	// disabled. The schedule is executed lazily on read: when NextRotationTime
	// is reached, RotateIfDue creates a new ENABLED version, makes it primary,
	// and advances NextRotationTime by the period.
	RotationPeriod time.Duration
	// NextRotationTime is when the next rotation is scheduled. Zero means no
	// rotation is scheduled (RotationPeriod is zero).
	NextRotationTime time.Time
}

// Version is a KMS crypto-key version with its own key material.
type Version struct {
	KeyID       string
	Version     string
	State       string
	Algorithm   string
	CreateTime  time.Time
	KeyMaterial []byte // DEK-wrapped symmetric/HMAC key (at rest)
	PrivateKey  []byte // DEK-wrapped PKCS8 private DER (asymmetric)
	PublicKey   []byte // DEK-wrapped PKIX public DER (asymmetric)

	// DestroyTime is when a DESTROY_SCHEDULED version's key material will be
	// destroyed and the state will move to DESTROYED. It is zero unless State
	// is "DESTROY_SCHEDULED" (mirrors the output-only destroy_time field).
	DestroyTime time.Time
	// DestroyEventTime is when a version actually reached DESTROYED. It is zero
	// unless State is "DESTROYED" (mirrors the output-only destroy_event_time
	// field).
	DestroyEventTime time.Time
}

// Store is the KMS store.
type Store interface {
	CreateKeyRing(ctx context.Context, projectID, location, id string, kr KeyRing) error
	GetKeyRing(ctx context.Context, projectID, location, id string) (KeyRing, error)
	ListKeyRings(ctx context.Context, projectID, location string) ([]KeyRing, error)

	CreateCryptoKey(ctx context.Context, projectID, location, keyringID, id string, ck CryptoKey) error
	GetCryptoKey(ctx context.Context, projectID, location, keyringID, id string) (CryptoKey, error)
	ListCryptoKeys(ctx context.Context, projectID, location, keyringID string) ([]CryptoKey, error)
	// UpdateCryptoKeyAtomic atomically reads the current crypto key, calls
	// mutate to compute the new value, and writes it back — no other
	// GetCryptoKey/UpdateCryptoKeyAtomic for this key can be observed or
	// applied in between. Callers doing a read-modify-write (a labels/rotation
	// patch) must use this instead of a separate GetCryptoKey+write pair.
	// Returns ErrNoSuchCryptoKey if the key doesn't exist (mutate is not
	// called in that case).
	UpdateCryptoKeyAtomic(ctx context.Context, projectID, location, keyringID, id string, mutate func(current CryptoKey) (CryptoKey, error)) (CryptoKey, error)

	// CreateVersion allocates the next version number and stores the version
	// with DEK-wrapped key material. Returns the assigned version string.
	CreateVersion(ctx context.Context, projectID, location, keyringID, keyID string, v Version) (string, error)
	GetVersion(ctx context.Context, projectID, location, keyringID, keyID, version string) (Version, error)
	ListVersions(ctx context.Context, projectID, location, keyringID, keyID string) ([]Version, error)
	// UpdateVersionState applies an ENABLED/DISABLED lifecycle transition and
	// clears any destruction timestamps. Callers should use DestroyVersion or
	// RestoreVersion for the destruction lifecycle.
	UpdateVersionState(ctx context.Context, projectID, location, keyringID, keyID, version, state string) error
	// DestroyVersion schedules a version for destruction: State becomes
	// DESTROY_SCHEDULED and DestroyTime is set to the supplied instant. It is
	// idempotent — a version already DESTROY_SCHEDULED or DESTROYED is returned
	// unchanged (Cloud KMS semantics). Returns ErrNoSuchVersion if the version
	// does not exist and ErrNotDestroyable if it is in a state that cannot be
	// destroyed (only ENABLED and DISABLED can).
	DestroyVersion(ctx context.Context, projectID, location, keyringID, keyID, version string, destroyTime time.Time) (Version, error)
	// RestoreVersion reverses a scheduled destruction: a DESTROY_SCHEDULED
	// version becomes DISABLED and its DestroyTime is cleared. Returns
	// ErrNoSuchVersion if the version does not exist and ErrNotRestorable if it
	// is not DESTROY_SCHEDULED.
	RestoreVersion(ctx context.Context, projectID, location, keyringID, keyID, version string) (Version, error)
	// PromoteDestroyed advances every DESTROY_SCHEDULED version of the crypto
	// key whose DestroyTime is at or before now to DESTROYED, recording the
	// promotion in DestroyEventTime. It returns the number of versions promoted.
	// The emulator has no background scheduler, so read paths call this lazily.
	PromoteDestroyed(ctx context.Context, projectID, location, keyringID, keyID string, now time.Time) (int, error)
	UpdatePrimaryVersion(ctx context.Context, projectID, location, keyringID, keyID, version string) error
	// DeleteVersion permanently removes a crypto-key version. Returns
	// ErrNoSuchVersion if it does not exist.
	DeleteVersion(ctx context.Context, projectID, location, keyringID, keyID, version string) error
	// DeleteCryptoKey permanently removes a crypto key. Callers must ensure no
	// versions remain (Cloud KMS requires deleting every version first).
	// Returns ErrNoSuchCryptoKey if it does not exist.
	DeleteCryptoKey(ctx context.Context, projectID, location, keyringID, id string) error

	// KeyMaterial returns the raw AES-256 key material for a version,
	// unwrapping the DEK-encrypted blob stored at rest.
	KeyMaterial(ctx context.Context, projectID, location, keyringID, keyID, version string) ([]byte, error)

	// PrivateKey returns the raw PKCS8 private DER for an asymmetric version.
	PrivateKey(ctx context.Context, projectID, location, keyringID, keyID, version string) ([]byte, error)

	// PublicKey returns the raw PKIX public DER for an asymmetric version.
	PublicKey(ctx context.Context, projectID, location, keyringID, keyID, version string) ([]byte, error)

	// ServerDEK returns the global server DEK.
	ServerDEK(ctx context.Context) ([]byte, error)

	Reset(ctx context.Context)
}
