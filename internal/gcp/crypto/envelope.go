package crypto

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
)

type EnvelopeEncryptor interface {
	// Wrap generates a 32-byte DEK. If kmsKeyName is empty, it returns the Server DEK
	// and nil wrappedDEK. If kmsKeyName is provided, it returns a random DEK and
	// the DEK encrypted by the KMS key (wrappedDEK).
	Wrap(ctx context.Context, accountID, kmsKeyName string) (rawDEK, wrappedDEK []byte, err error)

	// WrapKeys generates one 32-byte DEK and wraps it once per entry of
	// kmsKeyNames, returning the raw DEK and one wrapped blob per key in the
	// same order. It is the multi-replica form of Wrap: Secret Manager encrypts
	// a version's payload once and wraps the single DEK under each replica's
	// CMEK, so every replica can report the CryptoKeyVersion that protects it
	// while the one payload ciphertext stays valid. An empty kmsKeyNames slice
	// (no CMEK at all) is the server-DEK case: it returns the Server DEK and no
	// wrapped blobs, exactly like Wrap with an empty key name.
	WrapKeys(ctx context.Context, accountID string, kmsKeyNames []string) (rawDEK []byte, wrappedDEKs [][]byte, err error)

	// Unwrap decrypts wrappedDEK using kmsKeyName. If kmsKeyName is empty, it returns the Server DEK.
	Unwrap(ctx context.Context, accountID, kmsKeyName string, wrappedDEK []byte) (rawDEK []byte, err error)

	// ValidateKey resolves a customer-managed CryptoKey without encrypting
	// anything: the key must exist, be an ENCRYPT_DECRYPT key, and have a
	// primary CryptoKeyVersion in the ENABLED state that can wrap new payloads.
	// A missing or malformed key is INVALID_ARGUMENT; a key whose primary
	// version is disabled, scheduled for destruction or destroyed is
	// FAILED_PRECONDITION (the state real GCP reports when CMEK validation runs
	// at secret create/patch time). The account is not consulted: a CryptoKey
	// name is fully qualified, and Secret Manager accepts keys from another
	// project (the CMEK guide's separation-of-duties setup).
	ValidateKey(ctx context.Context, accountID, kmsKeyName string) error
}

type encryptor struct {
	kmsStore kms.Store
}

func NewEnvelopeEncryptor(kmsStore kms.Store) EnvelopeEncryptor {
	return &encryptor{kmsStore: kmsStore}
}

// parseCryptoKeyName splits "projects/{proj}/locations/{loc}/keyRings/{kr}/cryptoKeys/{key}"
// or returns empty strings if the format is invalid.
func parseCryptoKeyName(name string) (project, loc, kr, key string) {
	parts := strings.Split(name, "/")
	if len(parts) >= 8 && parts[0] == "projects" && parts[2] == "locations" && parts[4] == "keyRings" && parts[6] == "cryptoKeys" {
		return parts[1], parts[3], parts[5], parts[7]
	}
	return "", "", "", ""
}

// errKeyNotEnabled is resolveKey's sentinel for a CMEK that exists but whose
// primary CryptoKeyVersion cannot encrypt (disabled, DESTROY_SCHEDULED or
// DESTROYED). The callers map it differently: ValidateKey answers the
// FAILED_PRECONDITION real Secret Manager reports at secret create/patch,
// while Wrap/Unwrap keep the INVALID_ARGUMENT every other CMEK consumer
// (GCS, Pub/Sub) has always received.
var errKeyNotEnabled = errors.New("kms: key version not enabled")

// keyNotEnabled is the FAILED_PRECONDITION answer for a CMEK whose primary
// CryptoKeyVersion cannot encrypt: real GCP refuses the write/version instead
// of silently falling back to Google-managed encryption.
func keyNotEnabled(kmsKeyName string) error {
	return &model.ProviderError{
		Code:       "FailedPrecondition",
		HTTPStatus: 400,
		Status:     "FAILED_PRECONDITION",
		Message:    fmt.Sprintf("The KMS key %s is not enabled", kmsKeyName),
	}
}

// kmsKeyInvalid is the INVALID_ARGUMENT answer Wrap/Unwrap have always given
// for a key they cannot use for any reason.
func kmsKeyInvalid() error {
	return model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
}

// resolveKey looks up the CryptoKey a CMEK name names and checks that its
// primary version can wrap. A malformed or missing key is INVALID_ARGUMENT; a
// key that exists but has no usable primary version (disabled,
// DESTROY_SCHEDULED, DESTROYED) is FAILED_PRECONDITION.
func (e *encryptor) resolveKey(ctx context.Context, kmsKeyName string) (kms.CryptoKey, error) {
	project, loc, kr, key := parseCryptoKeyName(kmsKeyName)
	if project == "" {
		return kms.CryptoKey{}, model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
	}

	cryptoKey, err := e.kmsStore.GetCryptoKey(ctx, project, loc, kr, key)
	if err != nil {
		if err == kms.ErrNoSuchCryptoKey {
			return kms.CryptoKey{}, model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
		}
		return kms.CryptoKey{}, err
	}

	// Secret Manager uses the key's primary version, so the key must be a
	// symmetric ENCRYPT_DECRYPT key: an asymmetric/signing key has no material
	// an envelope DEK can be encrypted with. An empty Purpose is the store's
	// default (ENCRYPT_DECRYPT).
	if cryptoKey.Purpose != "" && cryptoKey.Purpose != "ENCRYPT_DECRYPT" {
		return kms.CryptoKey{}, model.NewProviderError("InvalidArgument",
			fmt.Sprintf("KMS key %s has purpose %s, but Secret Manager requires ENCRYPT_DECRYPT", kmsKeyName, cryptoKey.Purpose), 400)
	}

	version, err := e.kmsStore.GetVersion(ctx, project, loc, kr, key, cryptoKey.PrimaryVersion)
	if err != nil {
		if errors.Is(err, kms.ErrNoSuchVersion) {
			return kms.CryptoKey{}, errKeyNotEnabled
		}
		return kms.CryptoKey{}, err
	}
	if version.State != "ENABLED" {
		return kms.CryptoKey{}, errKeyNotEnabled
	}
	return cryptoKey, nil
}

// wrapDEK encrypts an existing DEK under the key's primary version and tags the
// blob with that CryptoKeyVersion, so Unwrap can fetch the matching key
// material even after the key's primary version is rotated
// (CryptoKeyUpdatePrimaryVersion) — mirrors CryptoKeyEncrypt/Decrypt in the
// direct KMS API (kms.go), which already does this. Without the tag, Unwrap
// would fetch whatever is the *current* primary version's key material, which
// cannot decrypt a DEK wrapped under a since-rotated-away version.
func (e *encryptor) wrapDEK(ctx context.Context, kmsKeyName string, cryptoKey kms.CryptoKey, rawDEK []byte) ([]byte, error) {
	project, loc, kr, key := parseCryptoKeyName(kmsKeyName)
	if project == "" {
		return nil, model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
	}
	rawKMSDEK, err := e.kmsStore.KeyMaterial(ctx, project, loc, kr, key, cryptoKey.PrimaryVersion)
	if err != nil {
		return nil, err
	}
	ct, err := kms.EncryptData(rawKMSDEK, rawDEK, nil)
	if err != nil {
		return nil, err
	}
	return kms.EncodeVersionedCiphertext(cryptoKey.PrimaryVersion, ct), nil
}

func (e *encryptor) Wrap(ctx context.Context, accountID, kmsKeyName string) ([]byte, []byte, error) {
	if kmsKeyName == "" {
		dek, err := e.kmsStore.ServerDEK(ctx)
		return dek, nil, err
	}

	cryptoKey, err := e.resolveKey(ctx, kmsKeyName)
	if err != nil {
		if errors.Is(err, errKeyNotEnabled) {
			return nil, nil, kmsKeyInvalid()
		}
		return nil, nil, err
	}

	rawDEK, err := kms.Generate32()
	if err != nil {
		return nil, nil, err
	}

	wrappedDEK, err := e.wrapDEK(ctx, kmsKeyName, cryptoKey, rawDEK)
	if err != nil {
		return nil, nil, err
	}
	return rawDEK, wrappedDEK, nil
}

func (e *encryptor) WrapKeys(ctx context.Context, accountID string, kmsKeyNames []string) ([]byte, [][]byte, error) {
	if len(kmsKeyNames) == 0 {
		dek, err := e.kmsStore.ServerDEK(ctx)
		return dek, nil, err
	}

	// Resolve every key (and its primary version) before generating or wrapping
	// anything, so a policy naming several keys fails atomically instead of
	// leaving a partially wrapped set.
	keys := make([]kms.CryptoKey, 0, len(kmsKeyNames))
	for _, name := range kmsKeyNames {
		cryptoKey, err := e.resolveKey(ctx, name)
		if err != nil {
			if errors.Is(err, errKeyNotEnabled) {
				return nil, nil, kmsKeyInvalid()
			}
			return nil, nil, err
		}
		keys = append(keys, cryptoKey)
	}

	rawDEK, err := kms.Generate32()
	if err != nil {
		return nil, nil, err
	}
	wrapped := make([][]byte, 0, len(keys))
	for i, cryptoKey := range keys {
		blob, err := e.wrapDEK(ctx, kmsKeyNames[i], cryptoKey, rawDEK)
		if err != nil {
			return nil, nil, err
		}
		wrapped = append(wrapped, blob)
	}
	return rawDEK, wrapped, nil
}

func (e *encryptor) ValidateKey(ctx context.Context, accountID, kmsKeyName string) error {
	_, err := e.resolveKey(ctx, kmsKeyName)
	if errors.Is(err, errKeyNotEnabled) {
		return keyNotEnabled(kmsKeyName)
	}
	return err
}

func (e *encryptor) Unwrap(ctx context.Context, accountID, kmsKeyName string, wrappedDEK []byte) ([]byte, error) {
	if kmsKeyName == "" {
		return e.kmsStore.ServerDEK(ctx)
	}

	project, loc, kr, key := parseCryptoKeyName(kmsKeyName)
	if project == "" {
		return nil, model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
	}

	// The wrapping version is tagged onto the blob by Wrap so rotating the
	// key's primary version (CryptoKeyUpdatePrimaryVersion) after a Wrap
	// doesn't strand data encrypted under the version that was primary at
	// wrap time.
	wrapVersion, ct, err := kms.DecodeVersionedCiphertext(wrappedDEK)
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
	}

	if _, err := e.kmsStore.GetCryptoKey(ctx, project, loc, kr, key); err != nil {
		if err == kms.ErrNoSuchCryptoKey {
			return nil, model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
		}
		return nil, err
	}

	version, err := e.kmsStore.GetVersion(ctx, project, loc, kr, key, wrapVersion)
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
	}

	if version.State != "ENABLED" {
		return nil, kmsKeyInvalid()
	}

	rawKMSDEK, err := e.kmsStore.KeyMaterial(ctx, project, loc, kr, key, wrapVersion)
	if err != nil {
		return nil, err
	}

	rawDEK, err := kms.DecryptData(rawKMSDEK, ct, nil)
	if err != nil {
		return nil, model.NewProviderError("InvalidArgument", "KMS key invalid or missing", 400)
	}

	return rawDEK, nil
}
