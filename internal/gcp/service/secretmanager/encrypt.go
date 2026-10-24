// SecretVersion payload encryption shared by the Secret Manager REST provider
// and gRPC service.
//
// Real Secret Manager encrypts a version's payload once with a DEK and wraps
// that DEK with each replica's CMEK key, so a user-managed secret can name a
// different key per replica and every CMEK replica reports the CryptoKeyVersion
// that protects it. The emulator keeps the same shape: one payload ciphertext
// (Version.Data) plus one wrapped DEK per CMEK binding (Version.WrappedDEK for
// the primary, Version.ReplicaDEKs for all of them).
package secretmanager

import (
	"context"
	"encoding/base64"

	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
)

// PayloadWrap is the encryption output a new SecretVersion stores.
type PayloadWrap struct {
	// Data is the base64 payload ciphertext.
	Data string
	// KmsKeyName/WrappedDEK are the primary wrap: the first CMEK binding, which
	// Access uses to unwrap Data. Empty/nil under Google-managed encryption.
	KmsKeyName string
	WrappedDEK []byte
	// ReplicaDEKs is one wrapped copy of the same DEK per CMEK binding. It is
	// populated only for user-managed replication, where the status is reported
	// per replica; automatic replication has a single binding, reported through
	// KmsKeyName/WrappedDEK.
	ReplicaDEKs []secretmanagerstore.ReplicaDEK
}

// EncryptPayload encrypts data for a new SecretVersion under repl's CMEK
// bindings. One DEK is generated and wrapped once per binding, and the payload
// is encrypted with that single DEK — the same construction real Secret Manager
// uses, so a policy whose replicas name different keys still stores one
// ciphertext and every replica can unwrap it. With no CMEK the payload is
// encrypted under the server DEK and no wraps are recorded.
func EncryptPayload(ctx context.Context, enc crypto.EnvelopeEncryptor, account string, repl *secretmanagerstore.Replication, data []byte) (PayloadWrap, error) {
	bindings := repl.CMEKBindings()
	keyNames := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		keyNames = append(keyNames, binding.KmsKeyName)
	}

	rawDEK, wrapped, err := enc.WrapKeys(ctx, account, keyNames)
	if err != nil {
		return PayloadWrap{}, err
	}
	ciphertext, err := kms.EncryptData(rawDEK, data, nil)
	if err != nil {
		return PayloadWrap{}, err
	}

	out := PayloadWrap{Data: base64.StdEncoding.EncodeToString(ciphertext)}
	if len(bindings) == 0 {
		return out, nil
	}
	out.KmsKeyName = bindings[0].KmsKeyName
	out.WrappedDEK = wrapped[0]
	if repl != nil && repl.UserManaged != nil {
		out.ReplicaDEKs = make([]secretmanagerstore.ReplicaDEK, 0, len(bindings))
		for i, binding := range bindings {
			out.ReplicaDEKs = append(out.ReplicaDEKs, secretmanagerstore.ReplicaDEK{
				Location:   binding.Location,
				KmsKeyName: binding.KmsKeyName,
				WrappedDEK: wrapped[i],
			})
		}
	}
	return out, nil
}

// DecryptVersion returns a stored version's plaintext. It unwraps the DEK with
// the version's primary wrap and, when that replica's key is unavailable
// (disabled, destroyed or missing), with any other replica's wrap. Real Secret
// Manager routes an access request to one of the secret's replica locations and
// the CMEK guide only promises access "as long as those key versions remain
// enabled", so a request routed at the affected replica would fail; the
// emulator has no request routing, and falling back keeps the single stored
// ciphertext readable through the other replica's wrap (a deliberate leniency,
// not verified real-GCP parity). The primary error is returned only when no
// wrap works.
func DecryptVersion(ctx context.Context, enc crypto.EnvelopeEncryptor, account string, v secretmanagerstore.Version) ([]byte, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(v.Data)
	if err != nil {
		return nil, err
	}
	rawDEK, primaryErr := enc.Unwrap(ctx, account, v.KmsKeyName, v.WrappedDEK)
	if primaryErr == nil {
		return kms.DecryptData(rawDEK, ciphertext, nil)
	}
	for _, replica := range v.ReplicaDEKs {
		if replica.KmsKeyName == v.KmsKeyName {
			continue
		}
		rawDEK, err := enc.Unwrap(ctx, account, replica.KmsKeyName, replica.WrappedDEK)
		if err != nil {
			continue
		}
		if plain, err := kms.DecryptData(rawDEK, ciphertext, nil); err == nil {
			return plain, nil
		}
	}
	return nil, primaryErr
}
