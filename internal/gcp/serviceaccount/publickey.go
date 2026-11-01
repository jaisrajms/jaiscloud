package serviceaccount

import (
	"context"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// uploadedKeyIDLen is the length of a real service-account key id: 20 bytes of
// hex (40 characters). The id is derived from the public key DER, so the same
// public key always maps to the same id and a duplicate upload collides.
const uploadedKeyIDLen = 40

// UploadedPublicKey is the parsed material of a public key supplied to
// projects.serviceAccounts.keys.upload.
type UploadedPublicKey struct {
	// PKIXDER is the canonical SubjectPublicKeyInfo encoding of the key.
	PKIXDER []byte
	// Algorithm is the ServiceAccountKey.keyAlgorithm enum value.
	Algorithm string
	// KeyID is the stable id derived from PKIXDER.
	KeyID string
}

// ParseUploadedPublicKey parses a service-account public key supplied to
// projects.serviceAccounts.keys.upload.
//
// Discovery documents one shape: "Must be an RSA public key that is wrapped in
// an X.509 v3 certificate. Include the first line, `-----BEGIN CERTIFICATE-----`,
// and the last line, `-----END CERTIFICATE-----`." That is the primary form
// here. As a lenient superset the emulator also accepts the bare public key
// encodings a caller may already have — a `PUBLIC KEY` (PKIX) or `RSA PUBLIC
// KEY` (PKCS#1) PEM block, or the same DER without a PEM envelope — because the
// API never sees the envelope, only the key.
//
// The ServiceAccountKey.keyAlgorithm enum (iam.projects.serviceAccounts.keys
// Discovery schema) has no 4096-bit member: only KEY_ALG_UNSPECIFIED,
// KEY_ALG_RSA_1024 and KEY_ALG_RSA_2048 exist. A key of any other size is
// therefore not representable and is rejected as INVALID_ARGUMENT rather than
// labelled with a wrong algorithm.
func ParseUploadedPublicKey(data []byte) (*UploadedPublicKey, error) {
	if len(data) == 0 {
		return nil, Invalid("publicKeyData is required")
	}
	pub, err := parseRSAPublicKey(data)
	if err != nil {
		return nil, err
	}
	algorithm := ""
	switch pub.N.BitLen() {
	case 1024:
		algorithm = "KEY_ALG_RSA_1024"
	case 2048:
		algorithm = "KEY_ALG_RSA_2048"
	default:
		return nil, Invalid(fmt.Sprintf(
			"unsupported RSA key size %d bits: ServiceAccountKey.keyAlgorithm supports only KEY_ALG_RSA_1024 and KEY_ALG_RSA_2048",
			pub.N.BitLen()))
	}
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, Invalid("publicKeyData is not a usable RSA public key")
	}
	sum := sha256.Sum256(der)
	return &UploadedPublicKey{
		PKIXDER:   der,
		Algorithm: algorithm,
		KeyID:     hex.EncodeToString(sum[:])[:uploadedKeyIDLen],
	}, nil
}

// parseRSAPublicKey decodes a certificate, PEM block or raw DER into an RSA
// public key, rejecting every other key type.
func parseRSAPublicKey(data []byte) (*rsa.PublicKey, error) {
	if block, _ := pem.Decode(data); block != nil {
		switch block.Type {
		case "CERTIFICATE":
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, Invalid("publicKeyData is not a valid X.509 certificate")
			}
			pub, ok := cert.PublicKey.(*rsa.PublicKey)
			if !ok {
				return nil, Invalid("publicKeyData must contain an RSA public key")
			}
			return pub, nil
		case "PUBLIC KEY":
			key, err := x509.ParsePKIXPublicKey(block.Bytes)
			if err != nil {
				return nil, Invalid("publicKeyData is not a valid SubjectPublicKeyInfo public key")
			}
			pub, ok := key.(*rsa.PublicKey)
			if !ok {
				return nil, Invalid("publicKeyData must contain an RSA public key")
			}
			return pub, nil
		case "RSA PUBLIC KEY":
			pub, err := x509.ParsePKCS1PublicKey(block.Bytes)
			if err != nil {
				return nil, Invalid("publicKeyData is not a valid PKCS#1 RSA public key")
			}
			return pub, nil
		default:
			return nil, Invalid("publicKeyData must be an RSA public key wrapped in an X.509 v3 certificate (-----BEGIN CERTIFICATE-----)")
		}
	}
	if key, err := x509.ParsePKIXPublicKey(data); err == nil {
		pub, ok := key.(*rsa.PublicKey)
		if !ok {
			return nil, Invalid("publicKeyData must contain an RSA public key")
		}
		return pub, nil
	}
	if pub, err := x509.ParsePKCS1PublicKey(data); err == nil {
		return pub, nil
	}
	return nil, Invalid("publicKeyData is not a PEM or DER-encoded public key")
}

// UploadKey stores a user-provided public key for a service account
// (projects.serviceAccounts.keys.upload). The record carries the public
// material only: the caller keeps the private half, so no private DER is
// written and signing with the key is impossible (see PrivDER).
//
// The key id is derived from the public key, so uploading the same key twice
// resolves to the same store id and answers ALREADY_EXISTS — a duplicate is a
// conflict, not a second key.
func UploadKey(ctx context.Context, resources store.ResourceStore, account, email string, publicKeyData []byte) (*Key, error) {
	parsed, err := ParseUploadedPublicKey(publicKeyData)
	if err != nil {
		return nil, err
	}
	k := Key{
		Email:      email,
		KeyID:      parsed.KeyID,
		Algorithm:  parsed.Algorithm,
		PublicDER:  base64.StdEncoding.EncodeToString(parsed.PKIXDER),
		ValidAfter: timestamp.Canonical(clock.Now()),
		KeyType:    KeyType,
		KeyOrigin:  KeyOrigin,
	}
	data, _ := json.Marshal(k)
	if err := resources.Create(ctx, account, store.GlobalRegion, store.ResourceEntry{Type: ResourceType, ID: k.KeyID, Data: data}); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, model.NewProviderError("AlreadyExists",
				"a key with this public key already exists for the service account", 409)
		}
		return nil, err
	}
	return &k, nil
}
