package kms

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
)

// IsRSAImportMethod reports whether method is one of the RSA-OAEP import
// methods (with or without the AES-KWP layer).
func IsRSAImportMethod(method string) bool { return strings.HasPrefix(method, "RSA_OAEP") }

// IsHPKEImportMethod reports whether method is one of the post-quantum HPKE-KEM
// import methods.
func IsHPKEImportMethod(method string) bool { return strings.HasPrefix(method, "HPKE_KEM") }

// ImportMethodHash returns the RSA-OAEP digest for an RSA import method.
func ImportMethodHash(method string) crypto.Hash {
	if strings.Contains(method, "SHA1") {
		return crypto.SHA1
	}
	return crypto.SHA256
}

// GenerateImportJobWrappingKey generates the wrapping key an ImportJob exposes
// to callers. RSA methods return a PKCS#8 private DER + PKIX public DER pair
// (rawPublic=false); HPKE methods return a KEM private seed + raw public
// encapsulation key (rawPublic=true).
func GenerateImportJobWrappingKey(method string) (priv, pub []byte, rawPublic bool, err error) {
	if IsHPKEImportMethod(method) {
		priv, pub, err = GenerateHPKEWrappingKey(method)
		return priv, pub, true, err
	}
	priv, pub, err = GenerateRSAKeyPair(rsaBitsForMethod(method))
	return priv, pub, false, err
}

// rsaBitsForMethod returns the RSA modulus size for an RSA import method.
func rsaBitsForMethod(method string) int {
	if strings.Contains(method, "4096") {
		return 4096
	}
	return 3072
}

// UnwrapImportedKeyMaterial unwraps the caller-supplied wrapped key material of
// an ImportCryptoKeyVersion request, returning the raw key material (symmetric
// or HMAC) or PKCS#8 DER (asymmetric). RSA-OAEP methods optionally carry an
// AES-KWP (RFC 5649) layer; HPKE-KEM methods are sealed per RFC 9180.
func UnwrapImportedKeyMaterial(method string, wrappingPriv, wrapped []byte) ([]byte, error) {
	switch {
	case IsRSAImportMethod(method):
		return unwrapRSAOAEP(method, wrappingPriv, wrapped)
	case IsHPKEImportMethod(method):
		return hpkeOpen(method, wrappingPriv, wrapped)
	default:
		return nil, fmt.Errorf("kms: unsupported import method %q", method)
	}
}

// UnwrapTrustedKey unwraps key material protected with AES-256-KWP by an HSM
// trusted importing key (Cloud KMS ExportTrustedKeyWrappedCryptoKeyVersion).
func UnwrapTrustedKey(wrappingKeyMaterial, wrapped []byte) ([]byte, error) {
	return AESKeyUnwrapWithPadding(wrappingKeyMaterial, wrapped)
}

// WrapTrustedKey wraps key material with AES-256-KWP using an HSM trusted
// wrapping key.
func WrapTrustedKey(wrappingKeyMaterial, material []byte) ([]byte, error) {
	return AESKeyWrapWithPadding(wrappingKeyMaterial, material)
}

// unwrapRSAOAEP implements RSASSA-OAEP unwrapping. Methods suffixed AES_256 wrap
// an ephemeral AES key with RSA-OAEP and the key material with AES-KWP; the
// bare SHA256 methods wrap the material directly with RSA-OAEP.
func unwrapRSAOAEP(method string, privDER, wrapped []byte) ([]byte, error) {
	key, err := x509.ParsePKCS8PrivateKey(privDER)
	if err != nil {
		return nil, fmt.Errorf("kms: parse wrapping key: %w", err)
	}
	rk, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("kms: wrapping key is not RSA")
	}
	hash := ImportMethodHash(method)
	if strings.HasSuffix(method, "AES_256") {
		size := rk.Size()
		if len(wrapped) <= size {
			return nil, errors.New("kms: wrapped key too short for RSA-OAEP+AES-KWP")
		}
		kwpKey, err := rsa.DecryptOAEP(hash.New(), rand.Reader, rk, wrapped[:size], nil)
		if err != nil {
			return nil, fmt.Errorf("kms: RSA-OAEP unwrap: %w", err)
		}
		return AESKeyUnwrapWithPadding(kwpKey, wrapped[size:])
	}
	return rsa.DecryptOAEP(hash.New(), rand.Reader, rk, wrapped, nil)
}
