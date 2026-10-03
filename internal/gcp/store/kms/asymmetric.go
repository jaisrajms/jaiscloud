package kms

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/asn1"
	"encoding/pem"
	"errors"
	"math/big"
	"strings"
)

// GenerateRSAKeyPair returns PKCS8 private DER + PKIX public DER for an RSA key.
func GenerateRSAKeyPair(bits int) (privDER, pubDER []byte, err error) {
	priv, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, nil, err
	}
	privDER, err = x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, nil, err
	}
	pubDER, err = x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return nil, nil, err
	}
	return privDER, pubDER, nil
}

// generateECKeyPair returns PKCS8 private DER + PKIX public DER for an EC key.
func generateECKeyPair(curve elliptic.Curve) (privDER, pubDER []byte, err error) {
	priv, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	privDER, err = x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, nil, err
	}
	pubDER, err = x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return nil, nil, err
	}
	return privDER, pubDER, nil
}

func rsaBits(algo string) int {
	switch {
	case strings.Contains(algo, "4096"):
		return 4096
	case strings.Contains(algo, "3072"):
		return 3072
	default:
		return 2048
	}
}

func ecCurve(algo string) elliptic.Curve {
	if strings.Contains(algo, "P384") {
		return elliptic.P384()
	}
	return elliptic.P256()
}

// hashAlgo maps a GCP algorithm name to its digest hash.
func hashAlgo(algo string) crypto.Hash {
	switch {
	case strings.Contains(algo, "SHA512"):
		return crypto.SHA512
	case strings.Contains(algo, "SHA384"):
		return crypto.SHA384
	case strings.Contains(algo, "SHA1"):
		return crypto.SHA1
	default:
		return crypto.SHA256
	}
}

// generateVersionMaterial produces key material for a crypto key version based
// on its algorithm. Symmetric/HMAC yield an AES key; RSA/EC yield a key pair.
func generateVersionMaterial(algorithm string) (keyMat, privDER, pubDER []byte, err error) {
	switch {
	case strings.HasPrefix(algorithm, "RSA_SIGN"), strings.HasPrefix(algorithm, "RSA_DECRYPT"):
		privDER, pubDER, err = GenerateRSAKeyPair(rsaBits(algorithm))
		return nil, privDER, pubDER, err
	case strings.HasPrefix(algorithm, "EC_SIGN"):
		privDER, pubDER, err = generateECKeyPair(ecCurve(algorithm))
		return nil, privDER, pubDER, err
	default: // GOOGLE_SYMMETRIC_ENCRYPTION, HMAC_*, or unknown → random 32 bytes
		keyMat, err = Generate32()
		return keyMat, nil, nil, err
	}
}

// RSASign signs a pre-hashed digest with an RSA PKCS8 key using PKCS1v15 or PSS
// per the algorithm name.
func RSASign(privDER, digest []byte, algo string) ([]byte, error) {
	key, err := x509.ParsePKCS8PrivateKey(privDER)
	if err != nil {
		return nil, err
	}
	rk, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("kms: not an RSA key")
	}
	h := hashAlgo(algo)
	if strings.Contains(algo, "_PSS_") {
		return rsa.SignPSS(rand.Reader, rk, h, digest, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: h})
	}
	return rsa.SignPKCS1v15(rand.Reader, rk, h, digest)
}

// ECSign signs a pre-hashed digest with an ECDSA key (ASN.1 DER R||S).
func ECSign(privDER, digest []byte) ([]byte, error) {
	key, err := x509.ParsePKCS8PrivateKey(privDER)
	if err != nil {
		return nil, err
	}
	ek, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		return nil, errors.New("kms: not an EC key")
	}
	r, s, err := ecdsa.Sign(rand.Reader, ek, digest)
	if err != nil {
		return nil, err
	}
	return asn1.Marshal(struct{ R, S *big.Int }{r, s})
}

// RSADecryptOAEP decrypts with an RSA PKCS8 key using OAEP.
func RSADecryptOAEP(privDER, ct []byte, algo string) ([]byte, error) {
	key, err := x509.ParsePKCS8PrivateKey(privDER)
	if err != nil {
		return nil, err
	}
	rk, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("kms: not an RSA key")
	}
	return rsa.DecryptOAEP(hashAlgo(algo).New(), rand.Reader, rk, ct, nil)
}

// HMACSign computes an HMAC over data using the named algorithm.
func HMACSign(key, data []byte, algo string) ([]byte, error) {
	h := hmac.New(hashAlgo(algo).New, key)
	if _, err := h.Write(data); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

// HMACVerify reports whether mac is a valid HMAC over data.
func HMACVerify(key, data, mac []byte, algo string) bool {
	expected, err := HMACSign(key, data, algo)
	if err != nil {
		return false
	}
	return hmac.Equal(expected, mac)
}

// PublicKeyPEM encodes a PKIX public key as a PEM block.
func PublicKeyPEM(pubDER []byte) (string, error) {
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})), nil
}

// PrivateKeyPEM encodes a PKCS8 private key as a PEM block.
func PrivateKeyPEM(privDER []byte) (string, error) {
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER})), nil
}

// wrapVersionMaterial generates key material for a version per its algorithm
// and DEK-wraps each component (key material / private key / public key).
func wrapVersionMaterial(dek []byte, keyID, algorithm string) (Version, error) {
	v := Version{Algorithm: algorithm}
	if v.Algorithm == "" {
		v.Algorithm = defaultAlgorithm
	}
	keyMat, privDER, pubDER, err := generateVersionMaterial(v.Algorithm)
	if err != nil {
		return v, err
	}
	if keyMat != nil {
		if v.KeyMaterial, err = EncryptData(dek, keyMat, []byte(keyID)); err != nil {
			return v, err
		}
	}
	if privDER != nil {
		if v.PrivateKey, err = EncryptData(dek, privDER, []byte(keyID)); err != nil {
			return v, err
		}
	}
	if pubDER != nil {
		if v.PublicKey, err = EncryptData(dek, pubDER, []byte(keyID)); err != nil {
			return v, err
		}
	}
	return v, nil
}
