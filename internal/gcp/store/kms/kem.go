package kms

import (
	"crypto/ecdh"
	"crypto/hpke"
	"crypto/mlkem"
	"crypto/rand"
	"crypto/sha3"
	"errors"
	"fmt"
	"strings"
)

// xwingLabel is the fixed X-Wing combiner label (draft-connolly-cfrg-xwing-kem),
// the six bytes `\.//^\`. It matches Go's crypto/hpke MLKEM768X25519 KEM so the
// emulator's keys interoperate with clients built on that KEM.
var xwingLabel = []byte("\\.//^\\")

// kemAlgorithm reports whether algorithm names a KEM key type (ML-KEM or the
// X-Wing hybrid), the key types Cloud KMS Decapsulate operates on.
func kemAlgorithm(algorithm string) bool {
	switch algorithm {
	case "ML_KEM_768", "ML_KEM_1024", "KEM_XWING":
		return true
	default:
		return false
	}
}

// IsKEMAlgorithm reports whether algorithm is a KEM key type.
func IsKEMAlgorithm(algorithm string) bool { return kemAlgorithm(algorithm) }

// GenerateKEMKeyPair generates a KEM key pair for a KEM algorithm. It returns
// the private key (a serialized seed) and the raw public encapsulation key.
func GenerateKEMKeyPair(algorithm string) (priv, pub []byte, err error) {
	switch algorithm {
	case "ML_KEM_768":
		dk, err := mlkem.GenerateKey768()
		if err != nil {
			return nil, nil, err
		}
		return dk.Bytes(), dk.EncapsulationKey().Bytes(), nil
	case "ML_KEM_1024":
		dk, err := mlkem.GenerateKey1024()
		if err != nil {
			return nil, nil, err
		}
		return dk.Bytes(), dk.EncapsulationKey().Bytes(), nil
	case "KEM_XWING":
		k, err := hpke.MLKEM768X25519().GenerateKey()
		if err != nil {
			return nil, nil, err
		}
		seed, err := k.Bytes()
		if err != nil {
			return nil, nil, err
		}
		return seed, k.PublicKey().Bytes(), nil
	default:
		return nil, nil, fmt.Errorf("kms: not a KEM algorithm: %q", algorithm)
	}
}

// DecapsulateKEM recovers the shared secret from a KEM ciphertext using a KEM
// private key (previously produced by GenerateKEMKeyPair).
func DecapsulateKEM(algorithm string, priv, ciphertext []byte) ([]byte, error) {
	switch algorithm {
	case "ML_KEM_768":
		dk, err := mlkem.NewDecapsulationKey768(priv)
		if err != nil {
			return nil, err
		}
		return dk.Decapsulate(ciphertext)
	case "ML_KEM_1024":
		dk, err := mlkem.NewDecapsulationKey1024(priv)
		if err != nil {
			return nil, err
		}
		return dk.Decapsulate(ciphertext)
	case "KEM_XWING":
		return xwingDecapsulate(priv, ciphertext)
	default:
		return nil, fmt.Errorf("kms: not a KEM algorithm: %q", algorithm)
	}
}

// EncapsulateKEM produces a shared secret and ciphertext for a KEM public key.
// It is the inverse of DecapsulateKEM and mirrors Go's crypto/hpke KEM
// encapsulation, so it interoperates with the emulator's generated keys.
func EncapsulateKEM(algorithm string, pub []byte) (sharedSecret, ciphertext []byte, err error) {
	switch algorithm {
	case "ML_KEM_768":
		ek, err := mlkem.NewEncapsulationKey768(pub)
		if err != nil {
			return nil, nil, err
		}
		ss, ct := ek.Encapsulate()
		return ss, ct, nil
	case "ML_KEM_1024":
		ek, err := mlkem.NewEncapsulationKey1024(pub)
		if err != nil {
			return nil, nil, err
		}
		ss, ct := ek.Encapsulate()
		return ss, ct, nil
	case "KEM_XWING":
		return xwingEncapsulate(pub)
	default:
		return nil, nil, fmt.Errorf("kms: not a KEM algorithm: %q", algorithm)
	}
}

// xwingEncapsulate implements X-Wing encapsulation (ML-KEM-768 + X25519),
// mirroring crypto/hpke's hybrid KEM: public key = mlkemPk || x25519Pk, the
// shared secret is SHA3-256(ssMLKEM || ssX25519 || ctX25519 || pkX25519 ||
// label), and the ciphertext is mlkemCt || ctX25519.
func xwingEncapsulate(pub []byte) ([]byte, []byte, error) {
	const pqLen = mlkem.EncapsulationKeySize768
	if len(pub) != pqLen+32 {
		return nil, nil, errors.New("kms: invalid X-Wing public key size")
	}
	pq, err := mlkem.NewEncapsulationKey768(pub[:pqLen])
	if err != nil {
		return nil, nil, err
	}
	ssPQ, ctPQ := pq.Encapsulate()

	pkX, err := ecdh.X25519().NewPublicKey(pub[pqLen:])
	if err != nil {
		return nil, nil, err
	}
	skE, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	ssT, err := skE.ECDH(pkX)
	if err != nil {
		return nil, nil, err
	}
	ctT := skE.PublicKey().Bytes()

	ss := xwingSharedSecret(ssPQ, ssT, ctT, pkX.Bytes())
	return ss, append(ctPQ, ctT...), nil
}

// xwingDecapsulate implements X-Wing decapsulation from a 32-byte seed, using
// the same SHAKE256 key expansion as crypto/hpke.
func xwingDecapsulate(seed, ciphertext []byte) ([]byte, error) {
	const pqLen = mlkem.CiphertextSize768
	if len(ciphertext) != pqLen+32 {
		return nil, errors.New("kms: invalid X-Wing ciphertext size")
	}
	s := sha3.NewSHAKE256()
	s.Write(seed)
	seedPQ := make([]byte, mlkem.SeedSize)
	s.Read(seedPQ)
	dk, err := mlkem.NewDecapsulationKey768(seedPQ)
	if err != nil {
		return nil, err
	}
	var x *ecdh.PrivateKey
	seedT := make([]byte, 32)
	for {
		s.Read(seedT)
		if x, err = ecdh.X25519().NewPrivateKey(seedT); err == nil {
			break
		}
	}

	ctPQ, ctT := ciphertext[:pqLen], ciphertext[pqLen:]
	ssPQ, err := dk.Decapsulate(ctPQ)
	if err != nil {
		return nil, err
	}
	pkX, err := ecdh.X25519().NewPublicKey(ctT)
	if err != nil {
		return nil, err
	}
	ssT, err := x.ECDH(pkX)
	if err != nil {
		return nil, err
	}
	return xwingSharedSecret(ssPQ, ssT, ctT, x.PublicKey().Bytes()), nil
}

// xwingSharedSecret is the X-Wing combiner (SHA3-256 over the ML-KEM and X25519
// shared secrets, the X25519 ciphertext and recipient public key, and label).
func xwingSharedSecret(ssPQ, ssT, ctT, pkT []byte) []byte {
	h := sha3.New256()
	h.Write(ssPQ)
	h.Write(ssT)
	h.Write(ctT)
	h.Write(pkT)
	h.Write(xwingLabel)
	return h.Sum(nil)
}

// hpkeKEM returns the crypto/hpke KEM and encapsulated-key size for an HPKE
// import method.
func hpkeKEM(method string) (kem hpke.KEM, encSize int, err error) {
	switch {
	case strings.Contains(method, "ML_KEM_768"):
		return hpke.MLKEM768(), mlkem.CiphertextSize768, nil
	case strings.Contains(method, "ML_KEM_1024"):
		return hpke.MLKEM1024(), mlkem.CiphertextSize1024, nil
	case strings.Contains(method, "XWING"):
		return hpke.MLKEM768X25519(), mlkem.CiphertextSize768 + 32, nil
	default:
		return nil, 0, fmt.Errorf("kms: unsupported HPKE import method: %q", method)
	}
}

// GenerateHPKEWrappingKey generates a KEM wrapping key pair for an HPKE import
// job: the private key seed and the raw public encapsulation key.
func GenerateHPKEWrappingKey(method string) (priv, pub []byte, err error) {
	kem, _, err := hpkeKEM(method)
	if err != nil {
		return nil, nil, err
	}
	k, err := kem.GenerateKey()
	if err != nil {
		return nil, nil, err
	}
	seed, err := k.Bytes()
	if err != nil {
		return nil, nil, err
	}
	return seed, k.PublicKey().Bytes(), nil
}

// hpkeOpen unwraps HPKE-sealed key material using a KEM private key seed. The
// sealed blob is enc || ciphertext (RFC 9180 base mode, empty info and AAD,
// HKDF-SHA256 / AES-256-GCM).
func hpkeOpen(method string, privSeed, sealed []byte) ([]byte, error) {
	kem, encSize, err := hpkeKEM(method)
	if err != nil {
		return nil, err
	}
	k, err := kem.NewPrivateKey(privSeed)
	if err != nil {
		return nil, err
	}
	if len(sealed) < encSize {
		return nil, errors.New("kms: HPKE wrapped key too short")
	}
	enc, ct := sealed[:encSize], sealed[encSize:]
	recipient, err := hpke.NewRecipient(enc, k, hpke.HKDFSHA256(), hpke.AES256GCM(), nil)
	if err != nil {
		return nil, err
	}
	return recipient.Open(nil, ct)
}

// HPKESeal is the inverse of hpkeOpen: it HPKE-seals plaintext to a raw KEM
// public key, producing enc || ciphertext. It is exported for conformance and
// round-trip tests.
func HPKESeal(method string, pubRaw, plaintext []byte) ([]byte, error) {
	kem, _, err := hpkeKEM(method)
	if err != nil {
		return nil, err
	}
	pk, err := kem.NewPublicKey(pubRaw)
	if err != nil {
		return nil, err
	}
	return hpke.Seal(pk, hpke.HKDFSHA256(), hpke.AES256GCM(), nil, plaintext)
}
