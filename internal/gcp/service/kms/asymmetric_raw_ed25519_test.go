package kms

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"testing"
)

// signKey creates an ASYMMETRIC_SIGN key with algorithm and returns its version
// 1 public key parsed from the PEM GetPublicKey serves.
func signKey(t *testing.T, core *Service, project, id, algorithm string) any {
	t.Helper()
	ctx := context.Background()
	if _, err := core.CreateCryptoKey(ctx, project, "global", "kr", id, CryptoKeyInput{
		Purpose:   "ASYMMETRIC_SIGN",
		Algorithm: algorithm,
	}); err != nil {
		t.Fatalf("CreateCryptoKey %s: %v", algorithm, err)
	}
	pk, err := core.GetPublicKey(ctx, project, "global", "kr", id, "1", "")
	if err != nil {
		t.Fatalf("GetPublicKey %s: %v", algorithm, err)
	}
	block, _ := pem.Decode([]byte(pk.PEM))
	if block == nil {
		t.Fatalf("%s: public key is not PEM", algorithm)
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("%s: parse public key: %v", algorithm, err)
	}
	return parsed
}

// TestCoreAsymmetricSignEd25519 covers the message-signing path: Ed25519 signs
// data directly (no hashing), and digest is rejected because the algorithm has
// no digest variant.
func TestCoreAsymmetricSignEd25519(t *testing.T) {
	ctx := context.Background()
	core, _ := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	pub := signKey(t, core, "proj", "ed", "EC_SIGN_ED25519")
	msg := []byte("hello ed25519")

	res, err := core.AsymmetricSign(ctx, "proj", "global", "kr", "ed", "1", nil, msg)
	if err != nil {
		t.Fatalf("AsymmetricSign(data): %v", err)
	}
	if !ed25519.Verify(pub.(ed25519.PublicKey), msg, res.Signature) {
		t.Fatal("Ed25519 signature did not verify with the public key")
	}

	digest := sha256.Sum256(msg)
	if _, err := core.AsymmetricSign(ctx, "proj", "global", "kr", "ed", "1", digest[:], nil); err == nil {
		t.Fatal("digest accepted for EC_SIGN_ED25519")
	}
}

// TestCoreAsymmetricSignRSA raw covers each raw size: the input is signed
// without a DigestInfo prefix, the input limit is modulus size minus the 11
// bytes of PKCS#1 v1.5 padding, and digest is rejected.
func TestCoreAsymmetricSignRSARaw(t *testing.T) {
	ctx := context.Background()
	core, _ := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	for _, tc := range []struct {
		id        string
		algorithm string
		bits      int
	}{
		{"raw2048", "RSA_SIGN_RAW_PKCS1_2048", 2048},
		{"raw3072", "RSA_SIGN_RAW_PKCS1_3072", 3072},
	} {
		t.Run(tc.algorithm, func(t *testing.T) {
			pub := signKey(t, core, "proj", tc.id, tc.algorithm)
			max := tc.bits/8 - 11
			data := bytes.Repeat([]byte{0x5A}, max)

			res, err := core.AsymmetricSign(ctx, "proj", "global", "kr", tc.id, "1", nil, data)
			if err != nil {
				t.Fatalf("AsymmetricSign(data): %v", err)
			}
			if err := rsa.VerifyPKCS1v15(pub.(*rsa.PublicKey), crypto.Hash(0), data, res.Signature); err != nil {
				t.Fatalf("raw signature did not verify: %v", err)
			}

			if _, err := core.AsymmetricSign(ctx, "proj", "global", "kr", tc.id, "1", nil, bytes.Repeat([]byte{0x5A}, max+1)); err == nil {
				t.Fatalf("raw sign accepted %d bytes for a %d-bit key", max+1, tc.bits)
			}
			if _, err := core.AsymmetricSign(ctx, "proj", "global", "kr", tc.id, "1", data, nil); err == nil {
				t.Fatal("digest accepted for a raw algorithm")
			}
		})
	}
}

// TestCoreAsymmetricSignHashFromData covers a hash-based algorithm receiving
// data: the core hashes with the algorithm's digest and signs that.
func TestCoreAsymmetricSignHashFromData(t *testing.T) {
	ctx := context.Background()
	core, _ := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	pub := signKey(t, core, "proj", "sha", "RSA_SIGN_PKCS1_2048_SHA256")
	msg := []byte("hash me")
	res, err := core.AsymmetricSign(ctx, "proj", "global", "kr", "sha", "1", nil, msg)
	if err != nil {
		t.Fatalf("AsymmetricSign(data): %v", err)
	}
	sum := sha256.Sum256(msg)
	if err := rsa.VerifyPKCS1v15(pub.(*rsa.PublicKey), crypto.SHA256, sum[:], res.Signature); err != nil {
		t.Fatalf("signature over the hashed data did not verify: %v", err)
	}
}

// TestCoreAsymmetricSignRequiresExactlyOneOfDigestAndData pins the transport
// contract the REST and gRPC adapters share.
func TestCoreAsymmetricSignRequiresExactlyOneOfDigestAndData(t *testing.T) {
	ctx := context.Background()
	core, _ := newCore()
	if _, err := core.CreateKeyRing(ctx, "proj", "global", "kr"); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	pub := signKey(t, core, "proj", "sha2", "RSA_SIGN_PKCS1_2048_SHA256")
	_ = pub
	digest := sha256.Sum256([]byte("both"))

	if _, err := core.AsymmetricSign(ctx, "proj", "global", "kr", "sha2", "1", nil, nil); err == nil {
		t.Fatal("neither digest nor data was accepted")
	}
	if _, err := core.AsymmetricSign(ctx, "proj", "global", "kr", "sha2", "1", digest[:], []byte("both")); err == nil {
		t.Fatal("both digest and data were accepted")
	}
}
