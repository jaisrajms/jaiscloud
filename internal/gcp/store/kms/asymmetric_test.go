package kms

import (
	"bytes"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"testing"
)

func TestRSASignVerify(t *testing.T) {
	priv, pub, err := GenerateRSAKeyPair(2048)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	digest := sha256.Sum256([]byte("hello"))
	sig, err := RSASign(priv, digest[:], "RSA_SIGN_PKCS1_2048_SHA256")
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	k, _ := x509.ParsePKIXPublicKey(pub)
	if err := rsa.VerifyPKCS1v15(k.(*rsa.PublicKey), crypto.SHA256, digest[:], sig); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestRSADecryptOAEP(t *testing.T) {
	priv, pub, err := GenerateRSAKeyPair(2048)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	k, _ := x509.ParsePKIXPublicKey(pub)
	ct, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, k.(*rsa.PublicKey), []byte("msg"), nil)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	pt, err := RSADecryptOAEP(priv, ct, "RSA_DECRYPT_OAEP_2048_SHA256")
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(pt, []byte("msg")) {
		t.Fatalf("round-trip mismatch: %q", pt)
	}
}

func TestHMACRoundTrip(t *testing.T) {
	mac, err := HMACSign([]byte("key"), []byte("data"), "HMAC_SHA256")
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if !HMACVerify([]byte("key"), []byte("data"), mac, "HMAC_SHA256") {
		t.Fatal("expected valid MAC")
	}
	if HMACVerify([]byte("key"), []byte("data"), []byte("bad"), "HMAC_SHA256") {
		t.Fatal("expected invalid MAC to fail")
	}
}

func TestGenerateVersionMaterial(t *testing.T) {
	// Symmetric → key material only.
	km, priv, pub, err := generateVersionMaterial("GOOGLE_SYMMETRIC_ENCRYPTION")
	if err != nil || len(km) != 32 || priv != nil || pub != nil {
		t.Fatalf("symmetric material = (%d,%v,%v) err=%v", len(km), priv != nil, pub != nil, err)
	}
	// HMAC → key material only.
	km, _, _, _ = generateVersionMaterial("HMAC_SHA256")
	if len(km) != 32 {
		t.Fatalf("hmac material len = %d", len(km))
	}
	// RSA → private + public only.
	km, priv, pub, err = generateVersionMaterial("RSA_SIGN_PKCS1_2048_SHA256")
	if err != nil || km != nil || priv == nil || pub == nil {
		t.Fatalf("rsa material = (%v,%v,%v) err=%v", km != nil, priv != nil, pub != nil, err)
	}
	// EC → private + public only.
	km, priv, pub, _ = generateVersionMaterial("EC_SIGN_P256_SHA256")
	if km != nil || priv == nil || pub == nil {
		t.Fatalf("ec material = (%v,%v,%v)", km != nil, priv != nil, pub != nil)
	}
}

func TestGenerateVersionMaterialEd25519(t *testing.T) {
	km, priv, pub, err := generateVersionMaterial("EC_SIGN_ED25519")
	if err != nil || km != nil || priv == nil || pub == nil {
		t.Fatalf("ed25519 material = (%v,%v,%v) err=%v", km != nil, priv != nil, pub != nil, err)
	}
	k, err := x509.ParsePKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("parse private key: %v", err)
	}
	if _, ok := k.(ed25519.PrivateKey); !ok {
		t.Fatalf("private key = %T, want ed25519.PrivateKey", k)
	}
	p, err := x509.ParsePKIXPublicKey(pub)
	if err != nil {
		t.Fatalf("parse public key: %v", err)
	}
	if _, ok := p.(ed25519.PublicKey); !ok {
		t.Fatalf("public key = %T, want ed25519.PublicKey", p)
	}
}

func TestECSignEd25519(t *testing.T) {
	_, priv, pub, _ := generateVersionMaterial("EC_SIGN_ED25519")
	msg := []byte("hello ed25519")
	sig, err := ECSign(priv, msg, "EC_SIGN_ED25519")
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if len(sig) != ed25519.SignatureSize {
		t.Fatalf("signature length = %d, want %d", len(sig), ed25519.SignatureSize)
	}
	p, _ := x509.ParsePKIXPublicKey(pub)
	if !ed25519.Verify(p.(ed25519.PublicKey), msg, sig) {
		t.Fatal("ed25519 signature did not verify")
	}
}

func TestRSASignRaw(t *testing.T) {
	priv, pub, err := GenerateRSAKeyPair(2048)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	// A raw PKCS#1 v1.5 signature still carries 11 bytes of padding, so the
	// largest input is modulus size minus 11 (245 bytes for 2048 bits).
	data := bytes.Repeat([]byte{0xAB}, 245)
	sig, err := RSASign(priv, data, "RSA_SIGN_RAW_PKCS1_2048")
	if err != nil {
		t.Fatalf("raw sign: %v", err)
	}
	k, _ := x509.ParsePKIXPublicKey(pub)
	if err := rsa.VerifyPKCS1v15(k.(*rsa.PublicKey), crypto.Hash(0), data, sig); err != nil {
		t.Fatalf("raw verify: %v", err)
	}
	if _, err := RSASign(priv, bytes.Repeat([]byte{0xAB}, 246), "RSA_SIGN_RAW_PKCS1_2048"); err == nil {
		t.Fatal("raw sign accepted input longer than modulus-11")
	}
}
