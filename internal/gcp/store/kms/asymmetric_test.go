package kms

import (
	"bytes"
	"crypto"
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
