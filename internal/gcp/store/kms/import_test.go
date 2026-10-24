package kms

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"testing"
)

func TestAESKeyWrapWithPaddingRFC5649Vector(t *testing.T) {
	kek, _ := hex.DecodeString("5840df6e29b02af1ab493b705bf16ea1ae8338f4dcc176a8")
	key, _ := hex.DecodeString("c37b7e6492584340bed12207808941155068f738")
	want, _ := hex.DecodeString("138bdeaa9b8fa7fc61f97742e72248ee5ae6ae5360d1ae6a5f54f373fa543b6a")

	got, err := AESKeyWrapWithPadding(kek, key)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("wrap = %x, want %x", got, want)
	}
	back, err := AESKeyUnwrapWithPadding(kek, got)
	if err != nil {
		t.Fatalf("unwrap: %v", err)
	}
	if !bytes.Equal(back, key) {
		t.Fatalf("unwrap = %x, want %x", back, key)
	}
}

func TestAESKeyWrapWithPaddingRoundTrip(t *testing.T) {
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{1, 7, 8, 9, 16, 31, 32, 33, 64, 128} {
		pt := make([]byte, n)
		if _, err := rand.Read(pt); err != nil {
			t.Fatal(err)
		}
		ct, err := AESKeyWrapWithPadding(kek, pt)
		if err != nil {
			t.Fatalf("n=%d wrap: %v", n, err)
		}
		back, err := AESKeyUnwrapWithPadding(kek, ct)
		if err != nil {
			t.Fatalf("n=%d unwrap: %v", n, err)
		}
		if !bytes.Equal(back, pt) {
			t.Fatalf("n=%d round-trip mismatch", n)
		}
	}
}

func TestAESKeyUnwrapWithPaddingRejectsTampered(t *testing.T) {
	kek := make([]byte, 32)
	rand.Read(kek)
	ct, _ := AESKeyWrapWithPadding(kek, []byte("hello world"))
	ct[3] ^= 0xff
	if _, err := AESKeyUnwrapWithPadding(kek, ct); err == nil {
		t.Fatal("expected integrity failure for tampered ciphertext")
	}
}

func TestUnwrapImportedKeyRSAOAEPWithAESKWP(t *testing.T) {
	const method = "RSA_OAEP_3072_SHA256_AES_256"
	privDER, pubDER, raw, err := GenerateImportJobWrappingKey(method)
	if err != nil {
		t.Fatalf("generate wrapping key: %v", err)
	}
	if raw {
		t.Fatal("RSA method should not return a raw public key")
	}
	pub, err := x509.ParsePKIXPublicKey(pubDER)
	if err != nil {
		t.Fatal(err)
	}
	rpub, ok := pub.(*rsa.PublicKey)
	if !ok {
		t.Fatal("wrapping public key is not RSA")
	}

	material := []byte("aes-256-key-material-for-import!!")
	kwpKey := make([]byte, 32)
	rand.Read(kwpKey)
	enc, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, rpub, kwpKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	wrappedKWP, err := AESKeyWrapWithPadding(kwpKey, material)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := append(enc, wrappedKWP...)

	got, err := UnwrapImportedKeyMaterial(method, privDER, wrapped)
	if err != nil {
		t.Fatalf("unwrap: %v", err)
	}
	if !bytes.Equal(got, material) {
		t.Fatalf("unwrap = %x, want %x", got, material)
	}
}

func TestUnwrapImportedKeyRSAOAEPDirect(t *testing.T) {
	const method = "RSA_OAEP_3072_SHA256"
	privDER, pubDER, _, err := GenerateImportJobWrappingKey(method)
	if err != nil {
		t.Fatal(err)
	}
	pubAny, _ := x509.ParsePKIXPublicKey(pubDER)
	rpub := pubAny.(*rsa.PublicKey)
	material := []byte("pkcs8-der-would-go-here")
	wrapped, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, rpub, material, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnwrapImportedKeyMaterial(method, privDER, wrapped)
	if err != nil {
		t.Fatalf("unwrap: %v", err)
	}
	if !bytes.Equal(got, material) {
		t.Fatalf("unwrap = %x, want %x", got, material)
	}
}

func TestUnwrapImportedKeyHPKERoundTrip(t *testing.T) {
	for _, method := range []string{
		"HPKE_KEM_ML_KEM_768_HKDF_SHA256_AES_256_GCM",
		"HPKE_KEM_ML_KEM_1024_HKDF_SHA256_AES_256_GCM",
		"HPKE_KEM_XWING_HKDF_SHA256_AES_256_GCM",
	} {
		priv, pub, raw, err := GenerateImportJobWrappingKey(method)
		if err != nil {
			t.Fatalf("%s generate: %v", method, err)
		}
		if !raw {
			t.Fatalf("%s should return a raw public key", method)
		}
		material := []byte("post-quantum-imported-material!")
		sealed, err := HPKESeal(method, pub, material)
		if err != nil {
			t.Fatalf("%s seal: %v", method, err)
		}
		got, err := UnwrapImportedKeyMaterial(method, priv, sealed)
		if err != nil {
			t.Fatalf("%s unwrap: %v", method, err)
		}
		if !bytes.Equal(got, material) {
			t.Fatalf("%s round-trip mismatch", method)
		}
	}
}

func TestUnwrapImportedKeyWrongKeyFails(t *testing.T) {
	const method = "RSA_OAEP_3072_SHA256_AES_256"
	_, pubDER, _, _ := GenerateImportJobWrappingKey(method)
	wrongPriv, _, _, _ := GenerateImportJobWrappingKey(method)
	pubAny, _ := x509.ParsePKIXPublicKey(pubDER)
	rpub := pubAny.(*rsa.PublicKey)
	kwpKey := make([]byte, 32)
	rand.Read(kwpKey)
	enc, _ := rsa.EncryptOAEP(sha256.New(), rand.Reader, rpub, kwpKey, nil)
	wrappedKWP, _ := AESKeyWrapWithPadding(kwpKey, []byte("material"))
	wrapped := append(enc, wrappedKWP...)
	if _, err := UnwrapImportedKeyMaterial(method, wrongPriv, wrapped); err == nil {
		t.Fatal("expected unwrap with the wrong key to fail")
	}
}

func TestKEMRoundTrip(t *testing.T) {
	for _, algo := range []string{"ML_KEM_768", "ML_KEM_1024", "KEM_XWING"} {
		priv, pub, err := GenerateKEMKeyPair(algo)
		if err != nil {
			t.Fatalf("%s generate: %v", algo, err)
		}
		ss, ct, err := EncapsulateKEM(algo, pub)
		if err != nil {
			t.Fatalf("%s encapsulate: %v", algo, err)
		}
		got, err := DecapsulateKEM(algo, priv, ct)
		if err != nil {
			t.Fatalf("%s decapsulate: %v", algo, err)
		}
		if !bytes.Equal(got, ss) {
			t.Fatalf("%s shared secret mismatch", algo)
		}
		if len(ss) != 32 {
			t.Fatalf("%s shared secret length = %d, want 32", algo, len(ss))
		}
	}
}
