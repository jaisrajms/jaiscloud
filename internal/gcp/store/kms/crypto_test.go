package kms

import (
	"bytes"
	"testing"
)

func TestEncryptDecryptDataRoundTrip(t *testing.T) {
	key, _ := Generate32()
	pt := []byte("hello, world")
	aad := []byte("my-context")

	ct, err := EncryptData(key, pt, aad)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	got, err := DecryptData(key, ct, aad)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(got, pt) {
		t.Errorf("round-trip mismatch: got %q, want %q", got, pt)
	}
}

func TestEncryptDecryptDataWrongAAD(t *testing.T) {
	key, _ := Generate32()
	ct, _ := EncryptData(key, []byte("secret"), []byte("aad-1"))
	if _, err := DecryptData(key, ct, []byte("aad-2")); err == nil {
		t.Fatal("expected decryption to fail with wrong AAD")
	}
}

func TestEncryptDataNondeterministic(t *testing.T) {
	key, _ := Generate32()
	a, _ := EncryptData(key, []byte("same"), nil)
	b, _ := EncryptData(key, []byte("same"), nil)
	if bytes.Equal(a, b) {
		t.Fatal("expected distinct ciphertext for identical plaintext")
	}
}

func TestDecryptDataShortCiphertext(t *testing.T) {
	key, _ := Generate32()
	if _, err := DecryptData(key, []byte("short"), nil); err == nil {
		t.Fatal("expected error on short ciphertext")
	}
}

func TestRawEncryptDecryptGCMRoundTrip(t *testing.T) {
	key, _ := Generate32()
	pt := []byte("raw plaintext")
	aad := []byte("raw-aad")
	iv := make([]byte, ivLen)
	for i := range iv {
		iv[i] = byte(i)
	}

	ct, err := RawEncryptGCM(key, pt, aad, iv)
	if err != nil {
		t.Fatalf("raw encrypt: %v", err)
	}
	got, err := RawDecryptGCM(key, ct, aad, iv)
	if err != nil {
		t.Fatalf("raw decrypt: %v", err)
	}
	if !bytes.Equal(got, pt) {
		t.Fatalf("round-trip mismatch: got %q, want %q", got, pt)
	}
	if _, err := RawDecryptGCM(key, ct, []byte("wrong"), iv); err == nil {
		t.Fatal("expected raw decrypt to fail with wrong AAD")
	}
}

func TestRawEncryptGCMRejectsBadIV(t *testing.T) {
	key, _ := Generate32()
	if _, err := RawEncryptGCM(key, []byte("pt"), nil, make([]byte, 8)); err == nil {
		t.Fatal("expected error on wrong IV length")
	}
	if _, err := RawDecryptGCM(key, []byte("ct"), nil, make([]byte, 8)); err == nil {
		t.Fatal("expected error on wrong IV length")
	}
}

func TestWrapUnwrapDEK(t *testing.T) {
	kek, _ := Generate32()
	dek, _ := Generate32()

	blob, err := WrapDEK(kek, dek)
	if err != nil {
		t.Fatalf("wrap: %v", err)
	}
	if blob[0] != versionAESGCM {
		t.Fatalf("expected version byte 0x01, got 0x%02x", blob[0])
	}
	got, err := UnwrapDEK(kek, blob)
	if err != nil {
		t.Fatalf("unwrap: %v", err)
	}
	if !bytes.Equal(got, dek) {
		t.Fatal("DEK round-trip mismatch")
	}

	// Wrong KEK fails.
	wrong, _ := Generate32()
	if _, err := UnwrapDEK(wrong, blob); err == nil {
		t.Fatal("expected unwrap to fail with wrong KEK")
	}

	// Plaintext blob round-trips.
	p := plaintextDEKBlob(dek)
	if got, err := UnwrapDEK(kek, p); err != nil || !bytes.Equal(got, dek) {
		t.Fatal("plaintext DEK blob round-trip failed")
	}
}

func TestParseHexKey(t *testing.T) {
	if _, err := ParseHexKey("not-hex"); err == nil {
		t.Fatal("expected error for non-hex")
	}
	if _, err := ParseHexKey("abcd"); err == nil {
		t.Fatal("expected error for wrong length")
	}
	kek, err := ParseHexKey("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	if err != nil || len(kek) != 32 {
		t.Fatalf("ParseHexKey = (%d bytes, %v)", len(kek), err)
	}
}
