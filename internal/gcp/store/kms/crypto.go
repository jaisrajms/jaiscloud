package kms

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
)

// ivLen is the AES-GCM nonce size used by this package (12 bytes, like the AWS
// key package's encryptData/decryptData).
const ivLen = 12

// DEK blob version bytes (mirror AWS key package).
const (
	versionPlaintext byte = 0x00
	versionAESGCM    byte = 0x01
)

// Generate32 returns 32 cryptographically-random bytes (an AES-256 key).
func Generate32() ([]byte, error) {
	b := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return nil, err
	}
	return b, nil
}

// ParseHexKey decodes a 32-byte hex-encoded KEK (master key) from a string.
func ParseHexKey(s string) ([]byte, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, errors.New("kms: invalid master key hex")
	}
	if len(b) != 32 {
		return nil, errors.New("kms: master key must be 32 bytes")
	}
	return b, nil
}

// WrapDEK encrypts a 32-byte DEK with the KEK using AES-256-GCM, producing
// VERSION(0x01) || IV(12) || CIPHERTEXT+TAG.
func WrapDEK(kek, dek []byte) ([]byte, error) {
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	iv := make([]byte, ivLen)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}
	ct := gcm.Seal(nil, iv, dek, nil)
	blob := make([]byte, 1+ivLen+len(ct))
	blob[0] = versionAESGCM
	copy(blob[1:], iv)
	copy(blob[1+ivLen:], ct)
	return blob, nil
}

// UnwrapDEK decrypts a wrapped DEK blob produced by WrapDEK (or returns the
// raw bytes for a plaintext version-0x00 blob).
func UnwrapDEK(kek, blob []byte) ([]byte, error) {
	if len(blob) == 0 {
		return nil, errors.New("kms: unwrap dek: empty blob")
	}
	if blob[0] == versionPlaintext {
		return blob[1:], nil
	}
	if blob[0] != versionAESGCM {
		return nil, errors.New("kms: unwrap dek: unknown version")
	}
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(blob) < 1+ivLen+16 {
		return nil, errors.New("kms: unwrap dek: blob too short")
	}
	return gcm.Open(nil, blob[1:1+ivLen], blob[1+ivLen:], nil)
}

// plaintextDEKBlob wraps a DEK as a version-0x00 blob for plaintext storage.
func plaintextDEKBlob(dek []byte) []byte {
	blob := make([]byte, 1+len(dek))
	blob[0] = versionPlaintext
	copy(blob[1:], dek)
	return blob
}

// EncryptData encrypts pt with AES-256-GCM using key, binding additionalData
// as the AEAD associated data. The returned ciphertext is iv || ct.
func EncryptData(key, pt, additionalData []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	iv := make([]byte, ivLen)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}
	return gcm.Seal(iv, iv, pt, additionalData), nil
}

// DecryptData decrypts an iv || ct blob produced by EncryptData.
func DecryptData(key, ct, additionalData []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(ct) < ivLen {
		return nil, errors.New("kms: ciphertext too short")
	}
	return gcm.Open(nil, ct[:ivLen], ct[ivLen:], additionalData)
}

// RawEncryptGCM encrypts pt with AES-GCM using the caller-supplied nonce iv,
// binding additionalData as the AEAD associated data. Unlike EncryptData the IV
// is not prepended: the ciphertext is ct||tag and the IV is returned separately
// by the caller (Cloud KMS RawEncrypt semantics). The nonce must be ivLen bytes.
func RawEncryptGCM(key, pt, additionalData, iv []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(iv) != gcm.NonceSize() {
		return nil, errors.New("kms: invalid initialization vector length")
	}
	return gcm.Seal(nil, iv, pt, additionalData), nil
}

// RawDecryptGCM decrypts a ct||tag blob produced by RawEncryptGCM using the
// caller-supplied nonce iv.
func RawDecryptGCM(key, ct, additionalData, iv []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(iv) != gcm.NonceSize() {
		return nil, errors.New("kms: invalid initialization vector length")
	}
	return gcm.Open(nil, iv, ct, additionalData)
}

// EncodeVersionedCiphertext prefixes a version number to the encrypted blob so
// Decrypt can select the correct key material after rotation. Format:
//
//	<version digits> ":" <iv || gcm(ct)>
func EncodeVersionedCiphertext(version string, ct []byte) []byte {
	out := make([]byte, 0, len(version)+1+len(ct))
	out = append(out, version...)
	out = append(out, ':')
	return append(out, ct...)
}

// DecodeVersionedCiphertext splits a versioned ciphertext blob back into its
// version number and encrypted payload.
func DecodeVersionedCiphertext(blob []byte) (version string, ct []byte, err error) {
	i := bytes.IndexByte(blob, ':')
	if i < 0 {
		return "", nil, errors.New("kms: invalid ciphertext: missing version")
	}
	return string(blob[:i]), blob[i+1:], nil
}
