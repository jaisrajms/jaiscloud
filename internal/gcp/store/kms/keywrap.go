package kms

import (
	"crypto/aes"
	"encoding/binary"
	"errors"
)

// kwpIVPrefix is the RFC 5649 alternative initial value (AIV) high word.
const kwpIVPrefix uint32 = 0xA65959A6

// AESKeyWrapWithPadding wraps plaintext with kek using AES Key Wrap with
// Padding (RFC 5649). kek must be a 16/24/32-byte AES key. The returned
// ciphertext is 8*(ceil(len(plaintext)/8)+1) bytes. Cloud KMS uses this to
// protect imported key material with an ephemeral AES-256 key.
func AESKeyWrapWithPadding(kek, plaintext []byte) ([]byte, error) {
	if len(plaintext) == 0 {
		return nil, errors.New("kms: kwp: empty plaintext")
	}
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	n := (len(plaintext) + 7) / 8
	a := make([]byte, 8)
	binary.BigEndian.PutUint32(a[:4], kwpIVPrefix)
	binary.BigEndian.PutUint32(a[4:], uint32(len(plaintext)))

	r := make([][]byte, n)
	padded := make([]byte, n*8)
	copy(padded, plaintext)
	for i := 0; i < n; i++ {
		r[i] = padded[i*8 : i*8+8]
	}

	var buf [16]byte
	for j := 0; j < 6; j++ {
		for i := 0; i < n; i++ {
			copy(buf[:8], a)
			copy(buf[8:], r[i])
			block.Encrypt(buf[:], buf[:])
			copy(a, buf[:8])
			xorUint64(a, uint64(n*j+i+1))
			copy(r[i], buf[8:])
		}
	}

	out := make([]byte, 8*(n+1))
	copy(out, a)
	for i := 0; i < n; i++ {
		copy(out[8+i*8:], r[i])
	}
	return out, nil
}

// AESKeyUnwrapWithPadding reverses AESKeyWrapWithPadding, verifying the AIV
// integrity value, the MLI length field, and the zero padding.
func AESKeyUnwrapWithPadding(kek, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < 16 || len(ciphertext)%8 != 0 {
		return nil, errors.New("kms: kwp: invalid ciphertext length")
	}
	n := len(ciphertext)/8 - 1
	a := make([]byte, 8)
	copy(a, ciphertext[:8])
	r := make([][]byte, n)
	for i := 0; i < n; i++ {
		r[i] = append([]byte(nil), ciphertext[8+i*8:16+i*8]...)
	}

	var buf [16]byte
	for j := 5; j >= 0; j-- {
		for i := n - 1; i >= 0; i-- {
			xorUint64(a, uint64(n*j+i+1))
			copy(buf[:8], a)
			copy(buf[8:], r[i])
			block.Decrypt(buf[:], buf[:])
			copy(a, buf[:8])
			copy(r[i], buf[8:])
		}
	}

	if binary.BigEndian.Uint32(a[:4]) != kwpIVPrefix {
		return nil, errors.New("kms: kwp: integrity check failed")
	}
	mli := int(binary.BigEndian.Uint32(a[4:]))
	if mli < 8*(n-1)+1 || mli > 8*n {
		return nil, errors.New("kms: kwp: invalid message length")
	}
	pt := make([]byte, 0, n*8)
	for i := 0; i < n; i++ {
		pt = append(pt, r[i]...)
	}
	for _, b := range pt[mli:] {
		if b != 0 {
			return nil, errors.New("kms: kwp: invalid padding")
		}
	}
	return pt[:mli], nil
}

// xorUint64 XORs the big-endian encoding of t into the 8-byte slice b.
func xorUint64(b []byte, t uint64) {
	var tb [8]byte
	binary.BigEndian.PutUint64(tb[:], t)
	for i := 0; i < 8; i++ {
		b[i] ^= tb[i]
	}
}
