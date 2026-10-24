//go:build gcp_conformance

package gcpconformance

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/mlkem"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// setEmulatorClock reconfigures the emulator clock through the admin control
// plane. spec is "real" (wall time) or a Go duration that offsets the clock
// from real time ("744h"). The control-plane exchange is deliberately not part
// of the transcript: only public-API responses are evidence.
func setEmulatorClock(client *http.Client, host, spec string) error {
	body := map[string]any{"mode": "real"}
	if spec != "real" {
		d, err := time.ParseDuration(spec)
		if err != nil {
			return fmt.Errorf("clock spec %q is neither \"real\" nor a duration: %w", spec, err)
		}
		body = map[string]any{"mode": "offset", "time": time.Now().Add(d).UTC().Format(time.RFC3339Nano)}
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, host+"/_jaiscloud/clock", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("clock control returned %d", resp.StatusCode)
	}
	return nil
}

// ─── Client-side cryptography for the curated chains ─────────────────────────
//
// These helpers stand in for the cryptography a real caller performs locally
// between two Cloud KMS calls: wrapping key material with an import job's
// public key, sealing plaintext with an asymmetric key, and encapsulating with
// a KEM public key. They are implemented from the public formats (RFC 5649,
// RFC 8017, FIPS 203) rather than by calling the emulator, so the recorded
// request bodies are what an independent client would send.

// b64 encodes bytes the way every Cloud KMS JSON request field expects them.
func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// parsePublicKeyPEM decodes an RSA PEM public key returned by
// cryptoKeyVersions.getPublicKey or an import job.
func parsePublicKeyPEM(pemStr string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, errors.New("public key is not PEM")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	pub, ok := parsed.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("public key is not RSA")
	}
	return pub, nil
}

// rsaOAEPEncrypt seals plaintext with an RSA public key using RSAES-OAEP and
// SHA-256, the scheme Cloud KMS RSA_DECRYPT_OAEP_*_SHA256 versions unwrap.
func rsaOAEPEncrypt(pemStr string, plaintext []byte) ([]byte, error) {
	pub, err := parsePublicKeyPEM(pemStr)
	if err != nil {
		return nil, err
	}
	return rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, plaintext, nil)
}

// wrapImportMaterial wraps key material for a Cloud KMS import job with the
// RSA_OAEP_*_SHA256_AES_256 import method: a fresh ephemeral AES-256 key wrapped
// with RSAES-OAEP, followed by the material wrapped with that key using
// AES-KWP (RFC 5649).
func wrapImportMaterial(pemStr string, material []byte) ([]byte, error) {
	pub, err := parsePublicKeyPEM(pemStr)
	if err != nil {
		return nil, err
	}
	kek := make([]byte, 32)
	if _, err := rand.Read(kek); err != nil {
		return nil, err
	}
	sealed, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, kek, nil)
	if err != nil {
		return nil, err
	}
	wrapped, err := aesKeyWrapWithPadding(kek, material)
	if err != nil {
		return nil, err
	}
	return append(sealed, wrapped...), nil
}

// mlkemEncapsulate encapsulates a shared secret with an ML-KEM-768 public key,
// returning the ciphertext a Cloud KMS Decapsulate call expects. (The shared
// secret stays inside the emulator; the transcript validates the response
// shape, not the key agreement.)
func mlkemEncapsulate(publicKey []byte) ([]byte, error) {
	ek, err := mlkem.NewEncapsulationKey768(publicKey)
	if err != nil {
		return nil, err
	}
	_, ciphertext := ek.Encapsulate()
	return ciphertext, nil
}

// aesKeyWrapWithPadding implements AES Key Wrap with Padding (RFC 5649) for a
// 256-bit KEK, the format Cloud KMS uses for trusted wrapping and for the
// AES-KWP layer of its RSA import methods.
func aesKeyWrapWithPadding(kek, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	if len(plaintext) == 0 || len(plaintext) > 0xffffffff {
		return nil, errors.New("aes-kwp: plaintext length out of range")
	}
	// RFC 5649 §3: AIV = 0xA65959A6 || MLI32 replaces the RFC 3394 default IV;
	// §4 pads the plaintext to an 8-byte multiple and runs the same six-round
	// wrap (including for a single block).
	padded := make([]byte, (len(plaintext)+7)/8*8)
	copy(padded, plaintext)
	aiv := make([]byte, 8)
	binary.BigEndian.PutUint32(aiv[:4], 0xA65959A6)
	binary.BigEndian.PutUint32(aiv[4:], uint32(len(plaintext)))
	return aesKeyWrapWithIV(block, aiv, padded)
}

// aesKeyWrapWithIV is the RFC 3394 key-wrap core over an 8-byte-aligned
// plaintext with a caller-supplied initial value (RFC 5649 substitutes its AIV
// for the RFC 3394 default).
func aesKeyWrapWithIV(block cipher.Block, a, plaintext []byte) ([]byte, error) {
	n := len(plaintext) / 8
	if n < 1 || len(plaintext)%8 != 0 {
		return nil, errors.New("aes-kwp: invalid input length")
	}
	a = append([]byte(nil), a...)
	r := make([][]byte, n)
	for i := 0; i < n; i++ {
		r[i] = append([]byte(nil), plaintext[i*8:i*8+8]...)
	}
	buf := make([]byte, 16)
	for j := 0; j <= 5; j++ {
		for i := 0; i < n; i++ {
			copy(buf[:8], a)
			copy(buf[8:], r[i])
			block.Encrypt(buf, buf)
			t := uint64(n*j + i + 1)
			copy(a, buf[:8])
			for k := 0; k < 8; k++ {
				a[7-k] ^= byte(t >> (8 * uint(k)))
			}
			copy(r[i], buf[8:])
		}
	}
	out := make([]byte, 0, 8*(n+1))
	out = append(out, a...)
	for _, ri := range r {
		out = append(out, ri...)
	}
	return out, nil
}
