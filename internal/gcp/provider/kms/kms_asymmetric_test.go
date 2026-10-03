package kms

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"testing"
)

func TestKMSAsymmetricSignAndPublicKey(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("keyring: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr", "cryptoKeyId": "k", "body": map[string]any{"purpose": "ASYMMETRIC_SIGN"}})); err != nil {
		t.Fatalf("cryptokey: %v", err)
	}

	digest := sha256.Sum256([]byte("hello"))
	sr, err := p.CryptoKeyVersionAsymmetricSign(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/1",
		"body": map[string]any{"digest": base64.StdEncoding.EncodeToString(digest[:])},
	}))
	if err != nil {
		t.Fatalf("asymmetricSign: %v", err)
	}
	sig, err := base64.StdEncoding.DecodeString(sr.Data["signature"].(string))
	if err != nil {
		t.Fatalf("decode sig: %v", err)
	}

	pr, err := p.CryptoKeyVersionGetPublicKey(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/1/publicKey"}))
	if err != nil {
		t.Fatalf("getPublicKey: %v", err)
	}
	block, _ := pem.Decode([]byte(pr.Data["pem"].(string)))
	if block == nil {
		t.Fatal("expected PEM block")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("parse public key: %v", err)
	}
	if err := rsa.VerifyPKCS1v15(pub.(*rsa.PublicKey), crypto.SHA256, digest[:], sig); err != nil {
		t.Fatalf("verify signature: %v", err)
	}
}

func TestKMSMacSignVerify(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("keyring: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr", "cryptoKeyId": "k", "body": map[string]any{"purpose": "MAC"}})); err != nil {
		t.Fatalf("cryptokey: %v", err)
	}

	data := base64.StdEncoding.EncodeToString([]byte("data"))
	sr, err := p.CryptoKeyVersionMacSign(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/1",
		"body": map[string]any{"data": data},
	}))
	if err != nil {
		t.Fatalf("macSign: %v", err)
	}
	mac := sr.Data["mac"].(string)

	vr, err := p.CryptoKeyVersionMacVerify(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/1",
		"body": map[string]any{"data": data, "mac": mac},
	}))
	if err != nil {
		t.Fatalf("macVerify: %v", err)
	}
	if vr.Data["success"] != true {
		t.Errorf("expected success=true, got %v", vr.Data["success"])
	}

	vr, _ = p.CryptoKeyVersionMacVerify(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/1",
		"body": map[string]any{"data": data, "mac": base64.StdEncoding.EncodeToString([]byte("wrong"))},
	}))
	if vr.Data["success"] != false {
		t.Errorf("expected success=false for wrong mac, got %v", vr.Data["success"])
	}
}
