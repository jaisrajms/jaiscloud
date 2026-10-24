package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var kmsClient kmspb.KeyManagementServiceClient

func initKMS() {
	conn, err := grpc.NewClient(grpcEndpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	kmsClient = kmspb.NewKeyManagementServiceClient(conn)
}

func kmsCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 20*time.Second)
}

// kmsLocation is a per-run project so list assertions see a clean key-ring set.
func kmsLocation() string { return "projects/kms-" + suffix + "/locations/global" }

func runKMS() {
	initKMS()

	cases := []struct {
		name string
		fn   func() error
	}{
		{"TestKeyRingCRUD", kmsKeyRingCRUD},
		{"TestSymmetricEncryptDecrypt", kmsSymmetricEncryptDecrypt},
		{"TestAsymmetricSignAndGetPublicKey", kmsAsymmetricSign},
		{"TestMacSignVerify", kmsMacSignVerify},
		{"TestDestroyVersion", kmsDestroyVersion},
	}
	for _, c := range cases {
		record("kms", c.name, c.fn())
	}
}

func kmsKeyRingCRUD() error {
	ctx, cancel := kmsCtx()
	defer cancel()
	parent := kmsLocation()
	kr, err := kmsClient.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{Parent: parent, KeyRingId: "my-ring"})
	if err != nil {
		return err
	}
	if kr.Name != parent+"/keyRings/my-ring" {
		return fmt.Errorf("wrong name %q", kr.Name)
	}
	got, err := kmsClient.GetKeyRing(ctx, &kmspb.GetKeyRingRequest{Name: kr.Name})
	if err != nil {
		return err
	}
	if got.Name != kr.Name {
		return fmt.Errorf("get name mismatch")
	}
	resp, err := kmsClient.ListKeyRings(ctx, &kmspb.ListKeyRingsRequest{Parent: parent})
	if err != nil {
		return err
	}
	if len(resp.KeyRings) != 1 {
		return fmt.Errorf("expected 1 key ring, got %d", len(resp.KeyRings))
	}
	return nil
}

func kmsSymmetricEncryptDecrypt() error {
	ctx, cancel := kmsCtx()
	defer cancel()
	parent := kmsLocation()
	if _, err := kmsClient.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{Parent: parent, KeyRingId: "ring1"}); err != nil {
		return err
	}
	ck, err := kmsClient.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent: parent + "/keyRings/ring1", CryptoKeyId: "sym-key",
		CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
	})
	if err != nil {
		return err
	}
	plaintext := []byte("hello world, this is a secret message")
	enc, err := kmsClient.Encrypt(ctx, &kmspb.EncryptRequest{Name: ck.Name, Plaintext: plaintext})
	if err != nil {
		return err
	}
	dec, err := kmsClient.Decrypt(ctx, &kmspb.DecryptRequest{Name: ck.Name, Ciphertext: enc.Ciphertext})
	if err != nil {
		return err
	}
	if string(dec.Plaintext) != string(plaintext) {
		return fmt.Errorf("round trip failed: %q", dec.Plaintext)
	}
	return nil
}

func kmsAsymmetricSign() error {
	ctx, cancel := kmsCtx()
	defer cancel()
	parent := kmsLocation()
	if _, err := kmsClient.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{Parent: parent, KeyRingId: "ring2"}); err != nil {
		return err
	}
	ck, err := kmsClient.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent: parent + "/keyRings/ring2", CryptoKeyId: "sign-key",
		CryptoKey: &kmspb.CryptoKey{
			Purpose:         kmspb.CryptoKey_ASYMMETRIC_SIGN,
			VersionTemplate: &kmspb.CryptoKeyVersionTemplate{Algorithm: kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256},
		},
	})
	if err != nil {
		return err
	}
	versionName := ck.Primary.Name
	pub, err := kmsClient.GetPublicKey(ctx, &kmspb.GetPublicKeyRequest{Name: versionName})
	if err != nil {
		return err
	}
	if pub.Pem == "" {
		return fmt.Errorf("expected PEM public key")
	}
	if pub.Algorithm != kmspb.CryptoKeyVersion_EC_SIGN_P256_SHA256 {
		return fmt.Errorf("algorithm %v", pub.Algorithm)
	}
	digest := sha256.Sum256([]byte("data to sign"))
	sig, err := kmsClient.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{
		Name: versionName, Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: digest[:]}},
	})
	if err != nil {
		return err
	}
	if len(sig.Signature) == 0 {
		return fmt.Errorf("empty signature")
	}
	return nil
}

func kmsMacSignVerify() error {
	ctx, cancel := kmsCtx()
	defer cancel()
	parent := kmsLocation()
	if _, err := kmsClient.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{Parent: parent, KeyRingId: "ring3"}); err != nil {
		return err
	}
	ck, err := kmsClient.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent: parent + "/keyRings/ring3", CryptoKeyId: "mac-key",
		CryptoKey: &kmspb.CryptoKey{
			Purpose:         kmspb.CryptoKey_MAC,
			VersionTemplate: &kmspb.CryptoKeyVersionTemplate{Algorithm: kmspb.CryptoKeyVersion_HMAC_SHA256},
		},
	})
	if err != nil {
		return err
	}
	versionName := ck.Primary.Name
	data := []byte("message to mac")
	mac, err := kmsClient.MacSign(ctx, &kmspb.MacSignRequest{Name: versionName, Data: data})
	if err != nil {
		return err
	}
	ok, err := kmsClient.MacVerify(ctx, &kmspb.MacVerifyRequest{Name: versionName, Data: data, Mac: mac.Mac})
	if err != nil {
		return err
	}
	if !ok.Success {
		return fmt.Errorf("expected MAC verify success")
	}
	bad, err := kmsClient.MacVerify(ctx, &kmspb.MacVerifyRequest{Name: versionName, Data: []byte("wrong"), Mac: mac.Mac})
	if err != nil {
		return err
	}
	if bad.Success {
		return fmt.Errorf("expected MAC verify failure for wrong data")
	}
	return nil
}

func kmsDestroyVersion() error {
	ctx, cancel := kmsCtx()
	defer cancel()
	parent := kmsLocation()
	if _, err := kmsClient.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{Parent: parent, KeyRingId: "ring4"}); err != nil {
		return err
	}
	ck, err := kmsClient.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent: parent + "/keyRings/ring4", CryptoKeyId: "doomed-key",
		CryptoKey: &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
	})
	if err != nil {
		return err
	}
	v, err := kmsClient.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: ck.Primary.Name})
	if err != nil {
		return err
	}
	if v.State != kmspb.CryptoKeyVersion_DESTROYED {
		return fmt.Errorf("state %v", v.State)
	}
	_, err = kmsClient.Encrypt(ctx, &kmspb.EncryptRequest{Name: ck.Name, Plaintext: []byte("test")})
	if err == nil {
		return fmt.Errorf("expected error encrypting with destroyed version")
	}
	return nil
}
