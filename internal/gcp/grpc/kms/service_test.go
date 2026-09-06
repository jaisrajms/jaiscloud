package kms

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"net"
	"testing"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	kmspb "cloud.google.com/go/kms/apiv1/kmspb"

	gcpcrypto "jaiscloud/internal/gcp/crypto"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// kmsTestService dials a real in-process gRPC server backed by the memory
// stores and returns the KMS and IAM clients.
func kmsTestService(t *testing.T) (kmspb.KeyManagementServiceClient, iampb.IAMPolicyClient, func()) {
	t.Helper()
	keys := kmsstore.NewMemoryStore()
	resources := store.NewMemoryResourceStore()
	svc := NewService(keys, resources, gcpcrypto.NewEnvelopeEncryptor(keys), "test")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	kmspb.RegisterKeyManagementServiceServer(srv, svc)
	iampb.RegisterIAMPolicyServer(srv, svc)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		conn.Close()
		srv.Stop()
	}
	return kmspb.NewKeyManagementServiceClient(conn), iampb.NewIAMPolicyClient(conn), cleanup
}

const (
	keyRing = "projects/test/locations/global/keyRings/kr"
	symKey  = keyRing + "/cryptoKeys/sym"
	signKey = keyRing + "/cryptoKeys/sign"
	macKey  = keyRing + "/cryptoKeys/mac"
)

func createKeyRing(t *testing.T, client kmspb.KeyManagementServiceClient, name string) {
	t.Helper()
	if _, err := client.CreateKeyRing(context.Background(), &kmspb.CreateKeyRingRequest{
		Parent: "projects/test/locations/global", KeyRingId: name,
	}); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
}

func TestKMSKeyRingKeyVersionCRUD(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()

	// KeyRing create + get + list.
	createdKR, err := client.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{
		Parent: "projects/test/locations/global", KeyRingId: "kr",
	})
	if err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if createdKR.GetName() != keyRing {
		t.Fatalf("CreateKeyRing name = %q, want %q", createdKR.GetName(), keyRing)
	}
	if _, err := client.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{
		Parent: "projects/test/locations/global", KeyRingId: "kr",
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate CreateKeyRing err = %v, want AlreadyExists", err)
	}
	gotKR, err := client.GetKeyRing(ctx, &kmspb.GetKeyRingRequest{Name: keyRing})
	if err != nil {
		t.Fatalf("GetKeyRing: %v", err)
	}
	if gotKR.GetName() != keyRing {
		t.Fatalf("GetKeyRing name = %q, want %q", gotKR.GetName(), keyRing)
	}
	krs, err := client.ListKeyRings(ctx, &kmspb.ListKeyRingsRequest{Parent: "projects/test/locations/global"})
	if err != nil {
		t.Fatalf("ListKeyRings: %v", err)
	}
	if len(krs.GetKeyRings()) != 1 || krs.GetKeyRings()[0].GetName() != keyRing {
		t.Fatalf("ListKeyRings = %v, want exactly [%s]", krs.GetKeyRings(), keyRing)
	}

	// CryptoKey create + get + list (symmetric default).
	createdKey, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent: keyRing, CryptoKeyId: "sym",
	})
	if err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	if createdKey.GetName() != symKey {
		t.Fatalf("CreateCryptoKey name = %q, want %q", createdKey.GetName(), symKey)
	}
	if createdKey.GetPurpose() != kmspb.CryptoKey_ENCRYPT_DECRYPT {
		t.Fatalf("CreateCryptoKey purpose = %v, want ENCRYPT_DECRYPT", createdKey.GetPurpose())
	}
	if createdKey.GetPrimary().GetName() != symKey+"/cryptoKeyVersions/1" {
		t.Fatalf("CreateCryptoKey primary = %q, want version 1", createdKey.GetPrimary().GetName())
	}
	gotKey, err := client.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: symKey})
	if err != nil {
		t.Fatalf("GetCryptoKey: %v", err)
	}
	if gotKey.GetName() != symKey {
		t.Fatalf("GetCryptoKey name = %q, want %q", gotKey.GetName(), symKey)
	}
	cks, err := client.ListCryptoKeys(ctx, &kmspb.ListCryptoKeysRequest{Parent: keyRing})
	if err != nil {
		t.Fatalf("ListCryptoKeys: %v", err)
	}
	if len(cks.GetCryptoKeys()) != 1 || cks.GetCryptoKeys()[0].GetName() != symKey {
		t.Fatalf("ListCryptoKeys = %v, want exactly [%s]", cks.GetCryptoKeys(), symKey)
	}

	// CryptoKeyVersion create + get + list.
	createdVer, err := client.CreateCryptoKeyVersion(ctx, &kmspb.CreateCryptoKeyVersionRequest{
		Parent: symKey,
	})
	if err != nil {
		t.Fatalf("CreateCryptoKeyVersion: %v", err)
	}
	if createdVer.GetName() != symKey+"/cryptoKeyVersions/2" {
		t.Fatalf("CreateCryptoKeyVersion name = %q, want version 2", createdVer.GetName())
	}
	if createdVer.GetState() != kmspb.CryptoKeyVersion_ENABLED {
		t.Fatalf("CreateCryptoKeyVersion state = %v, want ENABLED", createdVer.GetState())
	}
	gotVer, err := client.GetCryptoKeyVersion(ctx, &kmspb.GetCryptoKeyVersionRequest{Name: symKey + "/cryptoKeyVersions/2"})
	if err != nil {
		t.Fatalf("GetCryptoKeyVersion: %v", err)
	}
	if gotVer.GetName() != symKey+"/cryptoKeyVersions/2" {
		t.Fatalf("GetCryptoKeyVersion name = %q, want version 2", gotVer.GetName())
	}
	vers, err := client.ListCryptoKeyVersions(ctx, &kmspb.ListCryptoKeyVersionsRequest{Parent: symKey})
	if err != nil {
		t.Fatalf("ListCryptoKeyVersions: %v", err)
	}
	if len(vers.GetCryptoKeyVersions()) != 2 {
		t.Fatalf("ListCryptoKeyVersions = %d versions, want 2", len(vers.GetCryptoKeyVersions()))
	}

	// Missing key → NotFound.
	if _, err := client.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: keyRing + "/cryptoKeys/missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetCryptoKey missing err = %v, want NotFound", err)
	}
}

func TestKMSSymmetricEncryptDecrypt(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: keyRing, CryptoKeyId: "sym"}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}

	plaintext := []byte("the quick brown fox")
	aad := []byte("context")

	enc, err := client.Encrypt(ctx, &kmspb.EncryptRequest{
		Name:                        symKey,
		Plaintext:                   plaintext,
		AdditionalAuthenticatedData: aad,
	})
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if len(enc.GetCiphertext()) == 0 {
		t.Fatal("Encrypt ciphertext is empty")
	}
	if enc.GetProtectionLevel() != kmspb.ProtectionLevel_SOFTWARE {
		t.Fatalf("Encrypt protectionLevel = %v, want SOFTWARE", enc.GetProtectionLevel())
	}

	// Decrypt with the matching AAD recovers the plaintext.
	dec, err := client.Decrypt(ctx, &kmspb.DecryptRequest{
		Name:                        symKey,
		Ciphertext:                  enc.GetCiphertext(),
		AdditionalAuthenticatedData: aad,
	})
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if string(dec.GetPlaintext()) != string(plaintext) {
		t.Fatalf("Decrypt plaintext = %q, want %q", dec.GetPlaintext(), plaintext)
	}
	if !dec.GetUsedPrimary() {
		t.Fatal("Decrypt usedPrimary = false, want true")
	}

	// Decrypt with the wrong AAD fails.
	if _, err := client.Decrypt(ctx, &kmspb.DecryptRequest{
		Name: symKey, Ciphertext: enc.GetCiphertext(),
		AdditionalAuthenticatedData: []byte("wrong"),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Decrypt wrong AAD err = %v, want InvalidArgument", err)
	}
}

func TestKMSAsymmetricSignAndPublicKey(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent:      keyRing,
		CryptoKeyId: "sign",
		CryptoKey: &kmspb.CryptoKey{
			Purpose: kmspb.CryptoKey_ASYMMETRIC_SIGN,
		},
	}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}

	verName := signKey + "/cryptoKeyVersions/1"
	msg := []byte("message to sign")
	digest := sha256.Sum256(msg)

	signResp, err := client.AsymmetricSign(ctx, &kmspb.AsymmetricSignRequest{
		Name:   verName,
		Digest: &kmspb.Digest{Digest: &kmspb.Digest_Sha256{Sha256: digest[:]}},
	})
	if err != nil {
		t.Fatalf("AsymmetricSign: %v", err)
	}
	if len(signResp.GetSignature()) == 0 {
		t.Fatal("AsymmetricSign signature is empty")
	}

	// GetPublicKey returns a verifiable PEM.
	pub, err := client.GetPublicKey(ctx, &kmspb.GetPublicKeyRequest{Name: verName})
	if err != nil {
		t.Fatalf("GetPublicKey: %v", err)
	}
	if pub.GetProtectionLevel() != kmspb.ProtectionLevel_SOFTWARE {
		t.Fatalf("GetPublicKey protectionLevel = %v, want SOFTWARE", pub.GetProtectionLevel())
	}
	block, _ := pem.Decode([]byte(pub.GetPem()))
	if block == nil {
		t.Fatalf("GetPublicKey pem is not valid PEM: %q", pub.GetPem())
	}
	pubKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("parse public key: %v", err)
	}
	rsaPub, ok := pubKey.(*rsa.PublicKey)
	if !ok {
		t.Fatalf("public key type = %T, want *rsa.PublicKey", pubKey)
	}
	if err := rsa.VerifyPKCS1v15(rsaPub, crypto.SHA256, digest[:], signResp.GetSignature()); err != nil {
		t.Fatalf("signature verify failed: %v", err)
	}
}

func TestKMSMacSignVerify(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent:      keyRing,
		CryptoKeyId: "mac",
		CryptoKey: &kmspb.CryptoKey{
			Purpose: kmspb.CryptoKey_MAC,
		},
	}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}

	verName := macKey + "/cryptoKeyVersions/1"
	data := []byte("mac this data")

	signResp, err := client.MacSign(ctx, &kmspb.MacSignRequest{Name: verName, Data: data})
	if err != nil {
		t.Fatalf("MacSign: %v", err)
	}
	if len(signResp.GetMac()) == 0 {
		t.Fatal("MacSign mac is empty")
	}

	verifyResp, err := client.MacVerify(ctx, &kmspb.MacVerifyRequest{Name: verName, Data: data, Mac: signResp.GetMac()})
	if err != nil {
		t.Fatalf("MacVerify: %v", err)
	}
	if !verifyResp.GetSuccess() {
		t.Fatal("MacVerify success = false, want true")
	}

	// Tampered data fails verification.
	badVerify, err := client.MacVerify(ctx, &kmspb.MacVerifyRequest{Name: verName, Data: []byte("tampered"), Mac: signResp.GetMac()})
	if err != nil {
		t.Fatalf("MacVerify tampered: %v", err)
	}
	if badVerify.GetSuccess() {
		t.Fatal("MacVerify tampered success = true, want false")
	}
}

func TestKMSDestroyVersion(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: keyRing, CryptoKeyId: "sym"}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}

	verName := symKey + "/cryptoKeyVersions/1"

	destroyed, err := client.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: verName})
	if err != nil {
		t.Fatalf("DestroyCryptoKeyVersion: %v", err)
	}
	if destroyed.GetState() != kmspb.CryptoKeyVersion_DESTROYED {
		t.Fatalf("DestroyCryptoKeyVersion state = %v, want DESTROYED", destroyed.GetState())
	}

	// Restore brings it back to DISABLED (GCP semantics).
	restored, err := client.RestoreCryptoKeyVersion(ctx, &kmspb.RestoreCryptoKeyVersionRequest{Name: verName})
	if err != nil {
		t.Fatalf("RestoreCryptoKeyVersion: %v", err)
	}
	if restored.GetState() != kmspb.CryptoKeyVersion_DISABLED {
		t.Fatalf("RestoreCryptoKeyVersion state = %v, want DISABLED", restored.GetState())
	}

	// UpdateCryptoKeyVersion moves it back to ENABLED.
	enabled, err := client.UpdateCryptoKeyVersion(ctx, &kmspb.UpdateCryptoKeyVersionRequest{
		CryptoKeyVersion: &kmspb.CryptoKeyVersion{Name: verName, State: kmspb.CryptoKeyVersion_ENABLED},
	})
	if err != nil {
		t.Fatalf("UpdateCryptoKeyVersion: %v", err)
	}
	if enabled.GetState() != kmspb.CryptoKeyVersion_ENABLED {
		t.Fatalf("UpdateCryptoKeyVersion state = %v, want ENABLED", enabled.GetState())
	}
}

func TestKMSKeyRingIamPolicy(t *testing.T) {
	client, iam, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")

	// Empty policy by default.
	pol, err := iam.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: keyRing})
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	if len(pol.GetBindings()) != 0 {
		t.Fatalf("GetIamPolicy bindings = %v, want empty", pol.GetBindings())
	}

	// Set IAM policy.
	if _, err := iam.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: keyRing,
		Policy: &iampb.Policy{
			Bindings: []*iampb.Binding{{Role: "roles/cloudkms.cryptoKeyEncrypter", Members: []string{"allUsers"}}},
		},
	}); err != nil {
		t.Fatalf("SetIamPolicy: %v", err)
	}

	pol, err = iam.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: keyRing})
	if err != nil {
		t.Fatalf("GetIamPolicy after set: %v", err)
	}
	if len(pol.GetBindings()) != 1 || pol.GetBindings()[0].GetRole() != "roles/cloudkms.cryptoKeyEncrypter" {
		t.Fatalf("GetIamPolicy bindings = %v, want roles/cloudkms.cryptoKeyEncrypter", pol.GetBindings())
	}

	// Test permissions echoes the request (no-authz posture).
	tp, err := iam.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{
		Resource: keyRing, Permissions: []string{"cloudkms.cryptoKeys.get", "cloudkms.cryptoKeys.create"},
	})
	if err != nil {
		t.Fatalf("TestIamPermissions: %v", err)
	}
	if len(tp.GetPermissions()) != 2 {
		t.Fatalf("TestIamPermissions = %v, want 2 granted", tp.GetPermissions())
	}

	// IAM on a missing keyring → NotFound.
	if _, err := iam.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{
		Resource: "projects/test/locations/global/keyRings/missing",
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetIamPolicy missing err = %v, want NotFound", err)
	}
}

func TestKMSGenerateRandomBytes(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()

	resp, err := client.GenerateRandomBytes(ctx, &kmspb.GenerateRandomBytesRequest{
		Location:        "projects/test/locations/us-central1",
		LengthBytes:     32,
		ProtectionLevel: kmspb.ProtectionLevel_SOFTWARE,
	})
	if err != nil {
		t.Fatalf("GenerateRandomBytes: %v", err)
	}
	if len(resp.GetData()) != 32 {
		t.Fatalf("GenerateRandomBytes data length = %d, want 32", len(resp.GetData()))
	}
	// Two calls must not produce identical bytes.
	resp2, err := client.GenerateRandomBytes(ctx, &kmspb.GenerateRandomBytesRequest{
		Location: "projects/test/locations/us-central1", LengthBytes: 32,
	})
	if err != nil {
		t.Fatalf("GenerateRandomBytes (2): %v", err)
	}
	if string(resp.GetData()) == string(resp2.GetData()) {
		t.Fatal("GenerateRandomBytes returned identical bytes across calls")
	}

	// Negative length is rejected.
	if _, err := client.GenerateRandomBytes(ctx, &kmspb.GenerateRandomBytesRequest{
		Location: "projects/test/locations/us-central1", LengthBytes: 0,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GenerateRandomBytes length 0 err = %v, want InvalidArgument", err)
	}
}
