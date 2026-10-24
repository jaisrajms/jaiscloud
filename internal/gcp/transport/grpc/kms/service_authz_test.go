package kms

import (
	"context"
	"testing"

	iampb "cloud.google.com/go/iam/apiv1/iampb"
	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestKMSCryptoKeyPolicyEnforcedGRPC mirrors the REST provider test over gRPC:
// the cryptoKey resource-policy hook is default-permissive, a policy scoped to a
// role without crypto use denies with PermissionDenied, and a matching role
// allows.
func TestKMSCryptoKeyPolicyEnforcedGRPC(t *testing.T) {
	client, iam, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()

	const kr = "projects/test/locations/global/keyRings/authzkr"
	if _, err := client.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{
		Parent: "projects/test/locations/global", KeyRingId: "authzkr",
	}); err != nil {
		t.Fatalf("CreateKeyRing: %v", err)
	}
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: kr, CryptoKeyId: "sym"}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}
	key := kr + "/cryptoKeys/sym"

	encrypt := func() error {
		_, err := client.Encrypt(ctx, &kmspb.EncryptRequest{Name: key, Plaintext: []byte("hello")})
		return err
	}
	setRole := func(role string) {
		t.Helper()
		if _, err := iam.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
			Resource: key,
			Policy: &iampb.Policy{Bindings: []*iampb.Binding{
				{Role: role, Members: []string{"user:caller@example.com"}},
			}},
		}); err != nil {
			t.Fatalf("SetIamPolicy(%s): %v", role, err)
		}
	}

	// No policy → default-permissive.
	if err := encrypt(); err != nil {
		t.Fatalf("no-policy Encrypt: %v", err)
	}

	// A role without crypto use denies.
	setRole("roles/cloudkms.viewer")
	if err := encrypt(); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("viewer-only Encrypt err = %v, want PermissionDenied", err)
	}

	// The matching role allows.
	setRole("roles/cloudkms.cryptoKeyEncrypterDecrypter")
	if err := encrypt(); err != nil {
		t.Fatalf("encrypterDecrypter Encrypt: %v", err)
	}
}
