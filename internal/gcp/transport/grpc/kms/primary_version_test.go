package kms

import (
	"context"
	"testing"

	kmspb "cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestUpdateCryptoKeyPrimaryVersionRejectsNonSymmetricKey pins the gRPC answer
// real Cloud KMS gives for an ASYMMETRIC_SIGN key: FAILED_PRECONDITION with
// "Keys with purpose ASYMMETRIC_SIGN do not have a primary version."
// (verified against real GCP, parity-diff-jaiscloud, 2026-11-11). REST and gRPC
// share the core check, so both transports must answer alike.
func TestUpdateCryptoKeyPrimaryVersionRejectsNonSymmetricKey(t *testing.T) {
	client, _, cleanup := kmsTestService(t)
	defer cleanup()
	ctx := context.Background()
	createKeyRing(t, client, "kr")

	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent:      keyRing,
		CryptoKeyId: "signer",
		CryptoKey:   &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ASYMMETRIC_SIGN},
	}); err != nil {
		t.Fatalf("CreateCryptoKey: %v", err)
	}

	_, err := client.UpdateCryptoKeyPrimaryVersion(ctx, &kmspb.UpdateCryptoKeyPrimaryVersionRequest{
		Name:               keyRing + "/cryptoKeys/signer",
		CryptoKeyVersionId: "1",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("UpdateCryptoKeyPrimaryVersion err = %v, want FailedPrecondition", err)
	}
	if want := "Keys with purpose ASYMMETRIC_SIGN do not have a primary version."; status.Convert(err).Message() != want {
		t.Fatalf("message = %q, want %q", status.Convert(err).Message(), want)
	}

	// The symmetric path is unchanged.
	if _, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{Parent: keyRing, CryptoKeyId: "sym"}); err != nil {
		t.Fatalf("CreateCryptoKey sym: %v", err)
	}
	if _, err := client.UpdateCryptoKeyPrimaryVersion(ctx, &kmspb.UpdateCryptoKeyPrimaryVersionRequest{
		Name:               keyRing + "/cryptoKeys/sym",
		CryptoKeyVersionId: "1",
	}); err != nil {
		t.Fatalf("UpdateCryptoKeyPrimaryVersion sym: %v", err)
	}
}
