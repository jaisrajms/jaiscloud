package grpcconformance

import (
	"context"
	"fmt"

	"cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// kmsChecks covers the Cloud KMS surface
// (google.cloud.kms.v1.KeyManagementService) via the official KMS client.
//
// Cloud KMS has no DeleteKeyRing/DeleteCryptoKey RPC (keys are scheduled for
// destruction, not deleted), so the delete-side probe uses
// DestroyCryptoKeyVersion, which is the real API's delete analogue. Destroying
// a live version must report DESTROY_SCHEDULED with a destroy_time (the
// emulator promotes to DESTROYED lazily once the window elapses); DESTROYED is
// tolerated only for a version that was already promoted, so the probe remains
// safe if it re-runs against the same version.
func kmsChecks() []Check {
	checks := []Check{
		{Service: "kms", RPC: "CreateKeyRing", KeyField: "success", Run: checkKMSCreateKeyRing},
		{Service: "kms", RPC: "GetKeyRing", KeyField: "name", Run: checkKMSGetKeyRing},
		{Service: "kms", RPC: "ListKeyRings", KeyField: "keyRings[].name", Run: checkKMSListKeyRings},
		{Service: "kms", RPC: "CreateCryptoKey", KeyField: "name/purpose", Run: checkKMSCreateCryptoKey},
		{Service: "kms", RPC: "GetCryptoKey", KeyField: "name/primary", Run: checkKMSGetCryptoKey},
		{Service: "kms", RPC: "DestroyCryptoKeyVersion", KeyField: "name/state=destruction", Run: checkKMSDestroyCryptoKeyVersion},
	}
	return append(checks, kmsExtraChecks()...)
}

func newKMSClient(ctx context.Context, cfg Config) (*kms.KeyManagementClient, error) {
	return kms.NewKeyManagementClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

func kmsParent(cfg Config) string {
	return fmt.Sprintf("projects/%s/locations/global", cfg.Project)
}

func kmsRingName(cfg Config) string {
	return kmsParent(cfg) + "/keyRings/" + cfg.ResourceName("gcpc-grpc-ring")
}

func kmsKeyName(cfg Config) string {
	return kmsRingName(cfg) + "/cryptoKeys/" + cfg.ResourceName("gcpc-grpc-key")
}

func checkKMSCreateKeyRing(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	kr, err := client.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{
		Parent:    kmsParent(cfg),
		KeyRingId: cfg.ResourceName("gcpc-grpc-ring"),
	})
	if err != nil {
		return err
	}
	if kr.GetName() != kmsRingName(cfg) {
		return fmt.Errorf("CreateKeyRing returned name %q, want %q", kr.GetName(), kmsRingName(cfg))
	}
	return nil
}

func checkKMSGetKeyRing(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	kr, err := client.GetKeyRing(ctx, &kmspb.GetKeyRingRequest{Name: kmsRingName(cfg)})
	if err != nil {
		return err
	}
	if kr.GetName() != kmsRingName(cfg) {
		return fmt.Errorf("GetKeyRing returned name %q, want %q", kr.GetName(), kmsRingName(cfg))
	}
	return nil
}

func checkKMSListKeyRings(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	it := client.ListKeyRings(ctx, &kmspb.ListKeyRingsRequest{Parent: kmsParent(cfg)})
	for {
		kr, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		if kr.GetName() == kmsRingName(cfg) {
			return nil
		}
	}
	return fmt.Errorf("ListKeyRings did not include %q", kmsRingName(cfg))
}

func checkKMSCreateCryptoKey(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	key, err := client.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent:      kmsRingName(cfg),
		CryptoKeyId: cfg.ResourceName("gcpc-grpc-key"),
		CryptoKey:   &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
	})
	if err != nil {
		return err
	}
	if key.GetName() != kmsKeyName(cfg) {
		return fmt.Errorf("CreateCryptoKey returned name %q, want %q", key.GetName(), kmsKeyName(cfg))
	}
	if key.GetPurpose() != kmspb.CryptoKey_ENCRYPT_DECRYPT {
		return fmt.Errorf("CreateCryptoKey purpose = %v, want ENCRYPT_DECRYPT", key.GetPurpose())
	}
	return nil
}

func checkKMSGetCryptoKey(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	key, err := client.GetCryptoKey(ctx, &kmspb.GetCryptoKeyRequest{Name: kmsKeyName(cfg)})
	if err != nil {
		return err
	}
	if key.GetName() != kmsKeyName(cfg) {
		return fmt.Errorf("GetCryptoKey returned name %q, want %q", key.GetName(), kmsKeyName(cfg))
	}
	if key.GetPrimary() == nil {
		return fmt.Errorf("GetCryptoKey returned no primary version")
	}
	return nil
}

func checkKMSDestroyCryptoKeyVersion(ctx context.Context, cfg Config) error {
	client, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	version := kmsKeyName(cfg) + "/cryptoKeyVersions/1"
	destroyed, err := client.DestroyCryptoKeyVersion(ctx, &kmspb.DestroyCryptoKeyVersionRequest{Name: version})
	if err != nil {
		return err
	}
	if destroyed.GetName() != version {
		return fmt.Errorf("DestroyCryptoKeyVersion returned name %q, want %q", destroyed.GetName(), version)
	}
	switch destroyed.GetState() {
	case kmspb.CryptoKeyVersion_DESTROY_SCHEDULED:
		if destroyed.GetDestroyTime() == nil {
			return fmt.Errorf("DESTROY_SCHEDULED version returned no destroy_time")
		}
		return nil
	case kmspb.CryptoKeyVersion_DESTROYED:
		// The version was already promoted before this probe ran.
		return nil
	default:
		return fmt.Errorf("DestroyCryptoKeyVersion state = %v, want DESTROY_SCHEDULED or DESTROYED", destroyed.GetState())
	}
}
