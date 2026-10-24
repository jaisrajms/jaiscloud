package grpcconformance

import (
	"context"
	"fmt"
	"path"

	"cloud.google.com/go/kms/apiv1/kmspb"
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// secretManagerChecks covers the Secret Manager surface
// (google.cloud.secretmanager.v1.SecretManagerService) via the official client.
func secretManagerChecks() []Check {
	return []Check{
		{Service: "secretmanager", RPC: "CreateSecret", KeyField: "success", Run: checkSecretCreate},
		{Service: "secretmanager", RPC: "GetSecret", KeyField: "name", Run: checkSecretGet},
		{Service: "secretmanager", RPC: "GetSecret", KeyField: "replication.automatic.customerManagedEncryption.kmsKeyName", Run: checkSecretCMEKReplication},
		{Service: "secretmanager", RPC: "GetSecret", KeyField: "replication.userManaged.replicas[].location", Run: checkSecretUserManagedReplication},
		{Service: "secretmanager", RPC: "AddSecretVersion", KeyField: "replicationStatus.automatic.customerManagedEncryption.kmsKeyVersionName", Run: checkSecretVersionCMEKStatus},
		{Service: "secretmanager", RPC: "ListSecrets", KeyField: "secrets[].name", Run: checkSecretList},
		{Service: "secretmanager", RPC: "AddSecretVersion", KeyField: "name", Run: checkSecretAddVersion},
		{Service: "secretmanager", RPC: "AccessSecretVersion", KeyField: "payload.data", Run: checkSecretAccessVersion},
		{Service: "secretmanager", RPC: "DeleteSecret", KeyField: "success", Run: checkSecretDelete},
	}
}

const secretPayload = "grpc-conformance-secret-payload"

func newSecretClient(ctx context.Context, cfg Config) (*secretmanager.Client, error) {
	return secretmanager.NewClient(ctx,
		option.WithEndpoint(cfg.GRPCAddr()),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithoutAuthentication(),
	)
}

func secretProject(cfg Config) string { return "projects/" + cfg.Project }

func secretName(cfg Config) string {
	return secretProject(cfg) + "/secrets/" + cfg.ResourceName("gcpc-grpc-secret")
}

func checkSecretCreate(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	secret, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   secretProject(cfg),
		SecretId: cfg.ResourceName("gcpc-grpc-secret"),
		Secret: &secretmanagerpb.Secret{
			Replication: &secretmanagerpb.Replication{
				Replication: &secretmanagerpb.Replication_Automatic_{
					Automatic: &secretmanagerpb.Replication_Automatic{},
				},
			},
		},
	})
	if err != nil {
		return err
	}
	if secret.GetName() != secretName(cfg) {
		return fmt.Errorf("CreateSecret returned name %q, want %q", secret.GetName(), secretName(cfg))
	}
	return nil
}

func checkSecretGet(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	secret, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: secretName(cfg)})
	if err != nil {
		return err
	}
	if secret.GetName() != secretName(cfg) {
		return fmt.Errorf("GetSecret returned name %q, want %q", secret.GetName(), secretName(cfg))
	}
	return nil
}

// checkSecretCMEKReplication asserts that a Secret created with a
// customer-managed encryption key echoes the key back through the official
// client: real GCP returns the configured key under
// replication.automatic.customerManagedEncryption.kmsKeyName (DC31), not an
// empty automatic policy.
func checkSecretCMEKReplication(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-secret-cmek")
	name := secretProject(cfg) + "/secrets/" + id
	// CreateSecret resolves the CMEK key (DC58), so the key must exist. The
	// probe does not add a version, so nothing is wrapped.
	kmsClient, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new kms client: %w", err)
	}
	defer kmsClient.Close()
	ringID := cfg.ResourceName("gcpc-cmek")
	if _, err := kmsClient.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{
		Parent: kmsParent(cfg), KeyRingId: ringID, KeyRing: &kmspb.KeyRing{},
	}); err != nil {
		return fmt.Errorf("CreateKeyRing: %w", err)
	}
	createdKey, err := kmsClient.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent:      kmsParent(cfg) + "/keyRings/" + ringID,
		CryptoKeyId: "gcpc-cmek-key",
		CryptoKey:   &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
	})
	if err != nil {
		return fmt.Errorf("CreateCryptoKey: %w", err)
	}
	key := createdKey.GetName()

	created, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   secretProject(cfg),
		SecretId: id,
		Secret: &secretmanagerpb.Secret{
			Replication: &secretmanagerpb.Replication{
				Replication: &secretmanagerpb.Replication_Automatic_{
					Automatic: &secretmanagerpb.Replication_Automatic{
						CustomerManagedEncryption: &secretmanagerpb.CustomerManagedEncryption{KmsKeyName: key},
					},
				},
			},
		},
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = client.DeleteSecret(context.Background(), &secretmanagerpb.DeleteSecretRequest{Name: name})
	}()

	if got := created.GetReplication().GetAutomatic().GetCustomerManagedEncryption().GetKmsKeyName(); got != key {
		return fmt.Errorf("CreateSecret CMEK = %q, want %q", got, key)
	}
	got, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: name})
	if err != nil {
		return err
	}
	if gotKey := got.GetReplication().GetAutomatic().GetCustomerManagedEncryption().GetKmsKeyName(); gotKey != key {
		return fmt.Errorf("GetSecret CMEK = %q, want %q", gotKey, key)
	}
	return nil
}

// checkSecretUserManagedReplication asserts that a Secret created with a
// user-managed replication policy reads back exactly that policy through the
// official client — the oneof arm, every replica location and the per-replica
// CMEK key (DC45) — instead of the rewritten automatic policy the emulator used
// to return. The keys only have to be well-formed CryptoKey names in the
// replica's location: CreateSecret stores the policy without resolving them (the
// emulator does not validate key existence at create; recorded as DC58) and
// never wraps until a version is added, which this probe does not do.
func checkSecretUserManagedReplication(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-secret-um")
	name := secretProject(cfg) + "/secrets/" + id
	// CreateSecret resolves the replica CMEK key (DC58), so create it first.
	ringID := cfg.ResourceName("gcpc-um")
	keyID := cfg.ResourceName("gcpc-um-key")
	kmsClient, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new kms client: %w", err)
	}
	defer kmsClient.Close()
	keyParent := fmt.Sprintf("projects/%s/locations/us-east1", cfg.Project)
	if _, err := kmsClient.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{
		Parent: keyParent, KeyRingId: ringID, KeyRing: &kmspb.KeyRing{},
	}); err != nil {
		return fmt.Errorf("CreateKeyRing: %w", err)
	}
	if _, err := kmsClient.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent:      keyParent + "/keyRings/" + ringID,
		CryptoKeyId: keyID,
		CryptoKey:   &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
	}); err != nil {
		return fmt.Errorf("CreateCryptoKey: %w", err)
	}
	key := func(location string) string {
		return fmt.Sprintf("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s", cfg.Project, location, ringID, keyID)
	}
	replicas := []*secretmanagerpb.Replication_UserManaged_Replica{
		{Location: "us-east1", CustomerManagedEncryption: &secretmanagerpb.CustomerManagedEncryption{KmsKeyName: key("us-east1")}},
		{Location: "us-central1"},
	}

	created, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   secretProject(cfg),
		SecretId: id,
		Secret: &secretmanagerpb.Secret{
			Replication: &secretmanagerpb.Replication{
				Replication: &secretmanagerpb.Replication_UserManaged_{
					UserManaged: &secretmanagerpb.Replication_UserManaged{Replicas: replicas},
				},
			},
		},
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = client.DeleteSecret(context.Background(), &secretmanagerpb.DeleteSecretRequest{Name: name})
	}()

	verify := func(what string, sec *secretmanagerpb.Secret) error {
		if auto := sec.GetReplication().GetAutomatic(); auto != nil {
			return fmt.Errorf("%s returned the automatic arm, want userManaged", what)
		}
		got := sec.GetReplication().GetUserManaged().GetReplicas()
		if len(got) != len(replicas) {
			return fmt.Errorf("%s returned %d replicas, want %d", what, len(got), len(replicas))
		}
		for i, rep := range got {
			if rep.GetLocation() != replicas[i].GetLocation() {
				return fmt.Errorf("%s replica %d location = %q, want %q", what, i, rep.GetLocation(), replicas[i].GetLocation())
			}
			wantKey := replicas[i].GetCustomerManagedEncryption().GetKmsKeyName()
			if gotKey := rep.GetCustomerManagedEncryption().GetKmsKeyName(); gotKey != wantKey {
				return fmt.Errorf("%s replica %d CMEK = %q, want %q", what, i, gotKey, wantKey)
			}
		}
		return nil
	}
	if err := verify("CreateSecret", created); err != nil {
		return err
	}
	got, err := client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{Name: name})
	if err != nil {
		return err
	}
	return verify("GetSecret", got)
}

// checkSecretVersionCMEKStatus asserts that a version added to a CMEK secret
// reports the exact CryptoKeyVersion that wrapped its payload under
// replicationStatus.automatic.customerManagedEncryption.kmsKeyVersionName
// (DC44). It drives a real KMS CryptoKey so the wrap actually happens, rotates
// the key's primary version, and verifies the status is per-version: the older
// version keeps the CryptoKeyVersion that wrapped it.
func checkSecretVersionCMEKStatus(ctx context.Context, cfg Config) error {
	kmsClient, err := newKMSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new kms client: %w", err)
	}
	defer kmsClient.Close()

	ringID := cfg.ResourceName("gcpc-grpc-sm-cmek")
	// The key ring belongs to this probe, not to the KMS checks' fixture.
	if _, err := kmsClient.CreateKeyRing(ctx, &kmspb.CreateKeyRingRequest{
		Parent: kmsParent(cfg), KeyRingId: ringID, KeyRing: &kmspb.KeyRing{},
	}); err != nil {
		return fmt.Errorf("CreateKeyRing: %w", err)
	}
	key, err := kmsClient.CreateCryptoKey(ctx, &kmspb.CreateCryptoKeyRequest{
		Parent:      kmsParent(cfg) + "/keyRings/" + ringID,
		CryptoKeyId: "gcpc-sm-cmek-key",
		CryptoKey:   &kmspb.CryptoKey{Purpose: kmspb.CryptoKey_ENCRYPT_DECRYPT},
	})
	if err != nil {
		return fmt.Errorf("CreateCryptoKey: %w", err)
	}
	keyName := key.GetName()
	primaryVersion := path.Base(key.GetPrimary().GetName())
	if primaryVersion == "" || primaryVersion == "." {
		return fmt.Errorf("CreateCryptoKey returned no primary version")
	}

	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new secret client: %w", err)
	}
	defer client.Close()

	id := cfg.ResourceName("gcpc-grpc-secret-version-cmek")
	name := secretProject(cfg) + "/secrets/" + id
	if _, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   secretProject(cfg),
		SecretId: id,
		Secret: &secretmanagerpb.Secret{
			Replication: &secretmanagerpb.Replication{
				Replication: &secretmanagerpb.Replication_Automatic_{
					Automatic: &secretmanagerpb.Replication_Automatic{
						CustomerManagedEncryption: &secretmanagerpb.CustomerManagedEncryption{KmsKeyName: keyName},
					},
				},
			},
		},
	}); err != nil {
		return fmt.Errorf("CreateSecret: %w", err)
	}
	defer func() {
		_ = client.DeleteSecret(context.Background(), &secretmanagerpb.DeleteSecretRequest{Name: name})
	}()

	addVersion := func() (*secretmanagerpb.SecretVersion, error) {
		return client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
			Parent:  name,
			Payload: &secretmanagerpb.SecretPayload{Data: []byte(secretPayload)},
		})
	}
	keyVersionName := func(v *secretmanagerpb.SecretVersion) string {
		return v.GetReplicationStatus().GetAutomatic().GetCustomerManagedEncryption().GetKmsKeyVersionName()
	}

	// Secret Manager spells the key version `.../cryptoKeys/{key}/versions/{n}`
	// (not Cloud KMS's `.../cryptoKeyVersions/{n}`).
	wantVersion := func(n string) string { return keyName + "/versions/" + n }

	v1, err := addVersion()
	if err != nil {
		return fmt.Errorf("AddSecretVersion: %w", err)
	}
	if got, want := keyVersionName(v1), wantVersion(primaryVersion); got != want {
		return fmt.Errorf("version 1 kmsKeyVersionName = %q, want %q", got, want)
	}

	// Rotate the key's primary, then add a second version: the new version is
	// wrapped by the new primary, while version 1 keeps its own key version.
	next, err := kmsClient.CreateCryptoKeyVersion(ctx, &kmspb.CreateCryptoKeyVersionRequest{Parent: keyName})
	if err != nil {
		return fmt.Errorf("CreateCryptoKeyVersion: %w", err)
	}
	nextVersion := path.Base(next.GetName())
	if _, err := kmsClient.UpdateCryptoKeyPrimaryVersion(ctx, &kmspb.UpdateCryptoKeyPrimaryVersionRequest{
		Name: keyName, CryptoKeyVersionId: nextVersion,
	}); err != nil {
		return fmt.Errorf("UpdateCryptoKeyPrimaryVersion: %w", err)
	}
	v2, err := addVersion()
	if err != nil {
		return fmt.Errorf("AddSecretVersion after rotation: %w", err)
	}
	if got, want := keyVersionName(v2), wantVersion(nextVersion); got != want {
		return fmt.Errorf("version 2 kmsKeyVersionName = %q, want %q", got, want)
	}
	gotV1, err := client.GetSecretVersion(ctx, &secretmanagerpb.GetSecretVersionRequest{Name: v1.GetName()})
	if err != nil {
		return fmt.Errorf("GetSecretVersion: %w", err)
	}
	if got, want := keyVersionName(gotV1), wantVersion(primaryVersion); got != want {
		return fmt.Errorf("version 1 after rotation kmsKeyVersionName = %q, want %q", got, want)
	}
	return nil
}

func checkSecretList(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	it := client.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{Parent: secretProject(cfg)})
	for {
		secret, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		if secret.GetName() == secretName(cfg) {
			return nil
		}
	}
	return fmt.Errorf("ListSecrets did not include %q", secretName(cfg))
}

func checkSecretAddVersion(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	version, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent:  secretName(cfg),
		Payload: &secretmanagerpb.SecretPayload{Data: []byte(secretPayload)},
	})
	if err != nil {
		return err
	}
	if version.GetName() == "" {
		return fmt.Errorf("AddSecretVersion returned an empty name")
	}
	return nil
}

func checkSecretAccessVersion(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	resp, err := client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: secretName(cfg) + "/versions/1",
	})
	if err != nil {
		return err
	}
	if got := string(resp.GetPayload().GetData()); got != secretPayload {
		return fmt.Errorf("AccessSecretVersion payload = %q, want %q", got, secretPayload)
	}
	return nil
}

func checkSecretDelete(ctx context.Context, cfg Config) error {
	client, err := newSecretClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}
	defer client.Close()
	if err := client.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{Name: secretName(cfg)}); err != nil {
		return err
	}
	return nil
}
