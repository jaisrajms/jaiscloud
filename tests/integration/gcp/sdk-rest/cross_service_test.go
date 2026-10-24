package sdkrest_test

import (
	"context"
	"encoding/base64"
	"path"
	"testing"

	"google.golang.org/api/cloudkms/v1"
	"google.golang.org/api/pubsub/v1"
	"google.golang.org/api/secretmanager/v1"

	"github.com/stretchr/testify/require"
)

// TestCrossServiceCMEK validates the customer-managed encryption key
// (CMEK) feature end-to-end across services: a single Cloud KMS key is created
// by one "service", then reused by two other services to encrypt data — Secret
// Manager (at rest) and Pub/Sub (topic-level) — and a consumer reads/decrypts
// both. This proves the shared key is not isolated to a single service.
func TestCrossServiceCMEK(t *testing.T) {
	ctx := context.Background()

	// 1. KMS — the key-owning service.
	kms, err := cloudkms.NewService(ctx, opts()...)
	require.NoError(t, err)

	parent := "projects/proj/locations/global"
	krID := unique("kr")
	_, err = kms.Projects.Locations.KeyRings.Create(parent, &cloudkms.KeyRing{}).KeyRingId(krID).Do()
	require.NoError(t, err)

	krName := parent + "/keyRings/" + krID
	keyID := unique("k")
	ck, err := kms.Projects.Locations.KeyRings.CryptoKeys.Create(krName, &cloudkms.CryptoKey{
		Purpose: "ENCRYPT_DECRYPT",
	}).CryptoKeyId(keyID).Do()
	require.NoError(t, err)
	require.NotEmpty(t, ck.Name, "cryptoKey create must return the full resource name")
	kmsKeyName := ck.Name

	// 2. Secret Manager — a secrets service encrypting with the shared key.
	sm, err := secretmanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	secret, err := sm.Projects.Secrets.Create("projects/proj", &secretmanager.Secret{
		Replication: &secretmanager.Replication{
			Automatic: &secretmanager.Automatic{
				CustomerManagedEncryption: &secretmanager.CustomerManagedEncryption{
					KmsKeyName: kmsKeyName,
				},
			},
		},
	}).SecretId(unique("s")).Do()
	require.NoError(t, err)
	require.NotEmpty(t, secret.Name)

	// DC31: the Secret response echoes the configured CMEK key, so a client can
	// read back what it set instead of seeing an empty automatic policy (the
	// value also drives Terraform's computed kms_key_name and the console chip).
	secretCMEK := func(s *secretmanager.Secret) string {
		if s.Replication == nil || s.Replication.Automatic == nil ||
			s.Replication.Automatic.CustomerManagedEncryption == nil {
			return ""
		}
		return s.Replication.Automatic.CustomerManagedEncryption.KmsKeyName
	}
	require.Equal(t, kmsKeyName, secretCMEK(secret), "create must echo the CMEK key")

	readBack, err := sm.Projects.Secrets.Get(secret.Name).Do()
	require.NoError(t, err)
	require.Equal(t, kmsKeyName, secretCMEK(readBack), "get must echo the CMEK key")

	// DC44: the version reports the exact CryptoKeyVersion that wrapped its
	// payload, under replicationStatus.automatic.customerManagedEncryption
	// (Secret Manager spells it .../cryptoKeys/{key}/versions/{n}).
	keyVersionFrom := func(v *secretmanager.SecretVersion) string {
		if v.ReplicationStatus == nil || v.ReplicationStatus.Automatic == nil ||
			v.ReplicationStatus.Automatic.CustomerManagedEncryption == nil {
			return ""
		}
		return v.ReplicationStatus.Automatic.CustomerManagedEncryption.KmsKeyVersionName
	}
	keyVersionName := func(keyName string, primary *cloudkms.CryptoKeyVersion) string {
		require.NotNil(t, primary, "cryptoKey create must return its primary version")
		require.NotEmpty(t, primary.Name, "primary version must carry its resource name")
		return keyName + "/versions/" + path.Base(primary.Name)
	}

	v1, err := sm.Projects.Secrets.AddVersion(secret.Name, &secretmanager.AddSecretVersionRequest{
		Payload: &secretmanager.SecretPayload{Data: "aGk="},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, keyVersionName(kmsKeyName, ck.Primary), keyVersionFrom(v1),
		"add version must report the CryptoKeyVersion that wrapped it")

	acc, err := sm.Projects.Secrets.Versions.Access(secret.Name + "/versions/1").Do()
	require.NoError(t, err)
	decoded, err := base64.StdEncoding.DecodeString(acc.Payload.Data)
	require.NoError(t, err)
	require.Equal(t, "hi", string(decoded), "secret payload must decrypt to the plaintext")

	// Patching the replication CMEK is visible on the response and applies to
	// versions added afterwards (the documented semantics), while the existing
	// version keeps the key it was wrapped with.
	ck2, err := kms.Projects.Locations.KeyRings.CryptoKeys.Create(krName, &cloudkms.CryptoKey{
		Purpose: "ENCRYPT_DECRYPT",
	}).CryptoKeyId(unique("k2")).Do()
	require.NoError(t, err)
	patched, err := sm.Projects.Secrets.Patch(secret.Name, &secretmanager.Secret{
		Replication: &secretmanager.Replication{
			Automatic: &secretmanager.Automatic{
				CustomerManagedEncryption: &secretmanager.CustomerManagedEncryption{KmsKeyName: ck2.Name},
			},
		},
	}).UpdateMask("replication").Do()
	require.NoError(t, err)
	require.Equal(t, ck2.Name, secretCMEK(patched), "patch must echo the new CMEK key")

	gotV1, err := sm.Projects.Secrets.Versions.Get(secret.Name + "/versions/1").Do()
	require.NoError(t, err)
	require.Equal(t, keyVersionName(kmsKeyName, ck.Primary), keyVersionFrom(gotV1),
		"an earlier version keeps the CryptoKeyVersion that wrapped it")

	_, err = sm.Projects.Secrets.AddVersion(secret.Name, &secretmanager.AddSecretVersionRequest{
		Payload: &secretmanager.SecretPayload{Data: "aGk="},
	}).Do()
	require.NoError(t, err)
	acc2, err := sm.Projects.Secrets.Versions.Access(secret.Name + "/versions/2").Do()
	require.NoError(t, err)
	decoded, err = base64.StdEncoding.DecodeString(acc2.Payload.Data)
	require.NoError(t, err)
	require.Equal(t, "hi", string(decoded), "version added after the patch must decrypt under the new key")

	// The version added after the patch reports the new key's wrapping version,
	// while version 1 still reports the old key's (per-version status).
	v2, err := sm.Projects.Secrets.Versions.Get(secret.Name + "/versions/2").Do()
	require.NoError(t, err)
	require.Equal(t, keyVersionName(ck2.Name, ck2.Primary), keyVersionFrom(v2),
		"version added after the patch must report the new key's CryptoKeyVersion")

	// 3. Pub/Sub — a messaging service encrypting with the same shared key.
	ps, err := pubsub.NewService(ctx, opts()...)
	require.NoError(t, err)

	topicName := "projects/proj/topics/" + unique("t")
	_, err = ps.Projects.Topics.Create(topicName, &pubsub.Topic{Name: topicName, KmsKeyName: kmsKeyName}).Do()
	require.NoError(t, err)

	subName := "projects/proj/subscriptions/" + unique("s")
	_, err = ps.Projects.Subscriptions.Create(subName, &pubsub.Subscription{Name: subName, Topic: topicName}).Do()
	require.NoError(t, err)

	_, err = ps.Projects.Topics.Publish(topicName, &pubsub.PublishRequest{
		Messages: []*pubsub.PubsubMessage{{Data: "aGk="}},
	}).Do()
	require.NoError(t, err)

	pull, err := ps.Projects.Subscriptions.Pull(subName, &pubsub.PullRequest{MaxMessages: 10}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, pull.ReceivedMessages)
	require.Equal(t, "aGk=", pull.ReceivedMessages[0].Message.Data, "pubsub message must decrypt to the published data")
}
