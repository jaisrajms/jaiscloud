package sdkrest_test

import (
	"context"
	"encoding/base64"
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

	_, err = sm.Projects.Secrets.AddVersion(secret.Name, &secretmanager.AddSecretVersionRequest{
		Payload: &secretmanager.SecretPayload{Data: "aGk="},
	}).Do()
	require.NoError(t, err)

	acc, err := sm.Projects.Secrets.Versions.Access(secret.Name + "/versions/1").Do()
	require.NoError(t, err)
	decoded, err := base64.StdEncoding.DecodeString(acc.Payload.Data)
	require.NoError(t, err)
	require.Equal(t, "hi", string(decoded), "secret payload must decrypt to the plaintext")

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
