// Package sdkrest_test exercises the jaiscloud-gcp emulator's core messaging/security services
// (Pub/Sub, Secret Manager, KMS, IAM) through the official Google REST API
// clients. This validates wire-level parity with the real SDKs.
//
// Run with the GCP binary running and GCP_EMULATOR_ENDPOINT set:
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test -race ./...
//
// The high-level cloud.google.com/go/{pubsub,secretmanager,kms} clients speak
// gRPC, but the emulator is REST/JSON, so these tests use the REST apiary
// clients under google.golang.org/api instead.
package sdkrest_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"google.golang.org/api/cloudkms/v1"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
	"google.golang.org/api/pubsub/v1"
	"google.golang.org/api/secretmanager/v1"

	"github.com/stretchr/testify/require"
)

func endpoint() string {
	if e := os.Getenv("GCP_EMULATOR_ENDPOINT"); e != "" {
		return e
	}
	return "http://localhost:8080/"
}

func opts() []option.ClientOption {
	return []option.ClientOption{option.WithEndpoint(endpoint()), option.WithoutAuthentication()}
}

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// TestSDKPubSub covers topic create/publish, subscription create/pull, and list
// pagination through the REST client.
func TestSDKPubSub(t *testing.T) {
	ctx := context.Background()
	svc, err := pubsub.NewService(ctx, opts()...)
	require.NoError(t, err)

	topicName := "projects/proj/topics/" + unique("t")
	_, err = svc.Projects.Topics.Create(topicName, &pubsub.Topic{Name: topicName}).Do()
	require.NoError(t, err)

	// Create the subscription before publishing: Pub/Sub only delivers messages
	// published after the subscription exists.
	subName := "projects/proj/subscriptions/" + unique("s")
	_, err = svc.Projects.Subscriptions.Create(subName, &pubsub.Subscription{
		Name:  subName,
		Topic: topicName,
	}).Do()
	require.NoError(t, err)

	// Publish.
	pub, err := svc.Projects.Topics.Publish(topicName, &pubsub.PublishRequest{
		Messages: []*pubsub.PubsubMessage{{Data: "aGk="}},
	}).Do()
	require.NoError(t, err)
	require.Len(t, pub.MessageIds, 1)

	// Pull + ack.
	pull, err := svc.Projects.Subscriptions.Pull(subName, &pubsub.PullRequest{MaxMessages: 10}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, pull.ReceivedMessages)
	require.Equal(t, "aGk=", pull.ReceivedMessages[0].Message.Data)

	_, err = svc.Projects.Subscriptions.Acknowledge(subName, &pubsub.AcknowledgeRequest{
		AckIds: []string{pull.ReceivedMessages[0].AckId},
	}).Do()
	require.NoError(t, err)

	// List topics with pagination.
	list, err := svc.Projects.Topics.List("projects/proj").PageSize(1).Do()
	require.NoError(t, err)
	require.NotEmpty(t, list.Topics)
}

// TestSDKSecretManager covers secret create, addVersion, and access.
func TestSDKSecretManager(t *testing.T) {
	ctx := context.Background()
	svc, err := secretmanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	secretID := unique("s")
	secret, err := svc.Projects.Secrets.Create("projects/proj", &secretmanager.Secret{
		Replication: &secretmanager.Replication{Automatic: &secretmanager.Automatic{}},
	}).SecretId(secretID).Do()
	require.NoError(t, err)
	require.NotEmpty(t, secret.Name)

	// Add a version with payload.
	_, err = svc.Projects.Secrets.AddVersion(secret.Name, &secretmanager.AddSecretVersionRequest{
		Payload: &secretmanager.SecretPayload{Data: "aGVsbG8="},
	}).Do()
	require.NoError(t, err)

	// Access version 1 (payload + dataCrc32c).
	acc, err := svc.Projects.Secrets.Versions.Access(secret.Name + "/versions/1").Do()
	require.NoError(t, err)
	require.Equal(t, "aGVsbG8=", acc.Payload.Data)
	require.NotEmpty(t, acc.Payload.DataCrc32c)
}

// TestSDKKMS covers keyring/cryptoKey create and encrypt/decrypt.
func TestSDKKMS(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudkms.NewService(ctx, opts()...)
	require.NoError(t, err)

	parent := "projects/proj/locations/global"
	krID := unique("kr")
	_, err = svc.Projects.Locations.KeyRings.Create(parent, &cloudkms.KeyRing{}).KeyRingId(krID).Do()
	require.NoError(t, err)

	krName := parent + "/keyRings/" + krID
	keyID := unique("k")
	_, err = svc.Projects.Locations.KeyRings.CryptoKeys.Create(krName, &cloudkms.CryptoKey{
		Purpose: "ENCRYPT_DECRYPT",
	}).CryptoKeyId(keyID).Do()
	require.NoError(t, err)

	keyName := krName + "/cryptoKeys/" + keyID

	enc, err := svc.Projects.Locations.KeyRings.CryptoKeys.Encrypt(keyName, &cloudkms.EncryptRequest{
		Plaintext: "aGVsbG8=",
	}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, enc.Ciphertext)
	require.NotEmpty(t, enc.CiphertextCrc32c)
	require.NotEmpty(t, enc.ProtectionLevel)
	require.True(t, enc.VerifiedPlaintextCrc32c)

	dec, err := svc.Projects.Locations.KeyRings.CryptoKeys.Decrypt(keyName, &cloudkms.DecryptRequest{
		Ciphertext: enc.Ciphertext,
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "aGVsbG8=", dec.Plaintext)
	require.NotEmpty(t, dec.PlaintextCrc32c)
	require.True(t, dec.UsedPrimary)
}

// TestSDKIAM covers service account create/list/get.
func TestSDKIAM(t *testing.T) {
	ctx := context.Background()
	svc, err := iam.NewService(ctx, opts()...)
	require.NoError(t, err)

	accountID := unique("sa")
	sa, err := svc.Projects.ServiceAccounts.Create("projects/proj", &iam.CreateServiceAccountRequest{
		AccountId:      accountID,
		ServiceAccount: &iam.ServiceAccount{DisplayName: "SDK SA"},
	}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, sa.Email)
	require.NotEmpty(t, sa.Etag)
	require.Equal(t, "SDK SA", sa.DisplayName)

	// Get by name.
	got, err := svc.Projects.ServiceAccounts.Get(sa.Name).Do()
	require.NoError(t, err)
	require.Equal(t, sa.Email, got.Email)

	// List with pagination.
	list, err := svc.Projects.ServiceAccounts.List("projects/proj").PageSize(1).Do()
	require.NoError(t, err)
	require.NotEmpty(t, list.Accounts)
}

// TestSDKIAMPolicy covers service account getIamPolicy/setIamPolicy (including
// the etag optimistic-concurrency-control round-trip) through the REST client.
func TestSDKIAMPolicy(t *testing.T) {
	ctx := context.Background()
	svc, err := iam.NewService(ctx, opts()...)
	require.NoError(t, err)

	accountID := unique("sa")
	sa, err := svc.Projects.ServiceAccounts.Create("projects/proj", &iam.CreateServiceAccountRequest{
		AccountId: accountID,
	}).Do()
	require.NoError(t, err)

	// Default policy carries an etag.
	pol, err := svc.Projects.ServiceAccounts.GetIamPolicy(sa.Name).Do()
	require.NoError(t, err)
	require.NotEmpty(t, pol.Etag)

	// setIamPolicy with the matching etag (OCC) → 200 and the policy is stored.
	set, err := svc.Projects.ServiceAccounts.SetIamPolicy(sa.Name, &iam.SetIamPolicyRequest{
		Policy: &iam.Policy{
			Etag:     pol.Etag,
			Bindings: []*iam.Binding{{Role: "roles/iam.serviceAccountUser", Members: []string{"allUsers"}}},
		},
	}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, set.Etag)

	// getIamPolicy reflects the stored bindings.
	pol, err = svc.Projects.ServiceAccounts.GetIamPolicy(sa.Name).Do()
	require.NoError(t, err)
	require.Len(t, pol.Bindings, 1)
	require.Equal(t, "roles/iam.serviceAccountUser", pol.Bindings[0].Role)

	// A stale etag must be rejected with 409.
	_, err = svc.Projects.ServiceAccounts.SetIamPolicy(sa.Name, &iam.SetIamPolicyRequest{
		Policy: &iam.Policy{Etag: "BOGUS=", Bindings: []*iam.Binding{{Role: "roles/viewer", Members: []string{"allUsers"}}}},
	}).Do()
	require.Error(t, err)
}

// TestSDKPubSubDLQ verifies deadLetterPolicy over the wire: a message is
// delivered maxDeliveryAttempts times, then moved to the dead-letter topic.
func TestSDKPubSubDLQ(t *testing.T) {
	ctx := context.Background()
	svc, err := pubsub.NewService(ctx, opts()...)
	require.NoError(t, err)

	src := "projects/proj/topics/" + unique("src")
	dlq := "projects/proj/topics/" + unique("dlq")
	_, err = svc.Projects.Topics.Create(src, &pubsub.Topic{Name: src}).Do()
	require.NoError(t, err)
	_, err = svc.Projects.Topics.Create(dlq, &pubsub.Topic{Name: dlq}).Do()
	require.NoError(t, err)

	sub := "projects/proj/subscriptions/" + unique("sub")
	_, err = svc.Projects.Subscriptions.Create(sub, &pubsub.Subscription{
		Name:  sub,
		Topic: src,
		DeadLetterPolicy: &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     dlq,
			MaxDeliveryAttempts: 5,
		},
	}).Do()
	require.NoError(t, err)

	_, err = svc.Projects.Topics.Publish(src, &pubsub.PublishRequest{
		Messages: []*pubsub.PubsubMessage{{Data: "aGk="}},
	}).Do()
	require.NoError(t, err)

	pull := func() (int, []string) {
		r, err := svc.Projects.Subscriptions.Pull(sub, &pubsub.PullRequest{MaxMessages: 10}).Do()
		require.NoError(t, err)
		ids := make([]string, 0, len(r.ReceivedMessages))
		for _, m := range r.ReceivedMessages {
			ids = append(ids, m.AckId)
		}
		return len(r.ReceivedMessages), ids
	}
	// forceRedelivery makes claimed messages immediately visible again (ack
	// deadline = 0), simulating expiry without sleeping.
	forceRedelivery := func(ids []string) {
		_, err = svc.Projects.Subscriptions.ModifyAckDeadline(sub, &pubsub.ModifyAckDeadlineRequest{
			AckIds: ids, AckDeadlineSeconds: 0,
		}).Do()
		require.NoError(t, err)
	}

	// Delivered maxDeliveryAttempts (5) times, then moved to the DLQ.
	for i := 0; i < 5; i++ {
		n, ids := pull()
		require.Equal(t, 1, n)
		forceRedelivery(ids)
	}
	n, _ := pull()
	require.Equal(t, 0, n)
}
