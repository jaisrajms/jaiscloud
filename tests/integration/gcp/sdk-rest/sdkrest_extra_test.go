package sdkrest_test

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/api/cloudkms/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/pubsub/v1"
	"google.golang.org/api/secretmanager/v1"

	"github.com/stretchr/testify/require"
)

// b64 base64-encodes a string (the wire encoding for all GCP binary payloads).
func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// requireNotFound asserts err is a GCP 404 (googleapi.Error) rather than some
// other failure mode.
func requireNotFound(t *testing.T, err error) {
	t.Helper()
	var ae *googleapi.Error
	require.True(t, errors.As(err, &ae), "expected googleapi.Error, got %v", err)
	require.Equal(t, 404, ae.Code, "expected 404, got %d (%s)", ae.Code, ae.Message)
}

// TestSDKPubSubOrderingKeyAndAttributes publishes an ordered batch with custom
// attributes, then verifies FIFO delivery order, orderingKey, and attribute
// round-trip on pull.
func TestSDKPubSubOrderingKeyAndAttributes(t *testing.T) {
	ctx := context.Background()
	svc, err := pubsub.NewService(ctx, opts()...)
	require.NoError(t, err)

	topic := "projects/proj/topics/" + unique("ok")
	_, err = svc.Projects.Topics.Create(topic, &pubsub.Topic{Name: topic}).Do()
	require.NoError(t, err)

	// Subscribe before publishing (a subscription only receives later messages).
	sub := "projects/proj/subscriptions/" + unique("ok-sub")
	_, err = svc.Projects.Subscriptions.Create(sub, &pubsub.Subscription{Name: sub, Topic: topic}).Do()
	require.NoError(t, err)

	const n = 3
	// Publish one message per request so each gets a distinct publish time —
	// the emulator orders delivery by publish time, so a single batch (which
	// shares one timestamp) would make FIFO assertion non-deterministic.
	for i := 0; i < n; i++ {
		pub, err := svc.Projects.Topics.Publish(topic, &pubsub.PublishRequest{
			Messages: []*pubsub.PubsubMessage{{
				Data:        b64(string(rune('a' + i))),
				OrderingKey: "group-1",
				Attributes:  map[string]string{"seq": string(rune('0' + i))},
			}},
		}).Do()
		require.NoError(t, err)
		require.Len(t, pub.MessageIds, 1)
		time.Sleep(time.Millisecond)
	}

	// Ordering-key FIFO: only one message per key is in flight at a time, so
	// pull+ack one at a time and verify strict delivery order.
	for i := 0; i < n; i++ {
		pull, err := svc.Projects.Subscriptions.Pull(sub, &pubsub.PullRequest{MaxMessages: 10}).Do()
		require.NoError(t, err)
		require.Len(t, pull.ReceivedMessages, 1, "exactly one message per ordering-key group is delivered at a time")
		rm := pull.ReceivedMessages[0]
		require.Equal(t, "group-1", rm.Message.OrderingKey, "orderingKey must round-trip")
		require.Equal(t, b64(string(rune('a'+i))), rm.Message.Data, "FIFO order must be preserved")
		require.Equal(t, string(rune('0'+i)), rm.Message.Attributes["seq"])
		_, err = svc.Projects.Subscriptions.Acknowledge(sub, &pubsub.AcknowledgeRequest{
			AckIds: []string{rm.AckId},
		}).Do()
		require.NoError(t, err)
	}
}

// TestSDKPubSubModifyAckDeadlineRedelivery exercises ack-deadline reset: a
// claimed message whose deadline is dropped to 0 becomes immediately pullable
// again (redelivery without a dead-letter policy).
func TestSDKPubSubModifyAckDeadlineRedelivery(t *testing.T) {
	ctx := context.Background()
	svc, err := pubsub.NewService(ctx, opts()...)
	require.NoError(t, err)

	topic := "projects/proj/topics/" + unique("redel")
	_, err = svc.Projects.Topics.Create(topic, &pubsub.Topic{Name: topic}).Do()
	require.NoError(t, err)
	sub := "projects/proj/subscriptions/" + unique("redel-sub")
	_, err = svc.Projects.Subscriptions.Create(sub, &pubsub.Subscription{Name: sub, Topic: topic}).Do()
	require.NoError(t, err)

	_, err = svc.Projects.Topics.Publish(topic, &pubsub.PublishRequest{
		Messages: []*pubsub.PubsubMessage{{Data: b64("once")}},
	}).Do()
	require.NoError(t, err)

	pull, err := svc.Projects.Subscriptions.Pull(sub, &pubsub.PullRequest{MaxMessages: 1}).Do()
	require.NoError(t, err)
	require.Len(t, pull.ReceivedMessages, 1)
	first := pull.ReceivedMessages[0]
	require.Equal(t, b64("once"), first.Message.Data)
	require.Equal(t, int64(1), first.DeliveryAttempt)

	// Reset the ack deadline to 0 → the message is immediately visible again.
	_, err = svc.Projects.Subscriptions.ModifyAckDeadline(sub, &pubsub.ModifyAckDeadlineRequest{
		AckIds: []string{first.AckId}, AckDeadlineSeconds: 0,
	}).Do()
	require.NoError(t, err)

	pull, err = svc.Projects.Subscriptions.Pull(sub, &pubsub.PullRequest{MaxMessages: 1}).Do()
	require.NoError(t, err)
	require.Len(t, pull.ReceivedMessages, 1)
	require.Equal(t, b64("once"), pull.ReceivedMessages[0].Message.Data)
	require.Equal(t, int64(2), pull.ReceivedMessages[0].DeliveryAttempt, "redelivery must bump delivery attempt")

	_, err = svc.Projects.Subscriptions.Acknowledge(sub, &pubsub.AcknowledgeRequest{
		AckIds: []string{pull.ReceivedMessages[0].AckId},
	}).Do()
	require.NoError(t, err)
}

// TestSDKPubSubShortAckDeadline verifies a custom ackDeadlineSeconds is stored
// and read back on the subscription.
func TestSDKPubSubShortAckDeadline(t *testing.T) {
	ctx := context.Background()
	svc, err := pubsub.NewService(ctx, opts()...)
	require.NoError(t, err)

	topic := "projects/proj/topics/" + unique("deadline")
	_, err = svc.Projects.Topics.Create(topic, &pubsub.Topic{Name: topic}).Do()
	require.NoError(t, err)

	sub := "projects/proj/subscriptions/" + unique("deadline-sub")
	_, err = svc.Projects.Subscriptions.Create(sub, &pubsub.Subscription{
		Name: sub, Topic: topic, AckDeadlineSeconds: 5,
	}).Do()
	require.NoError(t, err)

	got, err := svc.Projects.Subscriptions.Get(sub).Do()
	require.NoError(t, err)
	require.Equal(t, int64(5), got.AckDeadlineSeconds)
}

// TestSDKPubSubDeleteNotFound verifies topic and subscription deletion, plus
// 404 on subsequent get.
func TestSDKPubSubDeleteNotFound(t *testing.T) {
	ctx := context.Background()
	svc, err := pubsub.NewService(ctx, opts()...)
	require.NoError(t, err)

	topic := "projects/proj/topics/" + unique("del")
	_, err = svc.Projects.Topics.Create(topic, &pubsub.Topic{Name: topic}).Do()
	require.NoError(t, err)

	// The subscription must be created while the topic still exists (Pub/Sub
	// rejects a subscription for a missing topic with NotFound).
	sub := "projects/proj/subscriptions/" + unique("del-sub")
	_, err = svc.Projects.Subscriptions.Create(sub, &pubsub.Subscription{Name: sub, Topic: topic}).Do()
	require.NoError(t, err)
	_, err = svc.Projects.Subscriptions.Delete(sub).Do()
	require.NoError(t, err)
	_, err = svc.Projects.Subscriptions.Get(sub).Do()
	requireNotFound(t, err)

	_, err = svc.Projects.Topics.Delete(topic).Do()
	require.NoError(t, err)
	_, err = svc.Projects.Topics.Get(topic).Do()
	requireNotFound(t, err)
}

// TestSDKPubSubTopicIAM exercises topic getIamPolicy/setIamPolicy round-trip.
func TestSDKPubSubTopicIAM(t *testing.T) {
	ctx := context.Background()
	svc, err := pubsub.NewService(ctx, opts()...)
	require.NoError(t, err)

	topic := "projects/proj/topics/" + unique("iam")
	_, err = svc.Projects.Topics.Create(topic, &pubsub.Topic{Name: topic}).Do()
	require.NoError(t, err)

	pol, err := svc.Projects.Topics.GetIamPolicy(topic).Do()
	require.NoError(t, err)
	require.NotEmpty(t, pol.Etag)

	set, err := svc.Projects.Topics.SetIamPolicy(topic, &pubsub.SetIamPolicyRequest{
		Policy: &pubsub.Policy{
			Etag:     pol.Etag,
			Bindings: []*pubsub.Binding{{Role: "roles/pubsub.publisher", Members: []string{"allUsers"}}},
		},
	}).Do()
	require.NoError(t, err)
	require.Len(t, set.Bindings, 1)
	require.Equal(t, "roles/pubsub.publisher", set.Bindings[0].Role)
}

// TestSDKSecretManagerLifecycle covers the full version lifecycle: create →
// addVersion → access, disable/enable (state transitions), and destroy.
func TestSDKSecretManagerLifecycle(t *testing.T) {
	ctx := context.Background()
	svc, err := secretmanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	secret, err := svc.Projects.Secrets.Create("projects/proj", &secretmanager.Secret{
		Replication: &secretmanager.Replication{Automatic: &secretmanager.Automatic{}},
	}).SecretId(unique("life")).Do()
	require.NoError(t, err)

	_, err = svc.Projects.Secrets.AddVersion(secret.Name, &secretmanager.AddSecretVersionRequest{
		Payload: &secretmanager.SecretPayload{Data: b64("lifecycle")},
	}).Do()
	require.NoError(t, err)

	// Disable → state DISABLED, then Enable → ENABLED, then Destroy → DESTROYED.
	_, err = svc.Projects.Secrets.Versions.Disable(secret.Name+"/versions/1", &secretmanager.DisableSecretVersionRequest{}).Do()
	require.NoError(t, err)
	v, err := svc.Projects.Secrets.Versions.Get(secret.Name + "/versions/1").Do()
	require.NoError(t, err)
	require.Equal(t, "DISABLED", v.State)

	_, err = svc.Projects.Secrets.Versions.Enable(secret.Name+"/versions/1", &secretmanager.EnableSecretVersionRequest{}).Do()
	require.NoError(t, err)
	v, err = svc.Projects.Secrets.Versions.Get(secret.Name + "/versions/1").Do()
	require.NoError(t, err)
	require.Equal(t, "ENABLED", v.State)

	_, err = svc.Projects.Secrets.Versions.Destroy(secret.Name+"/versions/1", &secretmanager.DestroySecretVersionRequest{}).Do()
	require.NoError(t, err)
	v, err = svc.Projects.Secrets.Versions.Get(secret.Name + "/versions/1").Do()
	require.NoError(t, err)
	require.Equal(t, "DESTROYED", v.State)
}

// TestSDKSecretManagerRotation verifies the rotation schedule is stored on
// create and round-trips on update.
func TestSDKSecretManagerRotation(t *testing.T) {
	ctx := context.Background()
	svc, err := secretmanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	secret, err := svc.Projects.Secrets.Create("projects/proj", &secretmanager.Secret{
		Replication: &secretmanager.Replication{Automatic: &secretmanager.Automatic{}},
		Rotation: &secretmanager.Rotation{
			NextRotationTime: "2030-01-01T00:00:00Z",
			RotationPeriod:   "86400s",
		},
	}).SecretId(unique("rot")).Do()
	require.NoError(t, err)
	require.NotNil(t, secret.Rotation)
	require.Equal(t, "86400s", secret.Rotation.RotationPeriod)

	got, err := svc.Projects.Secrets.Get(secret.Name).Do()
	require.NoError(t, err)
	require.NotNil(t, got.Rotation)
	require.Equal(t, "2030-01-01T00:00:00Z", got.Rotation.NextRotationTime)

	// Update the rotation schedule and read it back.
	patched, err := svc.Projects.Secrets.Patch(secret.Name, &secretmanager.Secret{
		Rotation: &secretmanager.Rotation{NextRotationTime: "2031-01-01T00:00:00Z", RotationPeriod: "3600s"},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "3600s", patched.Rotation.RotationPeriod)

	got, err = svc.Projects.Secrets.Get(secret.Name).Do()
	require.NoError(t, err)
	require.Equal(t, "2031-01-01T00:00:00Z", got.Rotation.NextRotationTime)
}

// TestSDKSecretManagerDeleteNotFound verifies secret deletion plus 404 on get.
func TestSDKSecretManagerDeleteNotFound(t *testing.T) {
	ctx := context.Background()
	svc, err := secretmanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	secret, err := svc.Projects.Secrets.Create("projects/proj", &secretmanager.Secret{
		Replication: &secretmanager.Replication{Automatic: &secretmanager.Automatic{}},
	}).SecretId(unique("del")).Do()
	require.NoError(t, err)

	_, err = svc.Projects.Secrets.Delete(secret.Name).Do()
	require.NoError(t, err)
	_, err = svc.Projects.Secrets.Get(secret.Name).Do()
	requireNotFound(t, err)
}

// kmsKeySet creates a key ring + crypto key for a purpose/algorithm, returning
// the crypto key full name.
func kmsKeySet(t *testing.T, svc *cloudkms.Service, purpose, algorithm string) string {
	t.Helper()
	parent := "projects/proj/locations/global"
	krID := unique("kr")
	_, err := svc.Projects.Locations.KeyRings.Create(parent, &cloudkms.KeyRing{}).KeyRingId(krID).Do()
	require.NoError(t, err)

	krName := parent + "/keyRings/" + krID
	keyID := unique("k")
	ck := &cloudkms.CryptoKey{Purpose: purpose}
	if algorithm != "" {
		ck.VersionTemplate = &cloudkms.CryptoKeyVersionTemplate{Algorithm: algorithm}
	}
	_, err = svc.Projects.Locations.KeyRings.CryptoKeys.Create(krName, ck).CryptoKeyId(keyID).Do()
	require.NoError(t, err)
	return krName + "/cryptoKeys/" + keyID
}

// TestSDKKMSRotation covers cryptoKeyVersions.create + updatePrimaryVersion:
// encrypt with the original primary, rotate to a new version, encrypt again,
// and confirm old ciphertext remains decryptable (versioned ciphertext).
func TestSDKKMSRotation(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudkms.NewService(ctx, opts()...)
	require.NoError(t, err)

	keyName := kmsKeySet(t, svc, "ENCRYPT_DECRYPT", "")

	// Encrypt with primary version 1.
	enc1, err := svc.Projects.Locations.KeyRings.CryptoKeys.Encrypt(keyName, &cloudkms.EncryptRequest{
		Plaintext: b64("v1"),
	}).Do()
	require.NoError(t, err)
	require.Equal(t, "1", enc1.Name[strings.LastIndex(enc1.Name, "/")+1:])

	// Create version 2 and promote it to primary.
	ver2, err := svc.Projects.Locations.KeyRings.CryptoKeys.CryptoKeyVersions.Create(
		keyName+"/cryptoKeyVersions", &cloudkms.CryptoKeyVersion{}).Do()
	require.NoError(t, err)
	require.Equal(t, "2", ver2.Name[strings.LastIndex(ver2.Name, "/")+1:])

	updated, err := svc.Projects.Locations.KeyRings.CryptoKeys.UpdatePrimaryVersion(keyName,
		&cloudkms.UpdateCryptoKeyPrimaryVersionRequest{CryptoKeyVersionId: "2"}).Do()
	require.NoError(t, err)
	require.Equal(t, "2", updated.Primary.Name[strings.LastIndex(updated.Primary.Name, "/")+1:])

	// New encryption uses version 2.
	enc2, err := svc.Projects.Locations.KeyRings.CryptoKeys.Encrypt(keyName, &cloudkms.EncryptRequest{
		Plaintext: b64("v2"),
	}).Do()
	require.NoError(t, err)
	dec2, err := svc.Projects.Locations.KeyRings.CryptoKeys.Decrypt(keyName, &cloudkms.DecryptRequest{
		Ciphertext: enc2.Ciphertext,
	}).Do()
	require.NoError(t, err)
	require.Equal(t, b64("v2"), dec2.Plaintext)
	require.True(t, dec2.UsedPrimary)

	// Old ciphertext still decrypts with version 1.
	dec1, err := svc.Projects.Locations.KeyRings.CryptoKeys.Decrypt(keyName, &cloudkms.DecryptRequest{
		Ciphertext: enc1.Ciphertext,
	}).Do()
	require.NoError(t, err)
	require.Equal(t, b64("v1"), dec1.Plaintext)
	require.False(t, dec1.UsedPrimary)
}

// TestSDKKMSAsymmetricSignVerify signs a digest with an RSA_SIGN key, fetches
// the public key, and cryptographically verifies the signature.
func TestSDKKMSAsymmetricSignVerify(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudkms.NewService(ctx, opts()...)
	require.NoError(t, err)

	keyName := kmsKeySet(t, svc, "ASYMMETRIC_SIGN", "RSA_SIGN_PKCS1_2048_SHA256")
	verName := keyName + "/cryptoKeyVersions/1"

	pubKey, err := svc.Projects.Locations.KeyRings.CryptoKeys.CryptoKeyVersions.GetPublicKey(verName).Do()
	require.NoError(t, err)
	require.Equal(t, "RSA_SIGN_PKCS1_2048_SHA256", pubKey.Algorithm)
	require.NotEmpty(t, pubKey.Pem)

	msg := []byte("hello asymmetric")
	digest := sha256.Sum256(msg)
	sig, err := svc.Projects.Locations.KeyRings.CryptoKeys.CryptoKeyVersions.AsymmetricSign(verName,
		&cloudkms.AsymmetricSignRequest{Digest: &cloudkms.Digest{Sha256: base64.StdEncoding.EncodeToString(digest[:])}}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, sig.Signature)
	require.True(t, sig.VerifiedDigestCrc32c)

	block, _ := pem.Decode([]byte(pubKey.Pem))
	require.NotNil(t, block, "public key PEM must be decodable")
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	require.NoError(t, err)
	rsaPub, ok := parsed.(*rsa.PublicKey)
	require.True(t, ok, "expected an RSA public key")

	rawSig, err := base64.StdEncoding.DecodeString(sig.Signature)
	require.NoError(t, err)
	require.NoError(t, rsa.VerifyPKCS1v15(rsaPub, crypto.SHA256, digest[:], rawSig))
}

// TestSDKKMSMacSignVerify signs and verifies an HMAC_SHA256 MAC, including a
// negative verification with tampered data.
func TestSDKKMSMacSignVerify(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudkms.NewService(ctx, opts()...)
	require.NoError(t, err)

	keyName := kmsKeySet(t, svc, "MAC", "HMAC_SHA256")
	verName := keyName + "/cryptoKeyVersions/1"

	signed, err := svc.Projects.Locations.KeyRings.CryptoKeys.CryptoKeyVersions.MacSign(verName,
		&cloudkms.MacSignRequest{Data: b64("payload")}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, signed.Mac)
	require.True(t, signed.VerifiedDataCrc32c)

	verified, err := svc.Projects.Locations.KeyRings.CryptoKeys.CryptoKeyVersions.MacVerify(verName,
		&cloudkms.MacVerifyRequest{Data: b64("payload"), Mac: signed.Mac}).Do()
	require.NoError(t, err)
	require.True(t, verified.Success)

	// Tampered data must fail verification.
	tampered, err := svc.Projects.Locations.KeyRings.CryptoKeys.CryptoKeyVersions.MacVerify(verName,
		&cloudkms.MacVerifyRequest{Data: b64("tampered"), Mac: signed.Mac}).Do()
	require.NoError(t, err)
	require.False(t, tampered.Success)
}

// TestSDKKMSVersionDestroyAndNotFound covers version destroy (the GCP-native
// way to retire a version — the KMS SDK has no per-version disable) and 404 for
// a missing version and missing key.
func TestSDKKMSVersionDestroyAndNotFound(t *testing.T) {
	ctx := context.Background()
	svc, err := cloudkms.NewService(ctx, opts()...)
	require.NoError(t, err)

	keyName := kmsKeySet(t, svc, "ENCRYPT_DECRYPT", "")
	ver2, err := svc.Projects.Locations.KeyRings.CryptoKeys.CryptoKeyVersions.Create(
		keyName+"/cryptoKeyVersions", &cloudkms.CryptoKeyVersion{}).Do()
	require.NoError(t, err)

	// Destroy version 2 → state DESTROYED.
	_, err = svc.Projects.Locations.KeyRings.CryptoKeys.CryptoKeyVersions.Destroy(ver2.Name,
		&cloudkms.DestroyCryptoKeyVersionRequest{}).Do()
	require.NoError(t, err)
	got, err := svc.Projects.Locations.KeyRings.CryptoKeys.CryptoKeyVersions.Get(ver2.Name).Do()
	require.NoError(t, err)
	require.Equal(t, "DESTROYED", got.State)

	// A missing version returns 404.
	_, err = svc.Projects.Locations.KeyRings.CryptoKeys.CryptoKeyVersions.Get(keyName + "/cryptoKeyVersions/99").Do()
	requireNotFound(t, err)

	// A missing crypto key returns 404.
	_, err = svc.Projects.Locations.KeyRings.CryptoKeys.Get(keyName[:strings.LastIndex(keyName, "/")] + "/missing").Do()
	requireNotFound(t, err)
}

// TestSDKIAMServiceAccountLifecycle covers create/get/list/delete, plus 404 on
// get after delete.
func TestSDKIAMServiceAccountLifecycle(t *testing.T) {
	ctx := context.Background()
	svc, err := iam.NewService(ctx, opts()...)
	require.NoError(t, err)

	sa, err := svc.Projects.ServiceAccounts.Create("projects/proj", &iam.CreateServiceAccountRequest{
		AccountId:      unique("sa"),
		ServiceAccount: &iam.ServiceAccount{DisplayName: "Lifecycle SA"},
	}).Do()
	require.NoError(t, err)

	got, err := svc.Projects.ServiceAccounts.Get(sa.Name).Do()
	require.NoError(t, err)
	require.Equal(t, sa.Email, got.Email)

	_, err = svc.Projects.ServiceAccounts.Delete(sa.Name).Do()
	require.NoError(t, err)
	_, err = svc.Projects.ServiceAccounts.Get(sa.Name).Do()
	requireNotFound(t, err)
}

// TestSDKIAMServiceAccountKeyLifecycle creates a key (with privateKeyData),
// lists it, then deletes it.
func TestSDKIAMServiceAccountKeyLifecycle(t *testing.T) {
	ctx := context.Background()
	svc, err := iam.NewService(ctx, opts()...)
	require.NoError(t, err)

	sa, err := svc.Projects.ServiceAccounts.Create("projects/proj", &iam.CreateServiceAccountRequest{
		AccountId: unique("sa"),
	}).Do()
	require.NoError(t, err)

	key, err := svc.Projects.ServiceAccounts.Keys.Create(sa.Name, &iam.CreateServiceAccountKeyRequest{
		KeyAlgorithm:   "KEY_ALG_RSA_2048",
		PrivateKeyType: "TYPE_GOOGLE_CREDENTIALS_FILE",
	}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, key.PrivateKeyData, "create must return privateKeyData")

	keys, err := svc.Projects.ServiceAccounts.Keys.List(sa.Name).Do()
	require.NoError(t, err)
	require.Len(t, keys.Keys, 1)

	_, err = svc.Projects.ServiceAccounts.Keys.Delete(key.Name).Do()
	require.NoError(t, err)
	keys, err = svc.Projects.ServiceAccounts.Keys.List(sa.Name).Do()
	require.NoError(t, err)
	require.Empty(t, keys.Keys)
}

// TestSDKIAMSignBlob signs a payload and asserts a non-empty signature + keyId.
func TestSDKIAMSignBlob(t *testing.T) {
	ctx := context.Background()
	svc, err := iam.NewService(ctx, opts()...)
	require.NoError(t, err)

	sa, err := svc.Projects.ServiceAccounts.Create("projects/proj", &iam.CreateServiceAccountRequest{
		AccountId: unique("sa"),
	}).Do()
	require.NoError(t, err)

	signed, err := svc.Projects.ServiceAccounts.SignBlob(sa.Name, &iam.SignBlobRequest{
		BytesToSign: b64("hello signBlob"),
	}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, signed.KeyId)
	require.NotEmpty(t, signed.Signature)
}

// TestSDKIAMSignJwt signs a JWT payload and asserts a 3-part token + keyId.
func TestSDKIAMSignJwt(t *testing.T) {
	ctx := context.Background()
	svc, err := iam.NewService(ctx, opts()...)
	require.NoError(t, err)

	sa, err := svc.Projects.ServiceAccounts.Create("projects/proj", &iam.CreateServiceAccountRequest{
		AccountId: unique("sa"),
	}).Do()
	require.NoError(t, err)

	signed, err := svc.Projects.ServiceAccounts.SignJwt(sa.Name, &iam.SignJwtRequest{
		Payload: `{"iss":"proj","aud":"emulator"}`,
	}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, signed.KeyId)

	parts := strings.Split(signed.SignedJwt, ".")
	require.Len(t, parts, 3, "signedJwt must be header.payload.signature")
	for _, p := range parts {
		require.NotEmpty(t, p)
	}
}
