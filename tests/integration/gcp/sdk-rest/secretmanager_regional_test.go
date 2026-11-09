// secretmanager_regional_test.go covers TF1-4: regional secrets
// (projects.locations.secrets) through the official apiary REST client, which
// exposes the regional surface as Projects.Locations.Secrets / .Versions.
//
// Asserted:
//   - create/get/list over the location-qualified namespace, with the documented
//     name and no replication policy;
//   - customerManagedEncryption round-trips (and must name a key in the secret's
//     location), and a version reports the top-level CMEK status instead of a
//     replicationStatus;
//   - the project-level list is scoped to global secrets and never leaks a
//     regional one (and vice versa);
//   - the IAM mixin works on the regional resource name;
//   - replication on a regional secret is INVALID_ARGUMENT;
//   - delete removes it (404 afterwards).
package sdkrest_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	cloudkms "google.golang.org/api/cloudkms/v1"
	secretmanager "google.golang.org/api/secretmanager/v1"
)

func TestSDKSecretManagerRegional(t *testing.T) {
	ctx := context.Background()
	svc, err := secretmanager.NewService(ctx, opts()...)
	require.NoError(t, err)
	kmsSvc, err := cloudkms.NewService(ctx, opts()...)
	require.NoError(t, err)

	const region = "us-central1"
	ring := unique("reg-ring")
	_, err = kmsSvc.Projects.Locations.KeyRings.Create("projects/proj/locations/"+region, &cloudkms.KeyRing{}).
		KeyRingId(ring).Do()
	require.NoError(t, err)
	keyID := unique("reg-key")
	ck, err := kmsSvc.Projects.Locations.KeyRings.CryptoKeys.Create(
		"projects/proj/locations/"+region+"/keyRings/"+ring, &cloudkms.CryptoKey{Purpose: "ENCRYPT_DECRYPT"}).
		CryptoKeyId(keyID).Do()
	require.NoError(t, err)

	secretID := unique("reg-secret")
	parent := "projects/proj/locations/" + region
	name := parent + "/secrets/" + secretID
	t.Cleanup(func() { _, _ = svc.Projects.Locations.Secrets.Delete(name).Do() })

	created, err := svc.Projects.Locations.Secrets.Create(parent, &secretmanager.Secret{
		Labels:                    map[string]string{"env": "conf"},
		Annotations:               map[string]string{"owner": "sdk"},
		CustomerManagedEncryption: &secretmanager.CustomerManagedEncryption{KmsKeyName: ck.Name},
	}).SecretId(secretID).Do()
	require.NoError(t, err)
	require.Equal(t, name, created.Name)
	require.Nil(t, created.Replication, "a regional secret must not carry a replication policy")
	require.NotNil(t, created.CustomerManagedEncryption)
	require.Equal(t, ck.Name, created.CustomerManagedEncryption.KmsKeyName)
	require.Equal(t, "conf", created.Labels["env"])

	got, err := svc.Projects.Locations.Secrets.Get(name).Do()
	require.NoError(t, err)
	require.Equal(t, name, got.Name)
	require.Equal(t, "sdk", got.Annotations["owner"])

	// A version carries the top-level CMEK status and no replicationStatus.
	version, err := svc.Projects.Locations.Secrets.AddVersion(name, &secretmanager.AddSecretVersionRequest{
		Payload: &secretmanager.SecretPayload{Data: "cmVnaW9uYWw="},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, name+"/versions/1", version.Name)
	require.Nil(t, version.ReplicationStatus)
	require.NotNil(t, version.CustomerManagedEncryption)
	require.NotEmpty(t, version.CustomerManagedEncryption.KmsKeyVersionName)

	accessed, err := svc.Projects.Locations.Secrets.Versions.Access(name + "/versions/1").Do()
	require.NoError(t, err)
	require.Equal(t, "cmVnaW9uYWw=", accessed.Payload.Data)

	// A labels-only patch keeps the CMEK configuration.
	patched, err := svc.Projects.Locations.Secrets.Patch(name, &secretmanager.Secret{
		Labels: map[string]string{"env": "prod"},
	}).UpdateMask("labels").Do()
	require.NoError(t, err)
	require.Equal(t, "prod", patched.Labels["env"])
	require.Equal(t, ck.Name, patched.CustomerManagedEncryption.KmsKeyName)

	// List scoping in both directions.
	regionalSecrets, err := svc.Projects.Locations.Secrets.List(parent).Do()
	require.NoError(t, err)
	require.True(t, containsSecret(regionalSecrets.Secrets, name), "regional list must contain %s", name)
	globalSecrets, err := svc.Projects.Secrets.List("projects/proj").Do()
	require.NoError(t, err)
	require.False(t, containsSecret(globalSecrets.Secrets, name), "project-level list must not contain %s", name)

	// IAM on the regional resource name.
	pol, err := svc.Projects.Locations.Secrets.GetIamPolicy(name).Do()
	require.NoError(t, err)
	pol.Bindings = []*secretmanager.Binding{{Role: "roles/secretmanager.secretAccessor", Members: []string{"user:regional@example.com"}}}
	set, err := svc.Projects.Locations.Secrets.SetIamPolicy(name, &secretmanager.SetIamPolicyRequest{Policy: pol}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, set.Etag)
	perms, err := svc.Projects.Locations.Secrets.TestIamPermissions(name, &secretmanager.TestIamPermissionsRequest{
		Permissions: []string{"secretmanager.versions.access"},
	}).Do()
	require.NoError(t, err)
	require.Contains(t, perms.Permissions, "secretmanager.versions.access")

	// Replication is rejected on a regional secret.
	_, err = svc.Projects.Locations.Secrets.Create(parent, &secretmanager.Secret{
		Replication: &secretmanager.Replication{Automatic: &secretmanager.Automatic{}},
	}).SecretId(unique("reg-bad")).Do()
	requireInvalidArgumentAPI(t, err, "regional create with replication")

	// A key outside the secret's location is rejected.
	_, err = svc.Projects.Locations.Secrets.Create(parent, &secretmanager.Secret{
		CustomerManagedEncryption: &secretmanager.CustomerManagedEncryption{
			KmsKeyName: "projects/proj/locations/europe-west1/keyRings/kr/cryptoKeys/k",
		},
	}).SecretId(unique("reg-badkey")).Do()
	requireInvalidArgumentAPI(t, err, "regional create with a key in another location")

	// Delete removes the secret; a later read is 404.
	_, err = svc.Projects.Locations.Secrets.Delete(name).Do()
	require.NoError(t, err)
	_, err = svc.Projects.Locations.Secrets.Get(name).Do()
	require.Error(t, err)
}

func containsSecret(secrets []*secretmanager.Secret, name string) bool {
	for _, s := range secrets {
		if s.Name == name {
			return true
		}
	}
	return false
}
