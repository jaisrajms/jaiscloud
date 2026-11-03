// iam_keytypes_test.go covers the repeated projects.serviceAccounts.keys.list
// keyTypes filter through the official apiary client (DC11). The client sends
// the filter as repeated keyTypes= query parameters (urlParams_.SetMulti), which
// is also what gcloud does for --managed-by, so this is the wire form the
// emulator must decode; the shared single-valued query decode keeps only the
// first value.
//
// It also pins the per-operation field scoping from the Discovery document and
// the Google-managed key's shape: privateKeyType/privateKeyData are create-only,
// publicKeyData is get-only and absent for the managed key, and list carries
// neither. A fresh account lists exactly one SYSTEM_MANAGED key, created with
// the account (NF15/DC26).
package sdkrest_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/iam/v1"
)

// nonExpiringKeyTime is what the API reports for a key that does not expire.
const nonExpiringKeyTime = "9999-12-31T23:59:59Z"

// systemManagedKeyLifetime is the window real IAM reports for a Google-managed
// key: validBeforeTime is 730 days after validAfterTime.
const systemManagedKeyLifetime = 730 * 24 * time.Hour

func TestSDKIAMKeyTypesFilter(t *testing.T) {
	ctx := context.Background()
	svc, err := iam.NewService(ctx, opts()...)
	require.NoError(t, err)

	sa, err := svc.Projects.ServiceAccounts.Create("projects/proj", &iam.CreateServiceAccountRequest{
		AccountId: unique("keys"),
	}).Do()
	require.NoError(t, err)

	// The account is born with its Google-managed key pair -- nothing has signed
	// yet, and no user-managed key exists.
	fresh, err := svc.Projects.ServiceAccounts.Keys.List(sa.Name).Do()
	require.NoError(t, err)
	require.Len(t, fresh.Keys, 1, "a fresh account lists exactly one key")
	managed := fresh.Keys[0]
	require.Equal(t, "SYSTEM_MANAGED", managed.KeyType)
	require.Equal(t, "GOOGLE_PROVIDED", managed.KeyOrigin)
	require.Equal(t, "KEY_ALG_RSA_2048", managed.KeyAlgorithm)
	require.NotEqual(t, nonExpiringKeyTime, managed.ValidBeforeTime, "a managed key expires")
	after, err := time.Parse(time.RFC3339Nano, managed.ValidAfterTime)
	require.NoError(t, err)
	before, err := time.Parse(time.RFC3339Nano, managed.ValidBeforeTime)
	require.NoError(t, err)
	require.Equal(t, systemManagedKeyLifetime, before.Sub(after), "managed key window is two years")

	for i := 0; i < 2; i++ {
		key, err := svc.Projects.ServiceAccounts.Keys.Create(sa.Name, &iam.CreateServiceAccountKeyRequest{}).Do()
		require.NoError(t, err)
		require.Equal(t, "USER_MANAGED", key.KeyType, "keys.create makes a user-managed key")
		require.Equal(t, "USER_PROVIDED", key.KeyOrigin)
		require.Equal(t, "TYPE_GOOGLE_CREDENTIALS_FILE", key.PrivateKeyType, "privateKeyType is create-only")
		require.NotEmpty(t, key.PrivateKeyData)
		require.Empty(t, key.PublicKeyData, "keys.create must not return publicKeyData (get-only per Discovery)")
		require.Equal(t, nonExpiringKeyTime, key.ValidBeforeTime, "a user-managed key never expires")
	}

	// No filter: every key of the account, and list carries neither create-only
	// nor get-only fields.
	all, err := svc.Projects.ServiceAccounts.Keys.List(sa.Name).Do()
	require.NoError(t, err)
	require.Len(t, all.Keys, 3, "two user-managed keys plus the managed one")
	var system *iam.ServiceAccountKey
	for _, k := range all.Keys {
		require.Empty(t, k.PublicKeyData, "keys.list must not return publicKeyData (get-only per Discovery)")
		require.Empty(t, k.PrivateKeyType, "keys.list must not return privateKeyType (create-only per Discovery)")
		require.Empty(t, k.PrivateKeyData)
		if k.KeyType == "SYSTEM_MANAGED" {
			system = k
		}
	}
	require.NotNil(t, system, "keys.list must carry the Google-managed key")

	userManaged, err := svc.Projects.ServiceAccounts.Keys.List(sa.Name).KeyTypes("USER_MANAGED").Do()
	require.NoError(t, err)
	require.Len(t, userManaged.Keys, 2, "USER_MANAGED keeps both created keys")

	systemManaged, err := svc.Projects.ServiceAccounts.Keys.List(sa.Name).KeyTypes("SYSTEM_MANAGED").Do()
	require.NoError(t, err)
	require.Len(t, systemManaged.Keys, 1, "SYSTEM_MANAGED returns the managed key")
	require.Equal(t, system.Name, systemManaged.Keys[0].Name)

	// signBlob signs with that same managed key; it does not mint another one.
	signed, err := svc.Projects.ServiceAccounts.SignBlob(sa.Name, &iam.SignBlobRequest{
		BytesToSign: b64("keyTypes"),
	}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, signed.KeyId)
	require.True(t, strings.HasSuffix(system.Name, "/keys/"+signed.KeyId),
		"signBlob must sign with the system-managed key: signed keyId %q, listed %q", signed.KeyId, system.Name)

	systemManaged, err = svc.Projects.ServiceAccounts.Keys.List(sa.Name).KeyTypes("SYSTEM_MANAGED").Do()
	require.NoError(t, err)
	require.Len(t, systemManaged.Keys, 1, "signing reuses the managed key rather than creating one")

	userManaged, err = svc.Projects.ServiceAccounts.Keys.List(sa.Name).KeyTypes("USER_MANAGED").Do()
	require.NoError(t, err)
	require.Len(t, userManaged.Keys, 2, "signing must not add a user-managed key")

	both, err := svc.Projects.ServiceAccounts.Keys.List(sa.Name).KeyTypes("USER_MANAGED", "SYSTEM_MANAGED").Do()
	require.NoError(t, err)
	require.Len(t, both.Keys, 3, "both filter values are a union")

	// keys.get is the only operation that returns publicKeyData, and a managed
	// key has none to return (real IAM addresses it from keys.list only).
	got, err := svc.Projects.ServiceAccounts.Keys.Get(system.Name).Do()
	require.NoError(t, err)
	require.Empty(t, got.PublicKeyData, "keys.get on a managed key must not return publicKeyData")
	require.Empty(t, got.PrivateKeyType, "keys.get must not return privateKeyType")
	require.Empty(t, got.PrivateKeyData, "keys.get must not return privateKeyData")
	require.Equal(t, system.ValidBeforeTime, got.ValidBeforeTime)

	userGot, err := svc.Projects.ServiceAccounts.Keys.Get(userManaged.Keys[0].Name).Do()
	require.NoError(t, err)
	require.NotEmpty(t, userGot.PublicKeyData, "keys.get returns publicKeyData for a user-managed key")

	// delete/disable/enable on the managed key are 404 NOT_FOUND.
	_, err = svc.Projects.ServiceAccounts.Keys.Delete(system.Name).Do()
	requireGCPStatus(t, err, 404, "NOT_FOUND")
	_, err = svc.Projects.ServiceAccounts.Keys.Disable(system.Name, &iam.DisableServiceAccountKeyRequest{}).Do()
	requireGCPStatus(t, err, 404, "NOT_FOUND")
	_, err = svc.Projects.ServiceAccounts.Keys.Enable(system.Name, &iam.EnableServiceAccountKeyRequest{}).Do()
	requireGCPStatus(t, err, 404, "NOT_FOUND")

	// KEY_TYPE_UNSPECIFIED and duplicate values are INVALID_ARGUMENT, as the
	// Discovery document specifies.
	_, err = svc.Projects.ServiceAccounts.Keys.List(sa.Name).KeyTypes("KEY_TYPE_UNSPECIFIED").Do()
	requireGCPStatus(t, err, 400, "INVALID_ARGUMENT")

	_, err = svc.Projects.ServiceAccounts.Keys.List(sa.Name).KeyTypes("USER_MANAGED", "USER_MANAGED").Do()
	requireGCPStatus(t, err, 400, "INVALID_ARGUMENT")
}
