// iam_keytypes_test.go covers the repeated projects.serviceAccounts.keys.list
// keyTypes filter through the official apiary client (DC11). The client sends
// the filter as repeated keyTypes= query parameters (urlParams_.SetMulti), which
// is also what gcloud does for --managed-by, so this is the wire form the
// emulator must decode; the shared single-valued query decode keeps only the
// first value.
//
// It also pins the per-operation field scoping from the Discovery document:
// privateKeyType/privateKeyData are create-only, publicKeyData is get-only, and
// list carries neither.
package sdkrest_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/iam/v1"
)

// nonExpiringKeyTime is what the API reports for a key that does not expire.
const nonExpiringKeyTime = "9999-12-31T23:59:59Z"

func TestSDKIAMKeyTypesFilter(t *testing.T) {
	ctx := context.Background()
	svc, err := iam.NewService(ctx, opts()...)
	require.NoError(t, err)

	sa, err := svc.Projects.ServiceAccounts.Create("projects/proj", &iam.CreateServiceAccountRequest{
		AccountId: unique("keys"),
	}).Do()
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		key, err := svc.Projects.ServiceAccounts.Keys.Create(sa.Name, &iam.CreateServiceAccountKeyRequest{}).Do()
		require.NoError(t, err)
		require.Equal(t, "USER_MANAGED", key.KeyType, "keys.create makes a user-managed key")
		require.Equal(t, "USER_PROVIDED", key.KeyOrigin)
		require.Equal(t, "TYPE_GOOGLE_CREDENTIALS_FILE", key.PrivateKeyType, "privateKeyType is create-only")
		require.NotEmpty(t, key.PrivateKeyData)
		require.Empty(t, key.PublicKeyData, "keys.create must not return publicKeyData (get-only per Discovery)")
		require.Equal(t, nonExpiringKeyTime, key.ValidBeforeTime)
	}

	// No filter: every key of the account, and list carries neither create-only
	// nor get-only fields.
	all, err := svc.Projects.ServiceAccounts.Keys.List(sa.Name).Do()
	require.NoError(t, err)
	require.Len(t, all.Keys, 2)
	for _, k := range all.Keys {
		require.Equal(t, "USER_MANAGED", k.KeyType)
		require.Empty(t, k.PublicKeyData, "keys.list must not return publicKeyData (get-only per Discovery)")
		require.Empty(t, k.PrivateKeyType, "keys.list must not return privateKeyType (create-only per Discovery)")
		require.Empty(t, k.PrivateKeyData)
	}

	userManaged, err := svc.Projects.ServiceAccounts.Keys.List(sa.Name).KeyTypes("USER_MANAGED").Do()
	require.NoError(t, err)
	require.Len(t, userManaged.Keys, 2, "USER_MANAGED keeps both created keys")

	systemManaged, err := svc.Projects.ServiceAccounts.Keys.List(sa.Name).KeyTypes("SYSTEM_MANAGED").Do()
	require.NoError(t, err)
	require.Empty(t, systemManaged.Keys, "no signing key exists yet")

	// signBlob materialises and uses the Google-managed key; a user-managed key
	// is never used for signing on real GCP either.
	signed, err := svc.Projects.ServiceAccounts.SignBlob(sa.Name, &iam.SignBlobRequest{
		BytesToSign: b64("keyTypes"),
	}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, signed.KeyId)

	systemManaged, err = svc.Projects.ServiceAccounts.Keys.List(sa.Name).KeyTypes("SYSTEM_MANAGED").Do()
	require.NoError(t, err)
	require.Len(t, systemManaged.Keys, 1, "signing creates the system-managed key")
	system := systemManaged.Keys[0]
	require.Equal(t, "SYSTEM_MANAGED", system.KeyType)
	require.Equal(t, "GOOGLE_PROVIDED", system.KeyOrigin)
	require.True(t, strings.HasSuffix(system.Name, "/keys/"+signed.KeyId),
		"signBlob must sign with the system-managed key: signed keyId %q, listed %q", signed.KeyId, system.Name)

	userManaged, err = svc.Projects.ServiceAccounts.Keys.List(sa.Name).KeyTypes("USER_MANAGED").Do()
	require.NoError(t, err)
	require.Len(t, userManaged.Keys, 2, "signing must not add a user-managed key")

	both, err := svc.Projects.ServiceAccounts.Keys.List(sa.Name).KeyTypes("USER_MANAGED", "SYSTEM_MANAGED").Do()
	require.NoError(t, err)
	require.Len(t, both.Keys, 3, "both filter values are a union")

	// keys.get is the only operation that returns publicKeyData.
	got, err := svc.Projects.ServiceAccounts.Keys.Get(system.Name).Do()
	require.NoError(t, err)
	require.NotEmpty(t, got.PublicKeyData, "keys.get must return publicKeyData")
	require.Empty(t, got.PrivateKeyType, "keys.get must not return privateKeyType")
	require.Empty(t, got.PrivateKeyData, "keys.get must not return privateKeyData")

	// KEY_TYPE_UNSPECIFIED and duplicate values are INVALID_ARGUMENT, as the
	// Discovery document specifies.
	_, err = svc.Projects.ServiceAccounts.Keys.List(sa.Name).KeyTypes("KEY_TYPE_UNSPECIFIED").Do()
	requireGCPStatus(t, err, 400, "INVALID_ARGUMENT")

	_, err = svc.Projects.ServiceAccounts.Keys.List(sa.Name).KeyTypes("USER_MANAGED", "USER_MANAGED").Do()
	requireGCPStatus(t, err, 400, "INVALID_ARGUMENT")
}
