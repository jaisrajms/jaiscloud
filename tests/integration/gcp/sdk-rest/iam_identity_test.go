// iam_identity_test.go covers DC25 through the official apiary client: the two
// output-only identifiers real IAM assigns to a service account. Discovery
// documents uniqueId as a stable numeric id that survives delete/undelete and
// differs for an account re-created under the same name, and oauth2ClientId as a
// separate numeric client id. The emulator used to return the email as uniqueId
// and an empty oauth2ClientId, so a client could not address an account by its
// unique id and the credentials file carried no real client id.
package sdkrest_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/iam/v1"
)

// numericIDRe is the shape real IAM assigns: 21 decimal digits, first non-zero.
// It is a string because the value overflows int64.
var numericIDRe = regexp.MustCompile(`^[1-9][0-9]{20}$`)

func TestSDKIAMServiceAccountNumericIDs(t *testing.T) {
	ctx := context.Background()
	svc, err := iam.NewService(ctx, opts()...)
	require.NoError(t, err)

	accountID := unique("ident")
	sa, err := svc.Projects.ServiceAccounts.Create("projects/proj", &iam.CreateServiceAccountRequest{
		AccountId: accountID,
	}).Do()
	require.NoError(t, err)
	require.Regexp(t, numericIDRe, sa.UniqueId, "uniqueId must be a 21-digit numeric id")
	require.Regexp(t, numericIDRe, sa.Oauth2ClientId, "oauth2ClientId must be a 21-digit numeric id")
	require.NotEqual(t, sa.UniqueId, sa.Oauth2ClientId, "real IAM assigns distinct uniqueId and oauth2ClientId")

	// The second documented identifier form: the unique id in place of the email.
	byID, err := svc.Projects.ServiceAccounts.Get("projects/proj/serviceAccounts/" + sa.UniqueId).Do()
	require.NoError(t, err)
	require.Equal(t, sa.Name, byID.Name, "get by unique id must return the canonical name")
	require.Equal(t, sa.Email, byID.Email)
	require.Equal(t, sa.UniqueId, byID.UniqueId)
	require.Equal(t, sa.Oauth2ClientId, byID.Oauth2ClientId)

	// The credentials file carries the account's oauth2ClientId, which is what
	// the official client libraries read as client_id.
	key, err := svc.Projects.ServiceAccounts.Keys.Create(sa.Name, &iam.CreateServiceAccountKeyRequest{
		KeyAlgorithm:   "KEY_ALG_RSA_2048",
		PrivateKeyType: "TYPE_GOOGLE_CREDENTIALS_FILE",
	}).Do()
	require.NoError(t, err)
	raw, err := base64.StdEncoding.DecodeString(key.PrivateKeyData)
	require.NoError(t, err)
	var creds map[string]any
	require.NoError(t, json.Unmarshal(raw, &creds))
	require.Equal(t, sa.Oauth2ClientId, creds["client_id"], "credentials client_id must be the oauth2ClientId")
	require.Equal(t, sa.Email, creds["client_email"])

	// Delete then re-create under the same name: the documented contract says
	// the new account has a different unique id, and the old one stops resolving.
	_, err = svc.Projects.ServiceAccounts.Delete(sa.Name).Do()
	require.NoError(t, err)
	reborn, err := svc.Projects.ServiceAccounts.Create("projects/proj", &iam.CreateServiceAccountRequest{
		AccountId: accountID,
	}).Do()
	require.NoError(t, err)
	require.NotEqual(t, sa.UniqueId, reborn.UniqueId, "a re-created account must get a new unique id")
	_, err = svc.Projects.ServiceAccounts.Get("projects/proj/serviceAccounts/" + sa.UniqueId).Do()
	requireNotFound(t, err)
}

// TestSDKIAMCreateIgnoresOutputOnlyFields asserts the create request cannot set
// the output-only identifiers or the disabled state: real IAM ignores them and
// answers the server-generated values.
func TestSDKIAMCreateIgnoresOutputOnlyFields(t *testing.T) {
	ctx := context.Background()
	svc, err := iam.NewService(ctx, opts()...)
	require.NoError(t, err)

	const (
		suppliedUnique = "123456789012345678901"
		suppliedClient = "987654321098765432109"
	)
	sa, err := svc.Projects.ServiceAccounts.Create("projects/proj", &iam.CreateServiceAccountRequest{
		AccountId: unique("oo"),
		ServiceAccount: &iam.ServiceAccount{
			DisplayName:    "Output Only",
			UniqueId:       suppliedUnique,
			Oauth2ClientId: suppliedClient,
			Disabled:       true,
		},
	}).Do()
	require.NoError(t, err)
	require.Regexp(t, numericIDRe, sa.UniqueId)
	require.Regexp(t, numericIDRe, sa.Oauth2ClientId)
	require.NotEqual(t, suppliedUnique, sa.UniqueId, "create echoed the caller's uniqueId")
	require.NotEqual(t, suppliedClient, sa.Oauth2ClientId, "create echoed the caller's oauth2ClientId")
	require.False(t, sa.Disabled, "create honored the output-only disabled field")
	require.Equal(t, "Output Only", sa.DisplayName)

	// The account is created enabled: disable is a separate verb.
	_, err = svc.Projects.ServiceAccounts.Disable(sa.Name, &iam.DisableServiceAccountRequest{}).Do()
	require.NoError(t, err)
	got, err := svc.Projects.ServiceAccounts.Get(sa.Name).Do()
	require.NoError(t, err)
	require.True(t, got.Disabled)
}
