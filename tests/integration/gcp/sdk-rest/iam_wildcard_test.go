// iam_wildcard_test.go covers DC24: the IAM management surface addressed the
// way the official clients address it. gcloud builds
// `projects/-/serviceAccounts/{email}` for every `iam service-accounts` command
// (command_lib/iam/iam_util.py EmailToAccountResourceName, used by describe,
// keys list, the policy verbs, …), and only signBlob/signJwt resolved the `-`
// wildcard before this fix, so the whole surface 404'd.
//
// The wildcard resolves the project from the service-account email, so responses
// must carry the canonical name — never "projects/-" — and a miss through the
// wildcard is the documented 403 PERMISSION_DENIED rather than 404.
package sdkrest_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iam/v1"
)

func TestSDKIAMWildcardProject(t *testing.T) {
	ctx := context.Background()
	svc, err := iam.NewService(ctx, opts()...)
	require.NoError(t, err)

	sa, err := svc.Projects.ServiceAccounts.Create("projects/proj", &iam.CreateServiceAccountRequest{
		AccountId: unique("wild"),
	}).Do()
	require.NoError(t, err)
	wild := "projects/-/serviceAccounts/" + sa.Email

	// get: canonical name and project, never "projects/-".
	got, err := svc.Projects.ServiceAccounts.Get(wild).Do()
	require.NoError(t, err, "gcloud addresses describe as %s", wild)
	require.Equal(t, sa.Name, got.Name, "wildcard get must return the canonical name")
	require.Equal(t, "proj", got.ProjectId)

	// patch.
	patched, err := svc.Projects.ServiceAccounts.Patch(wild, &iam.PatchServiceAccountRequest{
		ServiceAccount: &iam.ServiceAccount{DisplayName: "Wildcard"},
		UpdateMask:     "displayName",
	}).Do()
	require.NoError(t, err)
	require.Equal(t, sa.Name, patched.Name)
	require.Equal(t, "Wildcard", patched.DisplayName)

	// disable / enable. Both return Empty (per Discovery), so the effect is read
	// back through the wildcard.
	_, err = svc.Projects.ServiceAccounts.Disable(wild, &iam.DisableServiceAccountRequest{}).Do()
	require.NoError(t, err)
	got, err = svc.Projects.ServiceAccounts.Get(wild).Do()
	require.NoError(t, err)
	require.True(t, got.Disabled, "wildcard disable must apply")
	_, err = svc.Projects.ServiceAccounts.Enable(wild, &iam.EnableServiceAccountRequest{}).Do()
	require.NoError(t, err)
	got, err = svc.Projects.ServiceAccounts.Get(wild).Do()
	require.NoError(t, err)
	require.False(t, got.Disabled, "wildcard enable must apply")

	// keys: create/list/get/disable/delete through the wildcard.
	key, err := svc.Projects.ServiceAccounts.Keys.Create(wild, &iam.CreateServiceAccountKeyRequest{}).Do()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(key.Name, sa.Name+"/keys/"),
		"keys.create name = %q, want the %q prefix", key.Name, sa.Name+"/keys/")
	keyID := key.Name[strings.LastIndex(key.Name, "/")+1:]

	listed, err := svc.Projects.ServiceAccounts.Keys.List(wild).Do()
	require.NoError(t, err)
	require.Len(t, listed.Keys, 1)
	require.Equal(t, key.Name, listed.Keys[0].Name)

	wildKey := wild + "/keys/" + keyID
	gotKey, err := svc.Projects.ServiceAccounts.Keys.Get(wildKey).Do()
	require.NoError(t, err)
	require.Equal(t, key.Name, gotKey.Name)
	require.NotEmpty(t, gotKey.PublicKeyData)

	_, err = svc.Projects.ServiceAccounts.Keys.Disable(wildKey, &iam.DisableServiceAccountKeyRequest{}).Do()
	require.NoError(t, err)
	gotKey, err = svc.Projects.ServiceAccounts.Keys.Get(wildKey).Do()
	require.NoError(t, err)
	require.True(t, gotKey.Disabled, "wildcard keys.disable must apply")
	_, err = svc.Projects.ServiceAccounts.Keys.Enable(wildKey, &iam.EnableServiceAccountKeyRequest{}).Do()
	require.NoError(t, err)
	gotKey, err = svc.Projects.ServiceAccounts.Keys.Get(wildKey).Do()
	require.NoError(t, err)
	require.False(t, gotKey.Disabled, "keys.enable after keys.disable")

	_, err = svc.Projects.ServiceAccounts.Keys.Delete(wildKey).Do()
	require.NoError(t, err)

	// IAM policy verbs.
	_, err = svc.Projects.ServiceAccounts.GetIamPolicy(wild).Do()
	require.NoError(t, err)
	set, err := svc.Projects.ServiceAccounts.SetIamPolicy(wild, &iam.SetIamPolicyRequest{
		Policy: &iam.Policy{Bindings: []*iam.Binding{{
			Role:    "roles/iam.serviceAccountUser",
			Members: []string{"allUsers"},
		}}},
	}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, set.Etag)
	perms, err := svc.Projects.ServiceAccounts.TestIamPermissions(wild, &iam.TestIamPermissionsRequest{
		Permissions: []string{"iam.serviceAccounts.get"},
	}).Do()
	require.NoError(t, err)
	require.Equal(t, []string{"iam.serviceAccounts.get"}, perms.Permissions)

	// delete + undelete through the wildcard.
	_, err = svc.Projects.ServiceAccounts.Delete(wild).Do()
	require.NoError(t, err)
	restored, err := svc.Projects.ServiceAccounts.Undelete(wild, &iam.UndeleteServiceAccountRequest{}).Do()
	require.NoError(t, err)
	require.NotNil(t, restored.RestoredAccount)
	require.Equal(t, sa.Name, restored.RestoredAccount.Name)
}

// TestSDKIAMWildcardMissIsPermissionDenied pins the documented negative: IAM
// answers a miss through "projects/-" with 403 PERMISSION_DENIED ("it can cause
// response messages to contain misleading error codes"), while the concrete
// project path keeps 404 NOT_FOUND.
func TestSDKIAMWildcardMissIsPermissionDenied(t *testing.T) {
	ctx := context.Background()
	svc, err := iam.NewService(ctx, opts()...)
	require.NoError(t, err)

	email := unique("ghost") + "@proj.iam.gserviceaccount.com"

	_, err = svc.Projects.ServiceAccounts.Get("projects/-/serviceAccounts/" + email).Do()
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr, "wildcard miss")
	require.Equal(t, 403, apiErr.Code, "wildcard miss must be PERMISSION_DENIED")
	require.Contains(t, apiErr.Error(), "Permission denied")

	_, err = svc.Projects.ServiceAccounts.Get("projects/proj/serviceAccounts/" + email).Do()
	require.ErrorAs(t, err, &apiErr, "concrete miss")
	require.Equal(t, 404, apiErr.Code, "concrete miss must stay NOT_FOUND")

	_, err = svc.Projects.ServiceAccounts.Keys.List("projects/-/serviceAccounts/" + email + "/keys").Do()
	require.ErrorAs(t, err, &apiErr, "wildcard keys.list miss")
	require.Equal(t, 403, apiErr.Code, "wildcard keys.list miss must be PERMISSION_DENIED")

	// A key addressed under the same unresolved account is a wildcard miss too
	// (the concrete project path keeps its per-key "key not found" 404).
	_, err = svc.Projects.ServiceAccounts.Keys.Get("projects/-/serviceAccounts/" + email + "/keys/deadbeef").Do()
	require.ErrorAs(t, err, &apiErr, "wildcard keys.get miss")
	require.Equal(t, 403, apiErr.Code, "wildcard keys.get miss must be PERMISSION_DENIED")
}
