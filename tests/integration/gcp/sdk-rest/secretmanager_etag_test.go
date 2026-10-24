// secretmanager_etag_test.go closes DC13 (plan_docs/gcp-depth-closure-wave-plan.md):
// Secret Manager's documented etag precondition was rendered but never read, so
// a caller holding a stale etag could still delete a secret or transition a
// secret version.
//
// Asserted through the official apiary REST client:
//   - a secret metadata update changes Secret.etag; deleting with the
//     pre-update etag fails 409 ABORTED and leaves the secret in place, while
//     the current etag (and an omitted one) succeeds;
//   - SecretVersion.etag is present on responses and changes on every
//     lifecycle transition; disable/enable/destroy with a stale or bogus etag
//     fail 409 ABORTED without changing the state, and succeed with the
//     current one.
package sdkrest_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	secretmanager "google.golang.org/api/secretmanager/v1"
)

// requireAbortedAPI asserts the emulator answered the etag-precondition
// failure the way a real client sees it: HTTP 409 with the ABORTED google.rpc
// status in the error envelope.
func requireAbortedAPI(t *testing.T, err error, what string) {
	t.Helper()
	require.Error(t, err, what)
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr, "%s: want a googleapi.Error, got %T", what, err)
	require.Equal(t, 409, apiErr.Code, "%s: HTTP status", what)
	var envelope struct {
		Error struct {
			Status string `json:"status"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(apiErr.Body), &envelope), "%s: error body %q", what, apiErr.Body)
	require.Equal(t, "ABORTED", envelope.Error.Status, "%s: error body %q", what, apiErr.Body)
}

func TestSDKSecretManagerSecretEtagPrecondition(t *testing.T) {
	ctx := context.Background()
	svc, err := secretmanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	created, err := svc.Projects.Secrets.Create("projects/proj", &secretmanager.Secret{
		Replication: &secretmanager.Replication{Automatic: &secretmanager.Automatic{}},
	}).SecretId(unique("etag")).Do()
	require.NoError(t, err)
	require.NotEmpty(t, created.Etag, "Secret.etag must be returned")

	// A metadata update bumps the etag.
	updated, err := svc.Projects.Secrets.Patch(created.Name, &secretmanager.Secret{
		Labels: map[string]string{"env": "prod"},
		Etag:   created.Etag,
	}).UpdateMask("labels").Do()
	require.NoError(t, err, "update carrying the current etag")
	require.NotEqual(t, created.Etag, updated.Etag, "a metadata update must change Secret.etag")

	// The pre-update etag is stale: the delete is blocked and the secret lives.
	_, err = svc.Projects.Secrets.Delete(created.Name).Etag(created.Etag).Do()
	requireAbortedAPI(t, err, "delete with the pre-update etag")
	_, err = svc.Projects.Secrets.Get(created.Name).Do()
	require.NoError(t, err, "a rejected delete must not remove the secret")

	// A bogus token is rejected the same way.
	_, err = svc.Projects.Secrets.Delete(created.Name).Etag("BOGUS=").Do()
	requireAbortedAPI(t, err, "delete with a bogus etag")

	// The current etag passes.
	_, err = svc.Projects.Secrets.Delete(created.Name).Etag(updated.Etag).Do()
	require.NoError(t, err, "delete with the current etag")

	// Omitting the etag opts out of the precondition, as documented.
	noEtag, err := svc.Projects.Secrets.Create("projects/proj", &secretmanager.Secret{
		Replication: &secretmanager.Replication{Automatic: &secretmanager.Automatic{}},
	}).SecretId(unique("noetag")).Do()
	require.NoError(t, err)
	_, err = svc.Projects.Secrets.Delete(noEtag.Name).Do()
	require.NoError(t, err, "delete without an etag")
}

func TestSDKSecretManagerVersionEtagPrecondition(t *testing.T) {
	ctx := context.Background()
	svc, err := secretmanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	secret := createSecret(t, svc)

	added, err := svc.Projects.Secrets.AddVersion(secret, &secretmanager.AddSecretVersionRequest{
		Payload: &secretmanager.SecretPayload{Data: "aGVsbG8="},
	}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, added.Etag, "SecretVersion.etag must be returned")
	enabledEtag := added.Etag

	// A bogus etag does not transition the version.
	_, err = svc.Projects.Secrets.Versions.Disable(added.Name, &secretmanager.DisableSecretVersionRequest{
		Etag: "BOGUS=",
	}).Do()
	requireAbortedAPI(t, err, "disable with a bogus etag")
	still, err := svc.Projects.Secrets.Versions.Get(added.Name).Do()
	require.NoError(t, err)
	require.Equal(t, "ENABLED", still.State, "a rejected disable must not change the state")

	// The current etag passes, and the returned etag is bumped.
	disabled, err := svc.Projects.Secrets.Versions.Disable(added.Name, &secretmanager.DisableSecretVersionRequest{
		Etag: enabledEtag,
	}).Do()
	require.NoError(t, err, "disable with the current etag")
	require.Equal(t, "DISABLED", disabled.State)
	require.NotEqual(t, enabledEtag, disabled.Etag, "the version etag must change on a state transition")

	// The pre-disable etag is stale for enable.
	_, err = svc.Projects.Secrets.Versions.Enable(added.Name, &secretmanager.EnableSecretVersionRequest{
		Etag: enabledEtag,
	}).Do()
	requireAbortedAPI(t, err, "enable with the pre-disable etag")

	reenabled, err := svc.Projects.Secrets.Versions.Enable(added.Name, &secretmanager.EnableSecretVersionRequest{
		Etag: disabled.Etag,
	}).Do()
	require.NoError(t, err, "enable with the current etag")
	require.Equal(t, "ENABLED", reenabled.State)

	_, err = svc.Projects.Secrets.Versions.Destroy(added.Name, &secretmanager.DestroySecretVersionRequest{
		Etag: "BOGUS=",
	}).Do()
	requireAbortedAPI(t, err, "destroy with a bogus etag")

	destroyed, err := svc.Projects.Secrets.Versions.Destroy(added.Name, &secretmanager.DestroySecretVersionRequest{
		Etag: reenabled.Etag,
	}).Do()
	require.NoError(t, err, "destroy with the current etag")
	require.Equal(t, "DESTROYED", destroyed.State)
}
