// secretmanager_expiration_test.go closes DC70 through the official apiary REST
// client: `versionDestroyTtl` delays a version's destruction (DISABLED with a
// scheduledDestroyTime, then DESTROYED once the delay elapses) and
// `ttl`/`expireTime` expires the secret (404 afterwards, cascade over its
// versions).
package sdkrest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	secretmanager "google.golang.org/api/secretmanager/v1"
)

// sdkSetClock posts the emulator time-control payload and fails on any status
// other than 204.
func sdkSetClock(t *testing.T, body map[string]any) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := http.Post(endpoint()+"_jaiscloud/clock", "application/json", bytes.NewReader(raw))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode, "POST /_jaiscloud/clock %v", body)
}

func sdkFreezeAt(t *testing.T, at time.Time) {
	t.Helper()
	sdkSetClock(t, map[string]any{"mode": "fixed", "time": at})
	t.Cleanup(func() { sdkSetClock(t, map[string]any{"mode": "real"}) })
}

func TestSDKSecretManagerDelayedDestroyAndExpiration(t *testing.T) {
	ctx := context.Background()
	svc, err := secretmanager.NewService(ctx, opts()...)
	require.NoError(t, err)

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sdkFreezeAt(t, t0)

	// Delayed destruction: destroy disables and schedules the version.
	delayed, err := svc.Projects.Secrets.Create("projects/proj", &secretmanager.Secret{
		Replication:       &secretmanager.Replication{Automatic: &secretmanager.Automatic{}},
		VersionDestroyTtl: "86400s",
	}).SecretId(unique("delay")).Do()
	require.NoError(t, err)
	require.Equal(t, "86400s", delayed.VersionDestroyTtl)
	added, err := svc.Projects.Secrets.AddVersion(delayed.Name, &secretmanager.AddSecretVersionRequest{
		Payload: &secretmanager.SecretPayload{Data: "aGk="},
	}).Do()
	require.NoError(t, err)
	scheduled, err := svc.Projects.Secrets.Versions.Destroy(added.Name, &secretmanager.DestroySecretVersionRequest{}).Do()
	require.NoError(t, err)
	require.Equal(t, "DISABLED", scheduled.State, "a delayed destroy disables the version")
	require.NotEmpty(t, scheduled.ScheduledDestroyTime)
	require.Empty(t, scheduled.DestroyTime)
	scheduledAt, err := time.Parse(time.RFC3339Nano, scheduled.ScheduledDestroyTime)
	require.NoError(t, err)
	require.True(t, scheduledAt.Equal(t0.Add(24*time.Hour)), "scheduledDestroyTime = %v", scheduledAt)

	// Expiration: ttl is input-only and reads back as expireTime.
	expiring, err := svc.Projects.Secrets.Create("projects/proj", &secretmanager.Secret{
		Replication: &secretmanager.Replication{Automatic: &secretmanager.Automatic{}},
		Ttl:         "86400s",
	}).SecretId(unique("expire")).Do()
	require.NoError(t, err)
	expireAt, err := time.Parse(time.RFC3339Nano, expiring.ExpireTime)
	require.NoError(t, err, "expireTime must be returned: %q", expiring.ExpireTime)
	require.True(t, expireAt.Equal(t0.Add(24*time.Hour)), "expireTime = %v", expireAt)
	expiringVersion, err := svc.Projects.Secrets.AddVersion(expiring.Name, &secretmanager.AddSecretVersionRequest{
		Payload: &secretmanager.SecretPayload{Data: "aGk="},
	}).Do()
	require.NoError(t, err)
	require.NotEmpty(t, expiringVersion.Name)

	// Past both instants a read performs the sweeps.
	sdkSetClock(t, map[string]any{"mode": "fixed", "time": t0.Add(25 * time.Hour)})

	destroyed, err := svc.Projects.Secrets.Versions.Get(added.Name).Do()
	require.NoError(t, err)
	require.Equal(t, "DESTROYED", destroyed.State)
	require.NotEmpty(t, destroyed.DestroyTime)

	_, err = svc.Projects.Secrets.Get(expiring.Name).Do()
	var apiErr *googleapi.Error
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 404, apiErr.Code, "an expired secret must be gone")
	_, err = svc.Projects.Secrets.Versions.Get(expiring.Name + "/versions/1").Do()
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, 404, apiErr.Code, "the expired secret's versions must be gone")
}
