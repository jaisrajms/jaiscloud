// Lifecycle sweeps shared by the Secret Manager REST provider and gRPC service
// (DC70).
//
// The provider and gRPC service run a proactive sweep loop, but the same
// time-driven transitions are also applied by every read of the affected
// resource so the boundary is strict even between ticks:
//
//   - Secret expiration: a secret whose expireTime has passed is deleted (with
//     its versions) and SECRET_DELETE deleteType EXPIRATION is published.
//   - Delayed version destruction: a version whose scheduledDestroyTime has
//     passed becomes DESTROYED and SECRET_VERSION_DESTROY is published.
//
// Both transitions are re-checked under the store's lock (DeleteSecretIf /
// UpdateVersionIf), so a concurrent update that clears an expiration, cancels a
// schedule, or restarts a scheduled destroy cannot be raced by the sweep.
// Keeping them here means REST and gRPC cannot disagree about when a record is
// due or how the transition writes it; each transport only maps the error and
// publishes the event.
package secretmanager

import (
	"context"
	"errors"

	"jaiscloud/internal/clock"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
)

var (
	// errSecretNotExpired aborts the expiry sweep's atomic delete when the secret
	// is not (or no longer) due, so a concurrent read cannot delete a live one.
	errSecretNotExpired = errors.New("secret not expired")
	// errScheduledDestroyNotDue aborts the delayed-destruction transition when
	// the version is no longer due under the store's lock.
	errScheduledDestroyNotDue = errors.New("scheduled destruction not due")
)

// ExpireSecretIfDue deletes an expired secret (cascading its versions) and
// returns the pre-delete record with deleted=true so the caller can publish
// SECRET_DELETE. A secret that is not due, or is already gone, reports
// deleted=false and no error.
func ExpireSecretIfDue(ctx context.Context, secrets secretmanagerstore.Store, account, id string) (secretmanagerstore.Secret, bool, error) {
	now := clock.Now()
	var expired secretmanagerstore.Secret
	err := secrets.DeleteSecretIf(ctx, account, id, func(sec secretmanagerstore.Secret) error {
		if !Expired(sec, now) {
			return errSecretNotExpired
		}
		expired = sec
		return nil
	})
	if errors.Is(err, errSecretNotExpired) || errors.Is(err, secretmanagerstore.ErrNoSuchSecret) {
		return secretmanagerstore.Secret{}, false, nil
	}
	if err != nil {
		return secretmanagerstore.Secret{}, false, err
	}
	return expired, true, nil
}

// FinalizeScheduledDestroy applies a due delayed destruction: the version
// becomes DESTROYED with destroyTime equal to its scheduled instant. destroyed
// reports whether this call performed the transition (and therefore owes the
// SECRET_VERSION_DESTROY message); a version that is not due is returned
// unchanged.
func FinalizeScheduledDestroy(ctx context.Context, secrets secretmanagerstore.Store, account, secretID string, v secretmanagerstore.Version) (updated secretmanagerstore.Version, destroyed bool) {
	if !ScheduledDestroyDue(v, clock.Now()) {
		return v, false
	}
	out, err := secrets.UpdateVersionIf(ctx, account, secretID, v.VersionID, func(cur secretmanagerstore.Version) (secretmanagerstore.Version, error) {
		if !ScheduledDestroyDue(cur, clock.Now()) {
			return secretmanagerstore.Version{}, errScheduledDestroyNotDue
		}
		cur.State = "DESTROYED"
		cur.DestroyTime = cur.ScheduledDestroyTime
		return cur, nil
	})
	if err != nil {
		// Not due any more (a concurrent enable/disable or destroy won) or a
		// store error: report the stored state, never a stale snapshot.
		if fresh, gerr := secrets.GetVersion(ctx, account, secretID, v.VersionID); gerr == nil {
			return fresh, false
		}
		return v, false
	}
	return out, true
}
