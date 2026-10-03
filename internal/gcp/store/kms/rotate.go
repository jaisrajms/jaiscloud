package kms

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// rotateMu serializes rotation attempts. Rotation is rare, so a single
// package-level mutex is enough to keep concurrent readers from double-rotating.
var rotateMu sync.Mutex

// RotateIfDue lazily executes a crypto key's rotation schedule: when the key's
// NextRotationTime has arrived (and RotationPeriod > 0), it creates a new
// ENABLED CryptoKeyVersion with the key's algorithm, makes it primary, and
// advances NextRotationTime by the rotation period. Reads trigger rotation
// because the emulator has no scheduler (a documented approximation).
//
// It returns the newly created version number, or "" when nothing rotated. It
// is best-effort: a missing key or any store error yields "" so read paths are
// never broken by a rotation attempt (CreateVersion/UpdateCryptoKeyAtomic
// failures are logged).
//
// A missed schedule is coalesced: NextRotationTime advances to now + period
// (not by the number of elapsed periods), so if the clock jumps far past the
// due time only a single rotation happens on the next read. This is the
// deliberate emulator approximation.
//
// There is an accepted orphan-version window: CreateVersion precedes the atomic
// primary/schedule update, so a crash between the two calls — or a concurrent
// non-rotation write that clears the rotation period — can leave an ENABLED,
// non-primary version behind. Reads never fail on this; the version is simply
// addressable and unused.
func RotateIfDue(ctx context.Context, s Store, projectID, location, keyRingID, id string, now time.Time) string {
	ck, err := s.GetCryptoKey(ctx, projectID, location, keyRingID, id)
	if err != nil || !rotationDue(ck, now) {
		return ""
	}
	rotateMu.Lock()
	defer rotateMu.Unlock()
	// Re-read under the lock so concurrent readers cannot double-rotate.
	ck, err = s.GetCryptoKey(ctx, projectID, location, keyRingID, id)
	if err != nil || !rotationDue(ck, now) {
		return ""
	}
	version, err := s.CreateVersion(ctx, projectID, location, keyRingID, id, Version{CreateTime: now, Algorithm: ck.Algorithm})
	if err != nil {
		slog.Warn("kms: lazy rotation failed", "key", id, "err", err)
		return ""
	}
	if _, err := s.UpdateCryptoKeyAtomic(ctx, projectID, location, keyRingID, id, func(cur CryptoKey) (CryptoKey, error) {
		if !rotationDue(cur, now) {
			return cur, nil
		}
		cur.PrimaryVersion = version
		cur.NextRotationTime = now.Add(cur.RotationPeriod)
		return cur, nil
	}); err != nil {
		slog.Warn("kms: lazy rotation failed", "key", id, "err", err)
		return ""
	}
	return version
}

func rotationDue(ck CryptoKey, now time.Time) bool {
	return ck.RotationPeriod > 0 && !ck.NextRotationTime.IsZero() && !now.Before(ck.NextRotationTime)
}
