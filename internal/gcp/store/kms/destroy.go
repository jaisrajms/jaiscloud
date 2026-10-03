package kms

import (
	"context"
	"log/slog"
	"time"
)

// DefaultDestroyScheduledDuration is Cloud KMS's default window between
// DestroyCryptoKeyVersion (which moves a version to DESTROY_SCHEDULED) and the
// automatic transition to DESTROYED. Cloud KMS lets each CryptoKey override the
// period at creation (destroy_scheduled_duration, immutable) and organizations
// enforce a minimum; this emulator models only the documented 30-day default.
const DefaultDestroyScheduledDuration = 30 * 24 * time.Hour

// PromoteDestroyedIfDue lazily applies pending destruction for one crypto key:
// every DESTROY_SCHEDULED version whose DestroyTime has been reached is moved to
// DESTROYED. Cloud KMS performs this transition automatically; the emulator has
// no scheduler, so read and state-inspecting paths call this on access (mirrors
// RotateIfDue for rotation schedules).
//
// It is best-effort: store errors are logged and never returned, so reads are
// never broken by a failed promotion.
func PromoteDestroyedIfDue(ctx context.Context, s Store, projectID, location, keyringID, keyID string, now time.Time) {
	if _, err := s.PromoteDestroyed(ctx, projectID, location, keyringID, keyID, now); err != nil {
		slog.Warn("kms: lazy destroy promotion failed", "key", keyID, "err", err)
	}
}
