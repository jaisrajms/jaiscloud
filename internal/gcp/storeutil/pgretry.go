package storeutil

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// SerializableMaxAttempts bounds retries of a Postgres transaction that fails
// with a transient serialization conflict. Concurrent writers on the same row
// can keep aborting each other, so the budget is generous and the backoff is
// jittered (below); a persistent conflict still surfaces to the caller rather
// than looping forever.
const SerializableMaxAttempts = 10

// serializableBackoffUnit is the base of the exponential backoff between
// attempts.
const serializableBackoffUnit = 2 * time.Millisecond

// serializableBackoffMax caps a single backoff interval.
const serializableBackoffMax = 200 * time.Millisecond

// IsSerializationFailure reports whether err is a transient Postgres
// transaction conflict that is safe to retry: serialization_failure (40001),
// raised when concurrent read/write sets conflict under SERIALIZABLE, or
// deadlock_detected (40P01), raised when two transactions lock rows in opposite
// orders. Both mean "retry the whole transaction", not "the request is
// invalid" — surfacing them as a 500 makes a benign race look like a crash.
//
// This mirrors the GCS object store's internal retry (store/gcs/retry.go);
// it is shared here so every Postgres store's atomic read-modify-write method
// gets the same guarantee instead of each re-implementing it.
func IsSerializationFailure(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "40001" || pgErr.Code == "40P01"
}

// RetrySerializable runs fn, re-running it while it returns a transient
// serialization conflict, up to SerializableMaxAttempts times with a jittered
// exponential backoff. fn must be a self-contained transaction that rolls back
// (and returns) on failure so re-running it is safe: every caller opens its own
// transaction, defers Rollback, and returns without committing on error.
//
// The jitter is deliberate: concurrent writers that abort together would
// otherwise retry in lockstep and keep colliding on the same row. Spreading the
// retries out lets each attempt take a fresh snapshot against a settled row.
func RetrySerializable[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	var zero T
	for attempt := 1; ; attempt++ {
		v, err := fn()
		if err == nil || !IsSerializationFailure(err) || attempt >= SerializableMaxAttempts {
			return v, err
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(serializableBackoff(attempt)):
		}
	}
}

// serializableBackoff returns the backoff before retry attempt+1: an
// exponentially growing interval (capped) with full jitter, so contenders
// desynchronize instead of repeatedly colliding.
func serializableBackoff(attempt int) time.Duration {
	d := serializableBackoffUnit << (attempt - 1)
	if d > serializableBackoffMax || d <= 0 {
		d = serializableBackoffMax
	}
	return time.Duration(rand.Int64N(int64(d) + 1))
}

// RetrySerializableErr is RetrySerializable for a transaction whose body has no
// result value (e.g. a guard-and-delete atomic method). See RetrySerializable
// for the retry/backoff contract.
func RetrySerializableErr(ctx context.Context, fn func() error) error {
	_, err := RetrySerializable(ctx, func() (struct{}, error) {
		return struct{}{}, fn()
	})
	return err
}
