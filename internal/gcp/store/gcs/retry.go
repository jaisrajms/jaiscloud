package gcs

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// serializableMaxAttempts bounds retries of a transaction that fails with a
// transient Postgres conflict. A handful of attempts with a short linear
// backoff is ample for the write skew this store sees (concurrent JSON-API and
// Spark-executor writers touching overlapping object prefixes); a persistent
// conflict still surfaces to the caller rather than looping forever.
const serializableMaxAttempts = 5

// serializableBackoffUnit is the linear backoff increment between attempts.
const serializableBackoffUnit = 2 * time.Millisecond

// isSerializationFailure reports whether err is a transient Postgres
// transaction conflict that is safe to retry. SERIALIZABLE transactions raise
// serialization_failure (40001) when concurrent read/write sets conflict, and
// deadlock_detected (40P01) when two transactions lock rows in opposite
// orders. Both mean "retry the whole transaction", not "the request is
// invalid" — surfacing them as a 500 makes a benign race look like a crash.
func isSerializationFailure(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "40001" || pgErr.Code == "40P01"
}

// retrySerializable re-runs fn while it returns a transient serialization
// conflict, up to serializableMaxAttempts times, with a short backoff between
// tries. fn must be a self-contained transaction that rolls back (and returns)
// on failure so re-running it is safe: every caller here opens its own
// transaction, defers Rollback, and returns without committing on error.
func retrySerializable[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	var zero T
	for attempt := 1; ; attempt++ {
		v, err := fn()
		if err == nil || !isSerializationFailure(err) || attempt >= serializableMaxAttempts {
			return v, err
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(time.Duration(attempt) * serializableBackoffUnit):
		}
	}
}

// retrySerializableErr is retrySerializable for transactions with no value.
func retrySerializableErr(ctx context.Context, fn func() error) error {
	_, err := retrySerializable(ctx, func() (struct{}, error) {
		return struct{}{}, fn()
	})
	return err
}
