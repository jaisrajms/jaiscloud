package storeutil

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func pgErr(code string) error {
	return &pgconn.PgError{Code: code, Message: "synthetic"}
}

func TestIsSerializationFailure(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"serialization 40001", pgErr("40001"), true},
		{"deadlock 40P01", pgErr("40P01"), true},
		{"unique violation 23505", pgErr("23505"), false},
		{"plain error", errors.New("boom"), false},
		{"wrapped 40001", errors.Join(errors.New("ctx"), pgErr("40001")), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsSerializationFailure(tc.err); got != tc.want {
				t.Fatalf("IsSerializationFailure(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestRetrySerializable_RetriesTransientThenSucceeds(t *testing.T) {
	attempts := 0
	got, err := RetrySerializable(context.Background(), func() (int, error) {
		attempts++
		if attempts < 3 {
			return 0, pgErr("40001")
		}
		return 42, nil
	})
	if err != nil {
		t.Fatalf("RetrySerializable: %v", err)
	}
	if got != 42 || attempts != 3 {
		t.Fatalf("got=%d attempts=%d, want 42/3", got, attempts)
	}
}

func TestRetrySerializable_DoesNotRetryNonTransient(t *testing.T) {
	attempts := 0
	_, err := RetrySerializable(context.Background(), func() (int, error) {
		attempts++
		return 0, pgErr("23505")
	})
	if err == nil {
		t.Fatalf("expected the non-transient error to surface")
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d, want 1 (no retry for 23505)", attempts)
	}
}

func TestRetrySerializable_GivesUpAfterMaxAttempts(t *testing.T) {
	attempts := 0
	_, err := RetrySerializable(context.Background(), func() (int, error) {
		attempts++
		return 0, pgErr("40001")
	})
	if err == nil {
		t.Fatalf("expected a persistent serialization failure to surface")
	}
	if attempts != SerializableMaxAttempts {
		t.Fatalf("attempts=%d, want %d", attempts, SerializableMaxAttempts)
	}
}

func TestRetrySerializable_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	_, err := RetrySerializable(ctx, func() (int, error) {
		attempts++
		cancel() // cancel during the first attempt; the backoff must observe it
		return 0, pgErr("40001")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d, want 1", attempts)
	}
}

func TestRetrySerializable_RetriesDeadlock(t *testing.T) {
	attempts := 0
	err := RetrySerializableErr(context.Background(), func() error {
		attempts++
		if attempts < 2 {
			return pgErr("40P01")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("RetrySerializableErr: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d, want 2", attempts)
	}
}

func TestRetrySerializableErr_SurfacesPersistent(t *testing.T) {
	attempts := 0
	err := RetrySerializableErr(context.Background(), func() error {
		attempts++
		return pgErr("40001")
	})
	if err == nil {
		t.Fatalf("expected a persistent serialization failure to surface")
	}
	if attempts != SerializableMaxAttempts {
		t.Fatalf("attempts=%d, want %d", attempts, SerializableMaxAttempts)
	}
}
