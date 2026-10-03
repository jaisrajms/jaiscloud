package gcs

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsSerializationFailure(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"serialization_failure", &pgconn.PgError{Code: "40001"}, true},
		{"deadlock_detected", &pgconn.PgError{Code: "40P01"}, true},
		{"unique_violation", &pgconn.PgError{Code: "23505"}, false},
		{"plain", errors.New("boom"), false},
		{"wrapped", fmt.Errorf("tx commit: %w", &pgconn.PgError{Code: "40001"}), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isSerializationFailure(tc.err); got != tc.want {
				t.Fatalf("isSerializationFailure(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestRetrySerializableErr_RetriesThenSucceeds(t *testing.T) {
	calls := 0
	err := retrySerializableErr(context.Background(), func() error {
		calls++
		if calls < 3 {
			return &pgconn.PgError{Code: "40001"}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestRetrySerializableErr_GivesUpAfterMaxAttempts(t *testing.T) {
	calls := 0
	err := retrySerializableErr(context.Background(), func() error {
		calls++
		return &pgconn.PgError{Code: "40001"}
	})
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "40001" {
		t.Fatalf("err = %v, want 40001 PgError", err)
	}
	if calls != serializableMaxAttempts {
		t.Fatalf("calls = %d, want %d", calls, serializableMaxAttempts)
	}
}

func TestRetrySerializableErr_NonRetryableReturnsImmediately(t *testing.T) {
	sentinel := errors.New("not a conflict")
	calls := 0
	err := retrySerializableErr(context.Background(), func() error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want sentinel", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestRetrySerializable_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	_, err := retrySerializable(ctx, func() (int, error) {
		calls++
		return 0, &pgconn.PgError{Code: "40001"}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (no retry once cancelled)", calls)
	}
}
