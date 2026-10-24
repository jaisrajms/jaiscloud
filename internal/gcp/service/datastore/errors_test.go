package datastore

import (
	"errors"
	"fmt"
	"testing"

	"jaiscloud/internal/model"

	dsstore "jaiscloud/internal/gcp/store/datastore"
)

// asProviderError extracts the *model.ProviderError from an error, failing the
// test when err is not one (the core always surfaces structured errors).
func asProviderError(t *testing.T, err error) *model.ProviderError {
	t.Helper()
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("error %v (%T) is not a *model.ProviderError", err, err)
	}
	return pe
}

// TestErrorConstructorsCodeAndStatus locks the code/HTTP status each Datastore
// core error constructor emits. The transports map these to the wire status
// (gRPC code / REST envelope), so a drift here silently changes every failure
// response.
func TestErrorConstructorsCodeAndStatus(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		code   string
		status int
	}{
		{"invalidArgument", invalidArgument("m"), "InvalidArgument", 400},
		{"failedPrecondition", failedPrecondition("m", 412), "FailedPrecondition", 412},
		{"alreadyExists", alreadyExists("m"), "AlreadyExists", 409},
		{"notFound", notFound("m"), "NotFound", 404},
		{"aborted", aborted("m"), "Aborted", 409},
	}
	for _, tc := range cases {
		pe := asProviderError(t, tc.err)
		if pe.Code != tc.code {
			t.Errorf("%s code = %q, want %q", tc.name, pe.Code, tc.code)
		}
		if pe.HTTPStatus != tc.status {
			t.Errorf("%s HTTP status = %d, want %d", tc.name, pe.HTTPStatus, tc.status)
		}
		if pe.Message != "m" {
			t.Errorf("%s message = %q, want %q", tc.name, pe.Message, "m")
		}
	}
}

// TestMapStoreErrorMapsEverySentinel verifies each store sentinel maps to the
// documented transport-neutral error (code + HTTP status). The store returns
// sentinels via errors.Is, so a wrapped sentinel must map identically.
func TestMapStoreErrorMapsEverySentinel(t *testing.T) {
	cases := []struct {
		name   string
		in     error
		code   string
		status int
	}{
		{"aborted", dsstore.ErrAborted, "Aborted", 409},
		{"conflict", dsstore.ErrConflict, "FailedPrecondition", 400},
		{"entityExists", dsstore.ErrEntityExists, "AlreadyExists", 409},
		{"entityNotFound", dsstore.ErrEntityNotFound, "NotFound", 404},
		{"invalidKey", dsstore.ErrInvalidKey, "InvalidArgument", 400},
	}
	for _, tc := range cases {
		for _, in := range []error{tc.in, fmt.Errorf("wrapped: %w", tc.in)} {
			pe := asProviderError(t, mapStoreError(in))
			if pe.Code != tc.code || pe.HTTPStatus != tc.status {
				t.Errorf("mapStoreError(%s) = {%q,%d}, want {%q,%d}", tc.name, pe.Code, pe.HTTPStatus, tc.code, tc.status)
			}
		}
	}
}

// TestMapStoreErrorPassesUnknownThrough verifies a non-sentinel error is
// returned unchanged (not wrapped in a spurious ProviderError) so an
// unexpected backend failure keeps its own type/message.
func TestMapStoreErrorPassesUnknownThrough(t *testing.T) {
	unknown := errors.New("boom")
	if got := mapStoreError(unknown); got != unknown {
		t.Fatalf("mapStoreError(unknown) = %v, want the original error", got)
	}
}
