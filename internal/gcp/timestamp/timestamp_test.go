package timestamp

import (
	"testing"
	"time"
)

// TestProto pins the proto-typed counterpart of Canonical: a server instant
// carries no sub-microsecond digit, so a gRPC adapter and the REST renderer of
// the same google-datetime field hand a client the same value. (TD48)
func TestProto(t *testing.T) {
	in := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	got := Proto(in)
	if got.GetNanos() != 123_456_000 {
		t.Fatalf("Proto nanos = %d, want 123456000", got.GetNanos())
	}
	if want := time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC); !got.AsTime().Equal(want) {
		t.Fatalf("Proto instant = %v, want %v", got.AsTime(), want)
	}
	// A zero time is unchanged (timestamppb.New semantics are preserved): the
	// seconds/nanos must match, not merely be "not non-zero nanos".
	if z := Proto(time.Time{}); z.GetSeconds() != -62135596800 || z.GetNanos() != 0 {
		t.Fatalf("Proto(zero) = %ds/%dns, want -62135596800s/0ns", z.GetSeconds(), z.GetNanos())
	}
}

// TestCanonical pins the protojson spelling: 0/3/6-digit fraction groups, never
// trimmed below a group boundary, always UTC, truncated to microseconds.
func TestCanonical(t *testing.T) {
	tests := []struct {
		name string
		in   time.Time
		want string
	}{
		{
			name: "whole second omits the fraction",
			in:   time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC),
			want: "2024-03-01T12:00:00Z",
		},
		{
			name: "trailing-zero fraction keeps three digits",
			in:   time.Date(2024, 3, 1, 12, 0, 0, 500_000_000, time.UTC),
			want: "2024-03-01T12:00:00.500Z",
		},
		{
			name: "trailing-zero fraction keeps six digits",
			in:   time.Date(2024, 3, 1, 12, 0, 0, 123_450_000, time.UTC),
			want: "2024-03-01T12:00:00.123450Z",
		},
		{
			name: "microsecond part with no trailing zero keeps six digits",
			in:   time.Date(2024, 3, 1, 12, 0, 0, 123_456_000, time.UTC),
			want: "2024-03-01T12:00:00.123456Z",
		},
		{
			name: "sub-microsecond digits are truncated, then grouped",
			in:   time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC),
			want: "2024-03-01T12:00:00.123456Z",
		},
		{
			name: "sub-microsecond digits ending a group still truncate",
			in:   time.Date(2024, 3, 1, 12, 0, 0, 500_000_001, time.UTC),
			want: "2024-03-01T12:00:00.500Z",
		},
		{
			name: "non-UTC offset is normalized to UTC",
			in:   time.Date(2024, 3, 1, 7, 0, 0, 0, time.FixedZone("EST", -5*3600)),
			want: "2024-03-01T12:00:00Z",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Canonical(tc.in); got != tc.want {
				t.Fatalf("Canonical(%s) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestProtoJSON pins the no-truncation variant: the same 0/3/6/9 spelling, but
// the nanosecond part is the caller's and is echoed unchanged (a Firestore
// Value.timestampValue).
func TestProtoJSON(t *testing.T) {
	tests := []struct {
		nanos int
		want  string
	}{
		{0, "2024-03-01T12:00:00Z"},
		{500_000_000, "2024-03-01T12:00:00.500Z"},
		{100_000_000, "2024-03-01T12:00:00.100Z"},
		{123_456_000, "2024-03-01T12:00:00.123456Z"},
		{123_456_700, "2024-03-01T12:00:00.123456700Z"},
		{123_456_789, "2024-03-01T12:00:00.123456789Z"},
	}
	for _, tc := range tests {
		in := time.Date(2024, 3, 1, 12, 0, 0, tc.nanos, time.UTC)
		if got := ProtoJSON(in); got != tc.want {
			t.Fatalf("ProtoJSON(nanos=%d) = %q, want %q", tc.nanos, got, tc.want)
		}
		if got := in.Format(time.RFC3339Nano); tc.nanos == 500_000_000 && got != "2024-03-01T12:00:00.5Z" {
			t.Fatalf("precondition: RFC3339Nano = %q", got)
		}
	}
}

// TestCanonicalDistinguishesRFC3339Nano pins that the helper is not
// time.RFC3339Nano for the trailing-zero cases real GCP differs on.
func TestCanonicalDistinguishesRFC3339Nano(t *testing.T) {
	in := time.Date(2024, 3, 1, 12, 0, 0, 500_000_000, time.UTC)
	if got := in.Format(time.RFC3339Nano); got != "2024-03-01T12:00:00.5Z" {
		t.Fatalf("precondition: RFC3339Nano = %q", got)
	}
	if got := Canonical(in); got != "2024-03-01T12:00:00.500Z" {
		t.Fatalf("Canonical = %q, want .500", got)
	}
}

// TestTruncate pins the microsecond resolution.
func TestTruncate(t *testing.T) {
	in := time.Date(2024, 3, 1, 12, 0, 0, 123_456_789, time.UTC)
	got := Truncate(in)
	if got.Nanosecond() != 123_456_000 {
		t.Fatalf("Truncate nanos = %d, want 123456000", got.Nanosecond())
	}
}
