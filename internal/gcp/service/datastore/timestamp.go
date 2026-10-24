package datastore

import (
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/gcp/timestamp"
)

// microsecond is Datastore's timestamp resolution. Real Datastore stores a
// timestamp "precise only to microseconds; any additional precision is rounded
// down" (Datastore REST reference, Value.timestampValue):
// https://cloud.google.com/datastore/docs/reference/data/rest/Shared.Types/Value
// so a value carrying sub-microsecond digits is held and echoed without them.
const microsecond = time.Microsecond

// truncateTimestamp returns t at Datastore's timestamp resolution (microsecond),
// so a response-level timestamp — read_time, commit_time — carries no
// sub-microsecond digit real GCP would not send. Real Datastore holds every
// timestamp at microseconds, exactly as CanonicalTimestamp renders a stored
// value and as the store already truncates an entity's create/update stamps, so
// both transports hand a client the same instant.
func truncateTimestamp(t time.Time) time.Time {
	return timestamp.Truncate(t)
}

// The proto google.protobuf.Timestamp bounds real GCP enforces, from
// encoding/protojson/well_known_types.go unmarshalTimestamp: an instant lies in
// [0001-01-01T00:00:00Z, 9999-12-31T23:59:59Z], and a string form carries at
// most nine fractional-second digits.
const (
	minTimestampSeconds = -62135596800 // seconds between 1970-01-01 and 0001-01-01
	maxTimestampSeconds = 253402300799 // seconds between 1970-01-01 and 9999-12-31T23:59:59
	maxFractionDigits   = 9
)

// ValidateTimestamp reports whether t is in the range a proto
// google.protobuf.Timestamp can represent — [0001-01-01T00:00:00Z,
// 9999-12-31T23:59:59.999999999Z]. Real GCP rejects an out-of-range instant
// (protojson answers INVALID_ARGUMENT; the proto value itself is invalid) and a
// Go time.Time has no such bound, so every decode boundary must impose it before
// the instant is stored as a string the read path could not parse back. (TD46)
func ValidateTimestamp(t time.Time) error {
	if secs := t.Unix(); secs < minTimestampSeconds || secs > maxTimestampSeconds {
		return fmt.Errorf("timestamp %q out of range", t.Format(time.RFC3339Nano))
	}
	return nil
}

// ParseTimestamp parses the store's timestamp form: an RFC 3339 string. A
// Datastore timestamp is an instant (the wire proto Timestamp is seconds+nanos
// since the epoch), so the string's UTC offset and fractional-second spelling
// are presentation only; the parser accepts any RFC 3339 offset. It is as strict
// as real GCP's protojson: it rejects what Go's time.Parse silently accepts — a
// fractional part longer than nine digits (truncated to nanoseconds) and an
// instant outside the proto Timestamp range — both of which protojson's
// unmarshalTimestamp rejects. (TD41, TD46)
func ParseTimestamp(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, err
	}
	if err := ValidateTimestamp(t); err != nil {
		return time.Time{}, err
	}
	// protojson measures the raw fraction — from the '.' to the start of the
	// timezone field — against ".999999999" and rejects a longer one, where Go's
	// parser instead truncates it to nanoseconds. (TD46)
	if i := strings.LastIndexByte(s, '.'); i >= 0 {
		if j := strings.LastIndexAny(s, "Z-+"); j >= i && j-i-1 > maxFractionDigits {
			return time.Time{}, fmt.Errorf("invalid timestamp %q", s)
		}
	}
	return t, nil
}

// CanonicalTimestamp renders an instant in the store's canonical timestamp
// form: RFC 3339 in UTC, truncated to Datastore's microsecond resolution, and
// spelled the way real GCP's protojson transcoder spells a proto Timestamp.
//
// Real Datastore normalizes every timestamp to UTC on write and drops
// sub-microsecond precision; both transports hold this one form so a value
// written over one transport reads back byte-identical over the other, and a
// REST read echoes exactly what real GCP would (a value that differs only below
// the microsecond is the same stored value).
func CanonicalTimestamp(t time.Time) string {
	// The one protojson speller shared by every service that renders a
	// google-datetime field; Datastore's metadata timestamps use it verbatim
	// (RFC 3339 in UTC, 0/3/6-digit fraction, microsecond resolution).
	return timestamp.Canonical(t)
}

// NormalizeTimestamp parses an RFC 3339 timestamp and returns its canonical UTC
// spelling. An unparseable string is an error: real GCP rejects a malformed
// timestampValue with INVALID_ARGUMENT rather than storing it, and the store
// must never hold a value neither transport can render.
func NormalizeTimestamp(s string) (string, error) {
	// RFC 3339 — the form the proto JSON mapping specifies for a Timestamp and
	// the form real GCP emits — uses '.' as the fractional-second separator; Go's
	// time.Parse also accepts ISO 8601's comma. Go's own protojson inherits that
	// laxness (it delegates to time.Parse), so the comma is rejected explicitly
	// here rather than followed. (TD41; recorded as TD49.)
	if strings.ContainsRune(s, ',') {
		return "", fmt.Errorf("invalid timestamp %q", s)
	}
	t, err := ParseTimestamp(s)
	if err != nil {
		return "", err
	}
	return CanonicalTimestamp(t), nil
}

// timestampInstant parses a timestamp value to the instant Datastore compares
// at — truncated to the store's microsecond resolution. Real Datastore stores a
// timestamp truncated to microseconds, so two values that differ only below the
// microsecond are the same stored value and compare equal (and order-equal).
// Both the stored form and a query operand are canonicalized on write, so this
// is a backstop for a value that reached the store by another path.
func timestampInstant(s string) (time.Time, error) {
	t, err := ParseTimestamp(s)
	if err != nil {
		return time.Time{}, err
	}
	return t.Truncate(microsecond), nil
}
