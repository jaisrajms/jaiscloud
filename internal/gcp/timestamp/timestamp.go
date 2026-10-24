// Package timestamp renders GCP response timestamps the way real GCP does.
//
// A field whose Discovery document marks format "google-datetime" is a proto
// google.protobuf.Timestamp, and every GCP transport that serializes it to JSON
// does so through protojson's marshalTimestamp. That is one rule — RFC 3339 in
// UTC with a fractional part of 0, 3, 6, or 9 digits (nine digits trimmed one
// whole group of three at a time, so a trailing-zero fraction keeps its group
// boundary: ".500", never time.RFC3339Nano's ".5") and no sub-microsecond digit
// (real GCP's server clock is microsecond-resolution).
//
// A JSON-API "date-time" field (Cloud Storage's timeCreated/updated) is a plain
// RFC 3339 string that keeps its own spelling and must NOT be routed through
// here; classify each field against its Discovery `format` first.
package timestamp

import (
	"strings"
	"time"
)

// ProtoJSON renders t the way real GCP's protojson transcoder renders a
// google-datetime field — RFC 3339 in UTC with a fractional part of 0, 3, 6, or
// 9 digits (nine digits trimmed one whole group of three at a time, so a
// trailing-zero fraction keeps its group boundary: ".500", never
// time.RFC3339Nano's ".5") — WITHOUT changing the instant.
//
// Use Canonical, not this, for a server-generated metadata timestamp: real
// GCP's server clock is microsecond-resolution, so a clock.Now() instant must
// be truncated. This is for a timestamp whose nanoseconds are the client's own
// (a Firestore Value.timestampValue, which Firestore holds at nanosecond
// resolution and echoes unchanged), where dropping a sub-microsecond digit
// would change the value.
func ProtoJSON(t time.Time) string {
	t = t.UTC()
	s := t.Format("2006-01-02T15:04:05.000000000")
	s = strings.TrimSuffix(s, "000")
	s = strings.TrimSuffix(s, "000")
	s = strings.TrimSuffix(s, ".000")
	return s + "Z"
}

// Canonical renders t the way real GCP's protojson transcoder renders a
// google-datetime metadata field, truncating to the microsecond resolution real
// GCP's server timestamps carry so an instant from clock.Now() never gains a
// sub-microsecond digit real GCP would not send.
func Canonical(t time.Time) string {
	return ProtoJSON(Truncate(t))
}

// Truncate returns t at the microsecond resolution real GCP holds a
// google-datetime instant at; a sub-microsecond digit is dropped.
func Truncate(t time.Time) time.Time {
	return t.Truncate(time.Microsecond)
}
