package firestore

import (
	"encoding/json"
	"testing"
	"time"
)

// TestValueTimestampSpelling pins that the REST Value codec spells a
// timestampValue the way the gRPC protojson codec does (0/3/6/9-digit fraction
// groups) without dropping the client's nanosecond part: Firestore holds a
// timestamp at nanosecond resolution, so a trailing-zero fraction must not be
// spelled RFC3339Nano's way (".5") and the instant must be unchanged.
func TestValueTimestampSpelling(t *testing.T) {
	cases := []struct {
		in   time.Time
		want string
	}{
		{time.Date(2026, 9, 5, 12, 0, 0, 500_000_000, time.UTC), "2026-09-05T12:00:00.500Z"},
		{time.Date(2026, 9, 5, 12, 0, 0, 123_456_700, time.UTC), "2026-09-05T12:00:00.123456700Z"},
		{time.Date(2026, 9, 5, 12, 0, 0, 123_456_789, time.UTC), "2026-09-05T12:00:00.123456789Z"},
	}
	for _, tc := range cases {
		b, err := json.Marshal(TimestampVal(tc.in))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got := m["timestampValue"]; got != tc.want {
			t.Fatalf("timestampValue = %v, want %v", got, tc.want)
		}
	}
}
