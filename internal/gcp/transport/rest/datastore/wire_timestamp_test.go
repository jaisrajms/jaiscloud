package datastore

import (
	"strconv"
	"testing"
	"time"

	"jaiscloud/internal/model"
)

// timestampCommitBody builds a NON_TRANSACTIONAL commit upsert of one Task with
// the given properties.
func timestampCommitBody(name string, props map[string]any) map[string]any {
	return map[string]any{
		"mode": "NON_TRANSACTIONAL",
		"mutations": []any{map[string]any{"upsert": map[string]any{
			"key":        nameKey("Task", name),
			"properties": props,
		}}},
	}
}

// lookupProperty returns the wire value of one property of a stored Task.
func lookupProperty(t *testing.T, c *Codec, p *Provider, name, prop string) map[string]any {
	t.Helper()
	resp, err := call(t, c, p, "test", "lookup", map[string]any{"keys": []any{nameKey("Task", name)}})
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	found, _ := resp.Data["found"].([]any)
	if len(found) != 1 {
		t.Fatalf("found = %v, want 1", resp.Data["found"])
	}
	ent := found[0].(map[string]any)["entity"].(map[string]any)
	props := ent["properties"].(map[string]any)
	pv, _ := props[prop].(map[string]any)
	return pv
}

// TestRESTTimestampValueNormalizedOnWrite pins TD41: a timestampValue with a
// non-UTC offset is stored and echoed in canonical UTC (the gRPC transport's
// form), including nested inside an arrayValue.
func TestRESTTimestampValueNormalizedOnWrite(t *testing.T) {
	c, p := newTestProvider(t)
	_, err := call(t, c, p, "test", "commit", timestampCommitBody("ts", map[string]any{
		"t": map[string]any{"timestampValue": "2019-12-31T19:00:00-05:00"},
		"arr": map[string]any{"arrayValue": map[string]any{"values": []any{
			map[string]any{"timestampValue": "2020-01-01T05:30:00+05:30"},
		}}},
	}))
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	tv := lookupProperty(t, c, p, "ts", "t")
	if got := tv["timestampValue"]; got != "2020-01-01T00:00:00Z" {
		t.Fatalf("timestampValue = %v, want 2020-01-01T00:00:00Z", got)
	}

	av := lookupProperty(t, c, p, "ts", "arr")
	values, _ := av["arrayValue"].(map[string]any)["values"].([]any)
	if len(values) != 1 {
		t.Fatalf("arrayValue.values = %v", av)
	}
	if got := values[0].(map[string]any)["timestampValue"]; got != "2020-01-01T00:00:00Z" {
		t.Fatalf("nested timestampValue = %v, want 2020-01-01T00:00:00Z", got)
	}
}

// TestRESTTimestampValuePrecisionOnWrite pins TD42: a timestampValue is stored
// and echoed at Datastore's microsecond resolution (sub-microsecond digits are
// dropped) and spelled the way real GCP's protojson transcoder spells a proto
// Timestamp (0, 3, or 6 digits — a one-digit fraction is padded to three, not
// trimmed as time.RFC3339Nano would).
func TestRESTTimestampValuePrecisionOnWrite(t *testing.T) {
	cases := []struct{ in, want string }{
		{"2020-01-01T00:00:00.5Z", "2020-01-01T00:00:00.500Z"},
		{"2020-01-01T00:00:00.120Z", "2020-01-01T00:00:00.120Z"},
		{"2020-01-01T00:00:00.123456789Z", "2020-01-01T00:00:00.123456Z"},
		{"2020-01-01T00:00:00.9999999Z", "2020-01-01T00:00:00.999999Z"},
	}
	for i, tc := range cases {
		c, p := newTestProvider(t)
		name := "ts-" + strconv.Itoa(i)
		if _, err := call(t, c, p, "test", "commit", timestampCommitBody(name, map[string]any{
			"t": map[string]any{"timestampValue": tc.in},
		})); err != nil {
			t.Fatalf("commit(%q): %v", tc.in, err)
		}
		tv := lookupProperty(t, c, p, name, "t")
		if got := tv["timestampValue"]; got != tc.want {
			t.Errorf("commit(%q): timestampValue = %v, want %q", tc.in, got, tc.want)
		}
	}
}

// TestRESTUpdateTimePreconditionTruncated pins TD42: a client-supplied
// Mutation.update_time precondition carrying sub-microsecond digits is truncated
// to Datastore's microsecond resolution before it is compared, so it still
// matches the stored (microsecond) update_time instead of reporting a spurious
// conflict.
func TestRESTUpdateTimePreconditionTruncated(t *testing.T) {
	p := preconditionFromWire(map[string]any{"updateTime": "2020-01-01T00:00:00.123456789Z"})
	if p == nil || p.UpdateTime == nil {
		t.Fatalf("precondition = %+v, want an updateTime precondition", p)
	}
	want := time.Date(2020, 1, 1, 0, 0, 0, 123456000, time.UTC)
	if !p.UpdateTime.Equal(want) {
		t.Fatalf("UpdateTime = %v, want %v", p.UpdateTime, want)
	}
}

// TestRESTTimestampValueRejectsMalformed pins that an unparseable (or
// non-string) timestampValue is INVALID_ARGUMENT at the decode boundary rather
// than a value stored verbatim, including when nested inside an array; real GCP
// parses it as a proto Timestamp and rejects a malformed string. (TD41)
func TestRESTTimestampValueRejectsMalformed(t *testing.T) {
	cases := []struct {
		name  string
		value any
	}{
		{"unparseable string", map[string]any{"timestampValue": "not-a-time"}},
		{"date only", map[string]any{"timestampValue": "2020-01-01"}},
		{"non-string", map[string]any{"timestampValue": 12345}},
		// Go's parser silently truncates a fraction longer than nine digits and
		// accepts a year outside the proto Timestamp range; protojson rejects
		// both, so the decode boundary must too. (TD46)
		{"fraction longer than nine digits", map[string]any{"timestampValue": "2020-01-01T00:00:00.1234567890Z"}},
		{"year below the proto range", map[string]any{"timestampValue": "0000-01-01T00:00:00Z"}},
		{"offset past the proto range", map[string]any{"timestampValue": "9999-12-31T23:59:59-01:00"}},
		{"nested unparseable", map[string]any{"arrayValue": map[string]any{"values": []any{
			map[string]any{"timestampValue": "nope"},
		}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, p := newTestProvider(t)
			_, err := call(t, c, p, "test", "commit", timestampCommitBody("bad", map[string]any{"t": tc.value}))
			perr, ok := err.(*model.ProviderError)
			if !ok {
				t.Fatalf("err = %v (%T), want *model.ProviderError", err, err)
			}
			if perr.HTTPStatus != 400 || perr.Code != "InvalidArgument" {
				t.Fatalf("err = %+v, want 400/InvalidArgument", perr)
			}
		})
	}
}
