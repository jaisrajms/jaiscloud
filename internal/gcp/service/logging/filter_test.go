package logging

import (
	"testing"
	"time"

	loggingstore "jaiscloud/internal/gcp/store/logging"
)

func entry(severity int, logName string, ts time.Time) loggingstore.LogEntry {
	return loggingstore.LogEntry{LogName: logName, Severity: severity, Timestamp: ts}
}

func eval(t *testing.T, filter string, e loggingstore.LogEntry) bool {
	t.Helper()
	pred, err := CompileFilter(filter)
	if err != nil {
		t.Fatalf("CompileFilter(%q): %v", filter, err)
	}
	return pred.Match(e)
}

func TestFilterLogNameEquality(t *testing.T) {
	name := "projects/p1/logs/mylog"
	e := entry(200, name, time.Time{})
	if !eval(t, `logName="projects/p1/logs/mylog"`, e) {
		t.Fatal("expected logName match")
	}
	if eval(t, `logName="projects/p1/logs/other"`, e) {
		t.Fatal("expected logName mismatch")
	}
	// Single-quoted string is also accepted.
	if !eval(t, `logName='projects/p1/logs/mylog'`, e) {
		t.Fatal("expected single-quoted logName match")
	}
}

func TestFilterSeverity(t *testing.T) {
	errEntry := entry(500, "projects/p1/logs/a", time.Time{})  // ERROR
	infoEntry := entry(200, "projects/p1/logs/a", time.Time{}) // INFO

	cases := []struct {
		filter string
		e      loggingstore.LogEntry
		want   bool
	}{
		{"severity>=WARNING", errEntry, true},
		{"severity>=WARNING", infoEntry, false},
		{"severity>=400", errEntry, true},
		{"severity>WARNING", errEntry, true},
		{"severity>ERROR", errEntry, false},
		{"severity<WARNING", infoEntry, true},
		{"severity<=INFO", infoEntry, true},
		{"severity=ERROR", errEntry, true},
		{"severity=ERROR", infoEntry, false},
		{"severity!=ERROR", infoEntry, true},
	}
	for _, c := range cases {
		if got := eval(t, c.filter, c.e); got != c.want {
			t.Errorf("%q on severity=%d: got %v want %v", c.filter, c.e.Severity, got, c.want)
		}
	}
}

func TestFilterAndCombination(t *testing.T) {
	name := "projects/p1/logs/mylog"
	errEntry := entry(500, name, time.Time{})
	infoEntry := entry(200, name, time.Time{})

	filter := `logName="projects/p1/logs/mylog" AND severity>=WARNING`
	if !eval(t, filter, errEntry) {
		t.Fatal("expected AND match for ERROR")
	}
	if eval(t, filter, infoEntry) {
		t.Fatal("expected AND mismatch for INFO")
	}
}

func TestFilterOrNot(t *testing.T) {
	name := "projects/p1/logs/mylog"
	e := entry(500, name, time.Time{})

	if !eval(t, `logName="x" OR severity>=WARNING`, e) {
		t.Fatal("expected OR match via severity")
	}
	if !eval(t, `NOT logName="x"`, e) {
		t.Fatal("expected NOT match")
	}
	if eval(t, `NOT severity>=WARNING`, e) {
		t.Fatal("expected NOT mismatch")
	}
	// Parenthesized grouping.
	if !eval(t, `NOT (logName="x" OR logName="y")`, e) {
		t.Fatal("expected parenthesized NOT match")
	}
}

func TestFilterPrecedenceOrBindsTighter(t *testing.T) {
	eA := entry(200, "a", time.Time{}) // INFO, logName "a"
	eB := entry(200, "b", time.Time{}) // INFO, logName "b"
	eC := entry(500, "c", time.Time{}) // ERROR, logName "c"

	f := `logName="a" OR logName="b" AND severity>=ERROR`
	grouped := `(logName="a" OR logName="b") AND severity>=ERROR`

	// With OR binding tighter than AND, f == (a OR b) AND severity>=ERROR.
	// eA (a, INFO) must NOT match: (a OR b) is true but severity>=ERROR is false.
	if eval(t, f, eA) {
		t.Fatal("eA (a, INFO) must not match: OR binds tighter than AND")
	}
	// eC (c, ERROR) must NOT match: (a OR b) is false.
	if eval(t, f, eC) {
		t.Fatal("eC (c, ERROR) must not match")
	}
	// eB (b, INFO) must NOT match for the same reason as eA.
	if eval(t, f, eB) {
		t.Fatal("eB (b, INFO) must not match")
	}
	// The implicit grouping must equal the explicit parenthesization.
	for _, e := range []loggingstore.LogEntry{eA, eB, eC} {
		if eval(t, f, e) != eval(t, grouped, e) {
			t.Errorf("precedence mismatch for %+v: %q vs %q", e, f, grouped)
		}
	}
}

func TestFilterResourceType(t *testing.T) {
	e := loggingstore.LogEntry{LogName: "projects/p1/logs/a", Severity: 200, ResourceType: "global"}
	if !eval(t, `resource.type="global"`, e) {
		t.Fatal("expected resource.type match")
	}
	if eval(t, `resource.type="gce_instance"`, e) {
		t.Fatal("expected resource.type mismatch")
	}
	if !eval(t, `resource.type!="gce_instance"`, e) {
		t.Fatal("expected resource.type != match")
	}
}

func TestFilterTextPayload(t *testing.T) {
	e := loggingstore.LogEntry{LogName: "projects/p1/logs/a", Severity: 200, PayloadType: "text", TextPayload: "hello world"}
	if !eval(t, `textPayload:"world"`, e) {
		t.Fatal("expected textPayload substring match")
	}
	if eval(t, `textPayload:"missing"`, e) {
		t.Fatal("expected textPayload substring mismatch")
	}
	if !eval(t, `textPayload="hello world"`, e) {
		t.Fatal("expected textPayload equality match")
	}
}

func TestFilterTimestamp(t *testing.T) {
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	e := entry(200, "projects/p1/logs/a", ts)
	if !eval(t, `timestamp>="2025-12-31T00:00:00Z"`, e) {
		t.Fatal("expected timestamp match")
	}
	if eval(t, `timestamp>="2026-06-01T00:00:00Z"`, e) {
		t.Fatal("expected timestamp mismatch")
	}
}

func TestFilterEmptyAndErrors(t *testing.T) {
	e := entry(200, "projects/p1/logs/a", time.Time{})
	if !eval(t, "", e) {
		t.Fatal("empty filter should match all")
	}
	if !eval(t, "   ", e) {
		t.Fatal("blank filter should match all")
	}

	for _, bad := range []string{
		`logName=`,              // missing value
		`severity >`,            // missing value
		`foo>=1`,                // unsupported field
		`logName="x" AND`,       // dangling operator
		`(logName="x"`,          // missing close paren
		`severity ~= WARNING`,   // unsupported operator
		`logName="unterminated`, // unterminated string
		`logName>"x"`,           // supported field, but > isn't implemented for it
		`severity:5`,            // supported field, but : isn't implemented for it
		`resource.type>"x"`,     // supported field, but > isn't implemented for it
		`timestamp:"x"`,         // supported field, but : isn't implemented for it
	} {
		if _, err := CompileFilter(bad); err == nil {
			t.Errorf("expected error for filter %q", bad)
		}
	}
}
