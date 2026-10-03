package bigquery

import (
	"fmt"
	"testing"
	"time"
)

func TestInsertDedupTrackerTTL(t *testing.T) {
	d := newInsertDedupTracker()
	base := time.Now()
	rows := []Row{{InsertID: "a"}}

	if dups := d.filter("t", rows, base); len(dups) != 0 {
		t.Fatalf("first sighting should not be a duplicate: %v", dups)
	}
	if dups := d.filter("t", rows, base.Add(insertDedupWindow-time.Second)); len(dups) != 1 {
		t.Fatalf("id within the window should be a duplicate: %v", dups)
	}
	if dups := d.filter("t", rows, base.Add(insertDedupWindow)); len(dups) != 0 {
		t.Fatalf("id at the window boundary should have expired: %v", dups)
	}
}

func TestInsertDedupTrackerPerTableIsolation(t *testing.T) {
	d := newInsertDedupTracker()
	now := time.Now()
	if dups := d.filter("t1", []Row{{InsertID: "a"}}, now); len(dups) != 0 {
		t.Fatalf("first table: %v", dups)
	}
	if dups := d.filter("t2", []Row{{InsertID: "a"}}, now); len(dups) != 0 {
		t.Fatalf("same insertId in a different table must not dedup: %v", dups)
	}
}

func TestInsertDedupTrackerEmptyIDNeverDedups(t *testing.T) {
	d := newInsertDedupTracker()
	now := time.Now()
	rows := []Row{{InsertID: ""}}
	d.filter("t", rows, now)
	if dups := d.filter("t", rows, now); len(dups) != 0 {
		t.Fatalf("empty insertId must never dedup: %v", dups)
	}
	if len(d.tables["t"]) != 0 {
		t.Fatalf("empty insertId must not be recorded: %v", d.tables["t"])
	}
}

func TestInsertDedupTrackerBound(t *testing.T) {
	d := newInsertDedupTracker()
	now := time.Now()
	rows := make([]Row, 0, insertDedupMaxPerTable+250)
	for i := 0; i < insertDedupMaxPerTable+250; i++ {
		rows = append(rows, Row{InsertID: fmt.Sprintf("id-%d", i)})
	}
	d.filter("t", rows, now)
	if got := len(d.tables["t"]); got != insertDedupMaxPerTable {
		t.Fatalf("tracker must be bounded to %d, got %d", insertDedupMaxPerTable, got)
	}
}
