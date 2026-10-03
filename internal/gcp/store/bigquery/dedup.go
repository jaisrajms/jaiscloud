package bigquery

import (
	"sort"
	"sync"
	"time"
)

const (
	// insertDedupWindow is how long a streamed insertId is remembered for
	// duplicate suppression. BigQuery's insertId deduplication is documented as
	// best-effort over a short window; 10 minutes comfortably covers client
	// retry attempts without retaining IDs indefinitely.
	insertDedupWindow = 10 * time.Minute

	// insertDedupMaxPerTable caps how many insertIds are remembered per table.
	// Once the cap is exceeded the oldest entries are evicted, so a table that
	// receives only unique insertIds cannot grow the set without bound.
	insertDedupMaxPerTable = 10000
)

// insertDedupTracker remembers recently seen insertIds per table so repeated
// tabledata.insertAll retries are not double-inserted. It is best-effort and
// process-local: the window is TTL'd and the set is count-bounded.
type insertDedupTracker struct {
	mu     sync.Mutex
	tables map[string]map[string]time.Time // tableKey → insertId → first-seen time
}

func newInsertDedupTracker() *insertDedupTracker {
	return &insertDedupTracker{tables: map[string]map[string]time.Time{}}
}

// filter returns the ascending indices of rows whose non-empty InsertID was
// already recorded for tableKey, and records the InsertIDs of the remaining
// rows. now is caller-supplied so the TTL is testable. Holding d.mu across the
// whole check-and-mark makes concurrent same-id inserts deterministic: exactly
// one caller sees a row as new.
func (d *insertDedupTracker) filter(tableKey string, rows []Row, now time.Time) []int {
	d.mu.Lock()
	defer d.mu.Unlock()

	seen := d.tables[tableKey]
	if seen == nil {
		seen = map[string]time.Time{}
		d.tables[tableKey] = seen
	}
	for id, at := range seen {
		if now.Sub(at) >= insertDedupWindow {
			delete(seen, id)
		}
	}

	var dups []int
	for i := range rows {
		id := rows[i].InsertID
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			dups = append(dups, i)
			continue
		}
		seen[id] = now
	}
	if len(seen) > insertDedupMaxPerTable {
		evictOldest(seen, len(seen)-insertDedupMaxPerTable)
	}
	return dups
}

// forget drops all remembered insertIds for a single table.
func (d *insertDedupTracker) forget(tableKey string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.tables, tableKey)
}

// reset drops every remembered insertId.
func (d *insertDedupTracker) reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.tables = map[string]map[string]time.Time{}
}

// forgetPrefix drops all remembered insertIds for tables whose scope key starts
// with prefix (used when a whole dataset is deleted).
func (d *insertDedupTracker) forgetPrefix(prefix string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for k := range d.tables {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			delete(d.tables, k)
		}
	}
}

// evictOldest removes the n oldest entries from seen.
func evictOldest(seen map[string]time.Time, n int) {
	type entry struct {
		id string
		at time.Time
	}
	entries := make([]entry, 0, len(seen))
	for id, at := range seen {
		entries = append(entries, entry{id, at})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].at.Before(entries[j].at) })
	for i := 0; i < n && i < len(entries); i++ {
		delete(seen, entries[i].id)
	}
}
