package logging

import "testing"

// TestReseatSequenceSQL asserts the sequence-reseat statement used by the
// Postgres Restore is correct. It must run inside the restore transaction and
// re-seat the BIGSERIAL-backed id sequence to the max restored id (or 1 on an
// empty table), so the next Write does not collide with an explicit restored id.
func TestReseatSequenceSQL(t *testing.T) {
	want := "SELECT setval(pg_get_serial_sequence('jc_log_entries','id'), COALESCE(MAX(id),1)) FROM jc_log_entries"
	if reseatSequenceSQL != want {
		t.Fatalf("reseatSequenceSQL = %q, want %q", reseatSequenceSQL, want)
	}
}
