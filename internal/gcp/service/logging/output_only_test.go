package logging

import (
	"context"
	"testing"

	loggingstore "jaiscloud/internal/gcp/store/logging"
)

// TestWriteAssignsOutputOnlyFields checks that Cloud Logging's output-only
// insertId and receiveTimestamp are stamped at write (AUD6-5): a generated
// insertId when the client omitted one, and a receiveTimestamp on every entry.
func TestWriteAssignsOutputOnlyFields(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	const logName = "projects/test/logs/go-log"
	if err := s.WriteEntries(ctx, &WriteRequest{Entries: []loggingstore.LogEntry{
		textEntry(logName, "no-insert-id", 200),
	}}); err != nil {
		t.Fatalf("WriteEntries: %v", err)
	}

	res, err := s.ListEntries(ctx, &ListEntriesRequest{ResourceNames: []string{"projects/test"}})
	if err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	if len(res.Entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(res.Entries))
	}
	e := res.Entries[0]
	if e.InsertID == "" {
		t.Fatal("insertId must be generated when the client omitted one")
	}
	if e.ReceiveTimestamp.IsZero() {
		t.Fatal("receiveTimestamp must be stamped at write")
	}
}

// TestWritePreservesClientInsertId checks that a client-supplied insertId is not
// overwritten (Cloud Logging only assigns one when it is omitted).
func TestWritePreservesClientInsertId(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	e := textEntry("projects/test/logs/go-log", "client-id", 200)
	e.InsertID = "client-supplied"
	if err := s.WriteEntries(ctx, &WriteRequest{Entries: []loggingstore.LogEntry{e}}); err != nil {
		t.Fatalf("WriteEntries: %v", err)
	}

	res, err := s.ListEntries(ctx, &ListEntriesRequest{ResourceNames: []string{"projects/test"}})
	if err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	if got := res.Entries[0].InsertID; got != "client-supplied" {
		t.Fatalf("insertId = %q, want client-supplied", got)
	}
}
