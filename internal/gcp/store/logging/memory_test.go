package logging

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func testEntry(logName, text string, severity int, ts time.Time) LogEntry {
	return LogEntry{LogName: logName, Severity: severity, PayloadType: "text", TextPayload: text, Timestamp: ts}
}

func TestMemoryStoreCRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	base := time.Now().UTC()

	if got, err := s.List(ctx, "p1"); err != nil || len(got) != 0 {
		t.Fatalf("initial list = %v, %v", got, err)
	}

	if err := s.Write(ctx, "p1", testEntry("projects/p1/logs/a", "a1", 200, base)); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, "p1", testEntry("projects/p1/logs/b", "b1", 500, base.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, "p2", testEntry("projects/p2/logs/c", "c1", 400, base)); err != nil {
		t.Fatal(err)
	}

	got, err := s.List(ctx, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].TextPayload != "a1" || got[1].TextPayload != "b1" {
		t.Fatalf("list = %+v", got)
	}

	logs, err := s.ListLogs(ctx, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 2 || logs[0] != "projects/p1/logs/a" || logs[1] != "projects/p1/logs/b" {
		t.Fatalf("logs = %v", logs)
	}

	if err := s.DeleteLog(ctx, "p1", "projects/p1/logs/a"); err != nil {
		t.Fatal(err)
	}
	got, _ = s.List(ctx, "p1")
	if len(got) != 1 || got[0].TextPayload != "b1" {
		t.Fatalf("after delete = %+v", got)
	}
}

func TestMemoryStoreScopeIsolation(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	base := time.Now().UTC()

	if err := s.Write(ctx, "projects/p1", testEntry("projects/p1/logs/a", "proj-a", 200, base)); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ctx, "organizations/123", testEntry("organizations/123/logs/a", "org-a", 200, base)); err != nil {
		t.Fatal(err)
	}

	proj, err := s.List(ctx, "projects/p1")
	if err != nil || len(proj) != 1 || proj[0].TextPayload != "proj-a" {
		t.Fatalf("projects/p1 list = %+v, %v", proj, err)
	}
	org, err := s.List(ctx, "organizations/123")
	if err != nil || len(org) != 1 || org[0].TextPayload != "org-a" {
		t.Fatalf("organizations/123 list = %+v, %v", org, err)
	}

	projLogs, err := s.ListLogs(ctx, "projects/p1")
	if err != nil || len(projLogs) != 1 || projLogs[0] != "projects/p1/logs/a" {
		t.Fatalf("projects/p1 logs = %v, %v", projLogs, err)
	}
	orgLogs, err := s.ListLogs(ctx, "organizations/123")
	if err != nil || len(orgLogs) != 1 || orgLogs[0] != "organizations/123/logs/a" {
		t.Fatalf("organizations/123 logs = %v, %v", orgLogs, err)
	}

	if err := s.DeleteLog(ctx, "organizations/123", "organizations/123/logs/a"); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.List(ctx, "organizations/123"); len(got) != 0 {
		t.Fatalf("organizations/123 after delete = %+v, want empty", got)
	}
	if got, _ := s.List(ctx, "projects/p1"); len(got) != 1 {
		t.Fatalf("projects/p1 must be untouched by org delete = %+v", got)
	}
}

func TestMemoryStoreReset(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.Write(ctx, "p1", testEntry("projects/p1/logs/a", "a", 200, time.Now()))
	s.Reset(ctx)
	if got, _ := s.List(ctx, "p1"); len(got) != 0 {
		t.Fatalf("after reset = %+v", got)
	}
	if logs, _ := s.ListLogs(ctx, "p1"); len(logs) != 0 {
		t.Fatalf("logs after reset = %v", logs)
	}
}

func TestMemoryStoreSnapshot(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	_ = s.Write(ctx, "p1", testEntry("projects/p1/logs/a", "a", 200, time.Now().UTC()))

	var buf bytes.Buffer
	if err := s.Snapshot(ctx, &buf); err != nil {
		t.Fatal(err)
	}

	s2 := NewMemoryStore()
	if err := s2.Restore(ctx, &buf); err != nil {
		t.Fatal(err)
	}
	got, _ := s2.List(ctx, "p1")
	if len(got) != 1 || got[0].TextPayload != "a" {
		t.Fatalf("restored = %+v", got)
	}
	// nextID must advance past the restored entry's ID.
	if err := s2.Write(ctx, "p1", testEntry("projects/p1/logs/b", "b", 300, time.Now().UTC())); err != nil {
		t.Fatal(err)
	}
	got, _ = s2.List(ctx, "p1")
	if len(got) != 2 {
		t.Fatalf("after append = %+v", got)
	}
}
