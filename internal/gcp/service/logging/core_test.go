package logging

import (
	"context"
	"testing"

	loggingstore "jaiscloud/internal/gcp/store/logging"
)

func newTestService() *Service {
	return NewService(loggingstore.NewMemoryStore(), "test")
}

func textEntry(logName, text string, severity int) loggingstore.LogEntry {
	return loggingstore.LogEntry{
		LogName:      logName,
		ResourceType: "global",
		Severity:     severity,
		PayloadType:  "text",
		TextPayload:  text,
	}
}

func TestCoreWriteAndList(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	const logName = "projects/test/logs/go-log"
	if err := s.WriteEntries(ctx, &WriteRequest{Entries: []loggingstore.LogEntry{
		textEntry(logName, "info", 200),
		textEntry(logName, "error", 500),
	}}); err != nil {
		t.Fatalf("WriteEntries: %v", err)
	}

	res, err := s.ListEntries(ctx, &ListEntriesRequest{
		ResourceNames: []string{"projects/test"},
		Filter:        `logName="` + logName + `"`,
	})
	if err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	if len(res.Entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(res.Entries))
	}
	for _, e := range res.Entries {
		if e.Timestamp.IsZero() {
			t.Fatalf("entry %q has zero timestamp; defaults not applied", e.TextPayload)
		}
	}
}

func TestCoreWriteAppliesRequestDefaults(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	if err := s.WriteEntries(ctx, &WriteRequest{
		LogName:      "organizations/77/logs/default-log",
		ResourceType: "gce_instance",
		Labels:       map[string]string{"env": "test", "team": "core"},
		Entries: []loggingstore.LogEntry{
			{Severity: 200, PayloadType: "text", TextPayload: "via-default"},
		},
	}); err != nil {
		t.Fatalf("WriteEntries: %v", err)
	}

	res, err := s.ListEntries(ctx, &ListEntriesRequest{ResourceNames: []string{"organizations/77"}})
	if err != nil {
		t.Fatalf("ListEntries: %v", err)
	}
	if len(res.Entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(res.Entries))
	}
	e := res.Entries[0]
	if e.LogName != "organizations/77/logs/default-log" {
		t.Fatalf("log name = %q", e.LogName)
	}
	if e.ResourceType != "gce_instance" {
		t.Fatalf("resource type = %q, want gce_instance", e.ResourceType)
	}
	if e.Labels["env"] != "test" || e.Labels["team"] != "core" {
		t.Fatalf("labels = %v, want env/team defaults", e.Labels)
	}
}

func TestCoreWriteAtomicRejectAndPartialSuccess(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	// Atomic: one invalid entry rejects the whole batch.
	if err := s.WriteEntries(ctx, &WriteRequest{Entries: []loggingstore.LogEntry{
		textEntry("projects/test/logs/good", "good", 200),
		{LogName: "not-a-resource-name", PayloadType: "text", TextPayload: "bad"},
	}}); err == nil {
		t.Fatal("atomic write with an invalid entry should fail")
	}
	res, _ := s.ListEntries(ctx, &ListEntriesRequest{ResourceNames: []string{"projects/test"}})
	if len(res.Entries) != 0 {
		t.Fatalf("atomic reject persisted %d entries", len(res.Entries))
	}

	// partial_success: the invalid entry is dropped, the valid one written.
	if err := s.WriteEntries(ctx, &WriteRequest{PartialSuccess: true, Entries: []loggingstore.LogEntry{
		textEntry("projects/test/logs/ps", "good", 200),
		{LogName: "not-a-resource-name", PayloadType: "text", TextPayload: "bad"},
	}}); err != nil {
		t.Fatalf("partial_success write: %v", err)
	}
	res, _ = s.ListEntries(ctx, &ListEntriesRequest{ResourceNames: []string{"projects/test"}})
	if len(res.Entries) != 1 || res.Entries[0].TextPayload != "good" {
		t.Fatalf("partial_success entries = %+v, want one 'good'", res.Entries)
	}
}

func TestCoreDryRunDoesNotPersist(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if err := s.WriteEntries(ctx, &WriteRequest{
		DryRun:  true,
		Entries: []loggingstore.LogEntry{textEntry("projects/test/logs/dry", "x", 200)},
	}); err != nil {
		t.Fatalf("dry_run write: %v", err)
	}
	res, _ := s.ListEntries(ctx, &ListEntriesRequest{ResourceNames: []string{"projects/test"}})
	if len(res.Entries) != 0 {
		t.Fatalf("dry_run persisted %d entries", len(res.Entries))
	}
}

func TestCoreListLogsDeleteAndScopeIsolation(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	const (
		orgLog  = "organizations/123/logs/shared"
		projLog = "projects/test/logs/shared"
	)
	if err := s.WriteEntries(ctx, &WriteRequest{Entries: []loggingstore.LogEntry{
		textEntry(orgLog, "org", 200),
		textEntry(projLog, "proj", 200),
	}}); err != nil {
		t.Fatalf("WriteEntries: %v", err)
	}

	names, _, err := s.ListLogs(ctx, "organizations/123", "", 0, "")
	if err != nil {
		t.Fatalf("ListLogs: %v", err)
	}
	if len(names) != 1 || names[0] != orgLog {
		t.Fatalf("ListLogs = %v, want [%s]", names, orgLog)
	}

	if err := s.DeleteLog(ctx, orgLog); err != nil {
		t.Fatalf("DeleteLog: %v", err)
	}
	if err := s.DeleteLog(ctx, orgLog); err != nil {
		t.Fatalf("second DeleteLog should be idempotent: %v", err)
	}

	res, _ := s.ListEntries(ctx, &ListEntriesRequest{ResourceNames: []string{"projects/test"}})
	if len(res.Entries) != 1 || res.Entries[0].TextPayload != "proj" {
		t.Fatalf("project scope after org delete = %+v, want only proj", res.Entries)
	}
}

func TestCoreEncodedLogIDRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	const logName = "projects/test/logs/cloudaudit.googleapis.com%2Factivity"
	if err := s.WriteEntries(ctx, &WriteRequest{
		Entries: []loggingstore.LogEntry{textEntry(logName, "audit", 200)},
	}); err != nil {
		t.Fatalf("WriteEntries: %v", err)
	}
	names, _, err := s.ListLogs(ctx, "projects/test", "", 0, "")
	if err != nil {
		t.Fatalf("ListLogs: %v", err)
	}
	if len(names) != 1 || names[0] != logName {
		t.Fatalf("ListLogs = %v, want [%s]", names, logName)
	}
	if err := s.DeleteLog(ctx, logName); err != nil {
		t.Fatalf("DeleteLog: %v", err)
	}
	res, _ := s.ListEntries(ctx, &ListEntriesRequest{ResourceNames: []string{"projects/test"}})
	if len(res.Entries) != 0 {
		t.Fatalf("entries after encoded delete = %+v", res.Entries)
	}
}

func TestCoreOrderByValidation(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	if err := s.WriteEntries(ctx, &WriteRequest{
		Entries: []loggingstore.LogEntry{textEntry("projects/test/logs/o", "x", 200)},
	}); err != nil {
		t.Fatalf("WriteEntries: %v", err)
	}
	for _, ok := range []string{"", "timestamp asc", "timestamp desc"} {
		if _, err := s.ListEntries(ctx, &ListEntriesRequest{ResourceNames: []string{"projects/test"}, OrderBy: ok}); err != nil {
			t.Errorf("order_by %q should be valid: %v", ok, err)
		}
	}
	for _, bad := range []string{"timestamp", "severity desc", "timestamp ASC"} {
		if _, err := s.ListEntries(ctx, &ListEntriesRequest{ResourceNames: []string{"projects/test"}, OrderBy: bad}); err == nil {
			t.Errorf("order_by %q should be InvalidArgument", bad)
		}
	}
}

func TestCorePageSizeValidation(t *testing.T) {
	ctx := context.Background()
	s := newTestService()
	for _, ps := range []int{-1, 1001} {
		if _, err := s.ListEntries(ctx, &ListEntriesRequest{ResourceNames: []string{"projects/test"}, PageSize: ps}); err == nil {
			t.Fatalf("page_size %d should be InvalidArgument", ps)
		}
	}
	for _, ps := range []int{0, 1, 1000} {
		if _, err := s.ListEntries(ctx, &ListEntriesRequest{ResourceNames: []string{"projects/test"}, PageSize: ps}); err != nil {
			t.Fatalf("page_size %d should be valid: %v", ps, err)
		}
	}
}

func TestCoreListMonitoredResourceDescriptors(t *testing.T) {
	s := newTestService()

	all, next := s.ListMonitoredResourceDescriptors(0, "")
	if next != "" {
		t.Fatalf("unpaginated descriptors next = %q, want empty", next)
	}
	if len(all) < 10 {
		t.Fatalf("catalog = %d descriptors, want at least 10", len(all))
	}
	var found bool
	for _, d := range all {
		if d.Type != "gce_instance" {
			continue
		}
		found = true
		if d.DisplayName == "" {
			t.Fatal("gce_instance has no display name")
		}
		keys := map[string]bool{}
		for _, l := range d.Labels {
			keys[l.Key] = true
		}
		for _, k := range []string{"project_id", "instance_id", "zone"} {
			if !keys[k] {
				t.Fatalf("gce_instance missing label %q", k)
			}
		}
	}
	if !found {
		t.Fatal("catalog missing gce_instance")
	}

	// Paginated.
	seen := map[string]bool{}
	token := ""
	for pages := 0; ; pages++ {
		page, tok := s.ListMonitoredResourceDescriptors(3, token)
		if len(page) > 3 {
			t.Fatalf("page size = %d, want <= 3", len(page))
		}
		for _, d := range page {
			if seen[d.Type] {
				t.Fatalf("duplicate descriptor %q", d.Type)
			}
			seen[d.Type] = true
		}
		token = tok
		if token == "" {
			break
		}
		if pages > 20 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != len(all) {
		t.Fatalf("paginated = %d types, want %d", len(seen), len(all))
	}
}
