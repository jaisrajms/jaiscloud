package logging

import (
	"context"
	"testing"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"

	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLoggingOrganizationScopeIsolation(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	const (
		orgLog  = "organizations/123/logs/shared-id"
		projLog = "projects/test/logs/shared-id"
	)
	if _, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{
			logEntry(orgLog, "org-entry", ltype.LogSeverity_INFO),
			logEntry(projLog, "proj-entry", ltype.LogSeverity_INFO),
		},
	}); err != nil {
		t.Fatalf("WriteLogEntries: %v", err)
	}

	listScope := func(resource string) []*loggingpb.LogEntry {
		t.Helper()
		resp, err := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
			ResourceNames: []string{resource},
		})
		if err != nil {
			t.Fatalf("ListLogEntries(%q): %v", resource, err)
		}
		return resp.GetEntries()
	}

	orgEntries := listScope("organizations/123")
	if len(orgEntries) != 1 || orgEntries[0].GetTextPayload() != "org-entry" {
		t.Fatalf("organizations scope = %+v, want only org-entry", orgEntries)
	}
	projEntries := listScope("projects/test")
	if len(projEntries) != 1 || projEntries[0].GetTextPayload() != "proj-entry" {
		t.Fatalf("projects scope = %+v, want only proj-entry", projEntries)
	}

	logs, err := client.ListLogs(ctx, &loggingpb.ListLogsRequest{Parent: "organizations/123"})
	if err != nil {
		t.Fatalf("ListLogs(organizations/123): %v", err)
	}
	// The organizations scope must not surface the project's identically named log.
	if len(logs.GetLogNames()) != 1 || logs.GetLogNames()[0] != orgLog {
		t.Fatalf("ListLogs(organizations/123) = %v, want [%s]", logs.GetLogNames(), orgLog)
	}
}

func TestLoggingDeleteLogScopeIsolation(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	const (
		orgA  = "organizations/123/logs/a"
		orgB  = "organizations/123/logs/b"
		projA = "projects/test/logs/a"
	)
	if _, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{
			logEntry(orgA, "org-a", ltype.LogSeverity_INFO),
			logEntry(orgB, "org-b", ltype.LogSeverity_INFO),
			logEntry(projA, "proj-a", ltype.LogSeverity_INFO),
		},
	}); err != nil {
		t.Fatalf("WriteLogEntries: %v", err)
	}

	if _, err := client.DeleteLog(ctx, &loggingpb.DeleteLogRequest{LogName: orgA}); err != nil {
		t.Fatalf("DeleteLog(%s): %v", orgA, err)
	}

	orgEntries := func() []*loggingpb.LogEntry {
		resp, err := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
			ResourceNames: []string{"organizations/123"},
		})
		if err != nil {
			t.Fatalf("ListLogEntries: %v", err)
		}
		return resp.GetEntries()
	}
	if got := orgEntries(); len(got) != 1 || got[0].GetTextPayload() != "org-b" {
		t.Fatalf("organizations scope after delete = %+v, want only org-b", got)
	}
	// Deleting in the organizations scope must not touch the project's log of
	// the same ID.
	projResp, err := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{"projects/test"},
	})
	if err != nil {
		t.Fatalf("ListLogEntries(projects/test): %v", err)
	}
	if got := projResp.GetEntries(); len(got) != 1 || got[0].GetTextPayload() != "proj-a" {
		t.Fatalf("projects scope after org delete = %+v, want only proj-a", got)
	}

	// Idempotent: deleting a well-formed, now-absent log must not error.
	if _, err := client.DeleteLog(ctx, &loggingpb.DeleteLogRequest{LogName: orgA}); err != nil {
		t.Fatalf("second DeleteLog(%s) = %v, want no error", orgA, err)
	}
}

func TestLoggingDeleteLogMalformed(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	for _, name := range []string{"bots/x/logs/y", "projects/p/logs/", "projects//logs/x", "projects/p/logs"} {
		if _, err := client.DeleteLog(ctx, &loggingpb.DeleteLogRequest{LogName: name}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("DeleteLog(%q) err = %v, want InvalidArgument", name, err)
		}
	}
}

func TestLoggingWriteRequestLevelLogName(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	const logName = "organizations/77/logs/default-log"
	if _, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		LogName: logName,
		Entries: []*loggingpb.LogEntry{
			{Severity: ltype.LogSeverity_INFO, Payload: &loggingpb.LogEntry_TextPayload{TextPayload: "via-default"}},
		},
	}); err != nil {
		t.Fatalf("WriteLogEntries: %v", err)
	}

	resp, err := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{"organizations/77"},
	})
	if err != nil {
		t.Fatalf("ListLogEntries: %v", err)
	}
	if len(resp.GetEntries()) != 1 || resp.GetEntries()[0].GetTextPayload() != "via-default" {
		t.Fatalf("entries = %+v, want one via-default", resp.GetEntries())
	}
	if got := resp.GetEntries()[0].GetLogName(); got != logName {
		t.Fatalf("log name = %q, want %q", got, logName)
	}
}

func TestLoggingEncodedLogID(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	const logName = "projects/test/logs/cloudaudit.googleapis.com%2Factivity"
	if _, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{logEntry(logName, "audit", ltype.LogSeverity_INFO)},
	}); err != nil {
		t.Fatalf("WriteLogEntries: %v", err)
	}

	// ListLogs returns the URL-encoded log name, as real Cloud Logging does.
	logs, err := client.ListLogs(ctx, &loggingpb.ListLogsRequest{Parent: "projects/test"})
	if err != nil {
		t.Fatalf("ListLogs: %v", err)
	}
	if len(logs.GetLogNames()) != 1 || logs.GetLogNames()[0] != logName {
		t.Fatalf("ListLogs = %v, want [%s]", logs.GetLogNames(), logName)
	}

	if _, err := client.DeleteLog(ctx, &loggingpb.DeleteLogRequest{LogName: logName}); err != nil {
		t.Fatalf("DeleteLog: %v", err)
	}
	resp, err := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{"projects/test"},
	})
	if err != nil {
		t.Fatalf("ListLogEntries: %v", err)
	}
	if len(resp.GetEntries()) != 0 {
		t.Fatalf("entries after encoded delete = %+v, want none", resp.GetEntries())
	}
}

func TestLoggingMalformedResourceParent(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{"bots/x"},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListLogEntries(bots/x) err = %v, want InvalidArgument", err)
	}
	if _, err := client.ListLogs(ctx, &loggingpb.ListLogsRequest{
		Parent: "projects/p/logs",
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListLogs(projects/p/logs) err = %v, want InvalidArgument", err)
	}
}
