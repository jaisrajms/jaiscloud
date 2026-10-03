package logging

import (
	"context"
	"fmt"
	"net"
	"testing"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"

	core "jaiscloud/internal/gcp/service/logging"
	loggingstore "jaiscloud/internal/gcp/store/logging"

	mrpb "google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// loggingTestService dials a real in-process gRPC server backed by a memory
// store and returns the Logging client.
func loggingTestService(t *testing.T) (loggingpb.LoggingServiceV2Client, func()) {
	t.Helper()
	return loggingTestServiceWithStore(t, loggingstore.NewMemoryStore())
}

func loggingTestServiceWithStore(t *testing.T, store loggingstore.Store) (loggingpb.LoggingServiceV2Client, func()) {
	t.Helper()
	svc := NewService(core.NewService(store, "test"), "test")

	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	loggingpb.RegisterLoggingServiceV2Server(srv, svc)
	go srv.Serve(ln)

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		srv.Stop()
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		conn.Close()
		srv.Stop()
	}
	return loggingpb.NewLoggingServiceV2Client(conn), cleanup
}

func logEntry(logName, text string, severity ltype.LogSeverity) *loggingpb.LogEntry {
	return &loggingpb.LogEntry{
		LogName:  logName,
		Resource: &mrpb.MonitoredResource{Type: "global"},
		Severity: severity,
		Payload:  &loggingpb.LogEntry_TextPayload{TextPayload: text},
	}
}

func TestLoggingWriteAndList(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	const logName = "projects/test/logs/go-log"

	if _, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{
			logEntry(logName, "info-message", ltype.LogSeverity_INFO),
			logEntry(logName, "error-message", ltype.LogSeverity_ERROR),
		},
	}); err != nil {
		t.Fatalf("WriteLogEntries: %v", err)
	}

	// List all entries for the log.
	all, err := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{"projects/test"},
		Filter:        fmt.Sprintf("logName=%q", logName),
	})
	if err != nil {
		t.Fatalf("ListLogEntries: %v", err)
	}
	if len(all.GetEntries()) != 2 {
		t.Fatalf("ListLogEntries count = %d, want 2", len(all.GetEntries()))
	}
	for _, e := range all.GetEntries() {
		if e.GetResource().GetType() != "global" {
			t.Fatalf("resource type = %q, want global", e.GetResource().GetType())
		}
	}

	// Severity filter.
	sev, err := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{"projects/test"},
		Filter:        fmt.Sprintf("logName=%q AND severity>=WARNING", logName),
	})
	if err != nil {
		t.Fatalf("ListLogEntries severity: %v", err)
	}
	if len(sev.GetEntries()) != 1 || sev.GetEntries()[0].GetTextPayload() != "error-message" {
		t.Fatalf("severity filter = %+v", sev.GetEntries())
	}
	if sev.GetEntries()[0].GetSeverity() != ltype.LogSeverity_ERROR {
		t.Fatalf("severity = %v, want ERROR", sev.GetEntries()[0].GetSeverity())
	}
}

func TestLoggingListLogs(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	const logName = "projects/test/logs/mylog"
	if _, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{logEntry(logName, "x", ltype.LogSeverity_INFO)},
	}); err != nil {
		t.Fatalf("WriteLogEntries: %v", err)
	}

	resp, err := client.ListLogs(ctx, &loggingpb.ListLogsRequest{Parent: "projects/test"})
	if err != nil {
		t.Fatalf("ListLogs: %v", err)
	}
	found := false
	for _, name := range resp.GetLogNames() {
		if name == logName {
			found = true
		}
	}
	if !found {
		t.Fatalf("ListLogs = %v, want to contain %s", resp.GetLogNames(), logName)
	}
}

func TestLoggingDeleteLog(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	const logName = "projects/test/logs/del"
	if _, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{logEntry(logName, "x", ltype.LogSeverity_INFO)},
	}); err != nil {
		t.Fatalf("WriteLogEntries: %v", err)
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
		t.Fatalf("after DeleteLog, entries = %+v, want none", resp.GetEntries())
	}
}

func TestLoggingInvalidFilter(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{"projects/test"},
		Filter:        `logName=`,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid filter err = %v, want InvalidArgument", err)
	}
}

func TestLoggingOrderByValidation(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	const logName = "projects/test/logs/ord"
	if _, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{logEntry(logName, "x", ltype.LogSeverity_INFO)},
	}); err != nil {
		t.Fatalf("WriteLogEntries: %v", err)
	}

	for _, orderBy := range []string{"", "timestamp asc", "timestamp desc"} {
		if _, err := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
			ResourceNames: []string{"projects/test"},
			OrderBy:       orderBy,
		}); err != nil {
			t.Fatalf("order_by %q should be valid: %v", orderBy, err)
		}
	}

	for _, orderBy := range []string{"timestamp", "severity desc", "timestamp ASC"} {
		if _, err := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
			ResourceNames: []string{"projects/test"},
			OrderBy:       orderBy,
		}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("order_by %q err = %v, want InvalidArgument", orderBy, err)
		}
	}
}

func TestLoggingPageSizeValidation(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	for _, ps := range []int32{-1, 1001} {
		if _, err := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
			ResourceNames: []string{"projects/test"},
			PageSize:      ps,
		}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("page_size %d err = %v, want InvalidArgument", ps, err)
		}
	}

	for _, ps := range []int32{0, 1, 1000} {
		if _, err := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
			ResourceNames: []string{"projects/test"},
			PageSize:      ps,
		}); err != nil {
			t.Fatalf("page_size %d should be valid: %v", ps, err)
		}
	}
}

func TestLoggingResourceLabelsRoundTrip(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	const logName = "projects/test/logs/rl"
	e := &loggingpb.LogEntry{
		LogName:  logName,
		Resource: &mrpb.MonitoredResource{Type: "gce_instance", Labels: map[string]string{"instance_id": "123", "zone": "us-central1-a"}},
		Severity: ltype.LogSeverity_INFO,
		Payload:  &loggingpb.LogEntry_TextPayload{TextPayload: "with labels"},
	}
	if _, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{e},
	}); err != nil {
		t.Fatalf("WriteLogEntries: %v", err)
	}

	resp, err := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{"projects/test"},
	})
	if err != nil {
		t.Fatalf("ListLogEntries: %v", err)
	}
	if len(resp.GetEntries()) != 1 {
		t.Fatalf("entries = %d, want 1", len(resp.GetEntries()))
	}
	got := resp.GetEntries()[0].GetResource()
	if got.GetType() != "gce_instance" {
		t.Fatalf("resource type = %q, want gce_instance", got.GetType())
	}
	if got.GetLabels()["instance_id"] != "123" || got.GetLabels()["zone"] != "us-central1-a" {
		t.Fatalf("resource labels = %v, want instance_id/zone", got.GetLabels())
	}
}

func TestLoggingDryRun(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	const logName = "projects/test/logs/dry"
	if _, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		DryRun:  true,
		Entries: []*loggingpb.LogEntry{logEntry(logName, "x", ltype.LogSeverity_INFO)},
	}); err != nil {
		t.Fatalf("dry_run should succeed: %v", err)
	}

	resp, err := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{"projects/test"},
	})
	if err != nil {
		t.Fatalf("ListLogEntries: %v", err)
	}
	if len(resp.GetEntries()) != 0 {
		t.Fatalf("dry_run must not persist: %+v", resp.GetEntries())
	}
}

func TestLoggingWriteAtomicReject(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{
			logEntry("projects/test/logs/good", "good", ltype.LogSeverity_INFO),
			{LogName: "not-a-resource-name", Payload: &loggingpb.LogEntry_TextPayload{TextPayload: "bad"}},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}

	resp, err := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{"projects/test"},
	})
	if err != nil {
		t.Fatalf("ListLogEntries: %v", err)
	}
	if len(resp.GetEntries()) != 0 {
		t.Fatalf("atomic reject must persist nothing: %+v", resp.GetEntries())
	}
}

// TestLoggingPartialSuccessDropsInvalidEntries covers the field the
// google-cloud-logging high-level client always sets: with partial_success the
// valid entries are written and invalid ones dropped, rather than the whole
// batch failing (contrast TestLoggingWriteAtomicReject).
func TestLoggingPartialSuccessDropsInvalidEntries(t *testing.T) {
	client, cleanup := loggingTestService(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		PartialSuccess: true,
		Entries: []*loggingpb.LogEntry{
			logEntry("projects/test/logs/ps", "good", ltype.LogSeverity_INFO),
			{LogName: "not-a-resource-name", Payload: &loggingpb.LogEntry_TextPayload{TextPayload: "bad"}},
		},
	}); err != nil {
		t.Fatalf("partial_success write should succeed: %v", err)
	}

	resp, err := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{"projects/test"},
	})
	if err != nil {
		t.Fatalf("ListLogEntries: %v", err)
	}
	if len(resp.GetEntries()) != 1 {
		t.Fatalf("partial_success must persist the valid entry only, got %d", len(resp.GetEntries()))
	}
}
