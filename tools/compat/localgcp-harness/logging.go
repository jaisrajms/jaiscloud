package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var logClient loggingpb.LoggingServiceV2Client

func initLogging() {
	conn, err := grpc.NewClient(grpcEndpoint(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	logClient = loggingpb.NewLoggingServiceV2Client(conn)
}

func logCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 20*time.Second)
}

// logProject returns a per-case project so cases don't leak state into each
// other (localgcp gives each test a fresh store; the shared emulator does not).
func logProject(caseID string) string { return "logging-" + caseID + "-" + suffix }

func runLogging() {
	initLogging()

	cases := []struct {
		name string
		fn   func() error
	}{
		{"TestWriteAndListLogEntries", logWriteAndList},
		{"TestListLogs", logListLogs},
		{"TestDeleteLog", logDeleteLog},
		{"TestFilterBySeverity", logFilterBySeverity},
	}
	for _, c := range cases {
		record("logging", c.name, c.fn())
	}
}

func logWrite(ctx context.Context, entries ...*loggingpb.LogEntry) error {
	_, err := logClient.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Resource: &monitoredres.MonitoredResource{Type: "global"},
		Entries:  entries,
	})
	return err
}

func logEntry(proj, name string, sev ltype.LogSeverity, payload string) *loggingpb.LogEntry {
	return &loggingpb.LogEntry{
		LogName:  "projects/" + proj + "/logs/" + name,
		Severity: sev,
		Payload:  &loggingpb.LogEntry_TextPayload{TextPayload: payload},
	}
}

func logWriteAndList() error {
	ctx, cancel := logCtx()
	defer cancel()
	p := logProject("write-list")
	if err := logWrite(ctx,
		logEntry(p, "app", ltype.LogSeverity_INFO, "hello world"),
		logEntry(p, "app", ltype.LogSeverity_ERROR, "something broke"),
	); err != nil {
		return err
	}
	resp, err := logClient.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{ResourceNames: []string{"projects/" + p}})
	if err != nil {
		return err
	}
	if len(resp.Entries) != 2 {
		return fmt.Errorf("expected 2 entries, got %d", len(resp.Entries))
	}
	return nil
}

func logListLogs() error {
	ctx, cancel := logCtx()
	defer cancel()
	p := logProject("list-logs")
	if err := logWrite(ctx,
		logEntry(p, "app", ltype.LogSeverity_INFO, "a"),
		logEntry(p, "system", ltype.LogSeverity_INFO, "b"),
	); err != nil {
		return err
	}
	resp, err := logClient.ListLogs(ctx, &loggingpb.ListLogsRequest{Parent: "projects/" + p})
	if err != nil {
		return err
	}
	if len(resp.LogNames) != 2 {
		return fmt.Errorf("expected 2 log names, got %d", len(resp.LogNames))
	}
	return nil
}

func logDeleteLog() error {
	ctx, cancel := logCtx()
	defer cancel()
	p := logProject("delete-log")
	if err := logWrite(ctx,
		logEntry(p, "app", ltype.LogSeverity_INFO, "entry1"),
		logEntry(p, "keep", ltype.LogSeverity_INFO, "entry2"),
	); err != nil {
		return err
	}
	if _, err := logClient.DeleteLog(ctx, &loggingpb.DeleteLogRequest{LogName: "projects/" + p + "/logs/app"}); err != nil {
		return err
	}
	resp, err := logClient.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{ResourceNames: []string{"projects/" + p}})
	if err != nil {
		return err
	}
	if len(resp.Entries) != 1 {
		return fmt.Errorf("expected 1 entry after delete, got %d", len(resp.Entries))
	}
	if resp.Entries[0].GetTextPayload() != "entry2" {
		return fmt.Errorf("wrong entry survived: %q", resp.Entries[0].GetTextPayload())
	}
	return nil
}

func logFilterBySeverity() error {
	ctx, cancel := logCtx()
	defer cancel()
	p := logProject("filter-sev")
	if err := logWrite(ctx,
		logEntry(p, "app", ltype.LogSeverity_INFO, "info"),
		logEntry(p, "app", ltype.LogSeverity_ERROR, "error"),
		logEntry(p, "app", ltype.LogSeverity_DEBUG, "debug"),
	); err != nil {
		return err
	}
	resp, err := logClient.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{"projects/" + p},
		Filter:        "severity>=ERROR",
	})
	if err != nil {
		return err
	}
	if len(resp.Entries) != 1 {
		return fmt.Errorf("expected 1 ERROR+ entry, got %d", len(resp.Entries))
	}
	return nil
}
