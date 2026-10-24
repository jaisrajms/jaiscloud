package logging_test

import (
	"context"
	"fmt"
	"testing"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	mrpb "google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestLoggingEntriesFiltering writes a handful of entries to a unique log and
// pins the entries.list filter engine (the same predicate the sink/exclusion
// routing path evaluates): exact logName, severity comparison, textPayload
// contains, resource.type, AND / OR / NOT composition, and the InvalidArgument
// for an unsupported field.
func TestLoggingEntriesFiltering(t *testing.T) {
	ctx := context.Background()
	c := LoggingClient(ctx)
	defer c.Close()

	project := ProjectID()
	parent := fmt.Sprintf("projects/%s", project)
	logName := fmt.Sprintf("projects/%s/logs/%s", project, uniqueName("filter"))
	otherLog := fmt.Sprintf("projects/%s/logs/%s", project, uniqueName("other"))

	t.Cleanup(func() {
		_ = c.DeleteLog(ctx, &loggingpb.DeleteLogRequest{LogName: logName})
		_ = c.DeleteLog(ctx, &loggingpb.DeleteLogRequest{LogName: otherLog})
	})

	entry := func(log string, sev ltype.LogSeverity, text, resType string) *loggingpb.LogEntry {
		return &loggingpb.LogEntry{
			LogName:  log,
			Resource: &mrpb.MonitoredResource{Type: resType},
			Severity: sev,
			Payload:  &loggingpb.LogEntry_TextPayload{TextPayload: text},
		}
	}

	_, err := c.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{
			entry(logName, ltype.LogSeverity_INFO, "needle in the haystack", "global"),
			entry(logName, ltype.LogSeverity_ERROR, "something failed", "global"),
			entry(logName, ltype.LogSeverity_CRITICAL, "everything failed", "gce_instance"),
			entry(otherLog, ltype.LogSeverity_ERROR, "other log error", "global"),
		},
	})
	require.NoError(t, err)

	list := func(filter string) []*loggingpb.LogEntry {
		it := c.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
			ResourceNames: []string{parent},
			Filter:        filter,
			PageSize:      1000,
		})
		var out []*loggingpb.LogEntry
		for {
			e, err := it.Next()
			if err == iterator.Done {
				break
			}
			require.NoError(t, err)
			out = append(out, e)
		}
		return out
	}

	// Exact logName selects only this log's entries.
	require.Len(t, list(fmt.Sprintf("logName=%q", logName)), 3)

	// severity>=ERROR keeps ERROR and CRITICAL (numeric comparison).
	sevFiltered := list(fmt.Sprintf("logName=%q AND severity>=ERROR", logName))
	require.Len(t, sevFiltered, 2)
	for _, e := range sevFiltered {
		require.GreaterOrEqual(t, int32(e.GetSeverity()), int32(ltype.LogSeverity_ERROR))
	}

	// textPayload uses substring (:) matching.
	contains := list(fmt.Sprintf("logName=%q AND textPayload:\"needle\"", logName))
	require.Len(t, contains, 1)
	require.Contains(t, contains[0].GetTextPayload(), "needle")

	// resource.type narrows to a monitored resource.
	require.Len(t, list(fmt.Sprintf("logName=%q AND resource.type=\"gce_instance\"", logName)), 1)

	// NOT inverts a clause; NOT severity>=WARNING leaves only INFO.
	require.Len(t, list(fmt.Sprintf("logName=%q AND NOT severity>=WARNING", logName)), 1)

	// OR admits either log.
	both := list(fmt.Sprintf("logName=%q OR logName=%q", logName, otherLog))
	require.Len(t, both, 4)

	// An unsupported filter field is InvalidArgument (never a silent match-all).
	_, err = c.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{parent},
		Filter:        "bogusField=1",
	}).Next()
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// ListLogs enumerates both logs.
	logs := map[string]bool{}
	it := c.ListLogs(ctx, &loggingpb.ListLogsRequest{Parent: parent, PageSize: 1000})
	for {
		name, err := it.Next()
		if err == iterator.Done {
			break
		}
		require.NoError(t, err)
		logs[name] = true
	}
	require.True(t, logs[logName], "ListLogs must include the written log")
	require.True(t, logs[otherLog], "ListLogs must include the second written log")

	// DeleteLog removes one log; it then disappears from ListLogs.
	require.NoError(t, c.DeleteLog(ctx, &loggingpb.DeleteLogRequest{LogName: logName}))
	it = c.ListLogs(ctx, &loggingpb.ListLogsRequest{Parent: parent, PageSize: 1000})
	for {
		name, err := it.Next()
		if err == iterator.Done {
			break
		}
		require.NoError(t, err)
		require.NotEqual(t, logName, name, "a deleted log must not be listed")
	}
}
