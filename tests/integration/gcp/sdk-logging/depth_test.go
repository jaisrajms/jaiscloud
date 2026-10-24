// depth_test.go closes DC5 (plan_docs/gcp-depth-closure-wave-plan.md W2.3) for
// Cloud Logging: the one cell its record left exempt — p2 (concurrent log
// writes) — asserted through the official cloud.google.com/go/logging v2 client
// and the shared depth-test kit (tests/integration/gcp/testdepth).
//
// The write path stamps an omitted insert_id and a receive time in the core and
// appends under the store lock, so concurrent writers to one log must all be
// recorded, each exactly once, with a distinct server-assigned insert_id. A lost
// update or a collapsed insert_id under contention would drop a log line a real
// client can see.
//
// The case runs under -race (the module's gate adds it); a contention assertion
// in a binary without the race detector is a failure
// (testdepth.AssertRaceEnabled), not a silent downgrade.
package logging_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	logging "cloud.google.com/go/logging/apiv2"
	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	mrpb "google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/protobuf/types/known/timestamppb"
	testdepth "jaiscloud-gcp-testdepth"
)

// listTexts lists a log's entries and returns each text payload's occurrence
// count and the distinct insert_ids observed.
func listTexts(t *testing.T, ctx context.Context, client *logging.Client, parent, logName string) (map[string]int, map[string]int) {
	t.Helper()
	it := client.ListLogEntries(ctx, &loggingpb.ListLogEntriesRequest{
		ResourceNames: []string{parent},
		Filter:        fmt.Sprintf("logName=%q", logName),
	})
	texts := map[string]int{}
	insertIDs := map[string]int{}
	for {
		e, err := it.Next()
		if err == iterator.Done {
			break
		}
		require.NoError(t, err)
		texts[e.GetTextPayload()]++
		insertIDs[e.GetInsertId()]++
	}
	return texts, insertIDs
}

// TestLoggingDepthConcurrency is logging p2: concurrent writes to one log under
// -race. Every line is recorded exactly once and every server-assigned insert_id
// is distinct, whether the client supplied one or left it for the server.
func TestLoggingDepthConcurrency(t *testing.T) {
	ctx := context.Background()
	client := LoggingClient(ctx)
	defer client.Close()
	testdepth.AssertRaceEnabled(t)

	project := ProjectID()
	parent := fmt.Sprintf("projects/%s", project)
	logName := fmt.Sprintf("projects/%s/logs/%s", project, uniqueName("go-depth"))
	t.Cleanup(func() {
		_ = client.DeleteLog(ctx, &loggingpb.DeleteLogRequest{LogName: logName})
	})

	const writers = 16
	entry := func(text string) *loggingpb.LogEntry {
		return &loggingpb.LogEntry{
			LogName:   logName,
			Resource:  &mrpb.MonitoredResource{Type: "global"},
			Severity:  ltype.LogSeverity_INFO,
			Timestamp: timestamppb.New(time.Now()),
			Payload:   &loggingpb.LogEntry_TextPayload{TextPayload: text},
		}
	}

	t.Run("ClientSuppliedInsertIdsAllRecorded", func(t *testing.T) {
		errs := testdepth.Race(writers, func(i int) error {
			e := entry(fmt.Sprintf("depth-supplied-%d", i))
			e.InsertId = fmt.Sprintf("client-%d", i)
			_, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{Entries: []*loggingpb.LogEntry{e}})
			return err
		})
		for i, err := range errs {
			require.NoError(t, err, "write %d", i)
		}

		texts, _ := listTexts(t, ctx, client, parent, logName)
		require.Len(t, texts, writers, "every concurrent write must be recorded")
		for i := 0; i < writers; i++ {
			require.Equal(t, 1, texts[fmt.Sprintf("depth-supplied-%d", i)], "write %d recorded exactly once", i)
		}
	})

	t.Run("ServerAssignedInsertIdsAreDistinct", func(t *testing.T) {
		otherLog := fmt.Sprintf("projects/%s/logs/%s", project, uniqueName("go-depth-server"))
		t.Cleanup(func() {
			_ = client.DeleteLog(ctx, &loggingpb.DeleteLogRequest{LogName: otherLog})
		})

		errs := testdepth.Race(writers, func(i int) error {
			e := entry(fmt.Sprintf("depth-assigned-%d", i))
			e.LogName = otherLog
			_, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{Entries: []*loggingpb.LogEntry{e}})
			return err
		})
		for i, err := range errs {
			require.NoError(t, err, "write %d", i)
		}

		texts, insertIDs := listTexts(t, ctx, client, parent, otherLog)
		require.Len(t, texts, writers, "every concurrent write must be recorded")
		require.Len(t, insertIDs, writers, "the server must assign a distinct insert_id to each entry")
	})
}
