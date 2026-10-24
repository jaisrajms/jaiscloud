package logging_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/require"
	mrpb "google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// tailRecv is one result read off a TailLogEntries stream by the drain
// goroutine.
type tailRecv struct {
	resp *loggingpb.TailLogEntriesResponse
	err  error
}

// tryTailRecv returns the next tail response, or (nil, nil) when none arrives
// within d. A bounded wait — never a fixed "sleep to seed the cursor" delay.
func tryTailRecv(recvd <-chan tailRecv, d time.Duration) (*loggingpb.TailLogEntriesResponse, error) {
	select {
	case r := <-recvd:
		return r.resp, r.err
	case <-time.After(d):
		return nil, nil
	}
}

// TestLoggingTailEntries exercises the bidirectional TailLogEntries stream: a
// filter is applied to entries written after the stream begins, and an
// unmatched entry is not delivered. The emulator implements a bounded,
// store-polling approximation (documented in the core): a small buffer_window
// lowers the poll interval, and delivery is at-most-once relative to the read
// snapshot rather than real Logging's at-least-once.
func TestLoggingTailEntries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c := LoggingClient(ctx)
	defer c.Close()

	project := ProjectID()
	parent := fmt.Sprintf("projects/%s", project)
	logName := fmt.Sprintf("projects/%s/logs/%s", project, uniqueName("tail"))
	t.Cleanup(func() { _ = c.DeleteLog(ctx, &loggingpb.DeleteLogRequest{LogName: logName}) })

	stream, err := c.TailLogEntries(ctx)
	require.NoError(t, err)

	// Subscribe with a textPayload filter and a short buffer window so the
	// emulator polls frequently instead of every 2s.
	require.NoError(t, stream.Send(&loggingpb.TailLogEntriesRequest{
		ResourceNames: []string{parent},
		Filter:        `textPayload:"tail-target"`,
		BufferWindow:  durationpb.New(100 * time.Millisecond),
	}))

	// Drain the stream on a goroutine so the writer below can retry under a
	// wall-clock bound instead of blocking on a single Recv.
	recvd := make(chan tailRecv, 1)
	go func() {
		for {
			resp, rerr := stream.Recv()
			select {
			case recvd <- tailRecv{resp: resp, err: rerr}:
			case <-ctx.Done():
				return
			}
			if rerr != nil {
				return
			}
		}
	}()

	write := func(payloads ...string) {
		t.Helper()
		entries := make([]*loggingpb.LogEntry, 0, len(payloads))
		for _, p := range payloads {
			entries = append(entries, &loggingpb.LogEntry{
				LogName:  logName,
				Resource: &mrpb.MonitoredResource{Type: "global"},
				Severity: ltype.LogSeverity_INFO,
				Payload:  &loggingpb.LogEntry_TextPayload{TextPayload: p},
			})
		}
		_, werr := c.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{LogName: logName, Entries: entries})
		require.NoError(t, werr)
	}

	// Readiness handshake: the server seeds its write-id cursor when it accepts
	// the initial request, and entries written before that are (correctly)
	// treated as pre-existing backlog and never delivered. A fixed sleep is a
	// timing assumption that flakes under load, so instead keep writing a
	// matching entry until the stream yields one, bounded by a deadline.
	var first *loggingpb.TailLogEntriesResponse
	handshakeDeadline := time.Now().Add(15 * time.Second)
	for first == nil {
		write("tail-target handshake " + uniqueName("probe"))
		resp, rerr := tryTailRecv(recvd, 250*time.Millisecond)
		require.NoError(t, rerr)
		first = resp
		if first == nil && time.Now().After(handshakeDeadline) {
			t.Fatal("tail stream never delivered a matching entry after the write-id cursor was seeded")
		}
	}
	for _, e := range first.GetEntries() {
		require.Contains(t, e.GetTextPayload(), "tail-target", "only matching entries may be streamed")
	}

	// The cursor is now seeded, so a fresh matching write must be delivered while
	// a non-matching entry in the same request must not.
	write("tail-target second", "ignored by the filter")
	seenSecond := false
	secondDeadline := time.Now().Add(10 * time.Second)
	for !seenSecond && time.Now().Before(secondDeadline) {
		resp, rerr := tryTailRecv(recvd, 500*time.Millisecond)
		require.NoError(t, rerr)
		for _, e := range resp.GetEntries() {
			require.NotEqual(t, "ignored by the filter", e.GetTextPayload(),
				"a non-matching entry must not be streamed")
			if e.GetTextPayload() == "tail-target second" {
				seenSecond = true
			}
		}
	}
	require.True(t, seenSecond, "the second matching entry must be streamed on the open stream")
}

// TestLoggingTailInvalidFilter pins the fail-loud contract: a tail request with
// an uncompilable filter surfaces InvalidArgument on the stream rather than
// silently tailing nothing.
func TestLoggingTailInvalidFilter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c := LoggingClient(ctx)
	defer c.Close()

	stream, err := c.TailLogEntries(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&loggingpb.TailLogEntriesRequest{
		ResourceNames: []string{fmt.Sprintf("projects/%s", ProjectID())},
		Filter:        "bogusField=1",
	}))

	_, err = stream.Recv()
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
