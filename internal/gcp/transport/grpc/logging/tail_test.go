package logging

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"

	loggingstore "jaiscloud/internal/gcp/store/logging"

	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/protobuf/types/known/durationpb"
)

// listSignalStore wraps a store and signals after the first List completes.
// The tail's baseline read is its first store access, so a test can wait on
// `listed` and know an entry written afterwards will be treated as new.
type listSignalStore struct {
	loggingstore.Store
	once   sync.Once
	listed chan struct{}
}

func newListSignalStore() *listSignalStore {
	return &listSignalStore{Store: loggingstore.NewMemoryStore(), listed: make(chan struct{})}
}

func (s *listSignalStore) List(ctx context.Context, project string) ([]loggingstore.LogEntry, error) {
	entries, err := s.Store.List(ctx, project)
	s.once.Do(func() { close(s.listed) })
	return entries, err
}

// recvTail receives one TailLogEntriesResponse, failing the test on error or
// timeout.
func recvTail(t *testing.T, stream loggingpb.LoggingServiceV2_TailLogEntriesClient) *loggingpb.TailLogEntriesResponse {
	t.Helper()
	type result struct {
		resp *loggingpb.TailLogEntriesResponse
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		r, e := stream.Recv()
		ch <- result{r, e}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("TailLogEntries Recv: %v", r.err)
		}
		return r.resp
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for a tailed entry")
		return nil
	}
}

func startTail(t *testing.T, ctx context.Context, client loggingpb.LoggingServiceV2Client, logName string) loggingpb.LoggingServiceV2_TailLogEntriesClient {
	t.Helper()
	stream, err := client.TailLogEntries(ctx)
	if err != nil {
		t.Fatalf("TailLogEntries: %v", err)
	}
	if err := stream.Send(&loggingpb.TailLogEntriesRequest{
		ResourceNames: []string{"projects/test"},
		Filter:        fmt.Sprintf("logName=%q", logName),
		BufferWindow:  durationpb.New(50 * time.Millisecond),
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	return stream
}

func TestTailLogEntriesStreamsMatching(t *testing.T) {
	store := newListSignalStore()
	client, cleanup := loggingTestServiceWithStore(t, store)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const logName = "projects/test/logs/tail-match"
	stream := startTail(t, ctx, client, logName)

	// Wait for the baseline read, then write an entry that must be new.
	<-store.listed
	if _, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{logEntry(logName, "tail-match", ltype.LogSeverity_INFO)},
	}); err != nil {
		t.Fatalf("WriteLogEntries: %v", err)
	}

	resp := recvTail(t, stream)
	if len(resp.GetEntries()) != 1 {
		t.Fatalf("tailed entries = %d, want 1: %+v", len(resp.GetEntries()), resp.GetEntries())
	}
	if got := resp.GetEntries()[0].GetTextPayload(); got != "tail-match" {
		t.Fatalf("tailed text = %q, want tail-match", got)
	}
}

func TestTailLogEntriesSkipsNonMatching(t *testing.T) {
	store := newListSignalStore()
	client, cleanup := loggingTestServiceWithStore(t, store)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const logName = "projects/test/logs/tail-match"
	stream := startTail(t, ctx, client, logName)
	<-store.listed

	// A non-matching entry written first must never be streamed.
	if _, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{logEntry("projects/test/logs/other", "tail-other", ltype.LogSeverity_INFO)},
	}); err != nil {
		t.Fatalf("WriteLogEntries (non-matching): %v", err)
	}
	if _, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{logEntry(logName, "tail-match", ltype.LogSeverity_INFO)},
	}); err != nil {
		t.Fatalf("WriteLogEntries (matching): %v", err)
	}

	resp := recvTail(t, stream)
	if len(resp.GetEntries()) != 1 {
		t.Fatalf("tailed entries = %d, want only the matching entry: %+v", len(resp.GetEntries()), resp.GetEntries())
	}
	if got := resp.GetEntries()[0].GetLogName(); got != logName {
		t.Fatalf("streamed non-matching log %q, want %q", got, logName)
	}
}

func TestTailLogEntriesContextCancel(t *testing.T) {
	store := newListSignalStore()
	client, cleanup := loggingTestServiceWithStore(t, store)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const logName = "projects/test/logs/tail-cancel"
	stream := startTail(t, ctx, client, logName)
	<-store.listed

	if _, err := client.WriteLogEntries(ctx, &loggingpb.WriteLogEntriesRequest{
		Entries: []*loggingpb.LogEntry{logEntry(logName, "tail-live", ltype.LogSeverity_INFO)},
	}); err != nil {
		t.Fatalf("WriteLogEntries: %v", err)
	}
	recvTail(t, stream)

	cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("Recv returned nil after context cancel, want an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Recv did not return after context cancel")
	}
}
