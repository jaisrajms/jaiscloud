package esm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/aws/store/stream"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

type fakeQueue struct {
	mu       sync.Mutex
	receives int
	deleted  [][]string
	// recv decides the n-th (1-based) receive.
	recv func(n int) ([]InternalMessage, error)
}

func (q *fakeQueue) InternalReceive(context.Context, string, int, int) ([]InternalMessage, error) {
	q.mu.Lock()
	q.receives++
	n := q.receives
	q.mu.Unlock()
	return q.recv(n)
}

func (q *fakeQueue) InternalDeleteBatch(_ context.Context, _ string, handles []string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.deleted = append(q.deleted, handles)
	return nil
}

func (q *fakeQueue) deletes() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.deleted)
}

type fakeInvoker struct {
	mu    sync.Mutex
	calls int
	fn    func(n int) error
}

func (f *fakeInvoker) InvokeInternal(context.Context, string, []byte) ([]byte, error) {
	f.mu.Lock()
	f.calls++
	n := f.calls
	f.mu.Unlock()
	return nil, f.fn(n)
}

func (f *fakeInvoker) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type fakeDLQ struct {
	mu    sync.Mutex
	sends int
}

func (d *fakeDLQ) InternalSend(context.Context, string, string, map[string]SQSMessageAttribute, SQSSourceContext) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sends++
	return nil
}

type noStreams struct{}

func (noStreams) GetRecords(context.Context, string, int) ([]stream.Record, int, error) {
	return nil, 0, nil
}
func (noStreams) GetStreamInfo(context.Context, string) (stream.StreamInfo, bool, error) {
	return stream.StreamInfo{}, false, nil
}

// shortBackoff shortens package-level variables, so tests that call it must not use t.Parallel.
func shortBackoff(t *testing.T) {
	t.Helper()
	min, max := outageBackoffMin, outageBackoffMax
	outageBackoffMin, outageBackoffMax = time.Millisecond, 4*time.Millisecond
	t.Cleanup(func() { outageBackoffMin, outageBackoffMax = min, max })
}

func startMapping(t *testing.T, q *fakeQueue, inv *fakeInvoker, dlq *fakeDLQ) (*Provider, EventSourceMapping) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	p := New(ctx, store.NewMemoryResourceStore(), inv, q, noStreams{}, slog.New(slog.NewTextHandler(discard{}, nil)))
	p.SetSQSSender(dlq)
	t.Cleanup(func() { cancel(); p.wg.Wait() })

	m := EventSourceMapping{
		UUID: "u1", FunctionName: "fn", FunctionArn: "arn:aws:lambda:us-east-1:000000000000:function:fn",
		EventSourceArn: "arn:aws:sqs:us-east-1:000000000000:q", State: ESMStateEnabled, Enabled: true,
		SourceType: ESMSourceSQS, BatchSize: 10, Cloud: "aws", Region: "us-east-1", AccountID: "000000000000",
		DestinationConfig: DestinationConfig{OnFailure: DestinationDetail{Destination: "arn:aws:sqs:us-east-1:000000000000:dlq"}},
	}
	m = p.restoreTransientFields(m)
	if err := p.persistESM(ctx, m); err != nil {
		t.Fatal(err)
	}
	p.startPollerForMapping(m)
	return p, m
}

type discard struct{}

func (discard) Write(b []byte) (int, error) { return len(b), nil }

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second) //nolint:clock test deadline is real elapsed time
	for time.Now().Before(deadline) {           //nolint:clock
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func state(t *testing.T, p *Provider, id string) string {
	t.Helper()
	m, err := p.loadESM(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return m.State
}

var msg = InternalMessage{MessageID: "m1", ReceiptHandle: "rh1", Body: "hi"}

// A queue whose owner is unreachable for longer than the error threshold must
// not disable the mapping; it resumes when the owner is back.
func TestSQSPollerSurvivesAnUnreachableQueueOwner(t *testing.T) {
	shortBackoff(t)
	down := model.NewProviderError(model.CodePluginUnavailable, "plugin down", 503)
	q := &fakeQueue{recv: func(n int) ([]InternalMessage, error) {
		switch {
		case n <= 2*maxConsecutiveErrors:
			return nil, down
		case n == 2*maxConsecutiveErrors+1:
			return []InternalMessage{msg}, nil
		}
		return nil, nil
	}}
	inv := &fakeInvoker{fn: func(int) error { return nil }}
	p, m := startMapping(t, q, inv, &fakeDLQ{})

	waitFor(t, "the message to be delivered after the outage", func() bool { return q.deletes() == 1 })
	if got := state(t, p, m.UUID); got != ESMStateEnabled {
		t.Fatalf("state = %s; an outage of the queue's owner must not disable the mapping", got)
	}
}

// An unreachable function owner: the batch is neither dead-lettered nor deleted
// (it returns after its visibility timeout) and the mapping stays enabled.
func TestSQSPollerDoesNotDeadLetterWhenTheFunctionOwnerIsUnreachable(t *testing.T) {
	shortBackoff(t)
	down := model.NewProviderError(model.CodePluginTimeout, "slow", 504)
	var mu sync.Mutex
	delivered := 0
	q := &fakeQueue{recv: func(int) ([]InternalMessage, error) {
		mu.Lock()
		defer mu.Unlock()
		if delivered < 1 {
			return []InternalMessage{msg}, nil // visibility timeout elapsed: the same message is back
		}
		return nil, nil
	}}
	dlq := &fakeDLQ{}
	inv := &fakeInvoker{fn: func(n int) error {
		if n <= 2*maxConsecutiveErrors {
			return down
		}
		mu.Lock()
		delivered++
		mu.Unlock()
		return nil
	}}
	p, m := startMapping(t, q, inv, dlq)

	waitFor(t, "successful delivery after the outage", func() bool { return q.deletes() == 1 })
	dlq.mu.Lock()
	sends := dlq.sends
	dlq.mu.Unlock()
	if sends != 0 {
		t.Fatalf("%d messages were dead-lettered during an outage", sends)
	}
	if inv.count() <= 2*maxConsecutiveErrors {
		t.Fatalf("invoker called %d times", inv.count())
	}
	if got := state(t, p, m.UUID); got != ESMStateEnabled {
		t.Fatalf("state = %s", got)
	}
}

func TestIsOutageOnlyMatchesUnreachableOwners(t *testing.T) {
	for err, want := range map[error]bool{
		model.NewProviderError(model.CodePluginUnavailable, "x", 503):                        true,
		model.NewProviderError(model.CodePluginTimeout, "x", 504):                            true,
		fmt.Errorf("wrapped: %w", model.NewProviderError(model.CodePluginTimeout, "x", 504)): true,
		model.NewProviderError("PluginProtocolError", "x", 502):                              false,
		model.NewProviderError("InternalFailure", "x", 500):                                  false,
		errors.New("plain"): false,
	} {
		if got := model.IsOutage(err); got != want {
			t.Errorf("IsOutage(%v) = %v, want %v", err, got, want)
		}
	}
}

type oneRecordStream struct{}

func (s *oneRecordStream) GetRecords(_ context.Context, _ string, after int) ([]stream.Record, int, error) {
	if after >= 1 {
		return nil, after, nil
	}
	return []stream.Record{{SequenceNumber: 1, EventID: "e1", EventName: "INSERT"}}, 1, nil
}
func (s *oneRecordStream) GetStreamInfo(context.Context, string) (stream.StreamInfo, bool, error) {
	return stream.StreamInfo{Enabled: true}, true, nil
}

// The Streams poller retries the same batch after an outage instead of giving up.
func TestStreamsPollerRetriesTheSameBatchAfterAnOutage(t *testing.T) {
	shortBackoff(t)
	down := model.NewProviderError(model.CodePluginUnavailable, "plugin down", 503)
	inv := &fakeInvoker{fn: func(n int) error {
		if n <= 2*maxConsecutiveErrors {
			return down
		}
		return nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	p := New(ctx, store.NewMemoryResourceStore(), inv, &fakeQueue{recv: func(int) ([]InternalMessage, error) { return nil, nil }},
		&oneRecordStream{}, slog.New(slog.NewTextHandler(discard{}, nil)))
	t.Cleanup(func() { cancel(); p.wg.Wait() })

	m := EventSourceMapping{
		UUID: "s1", FunctionName: "fn", EventSourceArn: "arn:aws:dynamodb:us-east-1:000000000000:table/t/stream/2024",
		State: ESMStateEnabled, Enabled: true, SourceType: ESMSourceDynamoDBStreams, TableName: "t",
		Cloud: "aws", Region: "us-east-1", AccountID: "000000000000",
	}
	if err := p.persistESM(ctx, m); err != nil {
		t.Fatal(err)
	}
	p.startPollerForMapping(m)

	waitFor(t, "the batch to be delivered once the owner is back", func() bool { return inv.count() > 2*maxConsecutiveErrors })
	waitFor(t, "the mapping to record the successful delivery", func() bool {
		got, err := p.loadESM(context.Background(), m.UUID)
		return err == nil && got.LastProcessingResult == "OK"
	})
}

// flakyStream is unreachable for the first `down` reads, then serves one record.
type flakyStream struct {
	oneRecordStream
	mu    sync.Mutex
	down  int
	calls int
}

func (s *flakyStream) fail() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls <= s.down {
		return model.NewProviderError(model.CodePluginUnavailable, "plugin down", 503)
	}
	return nil
}

func (s *flakyStream) GetStreamInfo(ctx context.Context, t string) (stream.StreamInfo, bool, error) {
	if err := s.fail(); err != nil {
		return stream.StreamInfo{}, false, err
	}
	return s.oneRecordStream.GetStreamInfo(ctx, t)
}

// The stream owner being down is waited out: the mapping is neither disabled nor
// advanced, and the records are delivered once the owner is back.
func TestStreamsPollerWaitsOutAnUnreachableStream(t *testing.T) {
	shortBackoff(t)
	inv := &fakeInvoker{fn: func(int) error { return nil }}
	ctx, cancel := context.WithCancel(context.Background())
	fs := &flakyStream{down: 2 * maxConsecutiveErrors}
	p := New(ctx, store.NewMemoryResourceStore(), inv, &fakeQueue{recv: func(int) ([]InternalMessage, error) { return nil, nil }},
		fs, slog.New(slog.NewTextHandler(discard{}, nil)))
	t.Cleanup(func() { cancel(); p.wg.Wait() })

	m := EventSourceMapping{
		UUID: "s2", FunctionName: "fn", EventSourceArn: "arn:aws:dynamodb:us-east-1:000000000000:table/t/stream/2024",
		State: ESMStateEnabled, Enabled: true, SourceType: ESMSourceDynamoDBStreams, TableName: "t",
		Cloud: "aws", Region: "us-east-1", AccountID: "000000000000",
	}
	if err := p.persistESM(ctx, m); err != nil {
		t.Fatal(err)
	}
	p.startPollerForMapping(m)

	waitFor(t, "the record to be delivered after the stream came back", func() bool { return inv.count() >= 1 })
	got, err := p.loadESM(context.Background(), m.UUID)
	if err != nil || got.State != ESMStateEnabled {
		t.Fatalf("mapping = %+v, %v; an unreachable stream must not disable it", got, err)
	}
}
