package awslambda

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"sync"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/logstream"
)

// ringBuffer keeps the last cap bytes of data (drops oldest on overflow).
type ringBuffer struct {
	mu   sync.Mutex
	data []byte
	cap  int
}

func newRingBuffer(cap int) *ringBuffer { return &ringBuffer{cap: cap} }

func (r *ringBuffer) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data = append(r.data, p...)
	if len(r.data) > r.cap {
		r.data = r.data[len(r.data)-r.cap:]
	}
	return len(p), nil
}

func (r *ringBuffer) Bytes() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]byte, len(r.data))
	copy(out, r.data)
	return out
}

// LogStreamer tails an io.Reader, writes lines to CloudWatch Logs, and keeps a
// 4 KiB tail ring.
type LogStreamer struct {
	logsAPI       LogsIngestor
	ring          *ringBuffer
	logGroupName  string
	logStreamName string
}

// NewLogStreamer constructs a LogStreamer for the given function invocation.
func NewLogStreamer(logsAPI LogsIngestor, funcName, invocationID string) *LogStreamer {
	date := clock.RealNow().Format("2006/01/02")
	return &LogStreamer{
		logsAPI:       logsAPI,
		ring:          newRingBuffer(4096),
		logGroupName:  "/aws/lambda/" + funcName,
		logStreamName: fmt.Sprintf("%s/[$LATEST]%s", date, invocationID),
	}
}

// Stream reads from src line by line, writing to CW Logs and the tail ring.
// Blocks until EOF or ctx cancellation.
func (s *LogStreamer) Stream(ctx context.Context, src io.Reader) {
	if s.logsAPI != nil {
		_ = s.logsAPI.InternalCreateLogGroup(ctx, s.logGroupName)
	}
	scanner := bufio.NewScanner(src)
	var batch []logstream.Event
	for scanner.Scan() {
		line := scanner.Text()
		s.ring.Write([]byte(line + "\n"))
		if s.logsAPI != nil {
			batch = append(batch, logstream.Event{Timestamp: clock.RealNow().UnixMilli(), Message: line})
			if len(batch) >= 10 {
				_ = s.logsAPI.InternalPutEvents(ctx, s.logGroupName, s.logStreamName, batch)
				batch = batch[:0]
			}
		}
	}
	if s.logsAPI != nil && len(batch) > 0 {
		_ = s.logsAPI.InternalPutEvents(ctx, s.logGroupName, s.logStreamName, batch)
	}
}

// TailB64 returns the last <=4 KiB of log output, base64-encoded.
func (s *LogStreamer) TailB64() string {
	return base64.StdEncoding.EncodeToString(s.ring.Bytes())
}
