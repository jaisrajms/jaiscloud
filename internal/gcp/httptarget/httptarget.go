// Package httptarget performs a single HTTP delivery attempt on behalf of the
// engine-bearing GCP services (Cloud Scheduler's cron runner and Cloud Tasks'
// dispatch engine). Both engines must attach their service-specific headers and
// interpret a 2xx response as success; this package holds that shared logic so
// the two engines cannot drift.
package httptarget

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"

	"google.golang.org/grpc/codes"
)

// defaultDeadline bounds an attempt when the caller supplies no target
// deadline. Both engines normally pass their own (Cloud Scheduler uses the
// attempt deadline, Cloud Tasks the task's dispatchDeadline).
const defaultDeadline = 3 * time.Minute

// Request is one HTTP delivery attempt. Method may be empty or a proto
// unspecified sentinel, in which case POST is used. Headers are attached as-is;
// the caller supplies any service-specific headers. Body is sent verbatim.
//
// The two engines differ on Content-Type: Cloud Scheduler defaults a body's
// Content-Type to application/octet-stream, while Cloud Tasks documents that it
// "won't be set by Cloud Tasks". Callers therefore opt out with
// SkipContentTypeDefault.
type Request struct {
	Method   string
	URL      string
	Headers  map[string]string
	Body     []byte
	Deadline time.Duration
	// SkipContentTypeDefault suppresses the application/octet-stream default
	// when a body is present and no Content-Type was supplied.
	SkipContentTypeDefault bool
}

// Result reports the outcome of one attempt. Code is 0 when the target
// answered 2xx; otherwise it carries the HTTP status code, or a gRPC-style code
// (Unavailable/InvalidArgument) for a transport-level failure with no response.
// StatusCode is the HTTP status code when a response was received, else 0.
type Result struct {
	Code       int32
	Message    string
	StatusCode int
}

// Deliver performs the attempt and returns its outcome. The context bounds the
// attempt in addition to Request.Deadline.
func Deliver(ctx context.Context, client *http.Client, r Request) Result {
	method := r.Method
	switch method {
	case "", "HTTP_METHOD_UNSPECIFIED", "METHOD_UNSPECIFIED":
		method = http.MethodPost
	}
	var body io.Reader
	if len(r.Body) > 0 {
		body = bytes.NewReader(r.Body)
	}
	deadline := r.Deadline
	if deadline <= 0 {
		deadline = defaultDeadline
	}
	cctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	req, err := http.NewRequestWithContext(cctx, method, r.URL, body)
	if err != nil {
		return Result{Code: int32(codes.InvalidArgument), Message: err.Error()}
	}
	for k, v := range r.Headers {
		req.Header.Set(k, v)
	}
	if len(r.Body) > 0 && !r.SkipContentTypeDefault && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	resp, err := client.Do(req)
	if err != nil {
		return Result{Code: int32(codes.Unavailable), Message: err.Error()}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return Result{StatusCode: resp.StatusCode}
	}
	return Result{Code: int32(resp.StatusCode), Message: resp.Status, StatusCode: resp.StatusCode}
}
