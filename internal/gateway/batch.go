package gateway

import (
	"context"
	"net/http"
)

// BatchProcessFunc executes one embedded batch sub-request and returns the
// encoded response. The gateway supplies it to a BatchHandler; ctx is the
// outer batch request's context, r is the reconstructed sub-request, and body
// is the sub-request's fully-buffered request body.
type BatchProcessFunc func(ctx context.Context, r *http.Request, body []byte) (status int, headers http.Header, respBody []byte)

// BatchHandler is an optional interface a CloudAdapter may implement to expose
// its cloud's HTTP batch endpoint (the google-api-client batch protocol). It is
// consumed only by the gateway, so it lives with the consumer rather than in
// the cloud-agnostic adapter contract; clouds without a batch endpoint (AWS,
// Azure) simply do not implement it. The gateway detects a batch request before
// normal service detection and delegates the wire parsing/formatting to the
// implementation, while the gateway owns execution of each embedded sub-request
// via BatchProcessFunc.
type BatchHandler interface {
	// IsBatchRequest reports whether r targets the cloud's batch endpoint.
	IsBatchRequest(r *http.Request) bool
	// ServeBatch parses r's multipart body, invokes process for each embedded
	// sub-request, and writes the multiplexed response to w.
	ServeBatch(ctx context.Context, w http.ResponseWriter, r *http.Request, body []byte, process BatchProcessFunc)
}

// maxBatchSubResponseBytes caps the buffered body of one batch sub-response.
// Batch endpoints are metadata-only in the protocols the emulator serves, so a
// streaming sub-response is unexpected; the cap prevents an unbounded read if
// one ever occurs.
const maxBatchSubResponseBytes = 64 << 20
