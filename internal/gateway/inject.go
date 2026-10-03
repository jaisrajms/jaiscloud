package gateway

import (
	"net/http"

	"jaiscloud/internal/model"
)

// RequestFilter is an optional interface a CloudAdapter may implement to refuse
// requests before dispatch and to adjust the encoded error response. It follows
// the same optional-capability pattern as BatchHandler: the gateway type-asserts
// the adapter, so a cloud that does not implement it is completely unaffected.
//
// GCP uses it for the opt-in throttle/quota injector (internal/gcp/throttle):
// FilterRequest returns a retryable error for matching requests, and
// DecorateError attaches the service-appropriate retry hints to that response.
type RequestFilter interface {
	// FilterRequest returns a non-nil ProviderError to fail the request before
	// it reaches the registry, or nil to allow it. It runs after EnrichRequest,
	// so identity (e.g. the GCP project) is available on nr.
	FilterRequest(nr *model.NormalizedRequest) *model.ProviderError

	// DecorateError adjusts the response encoded for a ProviderError returned by
	// FilterRequest (for example to add a Retry-After header and a
	// google.rpc.RetryInfo detail). It returns the final status, headers, body.
	DecorateError(nr *model.NormalizedRequest, perr *model.ProviderError, status int, headers http.Header, body []byte) (int, http.Header, []byte)
}
