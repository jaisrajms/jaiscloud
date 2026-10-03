package gcp

import (
	"net/http"

	"jaiscloud/internal/gcp/throttle"
	"jaiscloud/internal/model"
)

// GCPAdapter implements gateway.RequestFilter when a throttle injector is
// installed. The interface is optional and type-asserted by the gateway, so an
// adapter without SetThrottle keeps the exact pre-existing behaviour.

// SetThrottle installs the opt-in throttle/quota injector. A nil injector
// (or a disabled one) makes FilterRequest always allow.
func (a *GCPAdapter) SetThrottle(inj *throttle.Injector) { a.throttle = inj }

// FilterRequest implements gateway.RequestFilter by consulting the injector
// with the resolved project and the decoded service/action. It returns nil when
// no injector is armed, so the request proceeds to dispatch normally.
func (a *GCPAdapter) FilterRequest(nr *model.NormalizedRequest) *model.ProviderError {
	if a.throttle == nil || nr == nil {
		return nil
	}
	return a.throttle.Check(nr.AccountID, nr.Service, nr.Action)
}

// DecorateError implements gateway.RequestFilter, adding the Retry-After header
// and google.rpc.RetryInfo detail to an injected error response.
func (a *GCPAdapter) DecorateError(_ *model.NormalizedRequest, perr *model.ProviderError, status int, headers http.Header, body []byte) (int, http.Header, []byte) {
	return throttle.DecorateError(perr, status, headers, body)
}
