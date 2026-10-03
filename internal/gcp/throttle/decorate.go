package throttle

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"jaiscloud/internal/model"
)

// retryInfoType is the protobuf Any type URL for google.rpc.RetryInfo.
const retryInfoType = "type.googleapis.com/google.rpc.RetryInfo"

// DecorateError enriches a REST error response produced by the injector: it
// sets the Retry-After header and, when the body is a Google JSON error
// envelope, merges a google.rpc.RetryInfo detail into error.details. It is a
// no-op for a nil error, a zero RetryAfter, or a non-JSON body, so it never
// alters a response that did not come from the throttle.
func DecorateError(perr *model.ProviderError, status int, headers http.Header, body []byte) (int, http.Header, []byte) {
	if perr == nil || perr.RetryAfter <= 0 {
		return status, headers, body
	}
	if headers == nil {
		headers = http.Header{}
	}
	headers.Set("Retry-After", strconv.Itoa(retryAfterSeconds(perr.RetryAfter)))

	var env map[string]any
	if err := json.Unmarshal(body, &env); err != nil {
		return status, headers, body
	}
	errObj, ok := env["error"].(map[string]any)
	if !ok {
		return status, headers, body
	}
	errObj["details"] = []any{map[string]any{
		"@type":      retryInfoType,
		"retryDelay": formatProtoDuration(perr.RetryAfter),
	}}
	out, err := json.Marshal(env)
	if err != nil {
		return status, headers, body
	}
	return status, headers, out
}

// retryAfterSeconds renders the integer seconds a Retry-After header expects,
// rounding up and never reporting less than one second.
func retryAfterSeconds(d time.Duration) int {
	s := int(math.Ceil(d.Seconds()))
	if s < 1 {
		s = 1
	}
	return s
}

// formatProtoDuration renders a time.Duration in the protobuf JSON duration
// form (e.g. "1s", "1.500s", "0.001500s") that google.rpc.RetryInfo.retryDelay
// uses. The protobuf JSON mapping always emits 0, 3, 6, or 9 fractional digits
// depending on the precision required.
func formatProtoDuration(d time.Duration) string {
	neg := ""
	if d < 0 {
		neg = "-"
		d = -d
	}
	secs := d / time.Second
	nanos := d - secs*time.Second
	if nanos == 0 {
		return fmt.Sprintf("%s%ds", neg, secs)
	}
	digits := 9
	switch {
	case nanos%1000000 == 0:
		digits = 3
	case nanos%1000 == 0:
		digits = 6
	}
	frac := fmt.Sprintf("%09d", nanos)[:digits]
	return fmt.Sprintf("%s%d.%ss", neg, secs, frac)
}
