package serviceaccount

import (
	"encoding/json"
	"strings"
	"time"

	"jaiscloud/internal/model"
)

// MaxJWTLifetime is the documented upper bound for the `exp` claim of a signed
// JWT and for a generated access-token lifetime (12 hours).
const MaxJWTLifetime = 12 * time.Hour

// Invalid returns a 400 InvalidArgument provider error. It is the shared
// spelling for the validation failures of both service-account surfaces.
func Invalid(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

// ValidateBlobPayload rejects an empty blob, which both the IAM and IAM
// Credentials signBlob methods require (real GCP returns INVALID_ARGUMENT).
func ValidateBlobPayload(raw []byte) error {
	if len(raw) == 0 {
		return Invalid("payload is required")
	}
	return nil
}

// ValidateJWTPayload enforces the documented SignJwt bounds on a claims set:
// the payload must be a serialized JSON object and, when it carries an `exp`
// claim, that claim must be an integer timestamp that is not in the past and no
// more than 12 hours in the future.
func ValidateJWTPayload(payload string, now time.Time) error {
	if strings.TrimSpace(payload) == "" {
		return Invalid("payload is required")
	}
	var claims map[string]any
	if err := json.Unmarshal([]byte(payload), &claims); err != nil {
		return Invalid("payload must be a serialized JSON object")
	}
	exp, ok := claims["exp"]
	if !ok {
		return nil
	}
	seconds, ok := exp.(float64)
	if !ok {
		return Invalid("exp claim must be an integer timestamp")
	}
	e := int64(seconds)
	if e < now.Unix() {
		return Invalid("exp claim is in the past")
	}
	if e > now.Add(MaxJWTLifetime).Unix() {
		return Invalid("exp claim is more than 12 hours in the future")
	}
	return nil
}
