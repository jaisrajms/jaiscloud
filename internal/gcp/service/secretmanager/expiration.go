// Secret expiration and delayed version destruction shared by the Secret
// Manager REST provider and gRPC service (DC70).
//
// Two lifecycle features were unmodelled, so two documented event variants had
// no trigger:
//
//   - `Secret.versionDestroyTtl` is the delayed-destruction delay. Destroying a
//     version on a secret that sets it does not destroy immediately: the version
//     moves to DISABLED with an output-only `scheduledDestroyTime`, and is
//     permanently destroyed when that instant passes. The attempt publishes
//     SECRET_VERSION_DESTROY_SCHEDULED (the destroy itself later publishes
//     SECRET_VERSION_DESTROY). Both the create and the patch surface take the
//     field; the guide bounds it to 1 day..1,000 days.
//   - `Secret.expireTime`/`ttl` is the secret's automatic deletion. `ttl` is
//     input-only and resolves to `expireTime` at write time (which is always
//     returned on output); at that instant the secret and all its versions are
//     irreversibly deleted and SECRET_DELETE is published with
//     deleteType: EXPIRATION. The guide bounds an expiration to 60 seconds..
//     100 years from the request.
//
// This package owns the parsing/validation and the due checks both transports
// share, so REST and gRPC cannot disagree about what a valid delay is or when a
// record has expired.
package secretmanager

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/model"
)

const (
	// MinSecretExpirationLead is the documented minimum time until expiration
	// ("A secret's expiration cannot be less than 60 seconds away").
	MinSecretExpirationLead = 60 * time.Second
	// MaxSecretExpiration is the documented maximum ("... or more than 100
	// years away"); the same 3,153,600,000s cap the rotation fields use.
	MaxSecretExpiration = 3153600000 * time.Second
	// MinVersionDestroyTTL/MaxVersionDestroyTTL bound the delayed-destruction
	// delay ("The minimum duration required is 1 day while maximum duration can
	// be set to 1000 days").
	MinVersionDestroyTTL = 24 * time.Hour
	MaxVersionDestroyTTL = 1000 * 24 * time.Hour
)

// invalidLifecycle is the 400 INVALID_ARGUMENT every expiration/delay violation
// answers with.
func invalidLifecycle(format string, args ...any) error {
	return model.NewProviderError("InvalidArgument", fmt.Sprintf(format, args...), 400)
}

// parseDurationSeconds parses a google-duration JSON value the way Secret
// Manager accepts its duration fields: seconds with the `s` suffix ("3600s",
// "86400s", "3600.500s"), matching protobuf's duration JSON encoding. Go's own
// duration syntax ("1h", "24h") is not the wire format, so it is rejected, and
// so is a negative value.
func parseDurationSeconds(field, s string) (time.Duration, error) {
	raw := strings.TrimSpace(s)
	if !strings.HasSuffix(raw, "s") {
		return 0, invalidLifecycle("%s must be a duration in seconds (for example \"3600s\")", field)
	}
	num := strings.TrimSuffix(raw, "s")
	// Parse integer seconds and up to nine fractional digits exactly: a
	// float64 cannot represent every nanosecond at multi-day magnitudes, which
	// would make a canonical value echo back one nanosecond off.
	whole, frac, _ := strings.Cut(num, ".")
	secs, err := strconv.ParseInt(whole, 10, 64)
	if err != nil || secs < 0 || strings.HasPrefix(whole, "+") {
		return 0, invalidLifecycle("%s must be a duration in seconds (for example \"3600s\")", field)
	}
	var nanos int64
	if frac != "" || strings.Contains(num, ".") {
		if len(frac) == 0 || len(frac) > 9 {
			return 0, invalidLifecycle("%s must be a duration in seconds (for example \"3600s\")", field)
		}
		for _, c := range frac {
			if c < '0' || c > '9' {
				return 0, invalidLifecycle("%s must be a duration in seconds (for example \"3600s\")", field)
			}
		}
		padded := frac + strings.Repeat("0", 9-len(frac))
		nanos, _ = strconv.ParseInt(padded, 10, 64)
	}
	// Bound the parsed value before converting it to a Duration: the
	// multiplication must not overflow the int64-nanosecond range.
	maxSecs := int64(math.MaxInt64 / int64(time.Second))
	if secs > maxSecs || (secs == maxSecs && nanos > int64(math.MaxInt64)-maxSecs*int64(time.Second)) {
		return 0, invalidLifecycle("%s must be a duration in seconds (for example \"3600s\")", field)
	}
	return time.Duration(secs)*time.Second + time.Duration(nanos), nil
}

// ParseVersionDestroyTTL parses a `versionDestroyTtl` value and validates the
// documented 1-day..1,000-day range.
func ParseVersionDestroyTTL(v string) (time.Duration, error) {
	d, err := parseDurationSeconds("versionDestroyTtl", v)
	if err != nil {
		return 0, err
	}
	if err := ValidateVersionDestroyTTL(d); err != nil {
		return 0, err
	}
	return d, nil
}

// ValidateVersionDestroyTTL checks a parsed delay against the documented range.
func ValidateVersionDestroyTTL(d time.Duration) error {
	if d < MinVersionDestroyTTL || d > MaxVersionDestroyTTL {
		return invalidLifecycle("versionDestroyTtl must be between 86400s (1 day) and 86400000s (1000 days), got %q", DurationString(d))
	}
	return nil
}

// ParseTTL parses the input-only `ttl` field and resolves it to the expiration
// instant it denotes, validating the documented window.
func ParseTTL(raw string, now time.Time) (time.Time, error) {
	d, err := parseDurationSeconds("ttl", raw)
	if err != nil {
		return time.Time{}, err
	}
	return ExpireTimeFromTTL(d, now)
}

// ExpireTimeFromTTL resolves the input-only `ttl` field to the output
// `expireTime` instant, validating the documented 60-second..100-year window.
func ExpireTimeFromTTL(ttl time.Duration, now time.Time) (time.Time, error) {
	if err := ValidateExpireTime(now.Add(ttl), now); err != nil {
		return time.Time{}, err
	}
	return now.Add(ttl), nil
}

// ParseExpireTime parses an `expireTime` timestamp and validates the documented
// window relative to the request instant.
func ParseExpireTime(raw string, now time.Time) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, invalidLifecycle("expireTime %q is not an RFC 3339 timestamp", raw)
	}
	if err := ValidateExpireTime(t, now); err != nil {
		return time.Time{}, err
	}
	return t, nil
}

// ValidateExpireTime checks the documented 60-second..100-year window.
func ValidateExpireTime(t, now time.Time) error {
	if t.Before(now.Add(MinSecretExpirationLead)) {
		return invalidLifecycle("expireTime must be at least 60 seconds in the future")
	}
	if t.After(now.Add(MaxSecretExpiration)) {
		return invalidLifecycle("expireTime must be at most 100 years in the future")
	}
	return nil
}

// Expired reports whether the secret's expiration has been reached. Zero means
// the secret does not expire.
func Expired(sec secretmanagerstore.Secret, now time.Time) bool {
	return !sec.ExpireTime.IsZero() && !now.Before(sec.ExpireTime)
}

// ScheduledDestroyDue reports whether a version scheduled for delayed
// destruction has reached its destruction instant and must now become
// DESTROYED. A DESTROYED version is never due again.
func ScheduledDestroyDue(v secretmanagerstore.Version, now time.Time) bool {
	return v.State != "DESTROYED" && !v.ScheduledDestroyTime.IsZero() && !now.Before(v.ScheduledDestroyTime)
}
