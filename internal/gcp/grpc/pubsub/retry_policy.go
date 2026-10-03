package pubsub

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"jaiscloud/internal/model"

	"google.golang.org/protobuf/types/known/durationpb"
)

// RetryPolicy bounds from the google.pubsub.v1.RetryPolicy documentation: each
// of minimum_backoff / maximum_backoff must be between 0 and 600 seconds
// (defaults 10s / 600s).
const (
	minRetryBackoff = 0 * time.Second
	maxRetryBackoff = 600 * time.Second
)

// normalizeRetryPolicy validates and canonicalizes a subscription's
// retry_policy into the stored camelCase map. Only the fields the client
// supplied are kept. An out-of-range backoff is InvalidArgument. The policy is
// stored so it round-trips on get/list/update; it has no effect on the
// emulator's fixed delivery backoff (documented limitation).
func normalizeRetryPolicy(rp *pubsubpb.RetryPolicy) (map[string]any, error) {
	out := map[string]any{}
	if d := rp.GetMinimumBackoff(); d != nil {
		v, err := validateRetryBackoff("minimumBackoff", d)
		if err != nil {
			return nil, err
		}
		out["minimumBackoff"] = v
	}
	if d := rp.GetMaximumBackoff(); d != nil {
		v, err := validateRetryBackoff("maximumBackoff", d)
		if err != nil {
			return nil, err
		}
		out["maximumBackoff"] = v
	}
	return out, nil
}

// validateRetryBackoff bounds-checks one backoff and renders it as a canonical
// google-duration string. It checks the raw seconds before calling AsDuration,
// which could itself overflow on a pathological input.
func validateRetryBackoff(field string, d *durationpb.Duration) (string, error) {
	if d.GetSeconds() < 0 || d.GetSeconds() > int64(maxRetryBackoff/time.Second) ||
		d.GetNanos() < 0 || d.GetNanos() >= 1_000_000_000 {
		return "", mapError(model.NewProviderError("InvalidArgument",
			fmt.Sprintf("%s must be between 0 and 600 seconds", field), 400))
	}
	return formatProtoDuration(d.AsDuration()), nil
}

// applyRetryPolicyUpdateProto applies an update_mask path to meta["retryPolicy"].
// A `retry_policy` root path replaces the whole policy (an absent/empty carried
// policy clears it); a nested `retry_policy.minimum_backoff` /
// `retry_policy.maximum_backoff` path updates only that leaf and preserves the
// other, matching AIP-161 field-mask semantics.
func applyRetryPolicyUpdateProto(meta map[string]any, in *pubsubpb.Subscription, path string) error {
	leaf, nested, ok := retryPolicyLeaf(path)
	if !ok {
		return mapError(model.NewProviderError("InvalidArgument", "unsupported update_mask path: "+path, 400))
	}
	normalized, err := normalizeRetryPolicy(in.GetRetryPolicy())
	if err != nil {
		return err
	}
	if !nested {
		if len(normalized) == 0 {
			delete(meta, "retryPolicy")
		} else {
			meta["retryPolicy"] = normalized
		}
		return nil
	}
	existing, _ := meta["retryPolicy"].(map[string]any)
	merged := make(map[string]any, len(existing)+1)
	for k, v := range existing {
		merged[k] = v
	}
	if v, present := normalized[leaf]; present {
		merged[leaf] = v
	} else {
		// The leaf is absent (or null) in the carried policy: clear just it.
		delete(merged, leaf)
	}
	if len(merged) == 0 {
		delete(meta, "retryPolicy")
	} else {
		meta["retryPolicy"] = merged
	}
	return nil
}

// retryPolicyLeaf resolves a raw field-mask path to the RetryPolicy leaf it
// names: ("", false, true) for the `retry_policy` root, ("minimumBackoff",
// true, true) / ("maximumBackoff", true, true) for a nested leaf, and
// ("", false, false) for anything else. Path spelling is folded so snake_case
// and camelCase resolve alike.
func retryPolicyLeaf(path string) (leaf string, nested, ok bool) {
	switch normalizeRetryPolicyPath(path) {
	case "retrypolicy":
		return "", false, true
	case "retrypolicy.minimumbackoff":
		return "minimumBackoff", true, true
	case "retrypolicy.maximumbackoff":
		return "maximumBackoff", true, true
	}
	return "", false, false
}

// normalizeRetryPolicyPath folds a field-mask path to a case/underscore-
// insensitive form.
func normalizeRetryPolicyPath(p string) string {
	return strings.ToLower(strings.ReplaceAll(p, "_", ""))
}

// parseProtoDuration parses a google.protobuf.Duration JSON string: a signed
// decimal number of seconds with up to nine fractional digits, suffixed with
// "s" (e.g. "10s", "600s", "1.500s").
func parseProtoDuration(s string) (time.Duration, error) {
	if !strings.HasSuffix(s, "s") {
		return 0, fmt.Errorf("duration %q must end in 's'", s)
	}
	body := strings.TrimSuffix(s, "s")
	neg := strings.HasPrefix(body, "-")
	body = strings.TrimPrefix(body, "-")
	intPart, fracPart, _ := strings.Cut(body, ".")
	if intPart == "" {
		return 0, fmt.Errorf("duration %q has no seconds", s)
	}
	secs, err := strconv.ParseInt(intPart, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid seconds in %q", s)
	}
	// Reject a magnitude that would overflow time.Duration before scaling.
	if secs > int64(math.MaxInt64)/int64(time.Second) {
		return 0, fmt.Errorf("duration %q is too large", s)
	}
	var nanos int64
	if fracPart != "" {
		if len(fracPart) > 9 {
			return 0, fmt.Errorf("duration %q has more than nine fractional digits", s)
		}
		nanos, err = strconv.ParseInt(fracPart+strings.Repeat("0", 9-len(fracPart)), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid fraction in %q", s)
		}
	}
	d := time.Duration(secs)*time.Second + time.Duration(nanos)
	if neg {
		d = -d
	}
	return d, nil
}

// formatProtoDuration renders a duration as a canonical google-duration string
// (seconds, with optional fractional digits, suffixed with "s").
func formatProtoDuration(d time.Duration) string {
	neg := d < 0
	if neg {
		d = -d
	}
	secs := d / time.Second
	nanos := d % time.Second
	s := strconv.FormatInt(int64(secs), 10)
	if nanos != 0 {
		s += "." + strings.TrimRight(fmt.Sprintf("%09d", nanos), "0")
	}
	if neg {
		s = "-" + s
	}
	return s + "s"
}
