package pubsub

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/model"
)

// RetryPolicy bounds from the google.pubsub.v1.RetryPolicy documentation: each
// of minimum_backoff / maximum_backoff must be between 0 and 600 seconds
// (defaults 10s / 600s).
const (
	minRetryBackoff = 0 * time.Second
	maxRetryBackoff = 600 * time.Second
)

// normalizeRetryPolicy validates and canonicalizes a REST RetryPolicy body
// ({minimumBackoff, maximumBackoff} as google-duration strings) into the stored
// camelCase map. Only the fields the client supplied are kept. A malformed or
// out-of-range duration is InvalidArgument.
func normalizeRetryPolicy(rp map[string]any) (map[string]any, error) {
	out := map[string]any{}
	for _, f := range []string{"minimumBackoff", "maximumBackoff"} {
		raw, ok := rp[f]
		if !ok || raw == nil {
			continue
		}
		s, _ := raw.(string)
		if s == "" {
			return nil, model.NewProviderError("InvalidArgument",
				fmt.Sprintf("%s must be a google-duration string (got %v)", f, raw), 400)
		}
		d, err := parseProtoDuration(s)
		if err != nil {
			return nil, model.NewProviderError("InvalidArgument",
				fmt.Sprintf("%s is not a valid duration: %v", f, err), 400)
		}
		if d < minRetryBackoff || d > maxRetryBackoff {
			return nil, model.NewProviderError("InvalidArgument",
				fmt.Sprintf("%s must be between 0 and 600 seconds", f), 400)
		}
		out[f] = formatProtoDuration(d)
	}
	return out, nil
}

// applyRetryPolicyUpdate applies a REST update_mask path to meta["retryPolicy"].
// A `retry_policy` root path replaces the whole policy (an absent/empty carried
// policy clears it); a nested `retry_policy.minimum_backoff` /
// `retry_policy.maximum_backoff` path updates only that leaf and preserves the
// other, matching AIP-161 field-mask semantics. path is the raw mask path.
func applyRetryPolicyUpdate(meta, in map[string]any, path string) error {
	leaf, nested, ok := retryPolicyLeaf(path)
	if !ok {
		return model.NewProviderError("InvalidArgument", "unsupported update_mask path: "+path, 400)
	}
	raw, present := in["retryPolicy"]
	carried, _ := raw.(map[string]any)
	if present && raw != nil && carried == nil {
		return model.NewProviderError("InvalidArgument", "retryPolicy must be an object", 400)
	}
	normalized, err := normalizeRetryPolicy(carried)
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
	// Reject a magnitude that would overflow time.Duration before scaling: an
	// unchecked secs*time.Second could wrap and defeat the caller's range check.
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
