package storage

import (
	"encoding/json"
	"strconv"
)

// OUTPUT_ONLY members inside the opaque policy blocks are server-owned: GCS
// ignores a value supplied by the caller rather than echoing it. A create/update
// body therefore stores only the writable members, and the server stamps the
// canonical effectiveTime (and preserves a retention lock, which is only set by
// buckets.lockRetentionPolicy). See the storage.v2 Bucket docs:
// RetentionPolicy.is_locked/effective_time and SoftDeletePolicy.effective_time
// are output only.

// normalizeRetentionPolicyInput strips the server-owned members from a
// caller-supplied retentionPolicy and stamps the effectiveTime the server
// assigns. Returns nil when the block carries no retentionPeriod.
func normalizeRetentionPolicyInput(rp map[string]any, now string) map[string]any {
	if rp == nil {
		return nil
	}
	period := decimalString(rp["retentionPeriod"])
	if period == "" {
		return nil
	}
	return map[string]any{
		"retentionPeriod": period,
		"effectiveTime":   now,
	}
}

// normalizeSoftDeletePolicyInput strips the server-owned effectiveTime and
// stamps the apply time. Returns nil when no retentionDurationSeconds is given.
func normalizeSoftDeletePolicyInput(sd map[string]any, now string) map[string]any {
	if sd == nil {
		return nil
	}
	seconds := decimalString(sd["retentionDurationSeconds"])
	if seconds == "" {
		return nil
	}
	return map[string]any{
		"retentionDurationSeconds": seconds,
		"effectiveTime":            now,
	}
}

// decimalString renders a JSON int64 leaf (the JSON decoder yields float64, an
// in-process caller may use a string or an integer) as the decimal string the
// JSON API uses on the wire. It returns "" for an absent/unusable value.
func decimalString(v any) string {
	switch n := v.(type) {
	case string:
		return n
	case float64:
		return strconv.FormatInt(int64(n), 10)
	case float32:
		return strconv.FormatInt(int64(n), 10)
	case int:
		return strconv.FormatInt(int64(n), 10)
	case int32:
		return strconv.FormatInt(int64(n), 10)
	case int64:
		return strconv.FormatInt(n, 10)
	case json.Number:
		return n.String()
	default:
		return ""
	}
}
