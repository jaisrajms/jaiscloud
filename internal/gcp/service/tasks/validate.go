package tasks

import (
	"strings"
	"time"

	tasksstore "jaiscloud/internal/gcp/store/tasks"
)

// Default rate-limit and retry values Cloud Tasks applies when a create/update
// omits them (echoed back on the wire).
func defaultRateLimits() *tasksstore.RateLimits {
	return &tasksstore.RateLimits{
		MaxDispatchesPerSecond:  500,
		MaxBurstSize:            100,
		MaxConcurrentDispatches: 1000,
	}
}

func defaultRetryConfig() *tasksstore.RetryConfig {
	return &tasksstore.RetryConfig{
		MaxAttempts:  100,
		MinBackoff:   100 * time.Millisecond,
		MaxBackoff:   time.Hour,
		MaxDoublings: 16,
	}
}

func applyQueueDefaults(q *tasksstore.Queue) {
	if q.RateLimits == nil {
		q.RateLimits = defaultRateLimits()
	}
	if q.RetryConfig == nil {
		q.RetryConfig = defaultRetryConfig()
	}
}

func validateQueue(q *tasksstore.Queue) error {
	if q.RateLimits != nil {
		r := q.RateLimits
		if r.MaxDispatchesPerSecond < 0 || r.MaxDispatchesPerSecond > 500 {
			return invalidArgument("rateLimits.maxDispatchesPerSecond must be between 0 and 500")
		}
		if r.MaxBurstSize < 0 {
			return invalidArgument("rateLimits.maxBurstSize must be non-negative")
		}
		if r.MaxConcurrentDispatches < 0 {
			return invalidArgument("rateLimits.maxConcurrentDispatches must be non-negative")
		}
	}
	if q.RetryConfig != nil {
		r := q.RetryConfig
		if r.MaxAttempts < -1 {
			return invalidArgument("retryConfig.maxAttempts must be >= -1")
		}
		if r.MaxDoublings < 0 {
			return invalidArgument("retryConfig.maxDoublings must be non-negative")
		}
		if r.MinBackoff < 0 || r.MaxBackoff < 0 || r.MaxRetryDuration < 0 {
			return invalidArgument("retryConfig durations must be non-negative")
		}
		if r.MinBackoff > 0 && r.MaxBackoff > 0 && r.MinBackoff > r.MaxBackoff {
			return invalidArgument("retryConfig.minBackoff must not exceed maxBackoff")
		}
	}
	return nil
}

// normalizeField maps a Discovery camelCase or proto snake_case update-mask
// path to the canonical lower-case, underscore-free key used by
// applyQueueUpdate and applyTaskUpdate.
func normalizeField(f string) string {
	f = strings.TrimSpace(f)
	if i := strings.IndexByte(f, '.'); i >= 0 {
		f = f[:i]
	}
	return strings.ReplaceAll(strings.ToLower(f), "_", "")
}

func applyQueueUpdate(cur, upd tasksstore.Queue, set map[string]bool, full bool) tasksstore.Queue {
	next := cur
	has := func(f string) bool { return full || set[f] }
	if has("appengineroutingoverride") {
		next.AppEngineRoutingOverride = upd.AppEngineRoutingOverride
	}
	if has("ratelimits") {
		next.RateLimits = upd.RateLimits
	}
	if has("retryconfig") {
		next.RetryConfig = upd.RetryConfig
	}
	if has("stackdriverloggingconfig") {
		next.StackdriverLoggingConfig = upd.StackdriverLoggingConfig
	}
	return next
}

// matchQueueFilter applies a Cloud Tasks list filter of the form
// `name="<substring>"` or a bare substring. Empty filters are handled by the
// caller.
func matchQueueFilter(filter string, q tasksstore.Queue) bool {
	f := strings.TrimSpace(filter)
	if i := strings.IndexByte(f, '='); i >= 0 {
		f = strings.TrimSpace(f[i+1:])
	}
	f = strings.Trim(f, `"`)
	if f == "" {
		return true
	}
	return strings.Contains(q.Name, f)
}
