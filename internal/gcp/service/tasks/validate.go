package tasks

import (
	"strings"
	"time"

	"jaiscloud/internal/gcp/aip160"
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

// queueFilter compiles a Cloud Tasks `projects.locations.queues.list` filter.
// The Discovery document defers to Stackdriver's Advanced Logs Filters syntax —
// `=` is equality on the field value, `:` is the contains operator — and allows
// any Queue field. The emulator implements the scalar Queue fields it renders
// (name, state and purgeTime) with the shared bounded AIP-160 compiler; a filter
// outside that subset (an unknown field, OR/NOT, parentheses, ordering
// comparisons or a nested message field) is INVALID_ARGUMENT rather than
// silently ignored. An empty filter matches every queue.
func queueFilter(project, location, filter string) (aip160.Filter[tasksstore.Queue], error) {
	return aip160.Compile("tasks queues.list", filter, aip160.Fields[tasksstore.Queue]{
		Desc: "name, state and purgeTime",
		Name: func(q tasksstore.Queue) string { return QueueName(project, location, q.Name) },
		Strings: map[string]func(tasksstore.Queue) string{
			"state": func(q tasksstore.Queue) string { return string(q.State) },
		},
		Times: map[string]func(tasksstore.Queue) time.Time{
			"purgetime": func(q tasksstore.Queue) time.Time { return q.PurgeTime },
		},
	})
}
