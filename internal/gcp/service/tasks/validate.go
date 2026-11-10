package tasks

import (
	"strings"
	"time"

	"jaiscloud/internal/gcp/aip160"
	tasksstore "jaiscloud/internal/gcp/store/tasks"
)

// Documented RateLimits defaults. The v2 Discovery document (revision
// 20261106) says a queue created without them gets a server-picked value:
// maxDispatchesPerSecond is "If unspecified when the queue is created, Cloud
// Tasks will pick the default" (500 dispatches/second, the App Engine `rate`
// default this field mirrors), maxConcurrentDispatches likewise (1000, the
// `max_concurrent_requests` default), and maxBurstSize is **output only**:
// "Cloud Tasks automatically sets an appropriate `max_burst_size` based on the
// value of `max_dispatches_per_second`". The Discovery does not state the
// numeric burst default; real Cloud Tasks answers 100 for a queue at the
// default rate, which is what these defaults carry (the same value the engine's
// queueLimits fallback uses). A caller-supplied value is always echoed.
const (
	defaultMaxDispatchesPerSecond  = 500
	defaultMaxBurstSize            = 100
	defaultMaxConcurrentDispatches = 1000
)

// defaultRateLimits is the whole-message default for a create/update that omits
// rateLimits entirely (echoed back on the wire).
func defaultRateLimits() *tasksstore.RateLimits {
	return &tasksstore.RateLimits{
		MaxDispatchesPerSecond:  defaultMaxDispatchesPerSecond,
		MaxBurstSize:            defaultMaxBurstSize,
		MaxConcurrentDispatches: defaultMaxConcurrentDispatches,
	}
}

// applyRateLimitDefaults fills the fields a caller left out of a rateLimits
// message. RateLimits is proto3 without field presence, so a zero on the wire is
// indistinguishable from an omission and real Cloud Tasks resolves an unset
// field to its default rather than storing 0 — a create with
// `rateLimits: {maxDispatchesPerSecond: 5}` reads back maxBurstSize 100 and
// maxConcurrentDispatches 1000, not 0. This also governs an update that names
// `rateLimits` in its mask: the mask replaces the whole message (the same
// replacement semantics the Discovery documents for a field mask), so its unset
// subfields take the defaults. A masked update that does not name `rateLimits`
// leaves the stored message untouched (applyQueueUpdate copies nothing), so an
// omitted field never clears a queue's rate limits.
func applyRateLimitDefaults(r *tasksstore.RateLimits) *tasksstore.RateLimits {
	if r == nil {
		return defaultRateLimits()
	}
	out := *r // never mutate the caller's message
	if out.MaxDispatchesPerSecond == 0 {
		out.MaxDispatchesPerSecond = defaultMaxDispatchesPerSecond
	}
	if out.MaxBurstSize == 0 {
		out.MaxBurstSize = defaultMaxBurstSize
	}
	if out.MaxConcurrentDispatches == 0 {
		out.MaxConcurrentDispatches = defaultMaxConcurrentDispatches
	}
	return &out
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
	q.RateLimits = applyRateLimitDefaults(q.RateLimits)
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
		if err := validateRetryConfig(q.RetryConfig); err != nil {
			return err
		}
	}
	return nil
}

// validateRetryConfig applies the documented RetryConfig constraints. It is
// shared by the queue and the task-level override because both are the same
// message: a task config is a full override of the queue's, validated by the
// same rules.
func validateRetryConfig(r *tasksstore.RetryConfig) error {
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
