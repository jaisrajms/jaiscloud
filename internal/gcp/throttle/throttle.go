// Package throttle implements the opt-in GCP throttle / quota fault injector.
//
// It is an emulator-only testing affordance (like /_jaiscloud/clock or
// JAISCLOUD_LRO_MODE), never a fidelity claim about real quota values: real GCP
// quotas are per-project/per-metric and cannot be triggered on demand, so
// client backoff / retry-budget / idempotency paths rarely run locally. With
// JAISCLOUD_GCP_THROTTLE set, matching requests are refused with a retryable
// 429 RESOURCE_EXHAUSTED or 503 UNAVAILABLE before dispatch, over both REST
// (via the gateway RequestFilter capability) and gRPC (via interceptors).
//
// Default is OFF, so an unconfigured binary and CI are unaffected.
package throttle

import (
	"context"
	"log/slog"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
)

// Environment variables read by FromEnv.
const (
	envMode       = "JAISCLOUD_GCP_THROTTLE"
	envRPS        = "JAISCLOUD_GCP_THROTTLE_RPS"
	envBurst      = "JAISCLOUD_GCP_THROTTLE_BURST"
	envServices   = "JAISCLOUD_GCP_THROTTLE_SERVICES"
	envFailFirst  = "JAISCLOUD_GCP_THROTTLE_FAIL_FIRST"
	envFailEvery  = "JAISCLOUD_GCP_THROTTLE_FAIL_EVERY"
	envFailCount  = "JAISCLOUD_GCP_THROTTLE_FAIL_COUNT"
	envStatus     = "JAISCLOUD_GCP_THROTTLE_STATUS"
	envRetryDelay = "JAISCLOUD_GCP_THROTTLE_RETRY_DELAY"
)

// Defaults applied when a mode is selected but its tuning variable is unset.
const (
	defaultRPS        = 10.0
	defaultStatus     = http.StatusTooManyRequests
	defaultRetryDelay = time.Second
)

// Config is the resolved injector configuration. The zero value is OFF: no
// injection on any path, so an unconfigured emulator behaves exactly as before.
type Config struct {
	// Rate enables the per-project token-bucket limiter.
	Rate bool
	// Fault enables the deterministic request-counter failures.
	Fault bool
	// RPS is the refill rate of a rate bucket (tokens per second).
	RPS float64
	// Burst is the bucket capacity (max instantaneous requests).
	Burst int
	// Services scopes injection to a set of service names. Empty means every
	// service. Entries are matched case-insensitively as substrings of the
	// service (REST wire name) or full gRPC method, so "all" also matches.
	Services []string
	// FailFirst fails the first N matching requests.
	FailFirst int
	// FailEvery, once FailFirst is exceeded, fails every Mth matching request.
	FailEvery int
	// FailCount fails K consecutive matching requests then recovers. It applies
	// only when FailFirst and FailEvery are both zero.
	FailCount int
	// Status is the injected HTTP status: 429 (default) or 503.
	Status int
	// RetryDelay is surfaced to clients as Retry-After and google.rpc.RetryInfo.
	RetryDelay time.Duration
}

// Enabled reports whether any injection mode is selected.
func (c Config) Enabled() bool { return c.Rate || c.Fault }

// FromEnv builds a Config from the environment. JAISCLOUD_GCP_THROTTLE selects
// off|rate|fault|both (default off; an unknown value warns and stays off).
func FromEnv() Config {
	cfg := Config{Status: defaultStatus, RetryDelay: defaultRetryDelay}
	switch strings.ToLower(strings.TrimSpace(os.Getenv(envMode))) {
	case "", "off":
		return cfg
	case "rate":
		cfg.Rate = true
	case "fault":
		cfg.Fault = true
	case "both":
		cfg.Rate, cfg.Fault = true, true
	default:
		slog.Warn("throttle: unknown JAISCLOUD_GCP_THROTTLE mode, disabling", "value", os.Getenv(envMode))
		return cfg
	}

	cfg.RPS = floatFromEnv(envRPS, defaultRPS)
	if cfg.Rate && cfg.RPS <= 0 {
		slog.Warn("throttle: JAISCLOUD_GCP_THROTTLE_RPS must be > 0, using default", "default", defaultRPS)
		cfg.RPS = defaultRPS
	}
	cfg.Burst = intFromEnv(envBurst, 0)
	if cfg.Burst <= 0 {
		cfg.Burst = int(math.Ceil(cfg.RPS))
		if cfg.Burst < 1 {
			cfg.Burst = 1
		}
	}
	cfg.Services = splitList(os.Getenv(envServices))
	cfg.FailFirst = intFromEnv(envFailFirst, 0)
	cfg.FailEvery = intFromEnv(envFailEvery, 0)
	cfg.FailCount = intFromEnv(envFailCount, 0)
	cfg.RetryDelay = durationFromEnv(envRetryDelay, defaultRetryDelay)
	switch s := intFromEnv(envStatus, defaultStatus); s {
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		cfg.Status = s
	default:
		slog.Warn("throttle: JAISCLOUD_GCP_THROTTLE_STATUS must be 429 or 503, using 429", "value", s)
	}
	return cfg
}

// Injector holds the per-scope rate buckets and fault counters. It is safe for
// concurrent use; Reset makes it an admin.Resetter so /_jaiscloud/reset clears
// accumulated counters between test runs. The configuration is mutable at
// runtime (SetConfig), which is what backs POST /_jaiscloud/throttle.
type Injector struct {
	mu      sync.Mutex
	cfg     Config
	buckets map[string]*bucket
	counts  map[string]int
}

// New returns an Injector for cfg. A disabled cfg yields an injector whose
// Check always allows, so callers may wire it unconditionally.
func New(cfg Config) *Injector {
	return &Injector{
		cfg:     cfg,
		buckets: map[string]*bucket{},
		counts:  map[string]int{},
	}
}

// Enabled reports whether the injector is armed. Safe on a nil receiver.
func (i *Injector) Enabled() bool {
	if i == nil {
		return false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.cfg.Enabled()
}

// Config returns a copy of the current configuration, including a copy of the
// Services slice so the caller cannot alias live state.
func (i *Injector) Config() Config {
	i.mu.Lock()
	defer i.mu.Unlock()
	cfg := i.cfg
	cfg.Services = append([]string(nil), i.cfg.Services...)
	return cfg
}

// SetConfig atomically replaces the configuration and clears accumulated rate
// buckets and fault counters, so a mid-test (re-)arm always starts clean. It
// backs the runtime control endpoint (POST /_jaiscloud/throttle).
func (i *Injector) SetConfig(cfg Config) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.cfg = cfg
	i.buckets = map[string]*bucket{}
	i.counts = map[string]int{}
}

// Check decides whether a matching request should be refused. project scopes
// the rate bucket; service is the REST wire service name (e.g. "storage");
// action is the dispatch action (e.g. "ObjectsGet"). It returns the injected
// ProviderError, or nil to allow the request.
func (i *Injector) Check(project, service, action string) *model.ProviderError {
	if i == nil {
		return nil
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	if !i.cfg.Enabled() || !i.cfg.allows(service, action) {
		return nil
	}

	if i.cfg.Fault {
		key := scopeKey(project, service)
		i.counts[key]++
		if i.cfg.shouldFail(i.counts[key]) {
			return i.error()
		}
	}
	if i.cfg.Rate && !i.allowRate(project) {
		return i.error()
	}
	return nil
}

// CheckMethod applies Check to a gRPC full method (e.g.
// "/google.storage.v2.Storage/GetObject"). Health and reflection are never
// throttled, and the bucket is global because a gRPC method carries no project.
func (i *Injector) CheckMethod(fullMethod string) *model.ProviderError {
	if !i.Enabled() {
		return nil
	}
	if strings.HasPrefix(fullMethod, "/grpc.health.") || strings.HasPrefix(fullMethod, "/grpc.reflection.") {
		return nil
	}
	return i.Check("", fullMethod, fullMethod)
}

// Reset clears the rate buckets and fault counters. Implements admin.Resetter.
func (i *Injector) Reset(context.Context) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.buckets = map[string]*bucket{}
	i.counts = map[string]int{}
}

// allows reports whether service (and optionally action) is inside the
// configured allow-list. The match target is "service/action", so a token can
// scope to a whole service ("storage") or a single method ("storage/objectsget").
// An empty list allows everything.
func (c Config) allows(service, action string) bool {
	if len(c.Services) == 0 {
		return true
	}
	t := strings.ToLower(service + "/" + action)
	for _, s := range c.Services {
		s = strings.ToLower(strings.TrimSpace(s))
		if s == "" {
			continue
		}
		if s == "all" || strings.Contains(t, s) {
			return true
		}
	}
	return false
}

// shouldFail implements the deterministic fault policy for the nth matching
// request. FailFirst fails the first N; FailEvery then fails every Mth after
// that; FailCount (used alone) fails K consecutive requests from the start.
func (c Config) shouldFail(n int) bool {
	if c.FailFirst > 0 && n <= c.FailFirst {
		return true
	}
	if c.FailEvery > 0 && n > c.FailFirst && (n-c.FailFirst)%c.FailEvery == 0 {
		return true
	}
	if c.FailFirst == 0 && c.FailEvery == 0 && c.FailCount > 0 && n <= c.FailCount {
		return true
	}
	return false
}

// bucket is a token bucket refilled on the real clock. RealNow (not the
// business clock) is deliberate: a frozen-clock test must not stall refill and
// deadlock every request after the burst is spent.
type bucket struct {
	tokens float64
	last   time.Time
}

// allowRate consumes one token from the project's bucket, refilling it first.
// An empty project shares a single global bucket.
func (i *Injector) allowRate(project string) bool {
	key := project
	if key == "" {
		key = "global"
	}
	b := i.buckets[key]
	if b == nil {
		b = &bucket{tokens: float64(i.cfg.Burst), last: clock.RealNow()}
		i.buckets[key] = b
	}
	now := clock.RealNow()
	b.tokens = math.Min(float64(i.cfg.Burst), b.tokens+now.Sub(b.last).Seconds()*i.cfg.RPS)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// error builds the injected ProviderError, choosing the code that gcperr maps
// to RESOURCE_EXHAUSTED or UNAVAILABLE. It also carries the GCS-specific
// reason/xmlCode so a GCS error surfaces the documented reason rather than the
// generic internalError fallback (the standard Google JSON envelope ignores
// Data and uses Status instead).
func (i *Injector) error() *model.ProviderError {
	code := "ResourceExhausted"
	msg := "Quota exceeded (jaiscloud throttle injector)"
	reason, xmlCode := "rateLimitExceeded", "RateLimitExceeded"
	if i.cfg.Status == http.StatusServiceUnavailable {
		code = "Unavailable"
		msg = "The service is currently unavailable (jaiscloud throttle injector)"
		reason, xmlCode = "backendError", "SlowDown"
	}
	pe := model.NewProviderError(code, msg, i.cfg.Status)
	pe.RetryAfter = i.cfg.RetryDelay
	pe.WithData(map[string]any{"reason": reason, "xmlCode": xmlCode})
	return pe
}

// scopeKey isolates fault counters per project and service so a fault aimed at
// one service does not exhaust another.
func scopeKey(project, service string) string {
	return project + "|" + service
}

func splitList(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func intFromEnv(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		slog.Warn("throttle: invalid integer env, using default", "key", key, "value", v, "default", def)
		return def
	}
	return n
}

func floatFromEnv(key string, def float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		slog.Warn("throttle: invalid float env, using default", "key", key, "value", v, "default", def)
		return def
	}
	return f
}

func durationFromEnv(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		slog.Warn("throttle: invalid duration env, using default", "key", key, "value", v, "default", def)
		return def
	}
	return d
}
