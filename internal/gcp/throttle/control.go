package throttle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"
)

// ControlRequest is the JSON body accepted by POST /_jaiscloud/throttle. It
// mirrors the environment configuration (FromEnv) but, unlike FromEnv, an
// invalid value is rejected instead of logged-and-defaulted: a control request
// that silently did nothing would be worse than a 400. Applying a request fully
// replaces the running configuration and clears accumulated counters.
//
//	mode:       off | rate | fault | both   (default off)
//	rps:        token-bucket refill rate     (default 10; only used by rate/both)
//	burst:      token-bucket capacity        (default ceil(rps))
//	services:   allow-list of wire services / gRPC methods (empty = all)
//	failFirst:  fail the first N matching requests
//	failEvery:  after the first N, fail every Mth matching request
//	failCount:  fail K consecutive then recover (applies when the two above are 0)
//	status:     429 (default) or 503
//	retryDelay: Go duration surfaced as Retry-After + RetryInfo (default 1s)
type ControlRequest struct {
	Mode       string   `json:"mode"`
	RPS        float64  `json:"rps,omitempty"`
	Burst      int      `json:"burst,omitempty"`
	Services   []string `json:"services,omitempty"`
	FailFirst  int      `json:"failFirst,omitempty"`
	FailEvery  int      `json:"failEvery,omitempty"`
	FailCount  int      `json:"failCount,omitempty"`
	Status     int      `json:"status,omitempty"`
	RetryDelay string   `json:"retryDelay,omitempty"`
}

// ControlStatus is the JSON response describing the current injector state. It
// is returned by both the POST (after applying) and GET handlers.
type ControlStatus struct {
	Enabled    bool     `json:"enabled"`
	Mode       string   `json:"mode"`
	Rate       bool     `json:"rate"`
	Fault      bool     `json:"fault"`
	RPS        float64  `json:"rps"`
	Burst      int      `json:"burst"`
	Services   []string `json:"services,omitempty"`
	FailFirst  int      `json:"failFirst,omitempty"`
	FailEvery  int      `json:"failEvery,omitempty"`
	FailCount  int      `json:"failCount,omitempty"`
	Status     int      `json:"status"`
	RetryDelay string   `json:"retryDelay"`
}

// SetThrottleConfig implements the admin.ThrottleController capability: it
// decodes a control document, applies it (replacing the configuration and
// clearing counters), and returns the resulting status document. Unknown JSON
// fields, trailing data, and invalid values are rejected so a typo cannot
// silently no-op.
func (i *Injector) SetThrottleConfig(body []byte) ([]byte, error) {
	var req ControlRequest
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return nil, fmt.Errorf("invalid throttle request: %w", err)
	}
	// Reject trailing content after the first JSON value (DisallowUnknownFields
	// does not see it). A clean end of input yields io.EOF.
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("invalid throttle request: unexpected trailing data")
	}
	cfg, err := req.config()
	if err != nil {
		return nil, err
	}
	i.SetConfig(cfg)
	return i.ThrottleStatus(), nil
}

// ThrottleStatus implements the admin.ThrottleController capability: it returns
// the current configuration as a JSON document.
func (i *Injector) ThrottleStatus() []byte {
	cfg := i.Config()
	out, err := json.Marshal(statusOf(cfg))
	if err != nil {
		// ControlStatus is a plain struct; marshalling it cannot fail.
		return []byte(`{}`)
	}
	return out
}

// config resolves a control request into a Config, applying the same defaults
// as FromEnv but returning an error for anything invalid.
func (c ControlRequest) config() (Config, error) {
	cfg := Config{Status: defaultStatus, RetryDelay: defaultRetryDelay}
	switch strings.ToLower(strings.TrimSpace(c.Mode)) {
	case "", "off":
	case "rate":
		cfg.Rate = true
	case "fault":
		cfg.Fault = true
	case "both":
		cfg.Rate, cfg.Fault = true, true
	default:
		return Config{}, fmt.Errorf("unknown mode %q: must be off, rate, fault, or both", c.Mode)
	}

	cfg.RPS = c.RPS
	if cfg.RPS == 0 {
		cfg.RPS = defaultRPS
	}
	if cfg.RPS < 0 {
		return Config{}, fmt.Errorf("rps must not be negative, got %v", c.RPS)
	}
	cfg.Burst = c.Burst
	if cfg.Burst < 0 {
		return Config{}, fmt.Errorf("burst must not be negative, got %d", c.Burst)
	}
	if cfg.Burst == 0 {
		cfg.Burst = int(math.Ceil(cfg.RPS))
		if cfg.Burst < 1 {
			cfg.Burst = 1
		}
	}
	if c.FailFirst < 0 || c.FailEvery < 0 || c.FailCount < 0 {
		return Config{}, fmt.Errorf("fault counters must not be negative (failFirst=%d, failEvery=%d, failCount=%d)",
			c.FailFirst, c.FailEvery, c.FailCount)
	}
	cfg.Services = append([]string(nil), c.Services...)
	cfg.FailFirst = c.FailFirst
	cfg.FailEvery = c.FailEvery
	cfg.FailCount = c.FailCount
	// Fault mode with no counter would report enabled but never refuse a
	// request; require at least one counter (including for "both", where the
	// caller explicitly asked for faults as well as rate limiting).
	if cfg.Fault && cfg.FailFirst == 0 && cfg.FailEvery == 0 && cfg.FailCount == 0 {
		return Config{}, fmt.Errorf("fault mode requires failFirst, failEvery, or failCount > 0")
	}
	if c.Status != 0 {
		if c.Status != http.StatusTooManyRequests && c.Status != http.StatusServiceUnavailable {
			return Config{}, fmt.Errorf("status must be 429 or 503, got %d", c.Status)
		}
		cfg.Status = c.Status
	}
	if c.RetryDelay != "" {
		d, err := time.ParseDuration(c.RetryDelay)
		if err != nil {
			return Config{}, fmt.Errorf("retryDelay: %w", err)
		}
		if d < 0 {
			return Config{}, fmt.Errorf("retryDelay must not be negative")
		}
		cfg.RetryDelay = d
	}
	return cfg, nil
}

// statusOf renders a Config as the control status document.
func statusOf(cfg Config) ControlStatus {
	mode := "off"
	switch {
	case cfg.Rate && cfg.Fault:
		mode = "both"
	case cfg.Rate:
		mode = "rate"
	case cfg.Fault:
		mode = "fault"
	}
	return ControlStatus{
		Enabled:    cfg.Enabled(),
		Mode:       mode,
		Rate:       cfg.Rate,
		Fault:      cfg.Fault,
		RPS:        cfg.RPS,
		Burst:      cfg.Burst,
		Services:   cfg.Services,
		FailFirst:  cfg.FailFirst,
		FailEvery:  cfg.FailEvery,
		FailCount:  cfg.FailCount,
		Status:     cfg.Status,
		RetryDelay: cfg.RetryDelay.String(),
	}
}
