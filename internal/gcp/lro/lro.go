// Package lro provides the shared, transport-neutral timing mode for GCP
// long-running operations (LROs).
//
// The default is synchronous: with the mode disabled every service stores and
// returns done=true operations inline, exactly as the v1.1.0 contract and the
// conformance transcripts require. The mode is opt-in (JAISCLOUD_LRO_MODE=async)
// so a client that polls Operations.GetOperation is genuinely exercised without
// changing the default behavior for anyone else.
//
// An enabled mode is clock-driven and lazy. A newly created operation is stored
// with done=false and a zero endTime; every read derives whether the delay has
// elapsed (Pending) and settles the operation in place. Nothing is scheduled and
// no goroutine runs, so the flip is a pure function of the persisted create time
// and the global clock, and a time-freeze test can advance it deterministically.
// This mirrors the Dataproc reference pattern (clusterReadyDelay /
// jobStateDelay, DPG2), which first proved the lazy read-time settle in the
// emulator. A Delay of zero settles on the first read.
package lro

import (
	"log/slog"
	"os"
	"time"

	"jaiscloud/internal/clock"
)

// Environment variables read by FromEnv.
const (
	envMode  = "JAISCLOUD_LRO_MODE"
	envDelay = "JAISCLOUD_LRO_DELAY"
)

// defaultDelay is the in-flight window applied when async mode is selected but
// JAISCLOUD_LRO_DELAY is unset or unparsable.
const defaultDelay = 250 * time.Millisecond

// Mode is the opt-in asynchronous LRO timing configuration. The zero value is
// synchronous: Async reports false and Pending always reports false, so a
// service stores every operation done=true inline.
type Mode struct {
	// Enabled selects asynchronous timing. It is false by default so an
	// unconfigured binary keeps the synchronous contract.
	Enabled bool
	// Delay is how long an operation stays in flight after its create time.
	// Zero means it settles on the first read.
	Delay time.Duration
}

// Async reports whether asynchronous timing is enabled.
func (m Mode) Async() bool { return m.Enabled }

// Pending reports whether an operation created at create is still in flight.
// It is always false when the mode is synchronous. In async mode the delay is
// measured against the global (business) clock, so a frozen-clock test controls
// the transition exactly.
func (m Mode) Pending(create time.Time) bool {
	if !m.Enabled {
		return false
	}
	return clock.Now().UTC().Before(create.Add(m.Delay))
}

// FromEnv builds a Mode from the environment. JAISCLOUD_LRO_MODE selects
// async/sync (default sync; an unknown value warns and stays sync), and
// JAISCLOUD_LRO_DELAY is a Go duration applied when async (default 250ms; a
// parse error warns and falls back to the default). The delay is ignored in
// sync mode.
func FromEnv() Mode {
	switch mode := os.Getenv(envMode); mode {
	case "", "sync":
		return Mode{}
	case "async":
		return Mode{Enabled: true, Delay: delayFromEnv()}
	default:
		slog.Warn("lro: unknown JAISCLOUD_LRO_MODE, defaulting to sync", "value", mode)
		return Mode{}
	}
}

// delayFromEnv parses JAISCLOUD_LRO_DELAY, falling back to defaultDelay when it
// is unset or unparsable.
func delayFromEnv() time.Duration {
	v := os.Getenv(envDelay)
	if v == "" {
		return defaultDelay
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		slog.Warn("lro: invalid JAISCLOUD_LRO_DELAY, using default", "value", v, "default", defaultDelay, "err", err)
		return defaultDelay
	}
	return d
}
