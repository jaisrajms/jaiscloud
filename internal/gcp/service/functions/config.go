package functions

import (
	"math"
	"strconv"
	"strings"

	functionsstore "jaiscloud/internal/gcp/store/functions"
)

// Cloud Functions instance/concurrency limits (FD6). Real GCP derives these
// from Cloud Run: instance counts 0..1000 and per-instance request concurrency
// 1..1000. Validation is faithful to the real API on purpose: a value the
// emulator accepts must not fail only once it reaches real GCP, since surfacing
// that class of bug locally is the emulator's job.
const (
	maxInstanceCountLimit              = 1000
	maxInstanceRequestConcurrencyLimit = 1000
	minAvailableCPU                    = 0.08
	minMemoryMB                        = 128
	// maxMemoryMB is 32 GiB, the largest 2nd gen allocation (the 1st gen enum
	// below tops out at 8 GiB).
	maxMemoryMB = 32768
)

// validV1MemoryMB is the set of memory sizes Cloud Functions v1 accepts. The
// Discovery document documents only the 256MB default; this is the documented
// 1st gen allocation set. Per-runtime restrictions (some runtimes reject the
// smallest sizes) are deliberately not modelled.
var validV1MemoryMB = map[int]bool{
	128: true, 256: true, 512: true, 1024: true,
	2048: true, 4096: true, 8192: true,
}

// validateConfigInput validates caller-supplied instance/concurrency values
// against the real Cloud Functions ranges. It runs on create and update before
// the merge; a zero value means "unset" and is always valid.
func validateConfigInput(in FunctionInput, v Version) error {
	if in.AvailableMemoryMB > 0 {
		if v == V1 && !validV1MemoryMB[in.AvailableMemoryMB] {
			return invalidArgument("availableMemoryMb must be one of 128, 256, 512, 1024, 2048, 4096, 8192")
		}
		if in.AvailableMemoryMB < minMemoryMB || in.AvailableMemoryMB > maxMemoryMB {
			return invalidArgument("availableMemory must be between 128M and 32Gi")
		}
	}
	if in.MinInstanceCount < 0 || in.MinInstanceCount > maxInstanceCountLimit {
		return invalidArgument("minInstanceCount must be between 0 and 1000")
	}
	if in.MaxInstanceCount < 0 || in.MaxInstanceCount > maxInstanceCountLimit {
		return invalidArgument("maxInstanceCount must be between 0 and 1000")
	}
	if in.MaxInstanceRequestConcurrency < 0 || in.MaxInstanceRequestConcurrency > maxInstanceRequestConcurrencyLimit {
		return invalidArgument("maxInstanceRequestConcurrency must be between 1 and 1000 when set")
	}
	if in.AvailableCPU != "" && !validAvailableCPU(in.AvailableCPU) {
		return invalidArgument("availableCpu must be 1, 2, 4, 6, or 8 vCPU, or a decimal from 0.08 to less than 1 in increments of 0.01")
	}
	return nil
}

// validAvailableCPU reports whether an availableCpu string is a CPU value Cloud
// Run accepts: 1, 2, 4, 6, or 8 vCPU, or 0.08..0.99 in increments of 0.01
// (https://cloud.google.com/run/docs/configuring/services/cpu). A Kubernetes
// milli suffix ("500m") is accepted and normalized, since Cloud Run expresses
// the limit as a container resource quantity. NaN/Inf and out-of-set values are
// rejected so a value the emulator accepts cannot fail only on real GCP.
func validAvailableCPU(s string) bool {
	_, ok := parseAvailableCPU(s)
	return ok
}

// parseAvailableCPU normalizes an availableCpu string to a vCPU count. ok is
// false when the value is not one Cloud Run accepts.
func parseAvailableCPU(s string) (float64, bool) {
	v := strings.TrimSpace(s)
	milli := strings.HasSuffix(v, "m")
	if milli {
		v = strings.TrimSpace(strings.TrimSuffix(v, "m"))
	}
	if v == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f <= 0 {
		return 0, false
	}
	if milli {
		f /= 1000
	}
	switch f {
	case 1, 2, 4, 6, 8:
		return f, true
	}
	if f >= minAvailableCPU && f < 1 {
		// Sub-vCPU values must be in increments of 0.01 (0.08, 0.09, …, 0.99).
		if math.Abs(f*100-math.Round(f*100)) < 1e-9 {
			return f, true
		}
	}
	return 0, false
}

// effectiveConcurrencyLimit returns a function's concurrent-invocation capacity
// for the admission gate (FP1). Cloud Functions 2nd gen scales to at most
// maxInstanceCount instances, each serving up to maxInstanceRequestConcurrency
// requests concurrently, so the capacity is their product. An unset
// maxInstanceRequestConcurrency counts as one request per instance (the default
// single-concurrency case); a zero or negative maxInstanceCount means "no
// configured limit" and returns 0 (unlimited), matching the stored-record
// convention documented on functionsstore.Function.
func effectiveConcurrencyLimit(f functionsstore.Function) int64 {
	if f.MaxInstanceCount <= 0 {
		return 0
	}
	per := f.MaxInstanceRequestConcurrency
	if per < 1 {
		per = 1
	}
	return int64(f.MaxInstanceCount) * int64(per)
}

// validateConfigRelations validates the relationships that are only checkable on
// the merged record: minInstanceCount must not exceed a configured
// maxInstanceCount, and a sub-1-vCPU function must keep request concurrency at
// 1 (Cloud Run's documented pairing). maxInstanceCount 0 means "no configured
// limit", so the min≤max relation is not checked against it — a function may
// set provisioned concurrency without a reserved cap, matching real Cloud
// Functions.
func validateConfigRelations(f functionsstore.Function) error {
	if f.MaxInstanceCount > 0 && f.MinInstanceCount > f.MaxInstanceCount {
		return invalidArgument("minInstanceCount must not exceed maxInstanceCount")
	}
	// Cloud Run requires per-instance request concurrency 1 when a function is
	// allocated less than one vCPU
	// (https://cloud.google.com/run/docs/configuring/services/cpu).
	if f.AvailableCPU != "" {
		if cpu, ok := parseAvailableCPU(f.AvailableCPU); ok && cpu < 1 && f.MaxInstanceRequestConcurrency > 1 {
			return invalidArgument("maxInstanceRequestConcurrency must be 1 when availableCpu is less than 1")
		}
	}
	return nil
}
