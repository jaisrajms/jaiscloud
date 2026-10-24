package clock

import "time"

// Stub mirroring jaiscloud/internal/clock for the analyse test: the two
// package functions the clock-realnow rule resolves against.
func Now() time.Time { return time.Time{} }

func RealNow() time.Time { return time.Time{} }
