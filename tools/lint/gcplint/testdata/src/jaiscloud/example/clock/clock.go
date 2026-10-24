package clock

import "time"

func bad() time.Time {
	return time.Now() // want `\[clock\]`
}

func badSince(start time.Time) time.Duration {
	return time.Since(start) // want `\[clock\]`
}

func badUntil(deadline time.Time) time.Duration {
	return time.Until(deadline) // want `\[clock\]`
}

func good() time.Time {
	return time.Unix(0, 0).UTC()
}
