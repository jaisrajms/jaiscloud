package suppress

import "time"

// ok is suppressed by the inline directive; it must not be reported.
func ok() time.Time {
	return time.Now() //gcplint:ignore clock
}
