package suppressfile

//gcplint:ignore-file clock

import "time"

// bad would be a violation, but the whole file is suppressed.
func bad() time.Time {
	return time.Now()
}
