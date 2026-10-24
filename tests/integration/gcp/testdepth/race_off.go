//go:build !race

package testdepth

// RaceEnabled reports whether this binary was built with the race detector
// (-race). AssertRaceEnabled fails a contention test when it was not.
func RaceEnabled() bool { return false }
