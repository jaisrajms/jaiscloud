//go:build race

package testdepth

// RaceEnabled reports whether this binary was built with the race detector
// (-race). See race_off.go for the default.
func RaceEnabled() bool { return true }
