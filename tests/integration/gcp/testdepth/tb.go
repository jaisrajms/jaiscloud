package testdepth

// TB is the subset of *testing.T the kit's assertions need. Taking the interface
// rather than *testing.T is what makes the kit self-verifying: the kit's own
// tests drive a recording TB through a seeded negative (a dropped resource, a
// wrong error code) and assert that the assertion fired.
type TB interface {
	Helper()
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
	Fatal(args ...any)
}
