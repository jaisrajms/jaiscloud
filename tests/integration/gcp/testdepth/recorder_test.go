package testdepth

import (
	"fmt"
	"strings"
)

// recordingTB captures the messages an assertion reported, so the kit's own
// tests can prove that a seeded negative fires — and that the clean case stays
// silent. It satisfies TB.
type recordingTB struct {
	msgs []string
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.msgs = append(r.msgs, fmt.Sprintf(format, args...))
}

func (r *recordingTB) Fatalf(format string, args ...any) {
	r.msgs = append(r.msgs, fmt.Sprintf(format, args...))
	panic(stopTest{})
}

func (r *recordingTB) Fatal(args ...any) {
	r.msgs = append(r.msgs, fmt.Sprint(args...))
	panic(stopTest{})
}

// reported is the assertion output as one string (empty when nothing fired).
func (r *recordingTB) reported() string { return strings.Join(r.msgs, "\n") }

// failed reports whether any assertion fired.
func (r *recordingTB) failed() bool { return len(r.msgs) > 0 }

// wantReported runs fn against a recording TB and returns what fired, absorbing
// the FailNow the kit's Fatalf paths raise.
func wantReported(fn func(tb TB)) *recordingTB {
	rec := &recordingTB{}
	func() {
		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(stopTest); !ok {
					panic(r)
				}
			}
		}()
		fn(rec)
	}()
	return rec
}

// stopTest stands in for testing.T.FailNow in a recording TB.
type stopTest struct{}
