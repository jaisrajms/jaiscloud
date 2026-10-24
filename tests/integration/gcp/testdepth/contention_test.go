package testdepth

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

var errConflict = errors.New("conflict")

func TestRaceExactlyOneWinner(t *testing.T) {
	if !RaceEnabled() {
		t.Skip("contention requires the race detector (-race)")
	}
	var flag int32
	errs := Race(8, func(int) error {
		if atomic.CompareAndSwapInt32(&flag, 0, 1) {
			return nil
		}
		return errConflict
	})
	AssertOneWinner(t, errs, func(err error) bool { return errors.Is(err, errConflict) })
	if n := atomic.LoadInt32(&flag); n != 1 {
		t.Fatalf("the contended flag = %d, want exactly one winner", n)
	}
	losers := 0
	for _, err := range errs {
		if err != nil {
			losers++
		}
	}
	if losers != 7 {
		t.Fatalf("losers = %d, want all 7 non-winning writers to observe the conflict", losers)
	}
}

func TestCheckOneWinnerSeededNegatives(t *testing.T) {
	conflict := func(err error) bool { return errors.Is(err, errConflict) }
	cases := []struct {
		name   string
		errs   []error
		isCfn  func(error) bool
		expect string
	}{
		{
			name:   "two winners",
			errs:   []error{nil, nil, errConflict},
			isCfn:  conflict,
			expect: "2 writers won ([0 1]); exactly one must win",
		},
		{
			name:   "no winner",
			errs:   []error{errConflict, errConflict},
			isCfn:  conflict,
			expect: "no writer won: all 2 returned an error (first: conflict)",
		},
		{
			name:   "loser without the documented conflict",
			errs:   []error{nil, errors.New("boom")},
			isCfn:  conflict,
			expect: "writer 1 lost with boom, which is not the documented conflict",
		},
		{
			name:   "no writers",
			errs:   nil,
			isCfn:  conflict,
			expect: "no writers were run",
		},
		{
			name:   "no conflict predicate still checks the winner count",
			errs:   []error{nil, errors.New("boom")},
			isCfn:  nil,
			expect: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := strings.Join(CheckOneWinner(tc.errs, tc.isCfn), "\n")
			if tc.expect == "" {
				if got != "" {
					t.Fatalf("CheckOneWinner reported %q, want none", got)
				}
				return
			}
			if !strings.Contains(got, tc.expect) {
				t.Fatalf("CheckOneWinner reported %q, want it to contain %q", got, tc.expect)
			}
		})
	}
}

func TestAssertOneWinnerFires(t *testing.T) {
	rec := wantReported(func(tb TB) { AssertOneWinner(tb, []error{nil, errConflict}, nil) })
	if !RaceEnabled() {
		// The assertion refuses to run at all without the race detector, so a
		// forgotten -race cannot silently weaken a contention test. The winner
		// semantics stay covered through the pure half.
		if !strings.Contains(rec.reported(), "contention assertions require the race detector") {
			t.Fatalf("AssertOneWinner reported %q in a non-race binary", rec.reported())
		}
		if got := strings.Join(CheckOneWinner([]error{nil, nil}, nil), "\n"); !strings.Contains(got, "2 writers won ([0 1])") {
			t.Fatalf("CheckOneWinner reported %q, want the two-winner violation", got)
		}
		return
	}
	if rec.failed() {
		t.Fatalf("a clean race reported %q", rec.reported())
	}
	multi := wantReported(func(tb TB) { AssertOneWinner(tb, []error{nil, nil}, nil) })
	if !strings.Contains(multi.reported(), "2 writers won ([0 1])") {
		t.Fatalf("a two-winner race reported %q", multi.reported())
	}
}

func TestAssertRaceEnabledFollowsTheBuildTag(t *testing.T) {
	rec := wantReported(func(tb TB) { AssertRaceEnabled(tb) })
	if RaceEnabled() {
		if rec.failed() {
			t.Fatalf("AssertRaceEnabled reported %q in a -race binary", rec.reported())
		}
		return
	}
	if !strings.Contains(rec.reported(), "contention assertions require the race detector") {
		t.Fatalf("AssertRaceEnabled reported %q in a non-race binary", rec.reported())
	}
}
