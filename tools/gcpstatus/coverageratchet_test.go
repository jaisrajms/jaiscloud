package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// pct is a small helper for the pointer-valued floor field.
func pct(v float64) *float64 { return &v }

// writeTemp creates a temp file with the given contents and returns its path.
func writeTemp(t *testing.T, name, contents string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	return p
}

func TestParseCoverProfile(t *testing.T) {
	profile := writeTemp(t, "cover.out", strings.Join([]string{
		"mode: set",
		"jaiscloud/internal/gcp/a/a.go:1.1,2.2 3 1",
		"jaiscloud/internal/gcp/a/a.go:3.1,4.2 2 0",
		"jaiscloud/internal/gcp/a/b.go:1.1,2.2 5 1",
		"jaiscloud/internal/gcp/b/b.go:1.1,2.2 4 0",
		"",
		"jaiscloud/internal/gcp/grpc/x/xpb/gen.pb.go:1.1,2.2 9 0",
	}, "\n"))

	stats, err := parseCoverProfile(profile)
	if err != nil {
		t.Fatalf("parseCoverProfile: %v", err)
	}
	// Package a: total 3+2+5=10, covered 3+5=8.
	if got := stats["internal/gcp/a"]; got.Total != 10 || got.Covered != 8 {
		t.Fatalf("package a = %+v, want covered=8 total=10", got)
	}
	if got := stats["internal/gcp/b"]; got.Total != 4 || got.Covered != 0 {
		t.Fatalf("package b = %+v, want covered=0 total=4", got)
	}
	// The generated pb package is still parsed here; eligibility filters it.
	if got := stats["internal/gcp/grpc/x/xpb"]; got.Total != 9 {
		t.Fatalf("pb package = %+v, want total=9", got)
	}
}

func TestParseCoverProfileRejectsMalformed(t *testing.T) {
	cases := map[string]string{
		"no header":      "jaiscloud/internal/gcp/a/a.go:1.1,2.2 3 1\n",
		"empty":          "",
		"two fields":     "mode: set\njaiscloud/internal/gcp/a/a.go:1.1,2.2 3\n",
		"bad statements": "mode: set\njaiscloud/internal/gcp/a/a.go:1.1,2.2 x 1\n",
		"bad count":      "mode: set\njaiscloud/internal/gcp/a/a.go:1.1,2.2 3 y\n",
		"no colon":       "mode: set\njaiscloud/internal/gcp/a/a.go 3 1\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseCoverProfile(writeTemp(t, "cover.out", body)); err == nil {
				t.Fatalf("expected an error for %q", name)
			}
		})
	}
}

func TestRatchetEligible(t *testing.T) {
	eligible := []string{"internal/gcp/service/kms", "internal/gcp/adapter", "internal/gcp/grpc/storage"}
	for _, p := range eligible {
		if !ratchetEligible(p) {
			t.Errorf("ratchetEligible(%q) = false, want true", p)
		}
	}
	for _, p := range []string{"internal/gcp/grpc/storage/storagepb", "internal/gcp/foo/pb"} {
		if ratchetEligible(p) {
			t.Errorf("ratchetEligible(%q) = true, want false (generated pb)", p)
		}
	}
}

func TestRatchetViolations(t *testing.T) {
	measured := map[string]float64{
		"internal/gcp/below":      50.00,
		"internal/gcp/ok":         80.00,
		"internal/gcp/unrecorded": 10.00,
		// Rounding must not trip an equal floor.
		"internal/gcp/rounded": 33.33,
		// A shortfall within the noise tolerance must pass.
		"internal/gcp/tolerant": 50.00,
	}
	floor := map[string]float64{
		"internal/gcp/below":    51.00,
		"internal/gcp/ok":       80.00,
		"internal/gcp/rounded":  33.33,
		"internal/gcp/tolerant": 50.40, // 0.40pp short — within coverageRatchetTolerance
		"internal/gcp/stale":    40.00,
	}

	v := ratchetViolations(measured, floor)
	got := map[string]string{}
	for _, x := range v {
		got[x.Package] = x.Kind
	}
	want := map[string]string{
		"internal/gcp/below":      "below",
		"internal/gcp/unrecorded": "unrecorded",
		"internal/gcp/stale":      "stale",
	}
	if len(v) != len(want) {
		t.Fatalf("violations = %+v, want %d (%+v)", v, len(want), want)
	}
	for pkg, kind := range want {
		if got[pkg] != kind {
			t.Errorf("violation for %s = %q, want %q", pkg, got[pkg], kind)
		}
	}
	// Sorted by package, deterministically.
	for i := 1; i < len(v); i++ {
		if v[i-1].Package > v[i].Package {
			t.Fatalf("violations not sorted: %+v", v)
		}
	}
}

func TestRatchetViolationsToleranceBoundary(t *testing.T) {
	// Exactly at the tolerance edge passes; just past it fails.
	measured := map[string]float64{"internal/gcp/a": 50.00}
	if v := ratchetViolations(measured, map[string]float64{"internal/gcp/a": 50.50}); len(v) != 0 {
		t.Fatalf("at the tolerance edge: got %+v, want no violation", v)
	}
	if v := ratchetViolations(measured, map[string]float64{"internal/gcp/a": 50.51}); len(v) != 1 {
		t.Fatalf("past the tolerance edge: got %+v, want one violation", v)
	}
}

func TestLoadCoverageFloor(t *testing.T) {
	good := writeTemp(t, "floor.yaml", `
packages:
  - package: internal/gcp/service/kms
    min_percent: 21.92
  - package: internal/gcp/store/gcs
    min_percent: 30.84
`)
	floor, err := loadCoverageFloor(good)
	if err != nil {
		t.Fatalf("loadCoverageFloor: %v", err)
	}
	if floor["internal/gcp/service/kms"] != 21.92 {
		t.Fatalf("kms floor = %v, want 21.92", floor["internal/gcp/service/kms"])
	}

	// A missing (or empty) file is an empty floor, not an error.
	if f, err := loadCoverageFloor(filepath.Join(t.TempDir(), "absent.yaml")); err != nil || len(f) != 0 {
		t.Fatalf("missing floor: got %v, %v; want empty, nil", f, err)
	}
	if f, err := loadCoverageFloor(writeTemp(t, "floor.yaml", "# just a comment\n")); err != nil || len(f) != 0 {
		t.Fatalf("empty floor: got %v, %v; want empty, nil", f, err)
	}

	bad := map[string]string{
		"no package":        "packages:\n  - min_percent: 5\n",
		"outside":           "packages:\n  - package: internal/aws/x\n    min_percent: 5\n",
		"out of range":      "packages:\n  - package: internal/gcp/a\n    min_percent: 101\n",
		"negative":          "packages:\n  - package: internal/gcp/a\n    min_percent: -1\n",
		"duplicate":         "packages:\n  - package: internal/gcp/a\n    min_percent: 5\n  - package: internal/gcp/a\n    min_percent: 6\n",
		"misspelled key":    "packages:\n  - package: internal/gcp/a\n    minpct: 99\n",
		"missing value":     "packages:\n  - package: internal/gcp/a\n",
		"unknown top field": "packagez:\n  - package: internal/gcp/a\n    min_percent: 5\n",
	}
	for name, body := range bad {
		t.Run(name, func(t *testing.T) {
			if _, err := loadCoverageFloor(writeTemp(t, "floor.yaml", body)); err == nil {
				t.Fatalf("expected an error for %q", name)
			}
		})
	}
}

func TestCoverageFloorRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "floor.yaml")
	entries := []coverageFloorEntry{
		{Package: "internal/gcp/store/gcs", MinPercent: pct(30.84)},
		{Package: "internal/gcp/adapter", MinPercent: pct(71.2)},
		{Package: "internal/gcp/service/kms", MinPercent: pct(21.92)},
	}
	if err := writeCoverageFloor(p, entries); err != nil {
		t.Fatalf("writeCoverageFloor: %v", err)
	}
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(raw), "# GCP per-package statement-coverage floor") {
		t.Fatalf("missing header comment:\n%s", raw)
	}
	// Sorted deterministically, and 71.2 renders without a trailing zero.
	body := string(raw)
	if strings.Index(body, "internal/gcp/adapter") > strings.Index(body, "internal/gcp/service/kms") {
		t.Fatalf("entries not sorted:\n%s", body)
	}
	if !strings.Contains(body, "min_percent: 71.2\n") {
		t.Fatalf("expected 71.2 without trailing zero:\n%s", body)
	}
	floor, err := loadCoverageFloor(p)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if len(floor) != 3 || floor["internal/gcp/adapter"] != 71.2 {
		t.Fatalf("round-trip mismatch: %+v", floor)
	}
}

func TestRunCoverageRatchetUpdate(t *testing.T) {
	profile := writeTemp(t, "cover.out", strings.Join([]string{
		"mode: set",
		"jaiscloud/internal/gcp/a/a.go:1.1,2.2 5 0", // a: 0/5 statements
		"jaiscloud/internal/gcp/a/a.go:3.1,4.2 5 1", // a: +5/5 -> 50%
		"jaiscloud/internal/gcp/b/b.go:1.1,2.2 8 1", // b: 100%
		"jaiscloud/internal/gcp/grpc/x/xpb/gen.pb.go:1.1,2.2 9 0",
	}, "\n"))

	floor := filepath.Join(t.TempDir(), "floor.yaml")
	// Existing floor: a is below its old floor (must hold), b is raised, old is stale.
	if err := writeCoverageFloor(floor, []coverageFloorEntry{
		{Package: "internal/gcp/a", MinPercent: pct(90)},
		{Package: "internal/gcp/b", MinPercent: pct(50)},
		{Package: "internal/gcp/old", MinPercent: pct(12)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := runCoverageRatchetUpdate(profile, floor, false); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, err := loadCoverageFloor(floor)
	if err != nil {
		t.Fatal(err)
	}
	if got["internal/gcp/a"] != 90 {
		t.Errorf("a floor = %v, want 90 (ratchet holds a higher recorded floor)", got["internal/gcp/a"])
	}
	if got["internal/gcp/b"] != 100 {
		t.Errorf("b floor = %v, want 100 (raised)", got["internal/gcp/b"])
	}
	if _, ok := got["internal/gcp/old"]; ok {
		t.Errorf("stale package `old` should have been dropped")
	}
	if _, ok := got["internal/gcp/grpc/x/xpb"]; ok {
		t.Errorf("generated pb package must not be recorded")
	}

	// With allowLower a deliberate drop is recorded.
	if err := runCoverageRatchetUpdate(profile, floor, true); err != nil {
		t.Fatalf("update allowLower: %v", err)
	}
	got, _ = loadCoverageFloor(floor)
	if got["internal/gcp/a"] != 50 {
		t.Errorf("a floor = %v, want 50 with allowLower", got["internal/gcp/a"])
	}
}

func TestRunCoverageRatchetGate(t *testing.T) {
	dir := t.TempDir()
	profile := filepath.Join(dir, "cover.out")
	if err := os.WriteFile(profile, []byte(strings.Join([]string{
		"mode: set",
		"jaiscloud/internal/gcp/a/a.go:1.1,2.2 10 1", // 10/10 covered
		"jaiscloud/internal/gcp/a/a.go:3.1,4.2 10 0", // 0/10 covered -> 50%
	}, "\n")), 0o644); err != nil {
		t.Fatal(err)
	}
	floor := filepath.Join(dir, "floor.yaml")

	// Passing floor (and within tolerance).
	if err := writeCoverageFloor(floor, []coverageFloorEntry{{Package: "internal/gcp/a", MinPercent: pct(50.4)}}); err != nil {
		t.Fatal(err)
	}
	if rc := runCoverageRatchet(profile, floor); rc != 0 {
		t.Fatalf("gate rc = %d, want 0 within the tolerance", rc)
	}
	// Dropped past the tolerance.
	if err := writeCoverageFloor(floor, []coverageFloorEntry{{Package: "internal/gcp/a", MinPercent: pct(55)}}); err != nil {
		t.Fatal(err)
	}
	if rc := runCoverageRatchet(profile, floor); rc != 1 {
		t.Fatalf("gate rc = %d, want 1 below the floor", rc)
	}
}

func TestRunCoverageRatchetGateUnrecordedAndStale(t *testing.T) {
	dir := t.TempDir()
	profile := filepath.Join(dir, "cover.out")
	if err := os.WriteFile(profile, []byte(strings.Join([]string{
		"mode: set",
		"jaiscloud/internal/gcp/a/a.go:1.1,2.2 4 1", // 100%
	}, "\n")), 0o644); err != nil {
		t.Fatal(err)
	}
	floor := filepath.Join(dir, "floor.yaml")

	// No floor entry -> unrecorded -> fail.
	if err := writeCoverageFloor(floor, nil); err != nil {
		t.Fatal(err)
	}
	if rc := runCoverageRatchet(profile, floor); rc != 1 {
		t.Fatalf("unrecorded: gate rc = %d, want 1", rc)
	}

	// A floor entry with no measured package -> stale -> fail.
	if err := writeCoverageFloor(floor, []coverageFloorEntry{
		{Package: "internal/gcp/a", MinPercent: pct(100)},
		{Package: "internal/gcp/gone", MinPercent: pct(80)},
	}); err != nil {
		t.Fatal(err)
	}
	if rc := runCoverageRatchet(profile, floor); rc != 1 {
		t.Fatalf("stale: gate rc = %d, want 1", rc)
	}
}
