// Per-package statement-coverage ratchet (TD11).
//
// The behavioural gates prove a suite *exists* (coverage.go, §9) and record
// which behaviour classes it exercises (depth.go, §10); neither measures how
// much of a package's code the tests actually execute. Statement coverage does
// not measure *assertion strength* — that is the mutation-testing axis (TD13) —
// but it does make a largely-unexercised surface visible, which is the axis
// this file adds:
//
//   - `make test-gcp-coverage` runs `go test -coverprofile` over
//     ./internal/gcp/... and feeds the profile to `-coverage-ratchet`;
//   - the committed floor docs/gcp-coverage-floor.yaml records, per package,
//     the statement-coverage percentage at the time it was last ratcheted;
//   - the gate fails when a measured package drops below its recorded floor
//     (by more than a small noise tolerance), when a measured package has no
//     floor entry, or when a floor entry names a package that no longer
//     produces coverage data.
//
// The floor is a *ratchet*, not a target: it starts at today's measured value
// and only moves up — `-coverage-floor-update` raises a recorded floor but
// refuses to lower one without `-allow-lower`.
//
// Two deliberate scope boundaries:
//
//   - Generated protobuf packages (a final path element ending in `pb`) carry
//     no hand-written behaviour and are excluded mechanically.
//   - Only packages that *produce coverage data* (i.e. have test files) are
//     governed; a package with no test files never appears in a profile and is
//     therefore out of scope for a regression ratchet.
package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// defaultCoverageFloorPath is the committed per-package coverage floor,
// relative to the repo root (where `make test-gcp-coverage` runs).
const defaultCoverageFloorPath = "docs/gcp-coverage-floor.yaml"

// gcpModulePrefix is the module path stripped from a coverprofile file path so
// the floor is keyed by repo-relative package dirs (internal/gcp/...).
const gcpModulePrefix = "jaiscloud/"

// gcpCoverageRoot is the only package subtree the ratchet governs: the profile
// may in principle carry other packages, which are ignored.
const gcpCoverageRoot = "internal/gcp/"

// coverageRatchetTolerance is the allowed shortfall, in percentage points,
// between a measured package and its recorded floor. A little slack is required
// because a handful of packages exercise timing-dependent `select` branches
// (e.g. a gRPC receive loop racing `ctx.Done`), so their statement coverage
// varies run-to-run by a fraction of a point. The tolerance absorbs that noise
// while a real regression (deleting a suite) fails by a wide margin.
const coverageRatchetTolerance = 0.5

// coverageFloorEntry is one recorded per-package floor. MinPercent is a pointer
// so a missing `min_percent` key is a loud error rather than a silent 0 floor.
type coverageFloorEntry struct {
	Package    string   `yaml:"package"`
	MinPercent *float64 `yaml:"min_percent"`
}

// coverageFloorFile is the committed floor document.
type coverageFloorFile struct {
	Packages []coverageFloorEntry `yaml:"packages"`
}

// pkgStatements is the raw statement tally for one package.
type pkgStatements struct {
	Covered int
	Total   int
}

// percent returns the statement-coverage percentage (0 for an empty tally).
func (s pkgStatements) percent() float64 {
	if s.Total == 0 {
		return 0
	}
	return 100 * float64(s.Covered) / float64(s.Total)
}

// roundPercent rounds a coverage percentage to two decimals, the precision the
// floor is stored at. Comparing rounded values keeps the gate repeatable despite
// float representation.
func roundPercent(p float64) float64 {
	return math.Round(p*100) / 100
}

// ratchetEligible reports whether a repo-relative package dir is governed by
// the ratchet. Generated protobuf packages (a final path element ending in
// "pb", e.g. `grpc/storage/storagepb`) carry no hand-written behaviour to
// cover, so they are excluded mechanically rather than by a hand-written
// exemption.
func ratchetEligible(pkg string) bool {
	return !strings.HasSuffix(path.Base(pkg), "pb")
}

// parseCoverProfile reads a Go coverprofile and tallies covered/total
// statements per repo-relative package dir. A malformed line (or a missing
// `mode:` header) is an error — the gate must never silently read a partial
// profile. Blank lines are tolerated.
func parseCoverProfile(profilePath string) (map[string]pkgStatements, error) {
	f, err := os.Open(profilePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := map[string]pkgStatements{}
	sc := bufio.NewScanner(f)
	// Coverprofile lines are short, but allow generous headroom.
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	first := true
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if first {
			if !strings.HasPrefix(line, "mode: ") {
				return nil, fmt.Errorf("%s:%d: missing `mode:` header", profilePath, lineNo)
			}
			first = false
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return nil, fmt.Errorf("%s:%d: malformed profile line %q", profilePath, lineNo, line)
		}
		file, _, ok := strings.Cut(fields[0], ":")
		if !ok {
			return nil, fmt.Errorf("%s:%d: profile line has no file:range %q", profilePath, lineNo, line)
		}
		stmts, err := strconv.Atoi(fields[1])
		if err != nil || stmts < 0 {
			return nil, fmt.Errorf("%s:%d: bad statement count %q", profilePath, lineNo, fields[1])
		}
		count, err := strconv.Atoi(fields[2])
		if err != nil || count < 0 {
			return nil, fmt.Errorf("%s:%d: bad execution count %q", profilePath, lineNo, fields[2])
		}
		pkg := path.Dir(strings.TrimPrefix(file, gcpModulePrefix))
		s := out[pkg]
		s.Total += stmts
		if count > 0 {
			s.Covered += stmts
		}
		out[pkg] = s
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if first {
		return nil, fmt.Errorf("%s: empty profile (no `mode:` header)", profilePath)
	}
	return out, nil
}

// loadCoverageFloor reads the committed floor. A missing (or empty) file yields
// an empty floor (the gate then reports every measured package as unrecorded).
// Decoding rejects unknown fields, and every entry must name a non-empty
// `internal/gcp/` package with a `min_percent` in [0,100]; a duplicate package
// fails loud — so a typo (an unknown or misspelled key, a missing value) can
// never silently shadow a real floor.
func loadCoverageFloor(floorPath string) (map[string]float64, error) {
	out := map[string]float64{}
	if strings.TrimSpace(floorPath) == "" {
		return out, nil
	}
	raw, err := os.ReadFile(floorPath)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	var f coverageFloorFile
	if err := dec.Decode(&f); err != nil {
		if err == io.EOF {
			return out, nil
		}
		return nil, fmt.Errorf("coverage floor %s: %w", floorPath, err)
	}
	for i, e := range f.Packages {
		pkg := strings.TrimSpace(e.Package)
		if pkg == "" {
			return nil, fmt.Errorf("coverage floor %s: entry %d has no package", floorPath, i+1)
		}
		if !strings.HasPrefix(pkg, gcpCoverageRoot) {
			return nil, fmt.Errorf("coverage floor %s: entry %d names %q, outside %s", floorPath, i+1, pkg, gcpCoverageRoot)
		}
		if e.MinPercent == nil {
			return nil, fmt.Errorf("coverage floor %s: entry %d (%s) has no min_percent", floorPath, i+1, pkg)
		}
		if *e.MinPercent < 0 || *e.MinPercent > 100 {
			return nil, fmt.Errorf("coverage floor %s: entry %d (%s) has min_percent %g, want 0–100", floorPath, i+1, pkg, *e.MinPercent)
		}
		if _, dup := out[pkg]; dup {
			return nil, fmt.Errorf("coverage floor %s: entry %d duplicates package %q", floorPath, i+1, pkg)
		}
		out[pkg] = *e.MinPercent
	}
	return out, nil
}

// ratchetViolation is one gate failure. Kind is "below" (measured under its
// floor by more than the tolerance), "unrecorded" (measured with no floor
// entry), or "stale" (a floor entry with no measured package).
type ratchetViolation struct {
	Package     string
	Kind        string
	Measured    float64
	Floor       float64
	HasMeasured bool
	HasFloor    bool
}

// ratchetViolations is the pure gate rule: it compares rounded per-package
// percentages against the floor (minus coverageRatchetTolerance) and returns
// the sorted violations. It is deterministic and I/O-free so the rule is
// unit-testable with synthetic maps.
func ratchetViolations(measured, floor map[string]float64) []ratchetViolation {
	var out []ratchetViolation
	for pkg, m := range measured {
		f, ok := floor[pkg]
		switch {
		case !ok:
			out = append(out, ratchetViolation{Package: pkg, Kind: "unrecorded", Measured: m, HasMeasured: true})
		case roundPercent(m) < roundPercent(f)-coverageRatchetTolerance:
			out = append(out, ratchetViolation{Package: pkg, Kind: "below", Measured: m, Floor: f, HasMeasured: true, HasFloor: true})
		}
	}
	for pkg, f := range floor {
		if _, ok := measured[pkg]; !ok {
			out = append(out, ratchetViolation{Package: pkg, Kind: "stale", Floor: f, HasFloor: true})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Package != out[j].Package {
			return out[i].Package < out[j].Package
		}
		return out[i].Kind < out[j].Kind
	})
	return out
}

// measuredPercentages filters a profile tally to the governed subtree and
// returns rounded per-package percentages.
func measuredPercentages(stats map[string]pkgStatements) map[string]float64 {
	out := map[string]float64{}
	for pkg, s := range stats {
		if !strings.HasPrefix(pkg, gcpCoverageRoot) || !ratchetEligible(pkg) {
			continue
		}
		out[pkg] = roundPercent(s.percent())
	}
	return out
}

// packageRatchetRow is one rendered table row: a union of the measured and
// floor sets.
type packageRatchetRow struct {
	Package     string
	Measured    float64
	Floor       float64
	HasMeasured bool
	HasFloor    bool
}

func ratchetRows(measured, floor map[string]float64) []packageRatchetRow {
	seen := map[string]bool{}
	var rows []packageRatchetRow
	for pkg, m := range measured {
		f, ok := floor[pkg]
		rows = append(rows, packageRatchetRow{Package: pkg, Measured: m, Floor: f, HasMeasured: true, HasFloor: ok})
		seen[pkg] = true
	}
	for pkg, f := range floor {
		if seen[pkg] {
			continue
		}
		rows = append(rows, packageRatchetRow{Package: pkg, Floor: f, HasFloor: true})
	}
	sort.Slice(rows, func(i, j int) bool {
		mi, mj := rows[i].Measured, rows[j].Measured
		if mi != mj {
			return mi < mj
		}
		return rows[i].Package < rows[j].Package
	})
	return rows
}

// renderRatchetTable prints the per-package table, sorted thinnest-first. Under
// GitHub Actions it is wrapped in a collapsed `::group::` so the CI log stays
// readable.
func renderRatchetTable(measured, floor map[string]float64) {
	rows := ratchetRows(measured, floor)
	group := os.Getenv("GITHUB_ACTIONS") == "true"
	if group {
		fmt.Println("::group::GCP per-package statement coverage (internal/gcp)")
	}
	fmt.Printf("%-58s %8s %8s %8s\n", "package", "coverage", "floor", "delta")
	for _, r := range rows {
		m, f, delta := "—", "—", "—"
		if r.HasMeasured {
			m = fmt.Sprintf("%.2f%%", r.Measured)
		}
		if r.HasFloor {
			f = fmt.Sprintf("%.2f%%", r.Floor)
		}
		if r.HasMeasured && r.HasFloor {
			delta = fmt.Sprintf("%+.2f", r.Measured-r.Floor)
		}
		fmt.Printf("%-58s %8s %8s %8s\n", r.Package, m, f, delta)
	}
	if group {
		fmt.Println("::endgroup::")
	}
}

// runCoverageRatchet is the gate: parse the profile, load the floor, print the
// table, and exit non-zero on any violation.
func runCoverageRatchet(profilePath, floorPath string) int {
	stats, err := parseCoverProfile(profilePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gcpstatus: coverage ratchet: %v\n", err)
		return 1
	}
	floor, err := loadCoverageFloor(floorPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gcpstatus: coverage ratchet: %v\n", err)
		return 1
	}
	measured := measuredPercentages(stats)
	renderRatchetTable(measured, floor)

	violations := ratchetViolations(measured, floor)
	if len(violations) > 0 {
		fmt.Printf("\ngcp-status coverage ratchet: %d package(s) failed the floor (%d measured, %d recorded, tolerance %.2fpp):\n",
			len(violations), len(measured), len(floor), coverageRatchetTolerance)
		for _, v := range violations {
			switch v.Kind {
			case "below":
				fmt.Printf("  - %s: %.2f%% < floor %.2f%% (shortfall exceeds %.2fpp)\n", v.Package, v.Measured, v.Floor, coverageRatchetTolerance)
			case "unrecorded":
				fmt.Printf("  - %s: %.2f%% has no floor entry (record it with `make test-gcp-coverage-update`)\n", v.Package, v.Measured)
			case "stale":
				fmt.Printf("  - %s: floor %.2f%% has no measured package (removed/renamed — re-run the update)\n", v.Package, v.Floor)
			}
		}
		fmt.Println("\nCoverage is a ratchet: never lower a floor to make this pass — add/restore tests.")
		return 1
	}
	fmt.Printf("\n=> every measured package (%d) is at or above its recorded floor (tolerance %.2fpp; never lower a floor to pass).\n",
		len(measured), coverageRatchetTolerance)
	return 0
}

// runCoverageRatchetUpdate rewrites the floor from a fresh profile. It is a
// ratchet: an existing floor is raised but never lowered unless allowLower is
// set, so a coverage drop for a package that still produces data cannot be
// laundered by re-running the update. An entry whose package no longer produces
// coverage data is dropped (a rename/removal) and reported.
func runCoverageRatchetUpdate(profilePath, floorPath string, allowLower bool) error {
	stats, err := parseCoverProfile(profilePath)
	if err != nil {
		return err
	}
	existing, err := loadCoverageFloor(floorPath)
	if err != nil {
		return err
	}
	measured := measuredPercentages(stats)
	if len(measured) == 0 {
		return fmt.Errorf("coverage floor update: no eligible %s packages in %s", gcpCoverageRoot, profilePath)
	}

	pkgs := make([]string, 0, len(measured))
	for pkg := range measured {
		pkgs = append(pkgs, pkg)
	}
	sort.Strings(pkgs)

	entries := make([]coverageFloorEntry, 0, len(pkgs))
	var added, raised, lowered, held, dropped int
	for _, pkg := range pkgs {
		m := measured[pkg]
		min := m
		if old, ok := existing[pkg]; ok {
			switch {
			case roundPercent(old) > roundPercent(m):
				if allowLower {
					lowered++
				} else {
					min = old // ratchet: hold the higher recorded floor
					held++
				}
			case roundPercent(old) < roundPercent(m):
				raised++
			}
		} else {
			added++
		}
		v := min
		entries = append(entries, coverageFloorEntry{Package: pkg, MinPercent: &v})
	}
	for pkg := range existing {
		if _, ok := measured[pkg]; !ok {
			dropped++
			fmt.Printf("coverage floor: dropping %s (no measured package)\n", pkg)
		}
	}

	if err := writeCoverageFloor(floorPath, entries); err != nil {
		return err
	}
	fmt.Printf("coverage floor: wrote %s — %d packages (%d added, %d raised, %d held, %d lowered, %d dropped)\n",
		floorPath, len(entries), added, raised, held, lowered, dropped)
	if held > 0 {
		fmt.Printf("  ! %d recorded floor(s) held above the current measurement — a regression was blocked, not recorded.\n", held)
	}
	if lowered > 0 {
		fmt.Printf("  ! %d floor(s) were LOWERED (allowLower) — confirm this was deliberate.\n", lowered)
	}
	return nil
}

// writeCoverageFloor writes the floor with a documenting header, deterministically
// sorted and formatted.
func writeCoverageFloor(floorPath string, entries []coverageFloorEntry) error {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Package < entries[j].Package })

	var b strings.Builder
	b.WriteString("# GCP per-package statement-coverage floor (TD11) — a ratchet, not a target.\n")
	b.WriteString("#\n")
	b.WriteString("# Regenerate from a fresh profile with `make test-gcp-coverage-update`; the\n")
	b.WriteString("# update RAISES a recorded floor but refuses to LOWER one unless ALLOW_LOWER=1\n")
	b.WriteString("# is set, so a coverage drop cannot be laundered by re-running it. The gate\n")
	b.WriteString("# (`make test-gcp-coverage`) fails when a measured package drops more than\n")
	b.WriteString("# 0.5pp below its floor (a small noise tolerance for timing-dependent tests),\n")
	b.WriteString("# when a measured package has no entry here, or when an entry names a package\n")
	b.WriteString("# that no longer produces coverage data. Generated protobuf packages (a final\n")
	b.WriteString("# path element ending in `pb`) are excluded mechanically, as are packages with\n")
	b.WriteString("# no test files (they produce no coverage data).\n")
	b.WriteString("#\n")
	b.WriteString("# package: repo-relative package directory (`internal/gcp/...`).\n")
	b.WriteString("# min_percent: statement-coverage floor, two decimals.\n")
	b.WriteString("packages:\n")
	for _, e := range entries {
		fmt.Fprintf(&b, "  - package: %s\n", e.Package)
		fmt.Fprintf(&b, "    min_percent: %s\n", strconv.FormatFloat(*e.MinPercent, 'f', -1, 64))
	}
	return os.WriteFile(floorPath, []byte(b.String()), 0o644)
}
