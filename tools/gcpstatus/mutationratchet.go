// Per-target mutation-score ratchet (TD19).
//
// The coverage ratchet (coverageratchet.go, §11) proves how much of a package's
// code the tests execute; it deliberately does not measure whether the
// assertions *bite* — that is the mutation-testing axis (§13, TD13). TD13 landed
// the spike (`make test-gcp-mutation PKG=…`) and scored the transport-neutral
// Datastore core, but left the score unguarded: nothing fails when a later test
// refactor weakens an assertion and lets a mutant survive again. This file adds
// that guard:
//
//   - `make test-gcp-mutation-ratchet PKG=…` runs the mutation spike over one
//     target and feeds its JSON report to `-mutation-ratchet`;
//   - the committed floor docs/gcp-mutation-floor.yaml records, per target, the
//     mutation score (MSI, percent) at the time it was last ratcheted;
//   - the gate fails when the measured MSI drops below its recorded floor (by
//     more than a small noise tolerance) or when the target has no floor entry.
//
// The floor is a *ratchet*, not a target: it starts at today's measured value
// and only moves up — `-mutation-floor-update` raises a recorded floor but
// refuses to lower one without `-allow-lower`.
//
// Scope boundary: the mutation spike is local only (a full package run is
// minutes, not a per-PR budget — TD13), so this gate runs on demand, not in PR
// CI. Because each invocation measures exactly one target, the gate reads only
// the requested target's entry; a floor entry for a *removed* target is left in
// place rather than flagged (there is no cross-target run to detect it).
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// defaultMutationFloorPath is the committed per-target mutation floor, relative
// to the repo root (where `make test-gcp-mutation-ratchet` runs).
const defaultMutationFloorPath = "docs/gcp-mutation-floor.yaml"

// mutationRatchetTolerance is the allowed shortfall, in percentage points,
// between a measured target and its recorded floor. A full Datastore-core run is
// ~1250 mutants, so one mutant flipping (a timing-dependent test) moves the score
// by well under 0.1pp; the tolerance absorbs that noise while a real regression
// (an emptied assertion table) fails by a wide margin.
const mutationRatchetTolerance = 0.5

// mutationFloorEntry is one recorded per-target floor. MinMSIPercent is a
// pointer so a missing key is a loud error rather than a silent 0 floor.
type mutationFloorEntry struct {
	Package       string   `yaml:"package"`
	MinMSIPercent *float64 `yaml:"min_msi_percent"`
}

// mutationFloorFile is the committed floor document.
type mutationFloorFile struct {
	Targets []mutationFloorEntry `yaml:"targets"`
}

// mutationMeasurement is the scored result for one target.
type mutationMeasurement struct {
	Target  string
	MSI     float64 // percent, 0–100
	Total   int
	Killed  int
	Escaped int
}

// roundMSI rounds a mutation score to two decimals, the precision the floor is
// stored at. Comparing rounded values keeps the gate repeatable despite float
// representation.
func roundMSI(p float64) float64 { return math.Round(p*100) / 100 }

// normalizeMutationTarget canonicalizes a mutation target (a Go package dir or
// a single file within one, as the Makefile passes it) to the repo-relative form
// the floor is keyed by: a leading "./" and any trailing "/" are stripped.
func normalizeMutationTarget(target string) string {
	t := strings.TrimSpace(target)
	t = strings.TrimPrefix(t, "./")
	t = strings.TrimSuffix(t, "/")
	return t
}

// mutationReport is the slice of go-mutesting's JSON report the ratchet reads.
type mutationReport struct {
	Stats struct {
		TotalMutantsCount int     `json:"totalMutantsCount"`
		KilledCount       int     `json:"killedCount"`
		EscapedCount      int     `json:"escapedCount"`
		MSI               float64 `json:"msi"`
	} `json:"stats"`
}

// parseMutationReport reads a go-mutesting JSON report and returns the measured
// score for a target. A report with no mutants is a loud error — it is what a
// wrong target, a failed run or a truncated file produce, and must never be read
// as a score of zero (which would silently pass or fail the gate).
func parseMutationReport(reportPath, target string) (mutationMeasurement, error) {
	raw, err := os.ReadFile(reportPath)
	if err != nil {
		return mutationMeasurement{}, err
	}
	var r mutationReport
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&r); err != nil {
		return mutationMeasurement{}, fmt.Errorf("mutation report %s: %w", reportPath, err)
	}
	if r.Stats.TotalMutantsCount <= 0 {
		return mutationMeasurement{}, fmt.Errorf("mutation report %s: %d mutants scored (empty or truncated report)", reportPath, r.Stats.TotalMutantsCount)
	}
	if r.Stats.KilledCount < 0 || r.Stats.KilledCount > r.Stats.TotalMutantsCount {
		return mutationMeasurement{}, fmt.Errorf("mutation report %s: killed %d of %d mutants", reportPath, r.Stats.KilledCount, r.Stats.TotalMutantsCount)
	}
	return mutationMeasurement{
		Target:  target,
		MSI:     roundMSI(100 * r.Stats.MSI),
		Total:   r.Stats.TotalMutantsCount,
		Killed:  r.Stats.KilledCount,
		Escaped: r.Stats.EscapedCount,
	}, nil
}

// loadMutationFloor reads the committed floor. A missing (or empty) file yields
// an empty floor (the gate then reports the requested target as unrecorded).
// Decoding rejects unknown fields, and every entry must name a non-empty package
// with a `min_msi_percent` in [0,100]; a duplicate target fails loud — so a typo
// (an unknown or misspelled key, a missing value) can never silently shadow a
// real floor.
func loadMutationFloor(floorPath string) (map[string]float64, error) {
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
	var f mutationFloorFile
	if err := dec.Decode(&f); err != nil {
		if err == io.EOF {
			return out, nil
		}
		return nil, fmt.Errorf("mutation floor %s: %w", floorPath, err)
	}
	for i, e := range f.Targets {
		pkg := normalizeMutationTarget(e.Package)
		if pkg == "" {
			return nil, fmt.Errorf("mutation floor %s: entry %d has no package", floorPath, i+1)
		}
		if e.MinMSIPercent == nil {
			return nil, fmt.Errorf("mutation floor %s: entry %d (%s) has no min_msi_percent", floorPath, i+1, pkg)
		}
		if *e.MinMSIPercent < 0 || *e.MinMSIPercent > 100 {
			return nil, fmt.Errorf("mutation floor %s: entry %d (%s) has min_msi_percent %g, want 0–100", floorPath, i+1, pkg, *e.MinMSIPercent)
		}
		if _, dup := out[pkg]; dup {
			return nil, fmt.Errorf("mutation floor %s: entry %d duplicates target %q", floorPath, i+1, pkg)
		}
		out[pkg] = *e.MinMSIPercent
	}
	return out, nil
}

// mutationRatchetViolation is one gate failure: "below" (measured under its
// floor by more than the tolerance) or "unrecorded" (no floor entry).
type mutationRatchetViolation struct {
	Target   string
	Kind     string
	Measured float64
	Floor    float64
}

// mutationRatchetViolationFor is the pure gate rule for one target: nil when the
// target meets its recorded floor (within tolerance), a violation otherwise. It
// is deterministic and I/O-free so the rule is unit-testable.
func mutationRatchetViolationFor(target string, measured float64, floor map[string]float64) *mutationRatchetViolation {
	f, ok := floor[target]
	if !ok {
		return &mutationRatchetViolation{Target: target, Kind: "unrecorded", Measured: measured}
	}
	if roundMSI(measured) < roundMSI(f)-mutationRatchetTolerance {
		return &mutationRatchetViolation{Target: target, Kind: "below", Measured: measured, Floor: f}
	}
	return nil
}

// renderMutationTable prints the measured-vs-floor line for a run.
func renderMutationTable(m mutationMeasurement, floor map[string]float64, hasFloor bool) {
	fmt.Printf("mutation target %s\n", m.Target)
	fmt.Printf("  score   %6.2f%%  (killed %d / total %d, escaped %d)\n", m.MSI, m.Killed, m.Total, m.Escaped)
	if hasFloor {
		fmt.Printf("  floor   %6.2f%%  (tolerance %.2fpp)  delta %+.2f\n", floor[m.Target], mutationRatchetTolerance, m.MSI-floor[m.Target])
	} else {
		fmt.Printf("  floor   %6s\n", "—")
	}
}

// runMutationRatchet is the gate: parse the report, load the floor, print the
// score, and exit non-zero on a violation.
func runMutationRatchet(reportPath, floorPath, target string) int {
	target = normalizeMutationTarget(target)
	if target == "" {
		fmt.Fprintln(os.Stderr, "gcpstatus: mutation ratchet: -mutation-target must name the mutation target the report was produced for")
		return 1
	}
	m, err := parseMutationReport(reportPath, target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gcpstatus: mutation ratchet: %v\n", err)
		return 1
	}
	floor, err := loadMutationFloor(floorPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gcpstatus: mutation ratchet: %v\n", err)
		return 1
	}
	_, hasFloor := floor[target]
	renderMutationTable(m, floor, hasFloor)

	if v := mutationRatchetViolationFor(target, m.MSI, floor); v != nil {
		switch v.Kind {
		case "below":
			fmt.Printf("\ngcp-status mutation ratchet: %s scored %.2f%% < floor %.2f%% (shortfall exceeds %.2fpp)\n", v.Target, v.Measured, v.Floor, mutationRatchetTolerance)
		case "unrecorded":
			fmt.Printf("\ngcp-status mutation ratchet: %s scored %.2f%% with no floor entry (record it with `make test-gcp-mutation-update PKG=%s`)\n", v.Target, v.Measured, v.Target)
		}
		fmt.Println("\nThe mutation score is a ratchet: never lower a floor to make this pass — strengthen the assertions.")
		return 1
	}
	fmt.Printf("\n=> %s is at or above its recorded mutation floor (%.2f%%; never lower a floor to pass).\n", target, floor[target])
	return 0
}

// runMutationRatchetUpdate rewrites the floor from a fresh report. It is a
// ratchet: an existing floor is raised but never lowered unless allowLower is
// set, so an assertion-strength drop cannot be laundered by re-running the
// update. Entries for other targets are preserved verbatim.
func runMutationRatchetUpdate(reportPath, floorPath, target string, allowLower bool) error {
	target = normalizeMutationTarget(target)
	if target == "" {
		return fmt.Errorf("mutation floor update: -mutation-target must name the mutation target the report was produced for")
	}
	m, err := parseMutationReport(reportPath, target)
	if err != nil {
		return err
	}
	existing, err := loadMutationFloor(floorPath)
	if err != nil {
		return err
	}

	min := m.MSI
	action := "added"
	switch old, ok := existing[target]; {
	case !ok:
		// added
	case roundMSI(old) > roundMSI(m.MSI):
		if allowLower {
			action = "lowered"
		} else {
			min = old // ratchet: hold the higher recorded floor
			action = "held"
		}
	case roundMSI(old) < roundMSI(m.MSI):
		action = "raised"
	default:
		action = "unchanged"
	}
	existing[target] = min

	entries := make([]mutationFloorEntry, 0, len(existing))
	for pkg, pct := range existing {
		v := pct
		entries = append(entries, mutationFloorEntry{Package: pkg, MinMSIPercent: &v})
	}
	if err := writeMutationFloor(floorPath, entries); err != nil {
		return err
	}
	fmt.Printf("mutation floor: wrote %s — %s %.2f%% → %s %.2f%% for %s (%d target(s))\n",
		floorPath, action, m.MSI, "floor", min, target, len(entries))
	if action == "held" {
		fmt.Printf("  ! the recorded floor was held above the current score — a regression was blocked, not recorded.\n")
	}
	if action == "lowered" {
		fmt.Printf("  ! the floor was LOWERED (ALLOW_LOWER) — confirm this was deliberate.\n")
	}
	return nil
}

// writeMutationFloor writes the floor with a documenting header, deterministically
// sorted and formatted.
func writeMutationFloor(floorPath string, entries []mutationFloorEntry) error {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Package < entries[j].Package })

	var b strings.Builder
	b.WriteString("# GCP per-target mutation-score floor (TD19) — a ratchet, not a target.\n")
	b.WriteString("#\n")
	b.WriteString("# Regenerate a target's floor from a fresh run with\n")
	b.WriteString("# `make test-gcp-mutation-update PKG=<target>`; the update RAISES a recorded floor\n")
	b.WriteString("# but refuses to LOWER one unless ALLOW_LOWER=1 is set, so an assertion-strength\n")
	b.WriteString("# drop cannot be laundered by re-running it. The gate\n")
	b.WriteString("# (`make test-gcp-mutation-ratchet PKG=<target>`) fails when the measured mutation\n")
	b.WriteString("# score (MSI) drops more than 0.5pp below its floor, or when the target has no\n")
	b.WriteString("# entry here. The spike is local only (a full package run is minutes), so this\n")
	b.WriteString("# gate runs on demand rather than in PR CI.\n")
	b.WriteString("#\n")
	b.WriteString("# package: repo-relative mutation target passed to `make test-gcp-mutation`\n")
	b.WriteString("#   — a Go package directory, or a single file within one.\n")
	b.WriteString("# min_msi_percent: mutation-score floor (killed / total mutants), two decimals.\n")
	b.WriteString("targets:\n")
	for _, e := range entries {
		fmt.Fprintf(&b, "  - package: %s\n", e.Package)
		fmt.Fprintf(&b, "    min_msi_percent: %s\n", strconv.FormatFloat(*e.MinMSIPercent, 'f', -1, 64))
	}
	return os.WriteFile(floorPath, []byte(b.String()), 0o644)
}
