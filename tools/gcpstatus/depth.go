// Per-service behavioural *depth* record (TD1).
//
// The behavioral-coverage inventory (coverage.go) gates the `p1` Definition of
// Done — a `ga` service has an official-client suite or a reasoned exemption.
// It says nothing about *how deep* that suite goes. This file adds the
// behavioural-depth axis borrowed from the AWS reference implementation's
// test-suite taxonomy (its p1–p5 behaviour classes): every `ga` service records,
// for each of `p2`–`p5`, either that the class is exercised (`present`, naming
// the official-client suite(s)) or a reasoned, owned, dated exemption with a
// future `review_by`. `p1` stays derived from the coverage inventory
// (`coverageRow.Covered`).
//
// The record lives one file per service under docs/test-depth/<canonical>.yaml
// (not a single central file) so per-service sessions can extend their own
// record without colliding — see docs/gcp/testability.md §10.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// defaultDepthDir is the committed per-service depth-record directory, relative
// to the repo root (where `make gcp-status` runs).
const defaultDepthDir = "docs/test-depth"

// depthClasses are the behaviour classes recorded beyond the `p1` lifecycle
// baseline. They are fixed by docs/gcp/testability.md §10; a record naming any
// other key fails loud.
var depthClasses = []string{"p2", "p3", "p4", "p5"}

// depthClass is one class cell of a service's depth record.
type depthClass struct {
	// State is "present" (the class is exercised) or "exempt" (reasoned gap).
	State string `yaml:"state"`
	// Suites names the official-client suite(s) asserting the class; required
	// for `present`, each must be one of the service's coverage suites.
	Suites []string `yaml:"suites,omitempty"`
	// Note is the short evidence line; required for `present`.
	Note string `yaml:"note,omitempty"`
	// Reason explains an exemption; required for `exempt`.
	Reason string `yaml:"reason,omitempty"`
	// Owner is the plan ID that will close or re-review an exemption (e.g.
	// "DC3"); required for `exempt`.
	Owner string `yaml:"owner,omitempty"`
	// Date is when the exemption was recorded (YYYY-MM-DD); required for `exempt`.
	Date string `yaml:"date,omitempty"`
	// ReviewBy is the date by which the exemption must be re-reviewed
	// (YYYY-MM-DD); required for `exempt` and must not have passed, so an
	// exemption is a dated promise rather than a permanent pass.
	ReviewBy string `yaml:"review_by,omitempty"`
}

// depthRecord is one docs/test-depth/<service>.yaml file.
type depthRecord struct {
	Service string                `yaml:"service"`
	Classes map[string]depthClass `yaml:"classes"`
}

// classState returns the recorded state for a class ("" when unrecorded).
func (r depthRecord) classState(class string) (string, bool) {
	c, ok := r.Classes[class]
	if !ok {
		return "", false
	}
	return c.State, true
}

// loadDepthDir reads every docs/test-depth/*.yaml record, keyed by canonical
// service name, and validates each against the derived coverage rows. A missing
// directory yields no records: runDepthGate stats the directory first and fails
// loudly, and the ledger simply renders no depth. Validation is strict so a typo
// can never silently inflate or swallow the record:
//
//   - the service must resolve to a known inventory row;
//   - no two files may record the same service;
//   - every class key must be one of depthClasses;
//   - a `present` cell needs a note and ≥1 suite, and every named suite must be
//     one of that service's official-client coverage suites;
//   - an `exempt` cell needs a reason, a parseable YYYY-MM-DD date, an owner (the
//     backlog ID that will close or re-review it) and a parseable YYYY-MM-DD
//     review_by.
//
// `loadDepthDir` validates shape only; `depthStaleExemptions` (called by the
// gate) fails an exemption whose review_by has passed.
func loadDepthDir(dir string, rows []coverageRow) (map[string]depthRecord, error) {
	out := map[string]depthRecord{}
	if strings.TrimSpace(dir) == "" {
		return out, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, fmt.Errorf("depth records %s: %w", dir, err)
	}

	known := map[string]coverageRow{}
	for _, r := range rows {
		known[canonicalService(r.Service)] = r
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("depth record %s: %w", path, err)
		}
		var rec depthRecord
		if err := yaml.Unmarshal(raw, &rec); err != nil {
			return nil, fmt.Errorf("depth record %s: %w", path, err)
		}
		if strings.TrimSpace(rec.Service) == "" {
			return nil, fmt.Errorf("depth record %s: missing service", path)
		}
		svc := canonicalService(rec.Service)
		row, ok := known[svc]
		if !ok {
			return nil, fmt.Errorf("depth record %s: unknown service %q", path, rec.Service)
		}
		if _, dup := out[svc]; dup {
			return nil, fmt.Errorf("depth record %s: service %q already recorded", path, rec.Service)
		}
		suiteSet := map[string]bool{}
		for _, s := range row.Suites {
			suiteSet[s] = true
		}
		for class, cell := range rec.Classes {
			if !isDepthClass(class) {
				return nil, fmt.Errorf("depth record %s: unknown class %q (want one of %s)", path, class, strings.Join(depthClasses, ", "))
			}
			switch cell.State {
			case "present":
				if strings.TrimSpace(cell.Note) == "" {
					return nil, fmt.Errorf("depth record %s: class %s is `present` but has no note", path, class)
				}
				if len(cell.Suites) == 0 {
					return nil, fmt.Errorf("depth record %s: class %s is `present` but names no suite", path, class)
				}
				for _, s := range cell.Suites {
					if !suiteSet[s] {
						return nil, fmt.Errorf("depth record %s: class %s names suite %q, which is not one of %s's coverage suites (%s)",
							path, class, s, rec.Service, suiteList(row.Suites))
					}
				}
			case "exempt":
				if strings.TrimSpace(cell.Reason) == "" {
					return nil, fmt.Errorf("depth record %s: class %s is `exempt` but has no reason", path, class)
				}
				if _, err := time.Parse("2006-01-02", cell.Date); err != nil {
					return nil, fmt.Errorf("depth record %s: class %s exemption needs a YYYY-MM-DD date, got %q", path, class, cell.Date)
				}
				if strings.TrimSpace(cell.Owner) == "" {
					return nil, fmt.Errorf("depth record %s: class %s is `exempt` but names no owner (the backlog ID that will close or re-review it)", path, class)
				}
				if _, err := time.Parse("2006-01-02", cell.ReviewBy); err != nil {
					return nil, fmt.Errorf("depth record %s: class %s exemption needs a YYYY-MM-DD review_by, got %q", path, class, cell.ReviewBy)
				}
			default:
				return nil, fmt.Errorf("depth record %s: class %s has state %q (want `present` or `exempt`)", path, class, cell.State)
			}
		}
		out[svc] = rec
	}
	return out, nil
}

func isDepthClass(class string) bool {
	for _, c := range depthClasses {
		if c == class {
			return true
		}
	}
	return false
}

func suiteList(suites []string) string {
	if len(suites) == 0 {
		return "none"
	}
	return strings.Join(suites, ", ")
}

// depthGateViolations returns the unaccounted-for `ga` service/class cells — a
// service with no record at all, or a record that omits one of p2–p5. It is
// pure (synthetic-row testable) and deterministic.
func depthGateViolations(rows []coverageRow, records map[string]depthRecord) []string {
	var out []string
	for _, r := range rows {
		if !r.GA {
			continue
		}
		rec, ok := records[canonicalService(r.Service)]
		if !ok {
			out = append(out, r.Service+" (no depth record)")
			continue
		}
		for _, class := range depthClasses {
			state, ok := rec.classState(class)
			if !ok || (state != "present" && state != "exempt") {
				out = append(out, r.Service+" "+class)
			}
		}
	}
	return out
}

// depthTotalsFor returns, over the `ga` rows only, how many have a record and
// how many present/exempt cells their records carry. Non-`ga` records are
// tolerated but must not inflate a `ga` count.
func depthTotalsFor(rows []coverageRow, records map[string]depthRecord) (recorded, present, exempt int) {
	for _, r := range rows {
		if !r.GA {
			continue
		}
		rec, ok := records[canonicalService(r.Service)]
		if !ok {
			continue
		}
		recorded++
		for _, class := range depthClasses {
			switch rec.Classes[class].State {
			case "present":
				present++
			case "exempt":
				exempt++
			}
		}
	}
	return recorded, present, exempt
}

// depthStaleExemptions returns the `exempt` cells whose `review_by` date has
// passed as of `today` (a YYYY-MM-DD string, so the comparison is calendar-only
// and deterministic). An exemption is a dated promise to re-review, not a
// permanent pass: the gate fails once that promise is overdue. It is pure and
// sorted for stable output; `today` is injected so tests need no clock.
func depthStaleExemptions(records map[string]depthRecord, today string) []string {
	var out []string
	svcs := make([]string, 0, len(records))
	for svc := range records {
		svcs = append(svcs, svc)
	}
	sort.Strings(svcs)
	for _, svc := range svcs {
		rec := records[svc]
		for _, class := range depthClasses {
			cell, ok := rec.Classes[class]
			if !ok || cell.State != "exempt" {
				continue
			}
			// A review_by equal to today is still due today, not yet overdue.
			if cell.ReviewBy < today {
				out = append(out, fmt.Sprintf("%s %s (review_by %s)", svc, class, cell.ReviewBy))
			}
		}
	}
	return out
}

// runDepthGate enforces the behavioural-depth DoD: every `ga` service must have
// a docs/test-depth record accounting for each of p2–p5 (present or an owned,
// unexpired exemption). Exit 1 on any violation (the CI gate).
func runDepthGate(matrixPath, testRoot, depthDir string) int {
	rows, err := gatherCoverage(matrixPath, testRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gcpstatus: depth gate: %v\n", err)
		return 1
	}
	if strings.TrimSpace(depthDir) != "" {
		if _, err := os.Stat(depthDir); err != nil {
			fmt.Fprintf(os.Stderr, "gcpstatus: depth gate: %v\n", err)
			return 1
		}
	}
	records, err := loadDepthDir(depthDir, rows)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gcpstatus: depth gate: %v\n", err)
		return 1
	}
	recorded, present, exempt := depthTotalsFor(rows, records)
	_, gaTotal := gaCoverageTotals(rows)
	fmt.Printf("gcp-status depth gate: %d/%d `ga` services have a depth record (%d present + %d exempt p2–p5 cells)\n",
		recorded, gaTotal, present, exempt)
	violations := depthGateViolations(rows, records)
	if len(violations) > 0 {
		fmt.Printf("\n=> %d unaccounted-for depth cell(s):\n", len(violations))
		for _, v := range violations {
			fmt.Printf("  - %s\n", v)
		}
		fmt.Printf("\nAdd a docs/test-depth/<service>.yaml record (or a reasoned, owned, dated exemption) for each above.\n")
		return 1
	}
	stale := depthStaleExemptions(records, time.Now().Format("2006-01-02"))
	if len(stale) > 0 {
		fmt.Printf("\n=> %d exemption(s) past their review_by (re-review, close, or re-date each):\n", len(stale))
		for _, s := range stale {
			fmt.Printf("  - %s\n", s)
		}
		return 1
	}
	fmt.Println("=> every `ga` service accounts for each of p2–p5 (present, or an owned exemption with a future review_by).")
	return 0
}
