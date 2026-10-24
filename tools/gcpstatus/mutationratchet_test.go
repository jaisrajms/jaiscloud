package main

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// mutationReportJSON renders a minimal go-mutesting report with a consistent MSI.
func mutationReportJSON(total, killed, escaped int) string {
	return fmt.Sprintf(
		`{"stats":{"totalMutantsCount":%d,"killedCount":%d,"escapedCount":%d,"msi":%v},"escaped":[],"killed":[]}`,
		total, killed, escaped, float64(killed)/float64(total),
	)
}

func TestNormalizeMutationTarget(t *testing.T) {
	cases := map[string]string{
		"./internal/gcp/service/datastore": "internal/gcp/service/datastore",
		"internal/gcp/service/datastore/":  "internal/gcp/service/datastore",
		"  internal/gcp/adapter  ":         "internal/gcp/adapter",
		"internal/gcp/adapter":             "internal/gcp/adapter",
	}
	for in, want := range cases {
		if got := normalizeMutationTarget(in); got != want {
			t.Errorf("normalizeMutationTarget(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseMutationReport(t *testing.T) {
	path := writeTemp(t, "report.json", mutationReportJSON(1000, 800, 200))
	m, err := parseMutationReport(path, "internal/gcp/service/datastore")
	if err != nil {
		t.Fatalf("parseMutationReport: %v", err)
	}
	if m.MSI != 80 || m.Total != 1000 || m.Killed != 800 || m.Escaped != 200 {
		t.Fatalf("measurement = %+v, want MSI 80 of 1000", m)
	}
	if m.Target != "internal/gcp/service/datastore" {
		t.Fatalf("target = %q", m.Target)
	}
}

func TestParseMutationReportRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"malformed json":  `{not json`,
		"empty report":    `{"stats":{"totalMutantsCount":0,"killedCount":0,"msi":0}}`,
		"missing stats":   `{}`,
		"killed > total":  `{"stats":{"totalMutantsCount":5,"killedCount":6,"msi":1.2}}`,
		"negative killed": `{"stats":{"totalMutantsCount":5,"killedCount":-1,"msi":0}}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			path := writeTemp(t, "report.json", body)
			if _, err := parseMutationReport(path, "x"); err == nil {
				t.Fatalf("parseMutationReport(%s) should fail", name)
			}
		})
	}
	if _, err := parseMutationReport("/nonexistent/report.json", "x"); err == nil {
		t.Fatal("parseMutationReport(missing file) should fail")
	}
}

func TestLoadMutationFloor(t *testing.T) {
	path := writeTemp(t, "floor.yaml", strings.Join([]string{
		"targets:",
		"  - package: internal/gcp/service/datastore",
		"    min_msi_percent: 78.5",
		"  - package: internal/gcp/adapter",
		"    min_msi_percent: 70",
		"",
	}, "\n"))
	floor, err := loadMutationFloor(path)
	if err != nil {
		t.Fatalf("loadMutationFloor: %v", err)
	}
	if len(floor) != 2 || floor["internal/gcp/service/datastore"] != 78.5 || floor["internal/gcp/adapter"] != 70 {
		t.Fatalf("floor = %v", floor)
	}

	// A missing or empty path yields an empty floor (not an error).
	empty, err := loadMutationFloor("/nonexistent/floor.yaml")
	if err != nil || len(empty) != 0 {
		t.Fatalf("missing floor = %v, %v; want empty", empty, err)
	}
	if empty, err := loadMutationFloor(""); err != nil || len(empty) != 0 {
		t.Fatalf("blank floor = %v, %v; want empty", empty, err)
	}
}

func TestLoadMutationFloorRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"unknown field":   "targets:\n  - package: a\n    min_msi_percent: 50\n    typo: 1\n",
		"missing percent": "targets:\n  - package: a\n",
		"no package":      "targets:\n  - min_msi_percent: 50\n",
		"over 100":        "targets:\n  - package: a\n    min_msi_percent: 101\n",
		"negative":        "targets:\n  - package: a\n    min_msi_percent: -1\n",
		"duplicate":       "targets:\n  - package: a\n    min_msi_percent: 50\n  - package: ./a/\n    min_msi_percent: 60\n",
		"not a mapping":   "- a\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			path := writeTemp(t, "floor.yaml", body)
			if _, err := loadMutationFloor(path); err == nil {
				t.Fatalf("loadMutationFloor(%s) should fail", name)
			}
		})
	}
}

func TestMutationRatchetViolationFor(t *testing.T) {
	floor := map[string]float64{"a": 80}
	if v := mutationRatchetViolationFor("a", 80, floor); v != nil {
		t.Fatalf("at-floor = %+v, want nil", v)
	}
	if v := mutationRatchetViolationFor("a", 79.6, floor); v != nil {
		t.Fatalf("within tolerance = %+v, want nil", v)
	}
	v := mutationRatchetViolationFor("a", 79.0, floor)
	if v == nil || v.Kind != "below" {
		t.Fatalf("below floor = %+v, want a below violation", v)
	}
	if v := mutationRatchetViolationFor("b", 90, floor); v == nil || v.Kind != "unrecorded" {
		t.Fatalf("unrecorded = %+v, want an unrecorded violation", v)
	}
}

func TestRunMutationRatchet(t *testing.T) {
	report := writeTemp(t, "report.json", mutationReportJSON(1000, 800, 200)) // 80%
	floor := writeTemp(t, "floor.yaml", "targets:\n  - package: internal/gcp/service/datastore\n    min_msi_percent: 79\n")
	if code := runMutationRatchet(report, floor, "./internal/gcp/service/datastore"); code != 0 {
		t.Fatalf("above-floor exit = %d, want 0", code)
	}

	lowReport := writeTemp(t, "report.json", mutationReportJSON(1000, 700, 300)) // 70%
	if code := runMutationRatchet(lowReport, floor, "internal/gcp/service/datastore"); code != 1 {
		t.Fatalf("below-floor exit = %d, want 1", code)
	}

	// A target with no floor entry fails loud.
	if code := runMutationRatchet(report, floor, "internal/gcp/adapter"); code != 1 {
		t.Fatalf("unrecorded exit = %d, want 1", code)
	}

	// A missing target or a missing report is a usage/report error, not a pass.
	if code := runMutationRatchet(report, floor, ""); code != 1 {
		t.Fatalf("empty-target exit = %d, want 1", code)
	}
	if code := runMutationRatchet("/nonexistent/report.json", floor, "internal/gcp/service/datastore"); code != 1 {
		t.Fatalf("missing-report exit = %d, want 1", code)
	}
}

func TestRunMutationRatchetUpdate(t *testing.T) {
	target := "internal/gcp/service/datastore"
	read := func(path string) map[string]float64 {
		t.Helper()
		f, err := loadMutationFloor(path)
		if err != nil {
			t.Fatalf("loadMutationFloor(%s): %v", path, err)
		}
		return f
	}

	// Add: no existing entry records the measured score.
	floor := writeTemp(t, "floor.yaml", "targets: []\n")
	high := writeTemp(t, "high.json", mutationReportJSON(1000, 800, 200))
	if err := runMutationRatchetUpdate(high, floor, "./"+target, false); err != nil {
		t.Fatalf("update add: %v", err)
	}
	if got := read(floor)[target]; got != 80 {
		t.Fatalf("added floor = %v, want 80", got)
	}

	// Raise: a higher measurement moves the floor up.
	higher := writeTemp(t, "higher.json", mutationReportJSON(1000, 850, 150))
	if err := runMutationRatchetUpdate(higher, floor, target, false); err != nil {
		t.Fatalf("update raise: %v", err)
	}
	if got := read(floor)[target]; got != 85 {
		t.Fatalf("raised floor = %v, want 85", got)
	}

	// Hold: a lower measurement without allowLower must not lower the floor.
	if err := runMutationRatchetUpdate(high, floor, target, false); err != nil {
		t.Fatalf("update hold: %v", err)
	}
	if got := read(floor)[target]; got != 85 {
		t.Fatalf("held floor = %v, want 85 (a regression must not be recorded)", got)
	}

	// Lower: allowLower permits it, and other entries are preserved.
	withOther := writeTemp(t, "multi.yaml", strings.Join([]string{
		"targets:",
		"  - package: internal/gcp/adapter",
		"    min_msi_percent: 64.3",
		"  - package: " + target,
		"    min_msi_percent: 85",
		"",
	}, "\n"))
	if err := runMutationRatchetUpdate(high, withOther, target, true); err != nil {
		t.Fatalf("update lower: %v", err)
	}
	got := read(withOther)
	if got[target] != 80 {
		t.Fatalf("lowered floor = %v, want 80", got[target])
	}
	if got["internal/gcp/adapter"] != 64.3 {
		t.Fatalf("other target = %v, want 64.3 (must be preserved)", got["internal/gcp/adapter"])
	}

	// A missing target is a usage error.
	if err := runMutationRatchetUpdate(high, floor, "", false); err == nil {
		t.Fatal("update with an empty target should fail")
	}
}

func TestWriteMutationFloorRoundTrip(t *testing.T) {
	path := writeTemp(t, "floor.yaml", "")
	entries := []mutationFloorEntry{
		{Package: "internal/gcp/service/datastore", MinMSIPercent: pct(78.5)},
		{Package: "internal/gcp/adapter", MinMSIPercent: pct(64.3)},
	}
	if err := writeMutationFloor(path, entries); err != nil {
		t.Fatalf("writeMutationFloor: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.HasPrefix(string(raw), "# GCP per-target mutation-score floor") {
		t.Fatalf("floor file missing its header:\n%s", raw)
	}
	floor, err := loadMutationFloor(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if len(floor) != 2 || floor["internal/gcp/service/datastore"] != 78.5 || floor["internal/gcp/adapter"] != 64.3 {
		t.Fatalf("round-tripped floor = %v", floor)
	}
}
