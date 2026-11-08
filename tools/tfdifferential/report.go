package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// caseResult is the per-case status row in the report.
type caseResult struct {
	Name         string
	Status       string // compared | not-recorded
	Excluded     bool
	Reason       string
	Diffs        int
	Unclassified int
	ReplanChange bool
}

// ReportDiff is one classified difference with its case.
type ReportDiff struct {
	CaseName string
	Classified
}

// Report is one compare run.
type Report struct {
	Generated   time.Time
	Corpus      string
	GoldenDir   string
	Endpoint    string
	Project     string
	Cases       []caseResult
	Differences []ReportDiff
}

// Render produces the committed markdown report.
func (r Report) Render() string {
	diffs := append([]ReportDiff(nil), r.Differences...)
	sort.SliceStable(diffs, func(i, j int) bool {
		if diffs[i].CaseName != diffs[j].CaseName {
			return diffs[i].CaseName < diffs[j].CaseName
		}
		return diffs[i].Path < diffs[j].Path
	})
	var b strings.Builder
	b.WriteString("# Terraform differential report\n\n")
	fmt.Fprintf(&b, "Generated: %s\n\n", r.Generated.UTC().Format(time.RFC3339))
	fmt.Fprintf(&b, "- corpus: `%s`\n", r.Corpus)
	fmt.Fprintf(&b, "- goldens: `%s`\n", r.GoldenDir)
	fmt.Fprintf(&b, "- emulator: `%s` (project `%s`)\n\n", r.Endpoint, r.Project)
	b.WriteString("The goldens are recorded from real GCP by `tools/tfdifferential -mode record`;\n")
	b.WriteString("this table is the emulator replay. Every difference is classified in\n")
	b.WriteString("`classifications.yaml`; an unclassified difference fails the run.\n\n")

	b.WriteString("## Cases\n\n")
	b.WriteString("| case | status | differences | unclassified | re-plan |\n")
	b.WriteString("|---|---|---|---|---|\n")
	for _, c := range r.Cases {
		replan := "no changes"
		if c.ReplanChange {
			replan = "**changes**"
		}
		if c.Status == "not-recorded" {
			replan = "—"
		}
		status := c.Status
		if c.Excluded {
			status += " (excluded)"
		}
		fmt.Fprintf(&b, "| %s | %s | %d | %d | %s |\n", c.Name, status, c.Diffs, c.Unclassified, replan)
	}

	b.WriteString("\n## Differences\n\n")
	if len(diffs) == 0 {
		b.WriteString("None: the emulator matches every recorded case.\n")
		return b.String()
	}
	b.WriteString("| case | path | real GCP | emulator | verdict | id | reason |\n")
	b.WriteString("|---|---|---|---|---|---|---|\n")
	for _, d := range diffs {
		fmt.Fprintf(&b, "| %s | `%s` | %s | %s | %s | %s | %s |\n",
			d.CaseName, d.Path, renderValue(d.Real), renderValue(d.Emulator),
			d.Verdict, orDash(d.ID), orDash(d.Reason))
	}

	b.WriteString("\n## Not recorded\n\n")
	var excluded []caseResult
	for _, c := range r.Cases {
		if c.Status == "not-recorded" {
			excluded = append(excluded, c)
		}
	}
	if len(excluded) == 0 {
		b.WriteString("None.\n")
		return b.String()
	}
	b.WriteString("| case | reason |\n|---|---|\n")
	for _, c := range excluded {
		fmt.Fprintf(&b, "| %s | %s |\n", c.Name, orDash(c.Reason))
	}
	return b.String()
}

// WriteReport writes the markdown report.
func (r Report) WriteReport(path string) error {
	return os.WriteFile(path, []byte(r.Render()), 0o644)
}

func renderValue(v any) string {
	if _, ok := v.(absent); ok {
		return "`<absent>`"
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	s := string(raw)
	s = strings.ReplaceAll(s, "|", `\|`)
	if len(s) > 160 {
		s = s[:157] + "…"
	}
	if s == "{}" {
		return "`{}`"
	}
	return "`" + s + "`"
}

func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}
